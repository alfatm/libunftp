#![allow(missing_docs)]

//! Data transfers as the storage backend sees them, and what a transfer leaves behind for the next one.
//!
//! These tests run against a storage backend whose behaviour is chosen by the file name, so a test can
//! make the backend fail at a precise point, or watch what it reads.

pub mod common;

use async_trait::async_trait;
use common::{TestAuthenticator, read_from_server, send_to_server, tcp_pasv_connect};
use lazy_static::lazy_static;
use libunftp::ServerBuilder;
use std::fmt::Debug;
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use unftp_core::auth::DefaultUser;
use unftp_core::storage::{Error, ErrorKind, FEATURE_RESTART, Fileinfo, Metadata, Result, StorageBackend};

const ADDR: &str = "127.0.0.1:2157";

/// What every `RETR` serves. Each byte names its own offset, so the bytes received show where the read began.
const CONTENT: &str = "0123456789abcdef";

/// `put` refuses files whose name starts with this.
const REFUSED: &str = "refused";

/// `put` reports to [`WATCHED_UPLOADS`] how it reads files whose name starts with this.
const WATCHED: &str = "watched";

#[derive(Debug, PartialEq)]
enum UploadEvent {
    /// The backend has read the first bytes.
    Started,
    /// The backend's reader ended: with the number of bytes read at EOF, or with the kind of the read error.
    Ended(std::result::Result<u64, std::io::ErrorKind>),
}

lazy_static! {
    static ref CONSUMERS: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
    static ref WATCHED_UPLOADS: (mpsc::UnboundedSender<UploadEvent>, Mutex<mpsc::UnboundedReceiver<UploadEvent>>) = {
        let (tx, rx) = mpsc::unbounded_channel();
        (tx, Mutex::new(rx))
    };
}

#[derive(Debug)]
struct Meta {
    len: u64,
}

impl Metadata for Meta {
    fn len(&self) -> u64 {
        self.len
    }

    fn is_dir(&self) -> bool {
        false
    }

    fn is_file(&self) -> bool {
        true
    }

    fn is_symlink(&self) -> bool {
        false
    }

    fn modified(&self) -> Result<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH)
    }

    fn gid(&self) -> u32 {
        0
    }

    fn uid(&self) -> u32 {
        0
    }
}

fn file_name(path: &Path) -> String {
    path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default()
}

#[derive(Debug)]
struct TestStorage;

#[async_trait]
impl StorageBackend<DefaultUser> for TestStorage {
    type Metadata = Meta;

    fn supported_features(&self) -> u32 {
        FEATURE_RESTART
    }

    async fn metadata<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, _path: P) -> Result<Self::Metadata> {
        Err(Error::from(ErrorKind::PermanentFileNotAvailable))
    }

    async fn list<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, _path: P) -> Result<Vec<Fileinfo<PathBuf, Self::Metadata>>> {
        Ok(vec![])
    }

    async fn get<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, _path: P, start_pos: u64) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        let rest = CONTENT.as_bytes().get(start_pos as usize..).unwrap_or_default();
        Ok(Box::new(Cursor::new(rest.to_vec())))
    }

    async fn put<P: AsRef<Path> + Send + Debug, R: AsyncRead + Send + Sync + Unpin + 'static>(
        &self,
        _user: &DefaultUser,
        mut input: R,
        path: P,
        _start_pos: u64,
    ) -> Result<u64> {
        let name = file_name(path.as_ref());
        if name.starts_with(REFUSED) {
            return Err(Error::from(ErrorKind::PermissionDenied));
        }
        if !name.starts_with(WATCHED) {
            return Ok(tokio::io::copy(&mut input, &mut tokio::io::sink()).await?);
        }
        let events = &WATCHED_UPLOADS.0;
        let mut buffer = [0_u8; 1024];
        let mut total = 0_u64;
        let ended = loop {
            let read = input.read(&mut buffer).await;
            match read {
                Ok(0) => break Ok(total),
                Ok(n) => {
                    if total == 0 {
                        events.send(UploadEvent::Started).unwrap();
                    }
                    total += n as u64;
                }
                Err(err) => break Err(err),
            }
        };
        events.send(UploadEvent::Ended(ended.as_ref().map(|n| *n).map_err(|err| err.kind()))).unwrap();
        ended.map_err(|err| Error::new(ErrorKind::ConnectionClosed, err))
    }

    async fn del<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, _path: P) -> Result<()> {
        Ok(())
    }

    async fn mkd<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, _path: P) -> Result<()> {
        Ok(())
    }

    async fn rename<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, _from: P, _to: P) -> Result<()> {
        Ok(())
    }

    async fn rmd<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, _path: P) -> Result<()> {
        Ok(())
    }

    async fn cwd<P: AsRef<Path> + Send + Debug>(&self, _user: &DefaultUser, _path: P) -> Result<()> {
        Ok(())
    }
}

async fn run_with_test_storage() {
    let server = ServerBuilder::new(Box::new(|| TestStorage))
        .authenticator(Arc::new(TestAuthenticator {}))
        .greeting("Welcome test")
        .build()
        .unwrap();
    server.listen(ADDR).await.unwrap();
}

async fn initialize() {
    let count = Arc::clone(&CONSUMERS);
    let mut lock = count.lock().await;
    *lock += 1;
    if *lock == 1 {
        tokio::spawn(run_with_test_storage());
    }
    drop(lock);
}

async fn finalize() {
    let count = Arc::clone(&CONSUMERS);
    let mut lock = count.lock().await;
    *lock -= 1;
    drop(lock);
    loop {
        let lock = count.lock().await;
        if *lock > 0 {
            drop(lock);
            tokio::time::sleep(Duration::new(1, 0)).await;
        } else {
            drop(lock);
            break;
        }
    }
}

async fn tcp_connect() -> std::io::Result<TcpStream> {
    let mut errcount: i32 = 0;
    loop {
        match TcpStream::connect(ADDR).await {
            Ok(s) => return Ok(s),
            Err(e) => {
                if errcount > 2 {
                    return Err(e);
                }
                errcount += 1;
                tokio::time::sleep(Duration::new(1, 0)).await;
                continue;
            }
        }
    }
}

async fn login(stream: &TcpStream, buffer: &mut [u8]) {
    assert_eq!(read_from_server(buffer, stream).await, "220 Welcome test\r\n");
    send_to_server("USER test\r\n", stream).await;
    assert_eq!(read_from_server(buffer, stream).await, "331 Password Required\r\n");
    send_to_server("PASS test\r\n", stream).await;
    assert_eq!(read_from_server(buffer, stream).await, "230 User logged in, proceed\r\n");
}

async fn pasv(stream: &TcpStream, buffer: &mut [u8]) -> SocketAddr {
    send_to_server("PASV\r\n", stream).await;
    let reply = read_from_server(buffer, stream).await;
    assert!(reply.starts_with("227 Entering Passive Mode"), "unexpected reply {reply:?}");
    let body = reply.split_once('(').and_then(|(_, rest)| rest.split_once(')')).unwrap().0;
    let nums: Vec<u8> = body.split(',').map(|s| s.trim().parse().unwrap()).collect();
    let port = u16::from(nums[4]) * 256 + u16::from(nums[5]);
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(nums[0], nums[1], nums[2], nums[3])), port)
}

async fn read_data_from_server(stream: &TcpStream) -> Vec<u8> {
    let mut data = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        stream.readable().await.unwrap();
        match stream.try_read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => data.extend_from_slice(&buffer[..n]),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{}", e),
        }
    }
    data
}

// RFC 3659 section 5: REST applies to the transfer command that immediately follows it. When that
// transfer fails, the offset must not carry over to the next one.
#[tokio::test(flavor = "current_thread")]
async fn rest_offset_is_consumed_by_a_failed_transfer() {
    initialize().await;

    let stream = tcp_connect().await.unwrap();
    let mut buffer = vec![0_u8; 1024];
    login(&stream, &mut buffer).await;

    let addr = pasv(&stream, &mut buffer).await;
    send_to_server("REST 10\r\n", &stream).await;
    assert_eq!(
        read_from_server(&mut buffer, &stream).await,
        "350 Restarting at 10. Now send STORE or RETRIEVE.\r\n"
    );
    send_to_server(&format!("STOR {REFUSED}.txt\r\n"), &stream).await;
    assert_eq!(read_from_server(&mut buffer, &stream).await, "150 Ready to receive data\r\n");
    let mut data_stream = tcp_pasv_connect(addr).await.unwrap();
    assert_eq!(read_from_server(&mut buffer, &stream).await, "550 Permission denied\r\n");
    let _ = data_stream.shutdown().await;
    drop(data_stream);

    let addr = pasv(&stream, &mut buffer).await;
    send_to_server("RETR content.txt\r\n", &stream).await;
    assert_eq!(read_from_server(&mut buffer, &stream).await, "150 Sending data\r\n");
    let data_stream = tcp_pasv_connect(addr).await.unwrap();
    let content = read_data_from_server(&data_stream).await;
    drop(data_stream);
    assert_eq!(
        String::from_utf8(content).unwrap(),
        CONTENT,
        "RETR after a failed restarted STOR must start at offset 0"
    );
    assert_eq!(read_from_server(&mut buffer, &stream).await, "226 Successfully sent\r\n");

    finalize().await;
}

/// Reads replies until `count` lines have arrived, or until none has come for a while.
async fn read_replies(stream: &TcpStream, count: usize) -> String {
    let mut buffer = [0_u8; 1024];
    let mut replies = String::new();
    while replies.matches("\r\n").count() < count {
        let read = tokio::time::timeout(Duration::from_secs(2), read_from_server(&mut buffer, stream)).await;
        match read {
            Ok(reply) => replies.push_str(reply),
            Err(_) => break,
        }
    }
    replies
}

// RFC 959 section 4.1.3: on ABOR while a transfer is in progress, "the server aborts the FTP service
// in progress and closes the data connection, returning a 426 reply to indicate that the service
// request terminated abnormally. The server then sends a 226 reply". The backend must see the abort
// as a failed read, so that a client closing the data connection cleanly afterwards does not turn the
// aborted upload into a finished one.
#[tokio::test(flavor = "current_thread")]
async fn abor_fails_the_upload_the_backend_is_reading() {
    initialize().await;

    let stream = tcp_connect().await.unwrap();
    let mut buffer = vec![0_u8; 1024];
    login(&stream, &mut buffer).await;
    let mut events = WATCHED_UPLOADS.1.lock().await;

    let addr = pasv(&stream, &mut buffer).await;
    send_to_server(&format!("STOR {WATCHED}.txt\r\n"), &stream).await;
    assert_eq!(read_from_server(&mut buffer, &stream).await, "150 Ready to receive data\r\n");
    let mut data_stream = tcp_pasv_connect(addr).await.unwrap();
    send_to_server("partial", &data_stream).await;
    let started = tokio::time::timeout(Duration::from_secs(5), events.recv()).await.unwrap();
    assert_eq!(started, Some(UploadEvent::Started));

    send_to_server("ABOR\r\n", &stream).await;
    assert_eq!(read_replies(&stream, 2).await, "426 Connection closed\r\n226 Closed data channel\r\n");
    let _ = data_stream.shutdown().await;
    drop(data_stream);

    let ended = tokio::time::timeout(Duration::from_secs(5), events.recv()).await.unwrap();
    assert_eq!(ended, Some(UploadEvent::Ended(Err(std::io::ErrorKind::ConnectionAborted))));

    finalize().await;
}

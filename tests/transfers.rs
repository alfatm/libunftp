#![allow(missing_docs)]

//! Data transfers as the storage backend sees them, and what a transfer leaves behind for the next one.
//!
//! These tests run against a storage backend whose behaviour is chosen by the file name, so a test can
//! make the backend fail at a precise point.

pub mod common;

use async_trait::async_trait;
use common::{TestAuthenticator, tcp_pasv_connect};
use lazy_static::lazy_static;
use libunftp::ServerBuilder;
use std::fmt::Debug;
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;
use unftp_core::auth::DefaultUser;
use unftp_core::storage::{Error, ErrorKind, FEATURE_RESTART, Fileinfo, Metadata, Result, StorageBackend};

const ADDR: &str = "127.0.0.1:2157";

/// How long a test waits for a reply or an event before it fails.
const PATIENCE: Duration = Duration::from_secs(5);

/// What every `RETR` serves. Each byte names its own offset, so the bytes received show where the read began.
const CONTENT: &str = "0123456789abcdef";

/// `put` refuses files whose name starts with this.
const REFUSED: &str = "refused";

lazy_static! {
    static ref CONSUMERS: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
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
        if file_name(path.as_ref()).starts_with(REFUSED) {
            return Err(Error::from(ErrorKind::PermissionDenied));
        }
        Ok(tokio::io::copy(&mut input, &mut tokio::io::sink()).await?)
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

/// A logged-in control connection that reads replies a line at a time.
struct Client {
    replies: BufReader<OwnedReadHalf>,
    commands: OwnedWriteHalf,
}

impl Client {
    async fn login() -> Client {
        let mut errcount = 0;
        let stream = loop {
            let connected = TcpStream::connect(ADDR).await;
            match connected {
                Ok(stream) => break stream,
                Err(err) if errcount > 2 => panic!("could not connect to the test server: {err}"),
                Err(_) => {
                    errcount += 1;
                    tokio::time::sleep(Duration::new(1, 0)).await;
                }
            }
        };
        let (replies, commands) = stream.into_split();
        let mut client = Client {
            replies: BufReader::new(replies),
            commands,
        };
        assert_eq!(client.reply().await, "220 Welcome test");
        client.command("USER test", "331 Password Required").await;
        client.command("PASS test", "230 User logged in, proceed").await;
        client
    }

    async fn send(&mut self, command: &str) {
        self.commands.write_all(format!("{command}\r\n").as_bytes()).await.unwrap();
    }

    /// The next reply line, without its line break.
    async fn reply(&mut self) -> String {
        let mut line = String::new();
        let read = tokio::time::timeout(PATIENCE, self.replies.read_line(&mut line)).await;
        read.expect("no reply in time").unwrap();
        line.trim_end_matches("\r\n").to_owned()
    }

    async fn command(&mut self, command: &str, expected: &str) {
        self.send(command).await;
        assert_eq!(self.reply().await, expected, "reply to {command}");
    }

    async fn pasv(&mut self) -> SocketAddr {
        self.send("PASV").await;
        let reply = self.reply().await;
        assert!(reply.starts_with("227 Entering Passive Mode"), "unexpected reply {reply:?}");
        let body = reply.split_once('(').and_then(|(_, rest)| rest.split_once(')')).unwrap().0;
        let nums: Vec<u8> = body.split(',').map(|s| s.trim().parse().unwrap()).collect();
        let port = u16::from(nums[4]) * 256 + u16::from(nums[5]);
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(nums[0], nums[1], nums[2], nums[3])), port)
    }
}

async fn read_to_end(mut data: TcpStream) -> String {
    let mut content = String::new();
    let read = tokio::time::timeout(PATIENCE, data.read_to_string(&mut content)).await;
    read.expect("the data connection was not closed in time").unwrap();
    content
}

// RFC 959 section 4.1.3: REST "shall be immediately followed by the appropriate FTP service command
// which shall cause file transfer to resume". When that transfer fails, the offset must not carry over
// to the next one.
#[tokio::test(flavor = "current_thread")]
async fn rest_offset_is_consumed_by_a_failed_transfer() {
    initialize().await;
    let mut client = Client::login().await;

    let addr = client.pasv().await;
    client.command("REST 10", "350 Restarting at 10. Now send STORE or RETRIEVE.").await;
    client.command(&format!("STOR {REFUSED}.txt"), "150 Ready to receive data").await;
    let data = tcp_pasv_connect(addr).await.unwrap();
    assert_eq!(client.reply().await, "550 Permission denied");
    drop(data);

    let addr = client.pasv().await;
    client.command("RETR content.txt", "150 Sending data").await;
    let content = read_to_end(tcp_pasv_connect(addr).await.unwrap()).await;
    assert_eq!(content, CONTENT, "RETR after a failed restarted STOR must start at offset 0");
    assert_eq!(client.reply().await, "226 Successfully sent");

    finalize().await;
}

// A REST that follows a transfer command belongs to the next transfer, even when the data connection of
// the first one opens only after the REST.
#[tokio::test(flavor = "current_thread")]
async fn rest_after_a_transfer_command_does_not_apply_to_it() {
    initialize().await;
    let mut client = Client::login().await;

    let addr = client.pasv().await;
    client.command("RETR content.txt", "150 Sending data").await;
    client.command("REST 5", "350 Restarting at 5. Now send STORE or RETRIEVE.").await;
    let content = read_to_end(tcp_pasv_connect(addr).await.unwrap()).await;
    assert_eq!(content, CONTENT, "a REST sent after RETR must not move the start of that RETR");
    assert_eq!(client.reply().await, "226 Successfully sent");

    let addr = client.pasv().await;
    client.command("RETR content.txt", "150 Sending data").await;
    let content = read_to_end(tcp_pasv_connect(addr).await.unwrap()).await;
    assert_eq!(content, &CONTENT[5..], "the REST applies to the transfer after it");
    assert_eq!(client.reply().await, "226 Successfully sent");

    finalize().await;
}

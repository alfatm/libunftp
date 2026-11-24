#![allow(missing_docs)]

//! Data transfers as the storage backend sees them, and what a transfer leaves behind for the next one.
//!
//! These tests run against a storage backend whose behaviour is chosen by the file name, so a test can
//! make the backend fail at a precise point, watch what it reads, or hold a transfer until the test lets
//! it go.

pub mod common;

use async_trait::async_trait;
use common::{TestAuthenticator, tcp_pasv_connect};
use lazy_static::lazy_static;
use libunftp::ServerBuilder;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, mpsc, oneshot};
use unftp_core::auth::DefaultUser;
use unftp_core::storage::{Error, ErrorKind, FEATURE_RESTART, Fileinfo, Metadata, Result, StorageBackend};

const ADDR: &str = "127.0.0.1:2157";

/// How long a test waits for a reply or an event before it fails.
const PATIENCE: Duration = Duration::from_secs(5);

/// What every `RETR` serves. Each byte names its own offset, so the bytes received show where the read began.
const CONTENT: &str = "0123456789abcdef";

/// `put` refuses files whose name starts with this.
const REFUSED: &str = "refused";

/// The backend reports to the [`probe`] of files whose name starts with this what it reads.
const WATCHED: &str = "watched";

/// Like [`WATCHED`], and the backend holds the transfer until the probe releases it: `put` once it has
/// read to EOF, `get_into` once it has sent the first half of [`CONTENT`].
const HELD: &str = "held";

/// What a [`probe`] reports of the backend's work on a file.
#[derive(Debug, PartialEq)]
enum Seen {
    /// `put` has read the first bytes.
    Started,
    /// `put`'s reader ended: with the number of bytes read at EOF, or with the kind of the read error.
    Ended(std::result::Result<u64, std::io::ErrorKind>),
    /// A held transfer was polled again, so something woke the data channel while the backend held it.
    PolledWhileHeld,
}

struct Probe {
    seen: mpsc::UnboundedSender<Seen>,
    release: oneshot::Receiver<()>,
}

lazy_static! {
    static ref CONSUMERS: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
    static ref PROBES: std::sync::Mutex<HashMap<String, Probe>> = std::sync::Mutex::new(HashMap::new());
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

/// Watches the backend's work on the file `name`: returns what the backend reports, and the sender that
/// releases a transfer it holds.
fn probe(name: &str) -> (mpsc::UnboundedReceiver<Seen>, oneshot::Sender<()>) {
    let (seen, seen_rx) = mpsc::unbounded_channel();
    let (release_tx, release) = oneshot::channel();
    PROBES.lock().unwrap().insert(name.to_owned(), Probe { seen, release });
    (seen_rx, release_tx)
}

fn take_probe(path: &Path) -> Probe {
    let name = file_name(path);
    PROBES.lock().unwrap().remove(&name).unwrap_or_else(|| panic!("no probe for {name}"))
}

/// Waits until `release` fires, and reports to `seen` if it is polled more than once meanwhile.
async fn hold(release: oneshot::Receiver<()>, seen: &mpsc::UnboundedSender<Seen>) {
    let mut release = release;
    let mut polls = 0;
    std::future::poll_fn(|cx| {
        polls += 1;
        if polls == 2 {
            seen.send(Seen::PolledWhileHeld).unwrap();
        }
        Pin::new(&mut release).poll(cx)
    })
    .await
    .unwrap();
}

async fn next(seen: &mut mpsc::UnboundedReceiver<Seen>) -> Seen {
    let next = tokio::time::timeout(PATIENCE, seen.recv()).await;
    next.expect("the backend reported nothing in time").unwrap()
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

    async fn get_into<'a, P, W: ?Sized>(&self, user: &DefaultUser, path: P, start_pos: u64, output: &'a mut W) -> Result<u64>
    where
        W: AsyncWrite + Unpin + Sync + Send,
        P: AsRef<Path> + Send + Debug,
    {
        if !file_name(path.as_ref()).starts_with(HELD) {
            let mut reader = self.get(user, path, start_pos).await?;
            return Ok(tokio::io::copy(&mut reader, output).await?);
        }
        let probe = take_probe(path.as_ref());
        let (first, second) = CONTENT.split_at(CONTENT.len() / 2);
        output.write_all(first.as_bytes()).await?;
        output.flush().await?;
        hold(probe.release, &probe.seen).await;
        output.write_all(second.as_bytes()).await?;
        Ok(CONTENT.len() as u64)
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
        if !name.starts_with(WATCHED) && !name.starts_with(HELD) {
            return Ok(tokio::io::copy(&mut input, &mut tokio::io::sink()).await?);
        }
        let probe = take_probe(path.as_ref());
        let mut buffer = [0_u8; 1024];
        let mut total = 0_u64;
        let ended = loop {
            let read = input.read(&mut buffer).await;
            match read {
                Ok(0) => break Ok(total),
                Ok(n) => {
                    if total == 0 {
                        probe.seen.send(Seen::Started).unwrap();
                    }
                    total += n as u64;
                }
                Err(err) => break Err(err),
            }
        };
        probe.seen.send(Seen::Ended(ended.as_ref().map(|n| *n).map_err(|err| err.kind()))).unwrap();
        let total = ended.map_err(|err| Error::new(ErrorKind::ConnectionClosed, err))?;
        if name.starts_with(HELD) {
            hold(probe.release, &probe.seen).await;
        }
        Ok(total)
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

// RFC 959 section 4.1.3: on ABOR while a transfer is in progress, "the server aborts the FTP service
// in progress and closes the data connection, returning a 426 reply to indicate that the service
// request terminated abnormally. The server then sends a 226 reply". The backend must see the abort
// as a failed read, so that a client closing the data connection cleanly afterwards does not turn the
// aborted upload into a finished one.
#[tokio::test(flavor = "current_thread")]
async fn abor_fails_the_upload_the_backend_is_reading() {
    initialize().await;
    let mut client = Client::login().await;
    let name = format!("{WATCHED}-aborted.txt");
    let (mut seen, _release) = probe(&name);

    let addr = client.pasv().await;
    client.command(&format!("STOR {name}"), "150 Ready to receive data").await;
    let mut data = tcp_pasv_connect(addr).await.unwrap();
    data.write_all(b"partial").await.unwrap();
    assert_eq!(next(&mut seen).await, Seen::Started);

    client.send("ABOR").await;
    assert_eq!(client.reply().await, "426 Connection closed; transfer aborted");
    assert_eq!(client.reply().await, "226 Closed data channel");
    assert_eq!(next(&mut seen).await, Seen::Ended(Err(std::io::ErrorKind::ConnectionAborted)));
    assert_eq!(read_to_end(data).await, "", "ABOR closes the data connection");

    finalize().await;
}

// RFC 959 section 4.1.3: "If the FTP service command was already completed, the server will close the
// data connection (if it is open) and respond with a 226 reply". An upload the backend has read to the
// end and is still committing when ABOR arrives is not aborted.
#[tokio::test(flavor = "current_thread")]
async fn abor_after_the_backend_read_the_whole_upload_keeps_it() {
    initialize().await;
    let mut client = Client::login().await;
    let name = format!("{HELD}-finished.txt");
    let (mut seen, release) = probe(&name);

    let addr = client.pasv().await;
    client.command(&format!("STOR {name}"), "150 Ready to receive data").await;
    let mut data = tcp_pasv_connect(addr).await.unwrap();
    data.write_all(b"complete").await.unwrap();
    data.shutdown().await.unwrap();
    assert_eq!(next(&mut seen).await, Seen::Started);
    assert_eq!(next(&mut seen).await, Seen::Ended(Ok(8)));

    // The backend holds the upload until the data channel has woken up for the ABOR.
    client.send("ABOR").await;
    assert_eq!(next(&mut seen).await, Seen::PolledWhileHeld);
    release.send(()).unwrap();
    assert_eq!(client.reply().await, "226 File successfully written");
    assert_eq!(client.reply().await, "226 Closed data channel");

    finalize().await;
}

// ABOR during a download is answered after the download's own reply. The NOOP shows that the control
// channel has handled the ABOR while the backend still holds the download.
#[tokio::test(flavor = "current_thread")]
async fn abor_during_retr_is_answered_after_the_transfer() {
    initialize().await;
    let mut client = Client::login().await;
    let name = format!("{HELD}-download.txt");
    let (_seen, release) = probe(&name);

    let addr = client.pasv().await;
    client.command(&format!("RETR {name}"), "150 Sending data").await;
    let mut data = tcp_pasv_connect(addr).await.unwrap();
    let mut first = [0_u8; CONTENT.len() / 2];
    tokio::time::timeout(PATIENCE, data.read_exact(&mut first)).await.unwrap().unwrap();

    client.send("ABOR").await;
    client.command("NOOP", "200 Successfully did nothing").await;
    release.send(()).unwrap();
    read_to_end(data).await;
    assert_eq!(client.reply().await, "226 Successfully sent");
    assert_eq!(client.reply().await, "226 Closed data channel");

    finalize().await;
}

// With no transfer in progress, ABOR gets a single 226: with no data connection, with one the client has
// not opened or that waits for a command, and after a transfer is done. The NOOP shows that no second
// reply follows.
#[tokio::test(flavor = "current_thread")]
async fn abor_with_no_transfer_gets_a_single_226() {
    initialize().await;
    let mut client = Client::login().await;

    client.command("ABOR", "226 Data channel already closed").await;
    client.command("NOOP", "200 Successfully did nothing").await;

    let addr = client.pasv().await;
    client.command("ABOR", "226 Closed data channel").await;
    client.command("NOOP", "200 Successfully did nothing").await;

    let data = tcp_pasv_connect(addr).await.unwrap();
    client.send("ABOR").await;
    assert!(client.reply().await.starts_with("226 "));
    client.command("NOOP", "200 Successfully did nothing").await;
    drop(data);

    let addr = client.pasv().await;
    client.command("RETR content.txt", "150 Sending data").await;
    assert_eq!(read_to_end(tcp_pasv_connect(addr).await.unwrap()).await, CONTENT);
    assert_eq!(client.reply().await, "226 Successfully sent");
    client.send("ABOR").await;
    assert!(client.reply().await.starts_with("226 "));
    client.command("NOOP", "200 Successfully did nothing").await;

    finalize().await;
}

// An ABOR sent while an earlier one still waits for the transfer is answered after the transfer too,
// not right away.
#[tokio::test(flavor = "current_thread")]
async fn second_abor_during_retr_is_answered_after_the_transfer() {
    initialize().await;
    let mut client = Client::login().await;
    let name = format!("{HELD}-download-twice.txt");
    let (_seen, release) = probe(&name);

    let addr = client.pasv().await;
    client.command(&format!("RETR {name}"), "150 Sending data").await;
    let mut data = tcp_pasv_connect(addr).await.unwrap();
    let mut first = [0_u8; CONTENT.len() / 2];
    tokio::time::timeout(PATIENCE, data.read_exact(&mut first)).await.unwrap().unwrap();

    client.send("ABOR").await;
    client.send("ABOR").await;
    client.command("NOOP", "200 Successfully did nothing").await;
    release.send(()).unwrap();
    read_to_end(data).await;
    assert_eq!(client.reply().await, "226 Successfully sent");
    assert!(client.reply().await.starts_with("226 "));
    assert!(client.reply().await.starts_with("226 "));
    client.command("NOOP", "200 Successfully did nothing").await;

    finalize().await;
}

// An ABOR right behind STOR reaches the data channel after the STOR, so it aborts the upload, and the 150
// is followed by the 426 and the 226.
#[tokio::test(flavor = "current_thread")]
async fn abor_right_after_stor_aborts_it() {
    initialize().await;
    let mut client = Client::login().await;
    let name = format!("{WATCHED}-aborted-at-once.txt");
    let (mut seen, _release) = probe(&name);

    let addr = client.pasv().await;
    let data = tcp_pasv_connect(addr).await.unwrap();
    client.send(&format!("STOR {name}\r\nABOR")).await;
    assert_eq!(client.reply().await, "150 Ready to receive data");
    assert_eq!(client.reply().await, "426 Connection closed; transfer aborted");
    assert_eq!(client.reply().await, "226 Closed data channel");
    assert_eq!(next(&mut seen).await, Seen::Ended(Err(std::io::ErrorKind::ConnectionAborted)));
    drop(data);

    finalize().await;
}

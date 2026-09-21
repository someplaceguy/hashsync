//! `hashsync serve` driven directly through its standard streams and its
//! rendezvous socket, the way the receiver and `attach` drive it. The
//! receiver's disappearance is fatal at every stage before the normal
//! exit (DESIGN.md §6.7): during rendezvous, while `serve` waits for the
//! control session, and while command stderr still drains after END_ACK.

use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

// The protocol constants and frame types of DESIGN.md §6.2, spelled out
// because the tests reach only the binary.
const PROTOCOL_VERSION: u32 = 2;
const WEAK_HASH_ID: &[u8] = b"poly64-9e3779b97f4a7c15-splitmix64";
const HELLO: u8 = 1;
const HASHLIST: u8 = 2;
const LISTDATA: u8 = 5;
const CACHE_RESULT: u8 = 7;
const LIT: u8 = 8;
const END: u8 = 11;
const END_ACK: u8 = 12;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

/// The empty hash list (DESIGN.md §5.4): the header alone.
fn empty_list() -> Vec<u8> {
    let mut list = vec![1, 64, 0];
    list.extend_from_slice(&0u64.to_le_bytes());
    list
}

fn string(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn list_id(out: &mut Vec<u8>) {
    string(out, b"blake3");
    out.extend_from_slice(blake3::hash(&empty_list()).as_bytes());
}

fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![kind];
    string(&mut out, payload);
    out
}

fn control_hello() -> Vec<u8> {
    let mut hello = PROTOCOL_VERSION.to_le_bytes().to_vec();
    string(&mut hello, b"test");
    frame(HELLO, &hello)
}

/// True while `pid` runs; a zombie has already exited.
fn running(pid: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    // The state follows the parenthesized command name.
    let state = stat.rsplit(')').next().unwrap_or_default().trim_start();
    !state.starts_with(['Z', 'X'])
}

/// Kills a process that still runs when a failed test unwinds.
struct KillOnDrop(u32);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if running(self.0) {
            let _ = Command::new("kill")
                .args(["-KILL", &self.0.to_string()])
                .status();
        }
    }
}

/// Wait until `done`, within a bound: a regression must fail the test,
/// not hang it. False when the bound passed first.
fn wait_until(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    true
}

struct Serve {
    child: Child,
    data_in: Option<ChildStdin>,
    data_out: ChildStdout,
    home: TempDir,
}

impl Drop for Serve {
    /// A failed test must not leave `serve` running. After a successful
    /// `try_wait`, `kill` sends nothing.
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

impl Serve {
    fn start() -> Self {
        let home = tempfile::Builder::new()
            .prefix("hashsync-serve-")
            .tempdir()
            .unwrap();
        std::fs::create_dir(home.path().join("runtime")).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_hashsync"))
            .args(["serve", "--token", TOKEN])
            .env("HOME", home.path())
            .env("XDG_CONFIG_HOME", home.path().join("config"))
            .env("XDG_CACHE_HOME", home.path().join("cache"))
            .env("XDG_RUNTIME_DIR", home.path().join("runtime"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        Serve {
            data_in: child.stdin.take(),
            data_out: child.stdout.take().unwrap(),
            child,
            home,
        }
    }

    fn socket(&self) -> PathBuf {
        self.home
            .path()
            .join(format!("runtime/hashsync/{TOKEN}.sock"))
    }

    fn send(&mut self, kind: u8, payload: &[u8]) {
        let data_in = self.data_in.as_mut().unwrap();
        data_in.write_all(&frame(kind, payload)).unwrap();
    }

    fn recv(&mut self) -> (u8, Vec<u8>) {
        let mut header = [0u8; 5];
        self.data_out.read_exact(&mut header).unwrap();
        let length = u32::from_le_bytes(header[1..].try_into().unwrap());
        let mut payload = vec![0; length as usize];
        self.data_out.read_exact(&mut payload).unwrap();
        (header[0], payload)
    }

    /// The data HELLOs: the receiver's, then the sender's reply.
    fn hello(&mut self, command: &[&str]) {
        let mut hello = PROTOCOL_VERSION.to_le_bytes().to_vec();
        string(&mut hello, b"test");
        hello.extend_from_slice(&4096u32.to_le_bytes());
        string(&mut hello, WEAK_HASH_ID);
        string(&mut hello, b"blake3");
        hello.push(64);
        hello.extend_from_slice(&[0xaa; 16]); // account ID
        hello.extend_from_slice(&[0xbb; 16]); // index ID
        list_id(&mut hello);
        hello.push(1); // sender_cache
        hello.extend_from_slice(&(90 * 24 * 3600u64).to_le_bytes()); // hashlist_max_age
        hello.extend_from_slice(&(256u64 << 20).to_le_bytes()); // lookahead
        hello.extend_from_slice(&0i32.to_le_bytes()); // compression level
        hello.extend_from_slice(&0u32.to_le_bytes()); // compression threads
        hello.extend_from_slice(&(100u64 << 20).to_le_bytes()); // negative cache
        hello.extend_from_slice(&(command.len() as u32).to_le_bytes());
        for arg in command {
            string(&mut hello, arg.as_bytes());
        }
        self.send(HELLO, &hello);
        assert_eq!(self.recv().0, HELLO);
    }

    /// Connect as `attach` does, through the socket directory's
    /// descriptor: a temporary path can exceed the `sun_path` limit.
    /// `serve` listens before it replies to HELLO.
    fn attach(&self) -> UnixStream {
        let dir = std::fs::File::open(self.socket().parent().unwrap()).unwrap();
        let address = format!("/proc/self/fd/{}/{TOKEN}.sock", dir.as_raw_fd());
        UnixStream::connect(address).unwrap()
    }

    /// The full list and its acknowledgement.
    fn exchange_list(&mut self) {
        let list = empty_list();
        let mut header = Vec::new();
        list_id(&mut header);
        header.extend_from_slice(&(list.len() as u64).to_le_bytes());
        self.send(HASHLIST, &header);
        self.send(LISTDATA, &list);
        assert_eq!(self.recv().0, CACHE_RESULT);
    }

    /// The receiver disappears: its end of the data session closes.
    fn lose_data_session(&mut self) {
        self.data_in = None;
    }

    /// Require a fatal exit with `message`, and the fatal cleanup of the
    /// rendezvous socket.
    fn expect_death(mut self, message: &str) {
        let mut status = None;
        let ended = wait_until(|| {
            status = self.child.try_wait().unwrap();
            status.is_some()
        });
        assert!(ended, "serve hangs instead of ending");
        let mut stderr = String::new();
        let mut pipe = self.child.stderr.take().unwrap();
        pipe.read_to_string(&mut stderr).unwrap();
        assert_eq!(status.unwrap().code(), Some(1), "stderr:\n{stderr}");
        assert!(stderr.contains(message), "stderr:\n{stderr}");
        assert!(!self.socket().exists(), "the socket file must be removed");
    }
}

#[test]
fn receiver_loss_during_rendezvous_is_fatal() {
    let mut serve = Serve::start();
    serve.hello(&["true"]);
    serve.lose_data_session();
    serve.expect_death("the receiver disappeared");
}

#[test]
fn control_loss_before_the_list_is_fatal() {
    let mut serve = Serve::start();
    serve.hello(&["true"]);
    let mut control = serve.attach();
    control.write_all(&control_hello()).unwrap();
    // Read `serve`'s HELLO, so that closing sends EOF, not a reset.
    let mut hello = vec![0u8; control_hello().len()];
    control.read_exact(&mut hello).unwrap();
    drop(control);
    serve.expect_death("the control session closed unexpectedly");
}

#[test]
fn receiver_loss_while_serve_waits_for_the_control_hello_is_fatal() {
    // The command starts only after both HELLOs (DESIGN.md §6.2).
    let mut serve = Serve::start();
    serve.hello(&["true"]);
    let _control = serve.attach();
    serve.exchange_list();
    serve.lose_data_session();
    serve.expect_death("the receiver disappeared");
}

#[test]
fn receiver_loss_after_end_ack_is_fatal_while_stderr_drains() {
    // A descendant keeps command stderr open, so the relay has not
    // finished when END_ACK arrives. The receiver's disappearance then
    // fails the run and kills the command's process group, descendant
    // included.
    let mut serve = Serve::start();
    serve.hello(&["sh", "-c", "( exec >/dev/null; exec sleep 60 ) & echo $!"]);
    let mut control = serve.attach();
    control.write_all(&control_hello()).unwrap();
    serve.exchange_list();
    let mut output = Vec::new();
    loop {
        match serve.recv() {
            (LIT, payload) => output.extend_from_slice(&payload),
            (END, _) => break,
            (kind, _) => panic!("unexpected frame type {kind}"),
        }
    }
    let descendant: u32 = String::from_utf8(output).unwrap().trim().parse().unwrap();
    let _guard = KillOnDrop(descendant);
    serve.send(END_ACK, b"");
    serve.lose_data_session();
    serve.expect_death("the receiver disappeared after END_ACK");
    assert!(
        wait_until(|| !running(descendant)),
        "the descendant survived the fatal cleanup"
    );
}

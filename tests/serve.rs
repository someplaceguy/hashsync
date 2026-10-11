//! `hashsync serve` driven directly through its standard streams and its
//! rendezvous socket, the way the receiver and `attach` drive it. The
//! receiver's disappearance is fatal at every stage before DONE
//! (DESIGN.md §6.9): during rendezvous, while `serve` waits for the
//! control session, while the data reader waits at the limit of the input
//! spool, and while command stderr still drains after END_ACK. After DONE,
//! stdin EOF is the normal end. Queries, answers, and INPUT_STOP travel on
//! the control session (DESIGN.md §6.2); obsolete queries still unsent at
//! the normal end do not fail it, also inside a compressed block. After
//! HELLO, the data frames of `serve` through END form one zstd frame, and
//! DONE follows it; after the features, each direction of the control
//! session is one zstd stream that no frame ends.

mod common;

use std::io::{BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::time::Duration;

use common::{
    ANSWER, CACHE_RESULT, DONE, END, END_ACK, FEATURES, HASHLIST, HELLO, INPUT, INPUT_END,
    INPUT_STOP, LIT, PROTOCOL_VERSION, QUERY, REF, frame, proc_state, recv_frame, try_recv_frame,
    version_and_features,
};
use rustix::event::{PollFd, PollFlags, poll};
use rustix::fs::{CWD, Mode, mkfifoat};
use rustix::io::retry_on_intr;
use rustix::process::{Pid, PidfdFlags, Signal, kill_process, pidfd_open};
use rustix::thread::{CpuSet, sched_getaffinity, sched_setaffinity};
use tempfile::TempDir;

// The weak hash of DESIGN.md §5.1, spelled out because the tests reach
// only the binary.
const WEAK_HASH_ID: &[u8] = b"poly64-9e3779b97f4a7c15-splitmix64";

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

/// The empty hash list (DESIGN.md §5.4): the header `N = 0` alone.
fn empty_list() -> Vec<u8> {
    0u64.to_le_bytes().to_vec()
}

/// The canonical list of sorted distinct `values` (DESIGN.md §5.4): `N`,
/// then each gap as `g >> k` one bits, a zero, and the low `k` bits, with
/// `k = 64 - ceil(log2(N))`, packed least significant bit first.
fn encode_list(values: &[u64]) -> Vec<u8> {
    let n = values.len() as u64;
    let k = if n <= 1 { 64 } else { (n - 1).leading_zeros() };
    let mut list = n.to_le_bytes().to_vec();
    let mut bits = 0usize;
    let mut push = |list: &mut Vec<u8>, bit: bool| {
        if bits.is_multiple_of(8) {
            list.push(0);
        }
        *list.last_mut().unwrap() |= u8::from(bit) << (bits % 8);
        bits += 1;
    };
    let mut previous = None;
    for &value in values {
        let gap = previous.map_or(value, |p| value - p - 1);
        previous = Some(value);
        let quotient = if k == 64 { 0 } else { gap >> k };
        for _ in 0..quotient {
            push(&mut list, true);
        }
        push(&mut list, false);
        for i in 0..k {
            push(&mut list, (gap >> i) & 1 == 1);
        }
    }
    list
}

/// The weak hash of a window (DESIGN.md §5.1 and §5.2).
fn weak_hash(window: &[u8]) -> u64 {
    let mut z = window.iter().fold(0u64, |p, &b| {
        p.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(u64::from(b))
    });
    z ^= z >> 30;
    z = z.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z ^= z >> 27;
    z = z.wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn string(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// The start of the receiver's HELLO payload, through the name of its
/// strong hash: the block size, then the weak and strong hashes.
fn hello_prefix(block_size: u32, strong: &[u8]) -> Vec<u8> {
    let mut hello = block_size.to_le_bytes().to_vec();
    string(&mut hello, WEAK_HASH_ID);
    string(&mut hello, strong);
    hello
}

/// The receiver's HELLO payload for `list`, with BLAKE3 as its strong
/// hash, zstd compression at `level`, 0 for none, and a sender input spool
/// that holds at most `input_limit` bytes.
fn hello_payload(
    block_size: u32,
    list: &[u8],
    command: &[&str],
    level: i32,
    input_limit: u64,
) -> Vec<u8> {
    let mut hello = hello_prefix(block_size, b"blake3");
    hello.extend_from_slice(blake3::hash(list).as_bytes()); // target
    hello.extend_from_slice(&[0xaa; 16]); // account ID
    hello.extend_from_slice(&[0xbb; 16]); // index ID
    hello.push(1); // sender_cache
    hello.extend_from_slice(&(90 * 24 * 3600u64).to_le_bytes()); // hashlist_max_age
    hello.extend_from_slice(&level.to_le_bytes()); // compression level
    hello.push(1); // compression_long
    hello.extend_from_slice(&0u32.to_le_bytes()); // compression threads
    hello.extend_from_slice(&0u32.to_le_bytes()); // probe threads
    hello.extend_from_slice(&(256u64 << 20).to_le_bytes()); // lookahead
    hello.extend_from_slice(&(128u64 << 20).to_le_bytes()); // negative cache
    hello.extend_from_slice(&(2u64 << 30).to_le_bytes()); // sender output spool limit
    hello.extend_from_slice(&input_limit.to_le_bytes()); // sender input spool limit
    hello.extend_from_slice(&(command.len() as u32).to_le_bytes());
    for arg in command {
        string(&mut hello, arg.as_bytes());
    }
    hello
}

/// Run `start` with this thread on one of its CPUs; a process that it
/// starts stays on that CPU with all its threads.
fn on_one_cpu<T>(start: impl FnOnce() -> T) -> T {
    let all = sched_getaffinity(None).unwrap();
    let first = (0..CpuSet::MAX_CPU).find(|&cpu| all.is_set(cpu)).unwrap();
    let mut one = CpuSet::new();
    one.set(first);
    sched_setaffinity(None, &one).unwrap();
    let started = start();
    sched_setaffinity(None, &all).unwrap();
    started
}

/// Wait until every thread of the process `pid` sleeps, polling with no
/// deadline. Fail when the process has ended: it stays a zombie until the
/// test reaps it.
fn wait_until_asleep(pid: u32) {
    loop {
        // A thread that ends meanwhile has no state.
        let states: Vec<Option<char>> = std::fs::read_dir(format!("/proc/{pid}/task"))
            .unwrap()
            .map(|task| proc_state(task.unwrap().path()))
            .collect();
        assert!(!states.contains(&Some('Z')), "the process ended");
        if states.iter().all(|&state| state == Some('S')) {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The default send buffer of a socket, `net.core.wmem_default`, in
/// bytes. A Unix socket holds at most about one and a half times as many.
fn send_buffer() -> usize {
    std::fs::read_to_string("/proc/sys/net/core/wmem_default")
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// The number of the system call in which the thread whose `syscall` file
/// is at `path` sleeps; `None` while it runs or sleeps outside a system
/// call. The file names a system call only while the thread sleeps in it.
fn sleeping_syscall(path: &Path) -> std::io::Result<Option<u64>> {
    let syscall = std::fs::read_to_string(path)?;
    Ok(syscall
        .split_whitespace()
        .next()
        .and_then(|nr| nr.parse().ok()))
}

/// The system call in which `UnixStream::write` sleeps on a full socket:
/// `sendto`, `send`, or `socketcall`, by architecture and C library.
fn blocking_send_syscall() -> u64 {
    let (full, reader) = UnixStream::pair().unwrap();
    blocking_write_syscall(full, reader, 4 * send_buffer())
}

/// The system call in which a write to a full pipe sleeps, as the write of
/// `serve` to the command's stdin does: `write`, by its number on this
/// architecture. 16 MiB are more than a pipe holds by default.
fn blocking_pipe_write_syscall() -> u64 {
    let (reader, full) = std::io::pipe().unwrap();
    blocking_write_syscall(full, reader, 16 << 20)
}

/// The system call in which a thread sleeps while it writes `size` bytes
/// to `full`, more than it holds, while nothing reads `reader`, its other
/// end. A thread of this process does that write, and a process can
/// always read the `syscall` files of its own threads. Then the drop of
/// `reader` ends the write.
fn blocking_write_syscall<R>(mut full: impl Write + Send + 'static, reader: R, size: usize) -> u64 {
    // The thread has its bytes before it tells its ID, so the write is the
    // only call that it can then sleep in.
    let bytes = vec![0; size];
    let (tid_tx, tid_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        tid_tx.send(rustix::thread::gettid()).unwrap();
        // The write fails once `reader` closes.
        full.write_all(&bytes).unwrap_err();
    });
    let tid = tid_rx.recv().unwrap().as_raw_nonzero();
    let path = PathBuf::from(format!("/proc/self/task/{tid}/syscall"));
    let nr = loop {
        if let Some(nr) = sleeping_syscall(&path).unwrap() {
            break nr;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    drop(reader);
    writer.join().unwrap();
    nr
}

/// Wait until the thread `name` of the process `pid` sleeps in the system
/// call of a write to a full socket, polling with no deadline. It stays
/// there while no one reads the socket.
fn wait_until_blocked_in_send(pid: u32, name: &str) {
    wait_until_asleep_in(pid, name, blocking_send_syscall());
}

/// Wait until the thread `name` of the process `pid` sleeps in the system
/// call `nr`, polling with no deadline. Reading its `syscall` file needs
/// the ptrace access that a parent has. Fail when the process has ended.
fn wait_until_asleep_in(pid: u32, name: &str, nr: u64) {
    loop {
        assert_ne!(
            proc_state(format!("/proc/{pid}")),
            Some('Z'),
            "the process ended"
        );
        for task in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
            let task = task.unwrap().path();
            // A thread that ends meanwhile has no files.
            let Ok(comm) = std::fs::read_to_string(task.join("comm")) else {
                continue;
            };
            if comm.trim_end() != name {
                continue;
            }
            match sleeping_syscall(&task.join("syscall")) {
                Ok(sleeping) if sleeping == Some(nr) => return,
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                    panic!("cannot see the system call of {name}: {e}")
                }
                _ => {}
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The frames from `reader` through END.
fn frames_through_end(reader: &mut impl Read) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    loop {
        let frame = recv_frame(reader);
        let end = frame.0 == END;
        frames.push(frame);
        if end {
            return frames;
        }
    }
}

/// This side of the control session after both versions and features, as
/// the receiver's: each direction as plain frames at level 0, else as one
/// zstd stream, which no frame ends (DESIGN.md §6.2).
struct Control {
    queries: Box<dyn Read>,
    answers: Box<dyn Write>,
    /// INPUT_STOP arrived. `serve` sends it once, at `EPIPE` or at the exit
    /// of the command, so it can come between any two queries (DESIGN.md
    /// §6.6).
    input_stopped: bool,
}

impl Control {
    fn new(socket: &UnixStream, level: i32) -> Self {
        let (read, write) = (socket.try_clone().unwrap(), socket.try_clone().unwrap());
        let (queries, answers): (Box<dyn Read>, Box<dyn Write>) = if level == 0 {
            (Box::new(read), Box::new(write))
        } else {
            (
                Box::new(zstd::stream::read::Decoder::new(read).unwrap()),
                Box::new(zstd::stream::write::Encoder::new(write, level).unwrap()),
            )
        };
        Control {
            queries,
            answers,
            input_stopped: false,
        }
    }

    /// The next frame from `serve` other than INPUT_STOP, which this
    /// notes; else the error that ends the session first, `UnexpectedEof`
    /// at its closure.
    fn next(&mut self) -> std::io::Result<(u8, Vec<u8>)> {
        loop {
            let frame = try_recv_frame(&mut self.queries)?;
            if frame.0 != INPUT_STOP {
                return Ok(frame);
            }
            assert!(frame.1.is_empty(), "INPUT_STOP with a payload");
            assert!(!self.input_stopped, "a second INPUT_STOP");
            self.input_stopped = true;
        }
    }

    /// The next QUERY: its offset and its digest.
    fn query(&mut self) -> (u64, Vec<u8>) {
        let (kind, mut payload) = self.next().unwrap();
        assert_eq!(kind, QUERY);
        let digest = payload.split_off(8);
        (u64::from_le_bytes(payload.try_into().unwrap()), digest)
    }

    /// Wait for INPUT_STOP, unless it arrived already; no other frame may
    /// come first. `serve` stops its input before END, and closes the
    /// session only after END_ACK, so a test that waits here before it
    /// sends END_ACK always gets INPUT_STOP (DESIGN.md §6.6).
    fn input_stop(&mut self) {
        if !self.input_stopped {
            assert_eq!(recv_frame(&mut self.queries), (INPUT_STOP, Vec::new()));
            self.input_stopped = true;
        }
    }

    /// ANSWER frames, flushed: `serve` waits for them.
    fn answer(&mut self, answers: &[(u64, bool)]) {
        for &(offset, found) in answers {
            let mut answer = offset.to_le_bytes().to_vec();
            answer.push(found.into());
            self.answers.write_all(&frame(ANSWER, &answer)).unwrap();
        }
        self.answers.flush().unwrap();
    }
}

struct Serve {
    child: Child,
    data_in: Option<ChildStdin>,
    data_out: ChildStdout,
    home: TempDir,
    /// The features that this side sends on both sessions, as the
    /// receiver: `FEATURES`, unless a test sets bits that `serve` does not
    /// know.
    features: u64,
}

impl Drop for Serve {
    /// A failed test must not leave `serve` running. After `wait`, `kill`
    /// sends nothing.
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
            features: FEATURES,
        }
    }

    fn socket(&self) -> PathBuf {
        self.home
            .path()
            .join(format!("runtime/hashsync/{TOKEN}.sock"))
    }

    fn send_raw(&mut self, bytes: &[u8]) {
        self.data_in.as_mut().unwrap().write_all(bytes).unwrap();
    }

    fn send(&mut self, kind: u8, payload: &[u8]) {
        self.send_raw(&frame(kind, payload));
    }

    /// Send this side's version and features on the data session. The
    /// features are `self.features`, so that a test can send bits that
    /// `serve` does not know.
    fn send_version_and_features(&mut self) {
        self.send_raw(&version_and_features(self.features));
    }

    fn recv_version_and_features(&mut self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        self.data_out.read_exact(&mut bytes).unwrap();
        bytes
    }

    fn recv(&mut self) -> (u8, Vec<u8>) {
        recv_frame(&mut self.data_out)
    }

    /// The versions and the features, then the data HELLOs: the
    /// receiver's, for the empty list and 4 KiB blocks, then the sender's
    /// reply.
    fn hello(&mut self, command: &[&str]) {
        self.hello_with(4096, &empty_list(), command, 0);
    }

    fn hello_with(&mut self, block_size: u32, list: &[u8], command: &[&str], level: i32) {
        self.send_hello(&hello_payload(block_size, list, command, level, 2 << 30));
    }

    /// The versions, the features, and the data HELLOs, as `hello`
    /// exchanges them, with a sender input spool that holds at most
    /// `input_limit` bytes.
    fn hello_with_input_limit(&mut self, command: &[&str], input_limit: u64) {
        self.send_hello(&hello_payload(4096, &empty_list(), command, 0, input_limit));
    }

    fn send_hello(&mut self, payload: &[u8]) {
        self.send_version_and_features();
        self.send(HELLO, payload);
        assert_eq!(
            self.recv_version_and_features(),
            version_and_features(FEATURES)
        );
        assert_eq!(self.recv().0, HELLO);
    }

    /// Open the control session as the receiver does: connect as
    /// `hashsync attach` does, then send this side's version and features
    /// at once (DESIGN.md §6.2). The features are `self.features`, as on
    /// the data session.
    fn attach(&self) -> UnixStream {
        let mut socket = self.attach_without_version();
        socket
            .write_all(&version_and_features(self.features))
            .unwrap();
        socket
    }

    /// Connect as `hashsync attach` does, through the socket directory's
    /// descriptor: a temporary path can exceed the `sun_path` limit.
    /// `serve` listens before it sends its version. Send nothing: some
    /// tests keep the control session open without the receiver's
    /// version.
    fn attach_without_version(&self) -> UnixStream {
        let dir = std::fs::File::open(self.socket().parent().unwrap()).unwrap();
        let address = format!("/proc/self/fd/{}/{TOKEN}.sock", dir.as_raw_fd());
        UnixStream::connect(address).unwrap()
    }

    /// Attach, send `list`, and exchange the versions and the features of
    /// the control session, whose later frames use compression at `level`.
    fn control_session(&mut self, list: &[u8], level: i32) -> Control {
        let mut socket = self.attach();
        self.send_list(list);
        let mut theirs = [0u8; 12];
        socket.read_exact(&mut theirs).unwrap();
        assert_eq!(theirs, version_and_features(FEATURES));
        Control::new(&socket, level)
    }

    /// The data frames after the list, from CACHE_RESULT through END,
    /// then the normal end of the data session (`end_ack`).
    fn frames_through_done(&mut self, level: i32) -> Vec<(u8, Vec<u8>)> {
        let frames = self.recv_through_end(level);
        self.end_ack();
        frames
    }

    /// The data frames after the list, from CACHE_RESULT through END. At a
    /// `level` other than 0, they form one zstd frame (DESIGN.md §6.2).
    fn recv_through_end(&mut self, level: i32) -> Vec<(u8, Vec<u8>)> {
        let mut transport = BufReader::new(&mut self.data_out);
        let frames = if level == 0 {
            frames_through_end(&mut transport)
        } else {
            let mut decoder = zstd::stream::read::Decoder::with_buffer(&mut transport)
                .unwrap()
                .single_frame();
            let frames = frames_through_end(&mut decoder);
            assert_eq!(decoder.read(&mut [0]).unwrap(), 0, "the frame goes on");
            frames
        };
        // `serve` sends nothing after END until END_ACK, so this buffer
        // holds no byte that `end_ack` must read.
        assert!(transport.buffer().is_empty(), "bytes after END");
        frames
    }

    /// The normal end of the data session after END: END_ACK, then DONE,
    /// which follows the zstd frame, outside it (DESIGN.md §6.2 and §6.9).
    fn end_ack(&mut self) {
        let data_in = self.data_in.as_mut().unwrap();
        data_in.write_all(&frame(END_ACK, b"")).unwrap();
        let mut transport = BufReader::new(&mut self.data_out);
        assert_eq!(recv_frame(&mut transport), (DONE, Vec::new()));
        assert!(transport.buffer().is_empty(), "bytes after DONE");
    }

    /// The full empty list, whose bytes follow HASHLIST directly.
    fn send_empty_list(&mut self) {
        self.send_list(&empty_list());
    }

    fn send_list(&mut self, list: &[u8]) {
        self.send(HASHLIST, &(list.len() as u64).to_le_bytes());
        self.send_raw(list);
    }

    /// CACHE_RESULT, the first data frame after the list, without a
    /// payload. `serve` flushes it: the receiver sends the input only
    /// after it (DESIGN.md §6.2).
    fn recv_result(&mut self) {
        assert_eq!(self.recv(), (CACHE_RESULT, Vec::new()));
    }

    /// Wait until `serve` has published the list in its cache, which it
    /// does right before CACHE_RESULT (DESIGN.md §5.6). No event tells the
    /// receiver when, so this polls the cache directory, with no deadline:
    /// a `serve` that neither publishes nor ends hangs the test, and a
    /// debugger can inspect it.
    fn wait_for_published_list(&mut self) {
        let dir = self.home.path().join("cache/hashsync/remote-hashlists");
        let published = || {
            let peers = std::fs::read_dir(&dir).into_iter().flatten().flatten();
            peers
                .flat_map(|peer| std::fs::read_dir(peer.path()))
                .any(|files| {
                    files
                        .flatten()
                        .any(|file| file.file_name().to_string_lossy().ends_with(".cache"))
                })
        };
        while !published() {
            if self.child.try_wait().unwrap().is_some() {
                let (status, stderr) = self.finish();
                panic!("serve ended before it published the list ({status}):\n{stderr}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Wait until `serve` ends, with no deadline: if `serve` hangs, the
    /// test hangs too, and a debugger can inspect it. Returns the exit
    /// status and stderr.
    fn finish(&mut self) -> (ExitStatus, String) {
        let mut stderr = String::new();
        let mut pipe = self.child.stderr.take().unwrap();
        pipe.read_to_string(&mut stderr).unwrap();
        (self.child.wait().unwrap(), stderr)
    }

    /// The normal exit after DONE: close stdin, as the receiver does.
    /// Nothing may follow DONE on stdout, and `serve` must exit with
    /// status 0.
    fn expect_success(mut self) {
        self.data_in = None;
        let mut rest = Vec::new();
        self.data_out.read_to_end(&mut rest).unwrap();
        assert!(rest.is_empty(), "bytes after DONE");
        let (status, stderr) = self.finish();
        assert_eq!(status.code(), Some(0), "stderr:\n{stderr}");
    }

    /// The receiver disappears: its end of the data session closes.
    fn lose_data_session(&mut self) {
        self.data_in = None;
    }

    /// Require a fatal exit with `message`, and the fatal cleanup of the
    /// rendezvous socket.
    fn expect_death(mut self, message: &str) {
        let (status, stderr) = self.finish();
        assert_eq!(status.code(), Some(1), "stderr:\n{stderr}");
        assert!(stderr.contains(message), "stderr:\n{stderr}");
        assert!(!self.socket().exists(), "the socket file must be removed");
    }
}

#[test]
fn a_different_protocol_version_is_fatal_before_the_features() {
    // `serve` sends its version and its features without waiting for the
    // receiver's, and it checks the receiver's version before anything
    // else: this receiver sends no features (DESIGN.md §6.2).
    let mut serve = Serve::start();
    assert_eq!(
        serve.recv_version_and_features(),
        version_and_features(FEATURES)
    );
    let other = PROTOCOL_VERSION + 1;
    serve.send_raw(&other.to_le_bytes());
    serve.expect_death(&format!(
        "the receiver speaks protocol version {other}, \
         this hashsync {} speaks {PROTOCOL_VERSION}",
        env!("CARGO_PKG_VERSION")
    ));
}

#[test]
fn feature_bits_that_serve_does_not_know_are_ignored() {
    // A later receiver can send feature bits that this `serve` does not
    // know, on both sessions. `serve` ignores them, sends its own
    // features, and the run goes on as with none (DESIGN.md §6.2).
    let mut serve = Serve::start();
    serve.features = u64::MAX;
    serve.hello(&["printf", "%s", "output"]);
    let mut control = serve.control_session(&empty_list(), 0);
    let frames = serve.recv_through_end(0);
    control.input_stop();
    serve.end_ack();
    assert_eq!(
        frames,
        [
            (CACHE_RESULT, Vec::new()),
            (LIT, b"output".to_vec()),
            (END, vec![0; 8])
        ]
    );
    serve.expect_success();
}

#[test]
fn an_unknown_strong_hash_is_rejected_before_the_target() {
    // The target's length depends on the strong hash, so `serve` rejects
    // an unknown one with `die` before it reads the target, as it would
    // for a newer receiver's algorithm (DESIGN.md §6.2 and §11). The
    // payload ends after the algorithm's name: a read of any later field
    // would panic on truncation instead.
    let mut serve = Serve::start();
    serve.send_version_and_features();
    serve.send(HELLO, &hello_prefix(4096, b"future-hash"));
    serve.expect_death("incompatible receiver: strong hash future-hash");
}

#[test]
fn an_unsupported_compression_level_is_rejected() {
    // The receiver's HELLO sets the run's parameters; the sender ends the
    // run before command startup if it does not support them
    // (DESIGN.md §6.2).
    let mut serve = Serve::start();
    serve.send_version_and_features();
    serve.send(
        HELLO,
        &hello_payload(4096, &empty_list(), &["true"], 20, 2 << 30),
    );
    serve.expect_death("incompatible receiver: compression level 20");
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
    // Read `serve`'s version and features, so that closing sends EOF, not
    // a reset.
    let mut theirs = [0u8; 12];
    control.read_exact(&mut theirs).unwrap();
    assert_eq!(theirs, version_and_features(FEATURES));
    drop(control);
    serve.expect_death("the control session closed unexpectedly");
}

#[test]
fn receiver_loss_while_serve_waits_for_the_control_version_is_fatal() {
    // The command starts only after the receiver's versions on both
    // sessions matched (DESIGN.md §6.2). After it published the list,
    // `serve` waits for the control session's version.
    let mut serve = Serve::start();
    serve.hello(&["true"]);
    let _control = serve.attach_without_version();
    serve.send_empty_list();
    serve.wait_for_published_list();
    serve.lose_data_session();
    serve.expect_death("the receiver disappeared");
}

#[test]
fn receiver_loss_while_the_input_waits_at_the_spool_limit_is_fatal() {
    // With a limit of 0, the data reader appends one INPUT payload to the
    // sender's input spool, then waits until the stdin writer takes it.
    // Before the receiver's control version, no command runs, so no stdin
    // writer exists, and the data reader waits for good: it reads nothing,
    // so it cannot see the loss of the data session. The session watcher
    // sees it instead (DESIGN.md §6.6). The control session stays open,
    // without its version, so only the data session shows the loss.
    let mut serve = Serve::start();
    serve.hello_with_input_limit(&["true"], 0);
    let _control = serve.attach_without_version();
    serve.send_empty_list();
    serve.recv_result();
    serve.send(INPUT, b"waits");
    serve.lose_data_session();
    serve.expect_death("the receiver disappeared");
}

#[test]
fn receiver_loss_after_end_ack_is_fatal_while_stderr_drains() {
    // A descendant keeps command stderr open, so the relay has not
    // finished when END_ACK arrives, and `serve` does not send DONE. The
    // receiver's disappearance, stdin EOF before DONE, then fails the run
    // and kills the command's process group, descendant included. The
    // descendant runs until this test process ends: it does not end by
    // itself, however slow the machine is, and a failed test does not
    // leave it behind.
    let mut serve = Serve::start();
    let script = format!(
        "( exec >/dev/null; exec tail --pid={} -f /dev/null ) & echo $!",
        std::process::id()
    );
    serve.hello(&["sh", "-c", &script]);
    let _control = serve.attach();
    serve.send_empty_list();
    serve.recv_result();
    let mut output = Vec::new();
    loop {
        match serve.recv() {
            (LIT, payload) => output.extend_from_slice(&payload),
            (END, _) => break,
            (kind, _) => panic!("unexpected frame type {kind}"),
        }
    }
    let descendant: i32 = String::from_utf8(output).unwrap().trim().parse().unwrap();
    // Nothing ends the descendant before the fatal cleanup, so no other
    // process can have its PID yet.
    let descendant = pidfd_open(Pid::from_raw(descendant).unwrap(), PidfdFlags::empty()).unwrap();
    serve.send(END_ACK, b"");
    serve.lose_data_session();
    serve.expect_death("the receiver disappeared after END_ACK");
    // A pidfd becomes readable when its process ends, also for a process
    // that is not a child of this one. A descendant that survived the fatal
    // cleanup hangs the test here.
    let mut fds = [PollFd::new(&descendant, PollFlags::IN)];
    retry_on_intr(|| poll(&mut fds, None)).unwrap();
}

#[test]
fn an_answer_decides_between_a_reference_and_literals() {
    // The receiver's list holds the weak hash of the command's only
    // window, so the scanner queries it on the control session. A yes
    // makes it a REF; a no makes the scanner rescan, and the window's
    // bytes go out as literals (DESIGN.md §6.3). With compression, each
    // direction of the control session is one zstd stream at the level of
    // the data session, and no frame ends it: the closure of the session
    // ends it, before DONE (DESIGN.md §6.2 and §6.9). The exit of the
    // command stops the input, which INPUT_STOP tells before END, between
    // any two frames of the control session (DESIGN.md §6.6).
    let block: Vec<u8> = (0..4096u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let list = encode_list(&[weak_hash(&block)]);
    for level in [0, 3] {
        for found in [true, false] {
            let mut serve = Serve::start();
            let file = serve.home.path().join("block");
            std::fs::write(&file, &block).unwrap();
            serve.hello_with(4096, &list, &["cat", file.to_str().unwrap()], level);
            let mut control = serve.control_session(&list, level);
            let digest = blake3::hash(&block).as_bytes().to_vec();
            assert_eq!(control.query(), (0, digest), "the window at offset 0");
            control.answer(&[(0, found)]);

            let frames = serve.recv_through_end(level);
            // INPUT_STOP before END_ACK, and only then the closure of the
            // session, which follows END_ACK.
            control.input_stop();
            serve.end_ack();
            assert_eq!(frames[0], (CACHE_RESULT, Vec::new()));
            let mut literal = Vec::new();
            let mut references = 0;
            for (kind, payload) in &frames[1..frames.len() - 1] {
                match *kind {
                    LIT => literal.extend_from_slice(payload),
                    REF => {
                        assert!(payload.is_empty());
                        references += 1;
                    }
                    kind => panic!("unexpected frame type {kind}"),
                }
            }
            if found {
                assert_eq!((references, literal.len()), (1, 0));
            } else {
                assert_eq!(references, 0);
                assert_eq!(literal, block);
            }
            let error = control.next().unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
            serve.expect_success();
        }
    }
}

#[test]
fn obsolete_queries_in_flight_do_not_fail_the_normal_end() {
    obsolete_queries_in_flight(0);
}

#[test]
fn a_compressed_block_in_flight_does_not_fail_the_normal_end() {
    obsolete_queries_in_flight(3);
}

/// Obsolete queries do not delay completion (DESIGN.md §6.3). Here they
/// are the last queries, and the receiver reads none of them, so the
/// control writer is still blocked on a full socket when the normal end
/// closes it. That failed write is expected closure (DESIGN.md §6.9),
/// also when compression at a `level` other than 0 leaves a zstd block
/// half written. On one CPU, the woken writer runs before the main thread
/// can exit, so a writer that took the closure as a failure would win.
///
/// With 64-byte windows, the first scan queries 0 and the chain
/// w_k = 74 + 64k. Once 0 is rejected, the rescan queries 20 and the
/// chain q_k = 92 + 64k, which jumps over the w chain. Once 20 is
/// rejected, the next rescan follows the w chain, answered yes, and jumps
/// over the q chain: every q query is obsolete.
fn obsolete_queries_in_flight(level: i32) {
    const B: u64 = 64;
    // A query keeps its 32-byte digest also when compressed, so the q
    // chain takes at least three send buffers, twice what the socket
    // holds, at any level.
    let chain = (3 * send_buffer() as u64 / 32).max(8000);
    let w: Vec<u64> = (0..=chain).map(|k| 74 + B * k).collect();
    let data = common::random((138 + B * chain + 10) as usize, 21);
    let window = |offset: u64| &data[offset as usize..(offset + B) as usize];
    let q = (0..chain).map(|k| 92 + B * k);
    let mut values: Vec<u64> = [0, 20]
        .into_iter()
        .chain(w.iter().copied())
        .chain(q)
        .map(|offset| weak_hash(window(offset)))
        .collect();
    values.sort_unstable();
    values.dedup();
    let list = encode_list(&values);

    let mut serve = on_one_cpu(Serve::start);
    // The wait for the blocked writer needs ptrace access to `serve`.
    if let Err(e) = std::fs::read_to_string(format!("/proc/{}/syscall", serve.child.id())) {
        eprintln!("skipped: cannot see the system calls of serve: {e}");
        return;
    }
    let file = serve.home.path().join("output");
    std::fs::write(&file, &data).unwrap();
    serve.hello_with(B as u32, &list, &["cat", file.to_str().unwrap()], level);
    let mut control = serve.control_session(&list, level);
    assert_eq!(control.query().0, 0);
    for &offset in &w {
        assert_eq!(control.query().0, offset);
    }
    let mut answers: Vec<(u64, bool)> = w.iter().map(|&offset| (offset, true)).collect();
    answers.push((0, false));
    control.answer(&answers);
    assert_eq!(control.query().0, 20);
    // The rescan queues the q chain, and nothing reads it: once the
    // control writer sleeps in a write, it stays there until the normal
    // end closes the socket.
    wait_until_blocked_in_send(serve.child.id(), "control-writer");
    control.answer(&[(20, false)]);

    let frames = serve.frames_through_done(level);
    assert_eq!(frames[0], (CACHE_RESULT, Vec::new()));
    let mut output = Vec::new();
    for (kind, payload) in &frames[1..frames.len() - 1] {
        match *kind {
            LIT => output.extend_from_slice(payload),
            REF => output.extend_from_slice(window(output.len() as u64)),
            kind => panic!("unexpected frame type {kind}"),
        }
    }
    assert!(output == data, "the output differs");

    // The closure cut the writer's stream: only the start of the q chain
    // arrived, with or without the INPUT_STOP of the command's exit, which
    // the writer queued at any point. Compressed, the stream ends inside
    // its zstd frame.
    let mut arrived = 0;
    let error = loop {
        match control.next() {
            Ok((kind, query)) => {
                assert_eq!(kind, QUERY);
                assert_eq!(query[..8], (92 + B * arrived).to_le_bytes());
                arrived += 1;
            }
            Err(e) => break e,
        }
    };
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(arrived < chain, "all {chain} q queries arrived");
    serve.expect_success();
}

#[test]
fn compressed_frames_form_one_zstd_frame_that_ends_with_end() {
    // After HELLO, the data frames share one zstd frame with a checksum,
    // which ends right after END. DONE follows the frame, outside it, and
    // nothing follows DONE (DESIGN.md §6.2).
    let mut serve = Serve::start();
    serve.hello_with(4096, &empty_list(), &["printf", "%s", "compressed"], 3);
    let _control = serve.attach();
    serve.send_empty_list();
    let Serve {
        data_in, data_out, ..
    } = &mut serve;
    let mut decoder = zstd::stream::read::Decoder::with_buffer(BufReader::new(data_out))
        .unwrap()
        .single_frame();
    assert_eq!(recv_frame(&mut decoder), (CACHE_RESULT, Vec::new()));
    assert_eq!(recv_frame(&mut decoder), (LIT, b"compressed".to_vec()));
    assert_eq!(recv_frame(&mut decoder).0, END);
    assert_eq!(decoder.read(&mut [0]).unwrap(), 0, "the frame goes on");
    let mut transport = decoder.finish();
    data_in
        .as_mut()
        .unwrap()
        .write_all(&frame(END_ACK, b""))
        .unwrap();
    assert_eq!(recv_frame(&mut transport), (DONE, Vec::new()));
    *data_in = None;
    let mut rest = Vec::new();
    transport.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty(), "bytes after DONE");
    serve.expect_success();
}

#[test]
fn the_input_reaches_the_command() {
    // `serve` flushes CACHE_RESULT, and the receiver sends the input after
    // it: the command, `cat`, writes no output before it reads the input.
    // The payloads of the INPUT frames are pieces of the input stream: at
    // level 3, one zstd frame from an independent encoder, and at level 0,
    // the input itself. INPUT_END ends the input, so the command reads EOF
    // after it, and the run ends normally (DESIGN.md §6.2 and §6.6).
    let input = b"the input of the command\n".repeat(10_000);
    for level in [0, 3] {
        let mut serve = Serve::start();
        serve.hello_with(4096, &empty_list(), &["cat"], level);
        let _control = serve.attach();
        serve.send_empty_list();
        let stream = match level {
            0 => input.clone(),
            level => zstd::stream::encode_all(&input[..], level).unwrap(),
        };
        let mut frames: Vec<u8> = stream
            .chunks(1000)
            .flat_map(|piece| frame(INPUT, piece))
            .collect();
        frames.extend_from_slice(&frame(INPUT_END, b""));
        let Serve {
            data_in, data_out, ..
        } = &mut serve;
        let mut transport = BufReader::new(data_out);
        let mut output = Vec::new();
        {
            let mut stream: Box<dyn Read> = match level {
                0 => Box::new(&mut transport),
                _ => Box::new(
                    zstd::stream::read::Decoder::with_buffer(&mut transport)
                        .unwrap()
                        .single_frame(),
                ),
            };
            assert_eq!(recv_frame(&mut stream), (CACHE_RESULT, Vec::new()));
            data_in.as_mut().unwrap().write_all(&frames).unwrap();
            loop {
                match recv_frame(&mut stream) {
                    (LIT, payload) => output.extend_from_slice(&payload),
                    // Status 0, without error text.
                    (END, payload) => break assert_eq!(payload, [0; 8]),
                    (kind, _) => panic!("unexpected frame type {kind}"),
                }
            }
            if level != 0 {
                assert_eq!(stream.read(&mut [0]).unwrap(), 0, "the frame goes on");
            }
        }
        assert!(output == input, "level {level}: the output differs");
        data_in
            .as_mut()
            .unwrap()
            .write_all(&frame(END_ACK, b""))
            .unwrap();
        assert_eq!(recv_frame(&mut transport), (DONE, Vec::new()));
        *data_in = None;
        let mut rest = Vec::new();
        transport.read_to_end(&mut rest).unwrap();
        assert!(rest.is_empty(), "bytes after DONE");
        serve.expect_success();
    }
}

#[test]
fn a_command_that_closes_its_stdin_stops_the_input_at_once() {
    // The command closes its stdin, and runs on until this test ends it.
    // The first write of the input then fails with EPIPE, and the input
    // has no use any more: `serve` sends INPUT_STOP while the command
    // still runs, and discards the INPUT frames that still arrive
    // (DESIGN.md §6.6). A `serve` that stopped the input only at the
    // command's exit would hang this test, which ends the command only
    // after INPUT_STOP.
    let mut serve = Serve::start();
    let (closed, go) = (
        serve.home.path().join("closed"),
        serve.home.path().join("go"),
    );
    for fifo in [&closed, &go] {
        mkfifoat(CWD, fifo, Mode::RUSR | Mode::WUSR).unwrap();
    }
    let script = format!(
        "exec <&-; echo closed >'{}'; read go <'{}'",
        closed.display(),
        go.display()
    );
    serve.hello(&["sh", "-c", &script]);
    let mut control = serve.control_session(&empty_list(), 0);
    serve.recv_result();
    // The open waits for the command to open the FIFO, after it closed
    // its stdin.
    let mut message = String::new();
    std::fs::File::open(&closed)
        .unwrap()
        .read_to_string(&mut message)
        .unwrap();
    assert_eq!(message, "closed\n");
    serve.send(INPUT, b"no process reads this");
    control.input_stop();
    serve.send(INPUT, b"discarded");
    std::fs::write(&go, b"go\n").unwrap();
    // Status 0, without error text.
    assert_eq!(serve.frames_through_done(0), [(END, vec![0; 8])]);
    serve.expect_success();
}

#[test]
fn the_exit_of_the_command_stops_the_input_while_stdout_stays_open() {
    // The command exits at once, but a descendant holds its stdout until
    // this test lets it end. The input has no use any more once the
    // command has exited, so `serve` stops it then, and sends INPUT_STOP,
    // while the scan still waits for the EOF of stdout (DESIGN.md §6.6). A
    // `serve` that stopped the input only at that EOF would hang this
    // test, which lets the descendant end only after INPUT_STOP.
    let mut serve = Serve::start();
    let go = serve.home.path().join("go");
    mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!("( read go <'{}' ) & exit 0", go.display());
    serve.hello(&["sh", "-c", &script]);
    let mut control = serve.control_session(&empty_list(), 0);
    serve.recv_result();
    control.input_stop();
    std::fs::write(&go, b"go\n").unwrap();
    // Status 0, without error text.
    assert_eq!(serve.frames_through_done(0), [(END, vec![0; 8])]);
    serve.expect_success();
}

#[test]
fn end_does_not_wait_for_a_descendant_that_holds_the_command_stdin() {
    // A descendant of the command holds the command's stdin and reads
    // nothing, so the stdin writer waits in a write once the pipe is full.
    // When the command exits, END still goes out at once: `serve` joins
    // the stdin writer only after END_ACK, as it joins the stderr relay
    // (DESIGN.md §6.9). Then the end waits for the descendant: once this
    // test kills it, the write fails with EPIPE, and DONE follows. The
    // descendant runs until this test process ends, so a `serve` that
    // waited for it before END would hang the test.
    let mut serve = Serve::start();
    // The wait for the blocked stdin writer needs ptrace access to `serve`.
    if let Err(e) = std::fs::read_to_string(format!("/proc/{}/syscall", serve.child.id())) {
        eprintln!("skipped: cannot see the system calls of serve: {e}");
        return;
    }
    let go = serve.home.path().join("go");
    mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!(
        "exec 3<&0; ( exec <&3 3<&- >/dev/null 2>&1; exec tail --pid={} -f /dev/null ) & \
         echo $!; exec 3<&-; read go <'{}'",
        std::process::id(),
        go.display()
    );
    serve.hello(&["sh", "-c", &script]);
    let _control = serve.attach();
    serve.send_empty_list();
    serve.recv_result();
    // 4 MiB, more than a pipe holds by default: the sender's input spool
    // takes them all, and the stdin writer fills the command's stdin.
    let input = vec![b'x'; 64 << 10];
    for _ in 0..64 {
        serve.send(INPUT, &input);
    }
    wait_until_asleep_in(
        serve.child.id(),
        "stdin-writer",
        blocking_pipe_write_syscall(),
    );
    std::fs::write(&go, b"go\n").unwrap();
    let mut output = Vec::new();
    loop {
        match serve.recv() {
            (LIT, payload) => output.extend_from_slice(&payload),
            // Status 0, without error text.
            (END, payload) => break assert_eq!(payload, [0; 8]),
            (kind, _) => panic!("unexpected frame type {kind}"),
        }
    }
    serve.send(END_ACK, b"");
    // Nothing ends the descendant before this kill, so no other process
    // can have its PID yet.
    let descendant: i32 = String::from_utf8(output).unwrap().trim().parse().unwrap();
    kill_process(Pid::from_raw(descendant).unwrap(), Signal::KILL).unwrap();
    assert_eq!(serve.recv(), (DONE, Vec::new()));
    serve.expect_success();
}

#[test]
fn stdin_eof_after_done_is_the_normal_end() {
    // `serve` sends DONE once it needs neither session: after END_ACK, and
    // after it shut down the control socket. Then it waits, with every
    // thread asleep, until its data reader reads the stdin EOF that the
    // receiver causes after DONE. It joins that thread and exits with
    // status 0 (DESIGN.md §6.9).
    let mut serve = Serve::start();
    serve.hello(&["printf", "%s", "output"]);
    let mut control = serve.attach();
    serve.send_empty_list();
    serve.recv_result();
    assert_eq!(serve.recv(), (LIT, b"output".to_vec()));
    assert_eq!(serve.recv().0, END);
    // After the version and the features, the INPUT_STOP of the command's
    // exit, which comes before END, and before the closure of the session
    // that END_ACK allows (DESIGN.md §6.6).
    let expected = [&version_and_features(FEATURES)[..], &frame(INPUT_STOP, b"")].concat();
    let mut stopped = vec![0; expected.len()];
    control.read_exact(&mut stopped).unwrap();
    assert_eq!(stopped, expected);
    serve.send(END_ACK, b"");
    assert_eq!(serve.recv(), (DONE, Vec::new()));
    let mut received = Vec::new();
    control.read_to_end(&mut received).unwrap();
    assert!(received.is_empty(), "the control session after INPUT_STOP");
    wait_until_asleep(serve.child.id());
    serve.expect_success();
}

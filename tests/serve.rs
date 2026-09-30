//! `hashsync serve` driven directly through its standard streams and its
//! rendezvous socket, the way the receiver and `attach` drive it. The
//! receiver's disappearance is fatal at every stage before DONE
//! (DESIGN.md §6.7): during rendezvous, while `serve` waits for the
//! control session, and while command stderr still drains after END_ACK.
//! After DONE, stdin EOF is the normal end. Queries and answers travel on
//! the control session (DESIGN.md §6.2); obsolete queries still unsent at
//! the normal end do not fail it, also inside a compressed block. After
//! HELLO, the data frames of `serve` through END form one zstd frame, and
//! DONE follows it; after the protocol version, each direction of the
//! control session is one zstd stream that no frame ends.

mod common;

use std::io::{BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::time::Duration;

use common::{
    ANSWER, CACHE_RESULT, DONE, END, END_ACK, HASHLIST, HELLO, LIT, PROTOCOL_VERSION, QUERY, REF,
    frame, proc_state, recv_frame, try_recv_frame, version,
};
use rustix::event::{PollFd, PollFlags, poll};
use rustix::io::retry_on_intr;
use rustix::process::{Pid, PidfdFlags, pidfd_open};
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
/// hash and zstd compression at `level`, 0 for none.
fn hello_payload(block_size: u32, list: &[u8], command: &[&str], level: i32) -> Vec<u8> {
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
/// `sendto`, `send`, or `socketcall`, by architecture and C library. A
/// thread of this process fills a socket that nothing reads, and a process
/// can always read the `syscall` files of its own threads.
fn blocking_send_syscall() -> u64 {
    let (mut full, reader) = UnixStream::pair().unwrap();
    // More than the socket holds. The thread has its bytes before it tells
    // its ID, so the write is the only call that it can then sleep in.
    let bytes = vec![0; 4 * send_buffer()];
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
/// there while no one reads the socket. Reading its `syscall` file needs
/// the ptrace access that a parent has. Fail when the process has ended.
fn wait_until_blocked_in_send(pid: u32, name: &str) {
    let send = blocking_send_syscall();
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
                Ok(nr) if nr == Some(send) => return,
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

/// This side of the control session after both versions, as the
/// receiver's: each direction as plain frames at level 0, else as one zstd
/// stream, which no frame ends (DESIGN.md §6.2).
struct Control {
    queries: Box<dyn Read>,
    answers: Box<dyn Write>,
}

impl Control {
    fn new(socket: &UnixStream, level: i32) -> Self {
        let (read, write) = (socket.try_clone().unwrap(), socket.try_clone().unwrap());
        if level == 0 {
            return Control {
                queries: Box::new(read),
                answers: Box::new(write),
            };
        }
        Control {
            queries: Box::new(zstd::stream::read::Decoder::new(read).unwrap()),
            answers: Box::new(zstd::stream::write::Encoder::new(write, level).unwrap()),
        }
    }

    /// The next QUERY: its offset and its digest.
    fn query(&mut self) -> (u64, Vec<u8>) {
        let (kind, mut payload) = recv_frame(&mut self.queries);
        assert_eq!(kind, QUERY);
        let digest = payload.split_off(8);
        (u64::from_le_bytes(payload.try_into().unwrap()), digest)
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

    fn recv_version(&mut self) -> [u8; 4] {
        let mut version = [0u8; 4];
        self.data_out.read_exact(&mut version).unwrap();
        version
    }

    fn recv(&mut self) -> (u8, Vec<u8>) {
        recv_frame(&mut self.data_out)
    }

    /// The versions, then the data HELLOs: the receiver's, for the empty
    /// list and 4 KiB blocks, then the sender's reply.
    fn hello(&mut self, command: &[&str]) {
        self.hello_with(4096, &empty_list(), command, 0);
    }

    fn hello_with(&mut self, block_size: u32, list: &[u8], command: &[&str], level: i32) {
        self.send_raw(&version());
        self.send(HELLO, &hello_payload(block_size, list, command, level));
        assert_eq!(self.recv_version(), version());
        assert_eq!(self.recv().0, HELLO);
    }

    /// Connect as `attach` does, through the socket directory's
    /// descriptor: a temporary path can exceed the `sun_path` limit.
    /// `serve` listens before it sends its version.
    fn attach(&self) -> UnixStream {
        let dir = std::fs::File::open(self.socket().parent().unwrap()).unwrap();
        let address = format!("/proc/self/fd/{}/{TOKEN}.sock", dir.as_raw_fd());
        UnixStream::connect(address).unwrap()
    }

    /// Attach, send `list`, and exchange the versions of the control
    /// session, whose later frames use compression at `level`.
    fn control_session(&mut self, list: &[u8], level: i32) -> Control {
        let mut socket = self.attach();
        socket.write_all(&version()).unwrap();
        self.send_list(list);
        let mut theirs = [0u8; 4];
        socket.read_exact(&mut theirs).unwrap();
        assert_eq!(theirs, version());
        Control::new(&socket, level)
    }

    /// The data frames after the list, from CACHE_RESULT through END,
    /// then the normal end of the data session: END_ACK, and DONE
    /// (DESIGN.md §6.7). At a `level` other than 0, the frames through END
    /// form one zstd frame, and DONE follows it, outside it
    /// (DESIGN.md §6.2).
    fn frames_through_done(&mut self, level: i32) -> Vec<(u8, Vec<u8>)> {
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
        let data_in = self.data_in.as_mut().unwrap();
        data_in.write_all(&frame(END_ACK, b"")).unwrap();
        assert_eq!(recv_frame(&mut transport), (DONE, Vec::new()));
        assert!(transport.buffer().is_empty(), "bytes after DONE");
        frames
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
    /// payload. `serve` does not flush it: it can arrive with the
    /// command's output (DESIGN.md §6.2).
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
fn a_different_protocol_version_is_fatal_before_any_frame() {
    // `serve` sends its version without waiting for the receiver's and
    // checks the receiver's before any frame (DESIGN.md §6.2).
    let mut serve = Serve::start();
    assert_eq!(serve.recv_version(), version());
    let other = PROTOCOL_VERSION + 1;
    serve.send_raw(&other.to_le_bytes());
    serve.expect_death(&format!(
        "the receiver speaks protocol version {other}, \
         this hashsync {} speaks {PROTOCOL_VERSION}",
        env!("CARGO_PKG_VERSION")
    ));
}

#[test]
fn an_unknown_strong_hash_is_rejected_before_the_target() {
    // The target's length depends on the strong hash, so `serve` rejects
    // an unknown one with `die` before it reads the target, as it would
    // for a newer receiver's algorithm (DESIGN.md §6.2 and §11). The
    // payload ends after the algorithm's name: a read of any later field
    // would panic on truncation instead.
    let mut serve = Serve::start();
    serve.send_raw(&version());
    serve.send(HELLO, &hello_prefix(4096, b"future-hash"));
    serve.expect_death("incompatible receiver: strong hash future-hash");
}

#[test]
fn an_unsupported_compression_level_is_rejected() {
    // The receiver's HELLO sets the run's parameters; the sender ends the
    // run before command startup if it does not support them
    // (DESIGN.md §6.2).
    let mut serve = Serve::start();
    serve.send_raw(&version());
    serve.send(HELLO, &hello_payload(4096, &empty_list(), &["true"], 20));
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
    control.write_all(&version()).unwrap();
    // Read `serve`'s version, so that closing sends EOF, not a reset.
    let mut theirs = [0u8; 4];
    control.read_exact(&mut theirs).unwrap();
    assert_eq!(theirs, version());
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
    let _control = serve.attach();
    serve.send_empty_list();
    serve.wait_for_published_list();
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
    let mut control = serve.attach();
    control.write_all(&version()).unwrap();
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
    // ends it, before DONE (DESIGN.md §6.2 and §6.7).
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

            let frames = serve.frames_through_done(level);
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
            let error = try_recv_frame(&mut control.queries).unwrap_err();
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
/// closes it. That failed write is expected closure (DESIGN.md §6.7),
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
    // arrived. Compressed, the stream ends inside its zstd frame.
    let mut arrived = 0;
    let error = loop {
        match try_recv_frame(&mut control.queries) {
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
    let mut control = serve.attach();
    control.write_all(&version()).unwrap();
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
fn stdin_eof_after_done_is_the_normal_end() {
    // `serve` sends DONE once it needs neither session: after END_ACK, and
    // after it shut down the control socket. Then it waits, with every
    // thread asleep, until its data reader reads the stdin EOF that the
    // receiver causes after DONE. It joins that thread and exits with
    // status 0 (DESIGN.md §6.7).
    let mut serve = Serve::start();
    serve.hello(&["printf", "%s", "output"]);
    let mut control = serve.attach();
    control.write_all(&version()).unwrap();
    serve.send_empty_list();
    serve.recv_result();
    assert_eq!(serve.recv(), (LIT, b"output".to_vec()));
    assert_eq!(serve.recv().0, END);
    serve.send(END_ACK, b"");
    assert_eq!(serve.recv(), (DONE, Vec::new()));
    let mut received = Vec::new();
    control.read_to_end(&mut received).unwrap();
    assert_eq!(received, version(), "the control session after the version");
    wait_until_asleep(serve.child.id());
    serve.expect_success();
}

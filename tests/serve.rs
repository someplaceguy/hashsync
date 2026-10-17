//! `hashsync serve` driven directly through its standard streams and its
//! rendezvous socket, the way the client and `attach` drive it. In a pull,
//! `serve` sends the command's stdout, the backward stream, deduplicated,
//! and writes the forward stream to the command's stdin. In a push, it
//! receives the forward stream deduplicated against its own index, which
//! it updates first, and sends the backward stream plain. The client's
//! disappearance is fatal at every stage before END (DESIGN.md §6.9):
//! during rendezvous, during the update of the index of a push, while
//! `serve` waits for the control session, while the data reader waits at
//! the limit of the forward stream's spool, and while command stderr still
//! drains. After END, stdin EOF is the normal end. Queries, answers, and
//! STOP travel on the control session (DESIGN.md §6.2); obsolete queries
//! still unsent at the normal end do not fail it, also inside a compressed
//! block. The stream frames of a stream form one zstd frame, whose pieces
//! travel in CHUNK frames; after the features, each direction of the
//! control session is one zstd stream that no frame ends.

mod common;

use std::io::{BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::time::Duration;

use common::{
    ANSWER, CACHE_RESULT, CHUNK, END, FEATURES, HASHLIST, HELLO, LIT, PROTOCOL_VERSION, QUERY, REF,
    REQUEST, RLE, STOP, STREAM_END, frame, ofd_lock, proc_state, recv_frame, shared_lock,
    toml_literal, try_recv_frame, version_and_features, wait_for_lock_wait,
};
use rustix::event::{PollFd, PollFlags, Timespec, poll};
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

/// The start of the client's HELLO payload, as the receiver of the
/// backward stream, through the name of its strong hash: the block size,
/// then the weak and strong hashes.
fn hello_prefix(block_size: u32, strong: &[u8]) -> Vec<u8> {
    let mut hello = block_size.to_le_bytes().to_vec();
    string(&mut hello, WEAK_HASH_ID);
    string(&mut hello, strong);
    hello
}

/// The deduplicated stream, as REQUEST encodes it (DESIGN.md §6.2): the
/// forward stream in a push, the backward stream in a pull.
const FORWARD: u8 = 1;
const BACKWARD: u8 = 2;

/// The client's REQUEST payload for `command`, run without a shell, as `-s`
/// sends it, with the `deduplicated` stream, zstd compression at `level`,
/// 0 for none, spools that hold at most `spool_limit` bytes, and the
/// command's stdout in `delivery`. The other settings have their defaults
/// (DESIGN.md §6.2).
fn request_payload(
    command: &[&str],
    deduplicated: u8,
    level: i32,
    spool_limit: u64,
    delivery: Delivery,
) -> Vec<u8> {
    let mut request = vec![1]; // the command's words, without a shell
    request.extend_from_slice(&(command.len() as u32).to_le_bytes());
    for arg in command {
        string(&mut request, arg.as_bytes());
    }
    request.push(deduplicated);
    request.push(0); // no_update
    request.push(1); // sender_cache
    request.extend_from_slice(&level.to_le_bytes()); // compression level
    request.push(1); // compression_long
    request.extend_from_slice(&0u32.to_le_bytes()); // compression threads
    request.extend_from_slice(&0u32.to_le_bytes()); // probe threads
    request.extend_from_slice(&0u64.to_le_bytes()); // lookahead: the default
    request.extend_from_slice(&(128u64 << 20).to_le_bytes()); // negative cache
    request.extend_from_slice(&spool_limit.to_le_bytes()); // spool limit
    request.push(0); // stdin delivery: eager
    request.push(delivery as u8); // stdout delivery
    request.push(0); // quiet
    request.push(1); // human_readable
    request
}

/// The client's HELLO payload for `list`, with BLAKE3 as its strong hash.
fn hello_payload(block_size: u32, list: &[u8]) -> Vec<u8> {
    let mut hello = hello_prefix(block_size, b"blake3");
    hello.extend_from_slice(blake3::hash(list).as_bytes()); // target
    hello.extend_from_slice(&[0xaa; 16]); // account ID
    hello.extend_from_slice(&[0xbb; 16]); // index ID
    hello
}

/// The client's forward stream with `input`: LIT frames of at most 4 KiB,
/// one zstd frame at `level`, unless it is 0, in CHUNK frames of at most
/// 1000 bytes, then STREAM_END (DESIGN.md §6.2).
fn forward_stream(input: &[u8], level: i32) -> Vec<u8> {
    let frames: Vec<(u8, &[u8])> = input.chunks(4096).map(|piece| (LIT, piece)).collect();
    stream_of(&frames, level)
}

/// A stream of the stream frames `frames`, as `forward_stream` sends them.
fn stream_of(frames: &[(u8, &[u8])], level: i32) -> Vec<u8> {
    let frames: Vec<u8> = frames
        .iter()
        .flat_map(|&(kind, payload)| frame(kind, payload))
        .collect();
    // As the client's data writer makes it: with its checksum, which marks
    // the normal end of the stream, and with the long-distance matching that
    // REQUEST asks for (DESIGN.md §6.2 and §6.6).
    let stream = match level {
        0 => frames,
        level => {
            let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), level).unwrap();
            encoder.include_checksum(true).unwrap();
            encoder.long_distance_matching(true).unwrap();
            encoder.window_log(27).unwrap();
            encoder.write_all(&frames).unwrap();
            encoder.finish().unwrap()
        }
    };
    let mut session: Vec<u8> = stream
        .chunks(1000)
        .flat_map(|piece| frame(CHUNK, piece))
        .collect();
    session.extend_from_slice(&frame(STREAM_END, b""));
    session
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

/// The system call in which a thread of this process sleeps while it runs
/// `wait`, which must wait until the drop of the returned value of `make`.
/// The thread tells its ID before the wait, so the wait is the only call
/// that it can then sleep in. A process can always read the `syscall` files
/// of its own threads.
fn sleeping_syscall_of<T: Send + 'static, D>(
    make: impl FnOnce() -> (T, D),
    wait: impl FnOnce(T) + Send + 'static,
) -> u64 {
    let (waited, ends) = make();
    let (tid_tx, tid_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        tid_tx.send(rustix::thread::gettid()).unwrap();
        wait(waited);
    });
    let tid = tid_rx.recv().unwrap().as_raw_nonzero();
    let path = PathBuf::from(format!("/proc/self/task/{tid}/syscall"));
    let nr = loop {
        if let Some(nr) = sleeping_syscall(&path).unwrap() {
            break nr;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    drop(ends);
    waiter.join().unwrap();
    nr
}

/// The system call in which `UnixStream::write` sleeps on a full socket:
/// `sendto`, `send`, or `socketcall`, by architecture and C library. The
/// write fails once the other end closes.
fn blocking_send_syscall() -> u64 {
    let size = 4 * send_buffer();
    sleeping_syscall_of(
        || UnixStream::pair().unwrap(),
        move |mut full| {
            full.write_all(&vec![0; size]).unwrap_err();
        },
    )
}

/// The system call in which `poll` sleeps, `ppoll` or `poll`, by its number
/// on this architecture: the wait of a pipe that nothing writes, until the
/// write end closes.
fn poll_syscall() -> u64 {
    sleeping_syscall_of(
        || {
            let (reader, writer) = std::io::pipe().unwrap();
            (reader, writer)
        },
        |reader| {
            let mut fds = [PollFd::new(&reader, PollFlags::IN)];
            retry_on_intr(|| poll(&mut fds, None)).unwrap();
        },
    )
}

/// Wait until the thread `name` of the process `pid` sleeps in the system
/// call of a write to a full socket, polling with no deadline. It stays
/// there while no one reads the socket.
fn wait_until_blocked_in_send(pid: u32, name: &str) {
    wait_until_asleep_in(pid, name, blocking_send_syscall());
}

/// Wait until the process `pid` has no thread named `name` any more,
/// polling with no deadline. Fail when the process has ended.
fn wait_until_gone(pid: u32, name: &str) {
    loop {
        assert_ne!(
            proc_state(format!("/proc/{pid}")),
            Some('Z'),
            "the process ended"
        );
        // A thread that ends meanwhile has no name.
        let running = std::fs::read_dir(format!("/proc/{pid}/task"))
            .unwrap()
            .filter_map(|task| std::fs::read_to_string(task.ok()?.path().join("comm")).ok())
            .any(|comm| comm.trim_end() == name);
        if !running {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
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

/// The bytes of the stream that `serve` sends on the data session, as they
/// arrive: the payloads of its CHUNK frames, until STREAM_END, which reads
/// as EOF.
struct Chunks<'a, R: Read> {
    session: &'a mut R,
    chunk: Vec<u8>,
    at: usize,
    ended: bool,
}

impl<'a, R: Read> Chunks<'a, R> {
    fn new(session: &'a mut R) -> Self {
        Chunks {
            session,
            chunk: Vec::new(),
            at: 0,
            ended: false,
        }
    }
}

impl<R: Read> Read for Chunks<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.at == self.chunk.len() {
            if self.ended {
                return Ok(0);
            }
            let (kind, payload) = recv_frame(self.session);
            match kind {
                CHUNK => {
                    assert!(!payload.is_empty(), "an empty CHUNK");
                    (self.chunk, self.at) = (payload, 0);
                }
                STREAM_END => {
                    assert!(payload.is_empty(), "STREAM_END with a payload");
                    self.ended = true;
                }
                kind => panic!("frame type {kind} inside the stream"),
            }
        }
        let n = buf.len().min(self.chunk.len() - self.at);
        buf[..n].copy_from_slice(&self.chunk[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// The stream frames of a stream that `serve` sends, from its CHUNK frames
/// through STREAM_END, as they arrive, `next` frame by frame. At a `level`
/// other than 0, they form one zstd frame, which must end with the last
/// CHUNK (DESIGN.md §6.2).
enum StreamFrames<'a, R: Read> {
    Plain(Chunks<'a, R>),
    Compressed(zstd::stream::read::Decoder<'static, BufReader<Chunks<'a, R>>>),
}

impl<'a, R: Read> StreamFrames<'a, R> {
    fn new(session: &'a mut R, level: i32) -> Self {
        let chunks = Chunks::new(session);
        match level {
            0 => StreamFrames::Plain(chunks),
            _ => StreamFrames::Compressed(
                zstd::stream::read::Decoder::with_buffer(BufReader::new(chunks))
                    .unwrap()
                    .single_frame(),
            ),
        }
    }

    fn stream(&mut self) -> &mut dyn Read {
        match self {
            StreamFrames::Plain(chunks) => chunks,
            StreamFrames::Compressed(decoder) => decoder,
        }
    }

    /// The next stream frame; `None` at the end of the stream frames.
    fn next(&mut self) -> Option<(u8, Vec<u8>)> {
        let stream = self.stream();
        let mut header = [0u8; 5];
        if stream.read(&mut header[..1]).unwrap() == 0 {
            return None;
        }
        stream.read_exact(&mut header[1..]).unwrap();
        let length = u32::from_le_bytes(header[1..].try_into().unwrap());
        let mut payload = vec![0; length as usize];
        stream.read_exact(&mut payload).unwrap();
        Some((header[0], payload))
    }

    /// All the remaining frames, through STREAM_END. The zstd frame must
    /// end with the last CHUNK.
    fn rest(mut self) -> Vec<(u8, Vec<u8>)> {
        let mut frames = Vec::new();
        while let Some(frame) = self.next() {
            frames.push(frame);
        }
        if let StreamFrames::Compressed(decoder) = self {
            let mut more = Vec::new();
            decoder.finish().read_to_end(&mut more).unwrap();
            assert!(more.is_empty(), "bytes after the zstd frame");
        }
        frames
    }
}

/// This side of the control session after both versions and features, as
/// the client's: each direction as plain frames at level 0, else as one
/// zstd stream, which no frame ends (DESIGN.md §6.2). In a pull, `serve`
/// sends queries and STOP, and the client answers; in a push, the client
/// sends the queries, and `serve` answers and sends STOP.
struct Control {
    incoming: Box<dyn Read>,
    outgoing: Box<dyn Write>,
    /// STOP arrived. `serve` sends it once, at `EPIPE` or at the exit of
    /// the command, so it can come between any two queries (DESIGN.md
    /// §6.6).
    stopped: bool,
}

impl Control {
    fn new(socket: &UnixStream, level: i32) -> Self {
        let (read, write) = (socket.try_clone().unwrap(), socket.try_clone().unwrap());
        let (incoming, outgoing): (Box<dyn Read>, Box<dyn Write>) = if level == 0 {
            (Box::new(read), Box::new(write))
        } else {
            (
                Box::new(zstd::stream::read::Decoder::new(read).unwrap()),
                Box::new(zstd::stream::write::Encoder::new(write, level).unwrap()),
            )
        };
        Control {
            incoming,
            outgoing,
            stopped: false,
        }
    }

    /// The next frame from `serve` other than STOP, which this notes; else
    /// the error that ends the session first, `UnexpectedEof` at its
    /// closure.
    fn next(&mut self) -> std::io::Result<(u8, Vec<u8>)> {
        loop {
            let frame = try_recv_frame(&mut self.incoming)?;
            if frame.0 != STOP {
                return Ok(frame);
            }
            assert!(frame.1.is_empty(), "STOP with a payload");
            assert!(!self.stopped, "a second STOP");
            self.stopped = true;
        }
    }

    /// The next QUERY: its offset and its digest.
    fn query(&mut self) -> (u64, Vec<u8>) {
        let (kind, mut payload) = self.next().unwrap();
        assert_eq!(kind, QUERY);
        let digest = payload.split_off(8);
        (u64::from_le_bytes(payload.try_into().unwrap()), digest)
    }

    /// Wait for STOP, unless it arrived already; no other frame may come
    /// first. `serve` queues STOP before it sends END, and shuts the session
    /// down only after the EOF of the data session, so a test that waits
    /// here before it closes the data session always gets STOP (DESIGN.md
    /// §6.6).
    fn stop(&mut self) {
        if !self.stopped {
            assert_eq!(recv_frame(&mut self.incoming), (STOP, Vec::new()));
            self.stopped = true;
        }
    }

    /// ANSWER frames, flushed: `serve` waits for them.
    fn answer(&mut self, answers: &[(u64, bool)]) {
        for &(offset, found) in answers {
            let mut answer = offset.to_le_bytes().to_vec();
            answer.push(found.into());
            self.outgoing.write_all(&frame(ANSWER, &answer)).unwrap();
        }
        self.outgoing.flush().unwrap();
    }

    /// STOP, flushed: the client stops the backward stream once no process
    /// reads its stdout (DESIGN.md §6.6).
    fn send_stop(&mut self) {
        self.outgoing.write_all(&frame(STOP, b"")).unwrap();
        self.outgoing.flush().unwrap();
    }

    /// In a push, a QUERY for the window at `offset` with the strong hash
    /// `digest`, flushed: the client waits for its answer.
    fn send_query(&mut self, offset: u64, digest: &[u8]) {
        let query = [&offset.to_le_bytes()[..], digest].concat();
        self.outgoing.write_all(&frame(QUERY, &query)).unwrap();
        self.outgoing.flush().unwrap();
    }

    /// In a push, the next ANSWER: its offset, and whether `serve` has the
    /// block.
    fn recv_answer(&mut self) -> (u64, bool) {
        let (kind, payload) = self.next().unwrap();
        assert_eq!(kind, ANSWER);
        let (offset, found) = payload.split_at(8);
        assert!(found == [0] || found == [1], "{found:?}");
        (u64::from_le_bytes(offset.try_into().unwrap()), found == [1])
    }
}

/// The delivery of the command's stdout, as REQUEST encodes it (DESIGN.md
/// §6.2).
#[derive(Clone, Copy, Debug)]
enum Delivery {
    Eager = 0,
    Batch = 1,
}

struct Serve {
    child: Child,
    data_in: Option<ChildStdin>,
    data_out: BufReader<ChildStdout>,
    home: TempDir,
    /// The features that this side sends on both sessions, as the client:
    /// `FEATURES`, unless a test sets bits that `serve` does not know.
    features: u64,
    /// The delivery of the command's stdout that REQUEST asks for: eager,
    /// the default, unless a test needs batch delivery.
    delivery: Delivery,
    /// The spool limit that the REQUEST of a push asks for: 2 GiB, unless
    /// a test needs a spool that fills.
    spool_limit: u64,
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
            .env("XDG_STATE_HOME", home.path().join("state"))
            .env("XDG_RUNTIME_DIR", home.path().join("runtime"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        Serve {
            data_in: child.stdin.take(),
            data_out: BufReader::new(child.stdout.take().unwrap()),
            child,
            home,
            features: FEATURES,
            delivery: Delivery::Eager,
            spool_limit: 2 << 30,
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

    /// The versions and the features, REQUEST, then the data HELLOs: the
    /// client's, for the empty list and 4 KiB blocks, then `serve`'s reply,
    /// whose payload this returns.
    fn hello(&mut self, command: &[&str]) -> Vec<u8> {
        self.hello_with(4096, &empty_list(), command, 0)
    }

    fn hello_with(
        &mut self,
        block_size: u32,
        list: &[u8],
        command: &[&str],
        level: i32,
    ) -> Vec<u8> {
        let request = request_payload(command, BACKWARD, level, 2 << 30, self.delivery);
        self.send_hello(&request, &hello_payload(block_size, list))
    }

    /// The versions, the features, REQUEST and the data HELLOs, as `hello`
    /// exchanges them, with spools that hold at most `spool_limit` bytes.
    fn hello_with_spool_limit(&mut self, command: &[&str], spool_limit: u64) {
        let request = request_payload(command, BACKWARD, 0, spool_limit, self.delivery);
        self.send_hello(&request, &hello_payload(4096, &empty_list()));
    }

    /// Returns the payload of `serve`'s HELLO.
    fn send_hello(&mut self, request: &[u8], hello: &[u8]) -> Vec<u8> {
        self.send_version_and_features();
        self.send(REQUEST, request);
        self.send(HELLO, hello);
        assert_eq!(
            self.recv_version_and_features(),
            version_and_features(FEATURES)
        );
        let (kind, reply) = self.recv();
        assert_eq!(kind, HELLO);
        reply
    }

    /// Open the control session as the client does: connect as `hashsync
    /// attach` does, then send this side's version and features at once
    /// (DESIGN.md §6.2). The features are `self.features`, as on the data
    /// session.
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
    /// tests keep the control session open without the client's version.
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

    /// The full empty list, whose bytes follow HASHLIST directly.
    fn send_empty_list(&mut self) {
        self.send_list(&empty_list());
    }

    fn send_list(&mut self, list: &[u8]) {
        self.send(HASHLIST, &(list.len() as u64).to_le_bytes());
        self.send_raw(list);
    }

    /// CACHE_RESULT, the first data frame after the list, without a
    /// payload. `serve` flushes it: the client starts the forward stream
    /// only after it (DESIGN.md §6.2).
    fn recv_result(&mut self) {
        assert_eq!(self.recv(), (CACHE_RESULT, Vec::new()));
    }

    /// The backward stream, as it arrives: its stream frames, from its
    /// CHUNK frames through STREAM_END.
    fn stream(&mut self, level: i32) -> StreamFrames<'_, BufReader<ChildStdout>> {
        StreamFrames::new(&mut self.data_out, level)
    }

    /// END, which follows the backward stream.
    fn recv_end(&mut self) -> EndFrame {
        let (kind, payload) = self.recv();
        assert_eq!(kind, END);
        EndFrame::parse(&payload)
    }

    /// From CACHE_RESULT: the backward stream's frames through its end, and
    /// END.
    fn result_through_end(&mut self, level: i32) -> (Vec<(u8, Vec<u8>)>, EndFrame) {
        self.recv_result();
        self.stream_through_end(level)
    }

    /// After CACHE_RESULT: the backward stream's frames through its end,
    /// and END. `serve` sends nothing after END until the client closes the
    /// data session.
    fn stream_through_end(&mut self, level: i32) -> (Vec<(u8, Vec<u8>)>, EndFrame) {
        let frames = self.stream(level).rest();
        let end = self.recv_end();
        assert!(self.data_out.buffer().is_empty(), "bytes after END");
        (frames, end)
    }

    /// Wait until `serve` has published the list in its cache, which it
    /// does before it ends its exchange and sends CACHE_RESULT (DESIGN.md
    /// §5.6). No event tells the client when, so this polls the cache
    /// directory, with no deadline: a `serve` that neither publishes nor
    /// ends hangs the test, and a debugger can inspect it.
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

    /// The normal end after END: close stdin, as the client does. Nothing
    /// may follow END on stdout, and `serve` must exit with status 0.
    fn expect_success(mut self) {
        self.data_in = None;
        let mut rest = Vec::new();
        self.data_out.read_to_end(&mut rest).unwrap();
        assert!(rest.is_empty(), "bytes after END");
        let (status, stderr) = self.finish();
        assert_eq!(status.code(), Some(0), "stderr:\n{stderr}");
    }

    /// Write the configuration of `serve`, which the receiver of a push
    /// needs (DESIGN.md §7): blocks of 4 KiB, its index, and the root
    /// `tree` in its home, with `files`.
    fn configure_push(&self, files: &[(&str, &[u8])]) {
        let home = self.home.path();
        std::fs::create_dir(home.join("tree")).unwrap();
        for &(name, data) in files {
            std::fs::write(home.join("tree").join(name), data).unwrap();
        }
        let config = format!(
            "block_size = 4096\nindex_path = '{}'\n[[roots]]\npath = '{}'\n",
            toml_literal(self.index().as_os_str().as_bytes()),
            toml_literal(home.join("tree").as_os_str().as_bytes())
        );
        std::fs::create_dir_all(home.join("config/hashsync")).unwrap();
        std::fs::write(home.join("config/hashsync/config.toml"), config).unwrap();
    }

    /// The index of `serve` in a push.
    fn index(&self) -> PathBuf {
        self.home.path().join("index")
    }

    /// The versions, the features, and REQUEST for a push to `command`, at
    /// compression `level`.
    fn push_request(&mut self, command: &[&str], level: i32) {
        self.send_version_and_features();
        let request = request_payload(command, FORWARD, level, self.spool_limit, self.delivery);
        self.send(REQUEST, &request);
        assert_eq!(
            self.recv_version_and_features(),
            version_and_features(FEATURES)
        );
    }

    /// A push to `command`, at compression `level`, up to CACHE_RESULT:
    /// `serve` updates its index and sends its HELLO, as the receiver of
    /// the forward stream; the client attaches, and sends its own HELLO as
    /// the sender, with no listed list; `serve` sends the full target list;
    /// the client sends CACHE_RESULT (DESIGN.md §5.6). Returns the target
    /// list, and the control session.
    fn push(&mut self, command: &[&str], level: i32) -> (Vec<u8>, Control) {
        self.push_request(command, level);
        let (kind, hello) = self.recv();
        assert_eq!(kind, HELLO);
        assert_eq!(hello[..4], 4096u32.to_le_bytes(), "the block size");
        let mut socket = self.attach();
        let mut sender_hello = vec![0xaa; 16]; // account ID
        sender_hello.extend_from_slice(&0u32.to_le_bytes()); // no list
        self.send(HELLO, &sender_hello);
        let (kind, length) = self.recv();
        assert_eq!(kind, HASHLIST);
        let length = u64::from_le_bytes(length.try_into().unwrap());
        let mut list = vec![0; length as usize];
        self.data_out.read_exact(&mut list).unwrap();
        self.send(CACHE_RESULT, b"");
        let mut theirs = [0u8; 12];
        socket.read_exact(&mut theirs).unwrap();
        assert_eq!(theirs, version_and_features(FEATURES));
        (list, Control::new(&socket, level))
    }

    /// The client disappears: its end of the data session closes.
    fn lose_data_session(&mut self) {
        self.data_in = None;
    }

    /// Require a fatal exit with `message`, on a line marked as the
    /// server's (DESIGN.md §8), and the fatal cleanup of the rendezvous
    /// socket.
    fn expect_death(mut self, message: &str) {
        let (status, stderr) = self.finish();
        assert_eq!(status.code(), Some(1), "stderr:\n{stderr}");
        assert!(
            stderr
                .lines()
                .any(|line| line.starts_with("hashsync (server): ") && line.contains(message)),
            "stderr:\n{stderr}"
        );
        assert!(!self.socket().exists(), "the socket file must be removed");
    }
}

/// END, as the client reads it (DESIGN.md §6.2).
#[derive(Debug)]
struct EndFrame {
    /// The command's status, and the error text of a startup failure.
    outcome: (i32, Vec<u8>),
    /// The server's counts of the forward stream: the bytes that the
    /// command's stdin took, by kind of stream frame, the bytes of the REF
    /// and RLE frames, the CHUNK bytes that its decoder decoded, and the
    /// queries that its lookups answered no: false positives, and blocks
    /// not found.
    written: [u64; 4],
    frames: [u64; 2],
    decoded: u64,
    misses: [u64; 2],
    /// The server's counts of the backward stream: the bytes that its
    /// stdout reader read from the command's stdout, and whether it read
    /// that stdout to its EOF.
    read: (u64, bool),
}

impl EndFrame {
    fn parse(payload: &[u8]) -> Self {
        let status = i32::from_le_bytes(payload[..4].try_into().unwrap());
        let length = u32::from_le_bytes(payload[4..8].try_into().unwrap()) as usize;
        let (error, counts) = payload[8..].split_at(length);
        assert_eq!(counts.len(), 10 * 8 + 1, "END: {payload:?}");
        let count = |i: usize| u64::from_le_bytes(counts[8 * i..8 * i + 8].try_into().unwrap());
        let eof = match counts[10 * 8] {
            0 => false,
            1 => true,
            flag => panic!("END: the EOF flag is {flag}"),
        };
        EndFrame {
            outcome: (status, error.to_vec()),
            written: [count(0), count(1), count(2), count(3)],
            frames: [count(4), count(5)],
            decoded: count(6),
            misses: [count(7), count(8)],
            read: (count(9), eof),
        }
    }

    fn outcome(&self) -> (i32, &[u8]) {
        (self.outcome.0, &self.outcome.1)
    }
}

/// Status 0 in END, without error text.
const SUCCESS: (i32, &[u8]) = (0, b"");

#[test]
fn a_different_protocol_version_is_fatal_before_the_features() {
    // `serve` sends its version and its features without waiting for the
    // client's, and it checks the client's version before anything else:
    // this client sends no features (DESIGN.md §6.2).
    let mut serve = Serve::start();
    assert_eq!(
        serve.recv_version_and_features(),
        version_and_features(FEATURES)
    );
    let other = PROTOCOL_VERSION + 1;
    serve.send_raw(&other.to_le_bytes());
    serve.expect_death(&format!(
        "the client speaks protocol version {other}, \
         this hashsync {} speaks {PROTOCOL_VERSION}",
        env!("CARGO_PKG_VERSION")
    ));
}

#[test]
fn feature_bits_that_serve_does_not_know_are_ignored() {
    // A later client can send feature bits that this `serve` does not
    // know, on both sessions. `serve` ignores them, sends its own
    // features, and the run goes on as with none (DESIGN.md §6.2).
    let mut serve = Serve::start();
    serve.features = u64::MAX;
    serve.hello(&["printf", "%s", "output"]);
    let mut control = serve.control_session(&empty_list(), 0);
    let (frames, end) = serve.result_through_end(0);
    assert_eq!(frames, [(LIT, b"output".to_vec())]);
    assert_eq!(end.outcome(), SUCCESS);
    control.stop();
    serve.expect_success();
}

#[test]
fn serve_advertises_its_listed_lists_and_removes_only_listed_files() {
    // Before its HELLO, `serve` lists its directory for this client and
    // index, and advertises every listed list of the run's strong hash.
    // After it published the target, it removes the listed files except
    // the target: here an older list, a list of another strong hash, and
    // an abandoned temporary. A file that appears after the listing stays,
    // because a later run can own it (DESIGN.md §5.6).
    let hex = |bytes: &[u8]| -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() };
    let mut serve = Serve::start();
    let dir = serve.home.path().join(format!(
        "cache/hashsync/remote-hashlists/{}-{}",
        "aa".repeat(16),
        "bb".repeat(16)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let older = [7u8; 32];
    let listed = [
        format!("blake3-{}.cache", hex(&older)),
        format!("sha256-{}.cache", hex(&[8; 32])),
        ".hashsync-0123456789abcdef".to_owned(),
    ];
    for name in &listed {
        std::fs::write(dir.join(name), b"").unwrap();
    }

    // The account ID, then the advertised lists: a count and the digests.
    let reply = serve.hello(&["true"]);
    assert_eq!(reply[16..], [&1u32.to_le_bytes()[..], &older].concat());
    let later = format!("blake3-{}.cache", hex(&[9; 32]));
    std::fs::write(dir.join(&later), b"").unwrap();

    let mut control = serve.control_session(&empty_list(), 0);
    let (frames, end) = serve.result_through_end(0);
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    let target = format!("blake3-{}.cache", blake3::hash(&empty_list()).to_hex());
    let mut expected = vec![later, target];
    expected.sort();
    assert_eq!(names, expected);

    assert!(frames.is_empty());
    assert_eq!(end.outcome(), SUCCESS);
    control.stop();
    serve.expect_success();
}

#[test]
fn an_unknown_strong_hash_is_rejected_before_the_target() {
    // The target's length depends on the strong hash, so `serve` rejects
    // an unknown one with `die` before it reads the target, as it would
    // for a newer client's algorithm (DESIGN.md §6.2 and §11). The payload
    // ends after the algorithm's name: a read of any later field would
    // panic on truncation instead.
    let mut serve = Serve::start();
    serve.send_version_and_features();
    serve.send(
        REQUEST,
        &request_payload(&["true"], BACKWARD, 0, 2 << 30, Delivery::Eager),
    );
    serve.send(HELLO, &hello_prefix(4096, b"future-hash"));
    serve.expect_death("incompatible receiver: strong hash future-hash");
}

#[test]
fn an_unsupported_compression_level_is_rejected() {
    // REQUEST sets the run's settings; the server ends the run before
    // command startup if it does not support them (DESIGN.md §6.2).
    let mut serve = Serve::start();
    serve.send_version_and_features();
    serve.send(
        REQUEST,
        &request_payload(&["true"], BACKWARD, 20, 2 << 30, Delivery::Eager),
    );
    serve.expect_death("incompatible client: compression level 20");
}

#[test]
fn client_loss_during_rendezvous_is_fatal() {
    // `attach` never connects here, so the main thread of `serve` keeps
    // waiting for it, and nothing reads stdin, where the target list would
    // arrive. Only the session watcher can notice that the data session is
    // gone (DESIGN.md §6.9).
    let mut serve = Serve::start();
    serve.hello(&["true"]);
    serve.lose_data_session();
    serve.expect_death("the client disappeared");
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
    serve.expect_death("the client disappeared");
}

#[test]
fn client_loss_while_serve_waits_for_the_control_version_is_fatal() {
    // The command starts only after the client's versions on both
    // sessions matched (DESIGN.md §6.2). After it published the list,
    // `serve` waits for the control session's version.
    let mut serve = Serve::start();
    serve.hello(&["true"]);
    let _control = serve.attach_without_version();
    serve.send_empty_list();
    serve.wait_for_published_list();
    serve.lose_data_session();
    serve.expect_death("the client disappeared");
}

#[test]
fn client_loss_while_the_data_reader_waits_at_the_spool_limit_is_fatal() {
    // With a spool limit of 0, the data reader appends one CHUNK payload to
    // the forward stream's spool, then waits until the decoder takes it.
    // Before the client's control version, no command runs, so no decoder
    // exists, and the data reader waits for good: it reads nothing, so it
    // cannot see the loss of the data session. The session watcher sees it
    // instead (DESIGN.md §6.6). The control session stays open, without its
    // version, so only the data session shows the loss.
    let mut serve = Serve::start();
    serve.hello_with_spool_limit(&["true"], 0);
    let _control = serve.attach_without_version();
    serve.send_empty_list();
    serve.recv_result();
    serve.send(CHUNK, &frame(LIT, b"waits"));
    serve.lose_data_session();
    serve.expect_death("the client disappeared");
}

/// `serve` runs a command whose descendant keeps command stderr open, and
/// writes its PID to the command's stdout: the stderr relay does not
/// finish, so END does not come. The descendant runs until this test
/// process ends: it does not end by itself, however slow the machine is,
/// and a failed test does not leave it behind. Returns the control session,
/// after the client's version and features, and the descendant, as a pidfd,
/// once the backward stream has ended.
fn run_with_a_descendant_that_holds_stderr(
    serve: &mut Serve,
) -> (UnixStream, std::os::fd::OwnedFd) {
    let script = format!(
        "( exec >/dev/null; exec tail --pid={} -f /dev/null ) & echo $!",
        std::process::id()
    );
    serve.hello(&["sh", "-c", &script]);
    let control = serve.attach();
    serve.send_empty_list();
    serve.recv_result();
    let frames = serve.stream(0).rest();
    let output: Vec<u8> = frames
        .into_iter()
        .flat_map(|(_, payload)| payload)
        .collect();
    let descendant: i32 = String::from_utf8(output).unwrap().trim().parse().unwrap();
    // Nothing ends the descendant before the fatal cleanup, so no other
    // process can have its PID yet.
    let pidfd = pidfd_open(Pid::from_raw(descendant).unwrap(), PidfdFlags::empty()).unwrap();
    (control, pidfd)
}

/// Wait until the process of `pidfd` has ended. A pidfd becomes readable
/// when its process ends, also for a process that is not a child of this
/// one. A descendant that survived the fatal cleanup hangs the test.
fn wait_for_the_end_of(pidfd: &std::os::fd::OwnedFd) {
    let mut fds = [PollFd::new(pidfd, PollFlags::IN)];
    retry_on_intr(|| poll(&mut fds, None)).unwrap();
}

#[test]
fn client_loss_before_end_is_fatal_while_stderr_drains() {
    // The backward stream has ended, but a descendant keeps command stderr
    // open, so `serve` does not send END. The client's disappearance,
    // stdin EOF before END, then fails the run and kills the command's
    // process group, descendant included (DESIGN.md §6.9).
    let mut serve = Serve::start();
    let (_control, descendant) = run_with_a_descendant_that_holds_stderr(&mut serve);
    serve.lose_data_session();
    serve.expect_death("the client disappeared");
    wait_for_the_end_of(&descendant);
}

#[test]
fn control_loss_after_the_end_of_the_backward_stream_is_fatal_before_end() {
    // The backward stream has ended, so its receiver, the client, has
    // entered Completing, but END has not come: a descendant keeps command
    // stderr open. The closure of the control session is still the loss of
    // the client then, which fails the run and kills the command's process
    // group (DESIGN.md §6.9).
    let mut serve = Serve::start();
    let (mut control, descendant) = run_with_a_descendant_that_holds_stderr(&mut serve);
    let mut theirs = [0u8; 12];
    control.read_exact(&mut theirs).unwrap();
    drop(control);
    serve.expect_death("the client disappeared");
    wait_for_the_end_of(&descendant);
}

#[test]
fn an_answer_decides_between_a_reference_and_literals() {
    // The client's list holds the weak hash of the command's only window,
    // so the scanner queries it on the control session. A yes makes it a
    // REF; a no makes the scanner rescan, and the window's bytes go out as
    // literals (DESIGN.md §6.3). With compression, each direction of the
    // control session is one zstd stream at the level of the data session,
    // and no frame ends it: the closure of the session ends it, after END
    // (DESIGN.md §6.2 and §6.9). The exit of the command stops the forward
    // stream, which STOP tells before END, between any two frames of the
    // control session (DESIGN.md §6.6).
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

            let (frames, end) = serve.result_through_end(level);
            assert_eq!(end.outcome(), SUCCESS);
            // STOP before the closure of the data session, and only then
            // the closure of the control session, which follows it.
            control.stop();
            let mut literal = Vec::new();
            let mut references = 0;
            for (kind, payload) in &frames {
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
            serve.lose_data_session();
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
/// are the last queries, and the client reads none of them, so the
/// control writer is still blocked on a full socket when the normal end
/// shuts the socket down. That failed write is expected closure after END
/// (DESIGN.md §6.9), also when compression at a `level` other than 0
/// leaves a zstd block half written. On one CPU, the woken writer runs
/// before the main thread can exit, so a writer that took the closure as a
/// failure would win.
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
    // In eager delivery, an idle point of the output of `cat` could make a
    // window of a chain literal, and change the queries (DESIGN.md §6.3).
    serve.delivery = Delivery::Batch;
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
    // end shuts the socket down.
    wait_until_blocked_in_send(serve.child.id(), "control-writer");
    control.answer(&[(20, false)]);

    let (frames, end) = serve.result_through_end(level);
    assert_eq!(end.outcome(), SUCCESS);
    let mut output = Vec::new();
    for (kind, payload) in &frames {
        match *kind {
            LIT => output.extend_from_slice(payload),
            REF => output.extend_from_slice(window(output.len() as u64)),
            kind => panic!("unexpected frame type {kind}"),
        }
    }
    assert!(output == data, "the output differs");

    // The client closes the data session after END; then `serve` shuts
    // the control socket down, which cuts the writer's stream: only the
    // start of the q chain arrived, with or without the STOP of the
    // command's exit, which the writer queued at any point. Compressed,
    // the stream ends inside its zstd frame.
    serve.lose_data_session();
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
fn a_compressed_stream_is_one_zstd_frame_in_chunks() {
    // The stream frames of the backward stream form one zstd frame with a
    // checksum, whose pieces are the payloads of the CHUNK frames, and
    // which ends with the last CHUNK. STREAM_END and END follow it, outside
    // it, and nothing follows END (DESIGN.md §6.2).
    let mut serve = Serve::start();
    serve.hello_with(4096, &empty_list(), &["printf", "%s", "compressed"], 3);
    let mut control = serve.control_session(&empty_list(), 3);
    serve.recv_result();
    let mut chunks = Vec::new();
    let stream = loop {
        match serve.recv() {
            (CHUNK, payload) => chunks.push(payload),
            (STREAM_END, payload) => break (payload, chunks.concat()),
            (kind, _) => panic!("unexpected frame type {kind}"),
        }
    };
    assert!(stream.0.is_empty(), "STREAM_END with a payload");
    let mut decoder = zstd::stream::read::Decoder::new(&stream.1[..])
        .unwrap()
        .single_frame();
    assert_eq!(recv_frame(&mut decoder), (LIT, b"compressed".to_vec()));
    assert_eq!(decoder.read(&mut [0]).unwrap(), 0, "the frame goes on");
    let mut rest = Vec::new();
    decoder.finish().read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty(), "bytes after the zstd frame");
    assert_eq!(serve.recv_end().outcome(), SUCCESS);
    control.stop();
    serve.expect_success();
}

#[test]
fn eager_delivery_sends_the_output_while_the_command_waits() {
    // The command writes fewer bytes than a window, and waits until this
    // test has read them, then writes more. In eager delivery, the idle
    // point of the command's stdout makes the scanner emit the bytes before
    // it, and the data writer flush the stream behind their frame: the
    // bytes arrive while the command still waits, with compression and
    // without (DESIGN.md §6.2 and §6.3). A `serve` that does not flush there
    // hangs this test.
    for level in [0, 3] {
        let mut serve = Serve::start();
        let go = serve.home.path().join("go");
        mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
        let script = format!("printf partial; read go <'{}'; printf more", go.display());
        serve.hello_with(4096, &empty_list(), &["sh", "-c", &script], level);
        let mut control = serve.control_session(&empty_list(), level);
        serve.recv_result();
        {
            let mut stream = serve.stream(level);
            assert_eq!(stream.next(), Some((LIT, b"partial".to_vec())));
            std::fs::write(&go, b"go\n").unwrap();
            assert_eq!(stream.rest(), [(LIT, b"more".to_vec())]);
        }
        assert_eq!(serve.recv_end().outcome(), SUCCESS);
        control.stop();
        serve.expect_success();
    }
}

#[test]
fn batch_delivery_keeps_the_output_while_the_command_waits() {
    // The command writes fewer bytes than a window, and waits until this
    // test lets it go on. In batch delivery, `serve` keeps those bytes, a
    // short last literal, even once all its threads sleep: they leave with
    // the bytes that follow, in one LIT, at EOF (DESIGN.md §6.3).
    for level in [0, 3] {
        let mut serve = Serve::start();
        let go = serve.home.path().join("go");
        mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
        let script = format!("printf partial; read go <'{}'; printf more", go.display());
        serve.delivery = Delivery::Batch;
        serve.hello_with(4096, &empty_list(), &["sh", "-c", &script], level);
        let mut control = serve.control_session(&empty_list(), level);
        // The open waits for the command to open the FIFO, after it wrote
        // its first bytes. Then `serve` has nothing more to do until the
        // command goes on.
        let mut fifo = std::fs::File::options().write(true).open(&go).unwrap();
        wait_until_asleep(serve.child.id());
        fifo.write_all(b"go\n").unwrap();
        drop(fifo);
        let (frames, end) = serve.result_through_end(level);
        assert_eq!(frames, [(LIT, b"partialmore".to_vec())]);
        assert_eq!(end.outcome(), SUCCESS);
        control.stop();
        serve.expect_success();
    }
}

#[test]
fn the_forward_stream_reaches_the_command() {
    // `serve` flushes CACHE_RESULT, and the client sends the forward stream
    // after it: the command, `cat`, writes no output before it reads its
    // stdin. The payloads of the CHUNK frames are pieces of the stream's
    // LIT frames: at level 3, one zstd frame from an independent encoder,
    // and at level 0, the frames themselves. STREAM_END ends the stream, so
    // the command reads EOF after it, and the run ends normally
    // (DESIGN.md §6.2 and §6.6). END gives the client the server's counts
    // of the stream: the whole input, all literal, and every CHUNK byte
    // decoded (DESIGN.md §8).
    let input = b"the stdin of the command\n".repeat(10_000);
    for level in [0, 3] {
        let mut serve = Serve::start();
        serve.hello_with(4096, &empty_list(), &["cat"], level);
        let mut control = serve.control_session(&empty_list(), level);
        serve.recv_result();
        let stream = forward_stream(&input, level);
        serve.send_raw(&stream);
        let output: Vec<u8> = serve
            .stream(level)
            .rest()
            .into_iter()
            .flat_map(|(kind, payload)| {
                assert_eq!(kind, LIT);
                payload
            })
            .collect();
        assert!(output == input, "level {level}: the output differs");
        let end = serve.recv_end();
        assert_eq!(end.outcome(), SUCCESS);
        let length = input.len() as u64;
        assert_eq!(end.written, [length, length, 0, 0], "level {level}");
        assert_eq!(end.decoded, chunk_payload_bytes(&stream), "level {level}");
        control.stop();
        serve.expect_success();
    }
}

/// The bytes of the CHUNK payloads among the session frames `session`.
fn chunk_payload_bytes(session: &[u8]) -> u64 {
    let mut rest = session;
    let mut chunks = 0;
    while !rest.is_empty() {
        let length = u32::from_le_bytes(rest[1..5].try_into().unwrap()) as usize;
        if rest[0] == CHUNK {
            chunks += length as u64;
        }
        rest = &rest[5 + length..];
    }
    chunks
}

#[test]
fn a_command_that_closes_its_stdin_stops_the_forward_stream_at_once() {
    // The command closes its stdin, and runs on until this test ends it.
    // The first write of the forward stream then fails with EPIPE, and the
    // stream has no use any more: `serve` sends STOP while the command
    // still runs, and discards the CHUNK frames that still arrive, and
    // STREAM_END, which the client can send before it learns of the stop
    // (DESIGN.md §6.6). A `serve` that stopped the stream only at the
    // command's exit would hang this test, which ends the command only
    // after STOP.
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
    serve.send(CHUNK, &frame(LIT, b"no process reads this"));
    control.stop();
    serve.send(CHUNK, &frame(LIT, b"discarded"));
    serve.send(STREAM_END, b"");
    std::fs::write(&go, b"go\n").unwrap();
    let (frames, end) = serve.stream_through_end(0);
    assert!(frames.is_empty());
    assert_eq!(end.outcome(), SUCCESS);
    // The command's stdin took none of the stream, and the decoder decoded
    // the first frame alone, whose write failed: the frame after STOP was
    // dropped (DESIGN.md §8).
    assert_eq!(end.written, [0; 4], "{end:?}");
    assert_eq!(
        end.decoded,
        5 + "no process reads this".len() as u64,
        "{end:?}"
    );
    serve.expect_success();
}

#[test]
fn the_exit_of_the_command_stops_the_forward_stream_while_stdout_stays_open() {
    // The command exits at once, but a descendant holds its stdout until
    // this test lets it end. The forward stream has no use any more once
    // the command has exited, so `serve` stops it then, and sends STOP,
    // while the scan still waits for the EOF of stdout (DESIGN.md §6.6). A
    // `serve` that stopped the stream only at that EOF would hang this
    // test, which lets the descendant end only after STOP.
    let mut serve = Serve::start();
    let go = serve.home.path().join("go");
    mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!("( read go <'{}' ) & exit 0", go.display());
    serve.hello(&["sh", "-c", &script]);
    let mut control = serve.control_session(&empty_list(), 0);
    serve.recv_result();
    control.stop();
    std::fs::write(&go, b"go\n").unwrap();
    let (frames, end) = serve.stream_through_end(0);
    assert!(frames.is_empty());
    assert_eq!(end.outcome(), SUCCESS);
    serve.expect_success();
}

#[test]
fn a_descendant_that_holds_the_command_stdin_delays_nothing() {
    // A descendant of the command holds the command's stdin and reads
    // nothing, so the decoder of the forward stream waits for room in the
    // full pipe. When the command exits, the stop ends that wait, and END
    // goes out at once; the run ends while the descendant still runs
    // (DESIGN.md §6.6 and §6.9). The descendant runs until this test
    // process ends, so a `serve` that waited for it would hang the test.
    let mut serve = Serve::start();
    // The wait for the blocked decoder needs ptrace access to `serve`.
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
    let mut control = serve.control_session(&empty_list(), 0);
    serve.recv_result();
    // 4 MiB, more than a pipe holds by default: the forward stream's spool
    // takes them all, and the decoder fills the command's stdin.
    let input = vec![b'x'; 4 << 20];
    let stream = forward_stream(&input, 0);
    serve.send_raw(&stream[..stream.len() - 5]);
    wait_until_asleep_in(serve.child.id(), "decoder", poll_syscall());
    std::fs::write(&go, b"go\n").unwrap();
    let (frames, end) = serve.stream_through_end(0);
    assert_eq!(end.outcome(), SUCCESS);
    control.stop();
    serve.expect_success();
    // Nothing ends the descendant before this kill, so no other process
    // can have its PID yet.
    let output: Vec<u8> = frames
        .into_iter()
        .flat_map(|(_, payload)| payload)
        .collect();
    let descendant: i32 = String::from_utf8(output).unwrap().trim().parse().unwrap();
    kill_process(Pid::from_raw(descendant).unwrap(), Signal::KILL).unwrap();
}

#[test]
fn a_stop_ends_the_writes_of_one_run_that_stands_for_terabytes() {
    // In a push, the forward stream is one RLE frame of 14 bytes that
    // stands for a terabyte. A descendant of the command holds the
    // command's stdin and reads nothing, so the decoder waits for room in
    // the full pipe, in the middle of that frame's writes. When the command
    // exits, the stop ends that wait, and the decoder writes no more of the
    // run; END goes out at once, and the run ends while the descendant still
    // runs (DESIGN.md §6.6 and §6.9). A decoder that waited for room alone,
    // or wrote the rest of the run, would hang the test.
    let mut serve = Serve::start();
    // The wait for the blocked decoder needs ptrace access to `serve`.
    if let Err(e) = std::fs::read_to_string(format!("/proc/{}/syscall", serve.child.id())) {
        eprintln!("skipped: cannot see the system calls of serve: {e}");
        return;
    }
    serve.configure_push(&[]);
    let go = serve.home.path().join("go");
    mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!(
        "exec 3<&0; ( exec <&3 3<&- >/dev/null 2>&1; exec tail --pid={} -f /dev/null ) & \
         echo $!; exec 3<&-; read go <'{}'",
        std::process::id(),
        go.display()
    );
    let (_, mut control) = serve.push(&["sh", "-c", &script], 0);
    let run = [&[0][..], &(1u64 << 40).to_le_bytes()].concat();
    let stream = stream_of(&[(RLE, &run)], 0);
    // Without STREAM_END: only the stop can end the decoder.
    serve.send_raw(&stream[..stream.len() - 5]);
    wait_until_asleep_in(serve.child.id(), "decoder", poll_syscall());
    std::fs::write(&go, b"go\n").unwrap();
    let (frames, end) = serve.stream_through_end(0);
    assert_eq!(end.outcome(), SUCCESS);
    // The RLE frame that the stop cut short counts whole: 14 bytes.
    assert_eq!(end.frames, [0, 14], "{end:?}");
    control.stop();
    serve.expect_success();
    // Nothing ends the descendant before this kill, so no other process
    // can have its PID yet.
    let output: Vec<u8> = frames
        .into_iter()
        .flat_map(|(_, payload)| payload)
        .collect();
    let descendant: i32 = String::from_utf8(output).unwrap().trim().parse().unwrap();
    kill_process(Pid::from_raw(descendant).unwrap(), Signal::KILL).unwrap();
}

#[test]
fn stop_from_the_client_closes_the_command_stdout_and_ends_the_backward_stream() {
    // In a push, the backward stream is the command's stdout, as it is.
    // `yes` writes without end, and the client reads none of the data
    // session, so the data writer sleeps in the write of its first CHUNK,
    // and the stdout reader waits for room in the spool, which holds at
    // most 64 KiB. Then the client stops the backward stream, as it does
    // once no process reads its stdout. The stop ends the wait of the
    // stdout reader, which closes the command's stdout: `yes` dies of
    // SIGPIPE at its next write. The data writer sends only the CHUNK that
    // is on its way, and no STREAM_END, and END follows the exit of the
    // command, with its status (DESIGN.md §6.6 and §6.9). A `serve` that
    // went on reading the command's stdout, or that waited for room in the
    // spool alone, would never see `yes` end, and would hang the test.
    let mut serve = Serve::start();
    let pid = serve.child.id();
    // The waits for the blocked threads need ptrace access to `serve`.
    if let Err(e) = std::fs::read_to_string(format!("/proc/{pid}/syscall")) {
        eprintln!("skipped: cannot see the system calls of serve: {e}");
        return;
    }
    serve.configure_push(&[]);
    serve.spool_limit = 64 << 10;
    // `yes` writes its first two bytes alone, and only then the rest in
    // large writes. In eager delivery, an idle point after those two bytes
    // can send them in a CHUNK of their own, ahead of the CHUNK that this
    // test waits for. In batch delivery, every CHUNK is full.
    serve.delivery = Delivery::Batch;
    let (_, mut control) = serve.push(&["yes"], 0);
    // The forward stream is empty: without compression, STREAM_END alone.
    serve.send(STREAM_END, b"");
    wait_until_asleep_in(pid, "hashsync", libc::SYS_writev as u64);
    wait_until_asleep_in(pid, "stdout-reader", libc::SYS_futex as u64);
    control.send_stop();
    // Once the stdout reader has ended, the stop is raised, and only then
    // does the data writer's write go on.
    wait_until_gone(pid, "stdout-reader");
    let (kind, payload) = serve.recv();
    assert_eq!(
        (kind, payload.len()),
        (CHUNK, 256 << 10),
        "the CHUNK on its way"
    );
    let end = serve.recv_end();
    assert_eq!(end.outcome(), (128 + libc::SIGPIPE, &b""[..]));
    // The stdout reader read at least that CHUNK, and not to the EOF.
    let (read, eof) = end.read;
    assert!(read >= 256 << 10 && !eof, "{end:?}");
    // The exit of the command stops the forward stream too, which had
    // ended already.
    control.stop();
    serve.expect_success();
}

#[test]
fn a_second_stop_from_the_client_is_a_protocol_error() {
    // The client stops the backward stream at most once (DESIGN.md §6.2).
    // A second STOP is a bug, so `serve` panics, reports it on lines that
    // it marks as its own, kills the command's process group, and aborts
    // (DESIGN.md §6.7 and §8). The command runs until this test process
    // ends, so a `serve` that took the second STOP would hang the test.
    let mut serve = Serve::start();
    // The abort must leave no core file behind.
    let pid = Pid::from_raw(serve.child.id() as i32).unwrap();
    let no_core = rustix::process::Rlimit {
        current: Some(0),
        maximum: Some(0),
    };
    rustix::process::prlimit(Some(pid), rustix::process::Resource::Core, no_core).unwrap();
    let until_the_test_ends = format!("--pid={}", std::process::id());
    serve.hello(&["tail", &until_the_test_ends, "-f", "/dev/null"]);
    let mut control = serve.control_session(&empty_list(), 0);
    serve.recv_result();
    control.send_stop();
    control.send_stop();
    let (status, stderr) = serve.finish();
    assert_eq!(status.signal(), Some(libc::SIGABRT), "stderr:\n{stderr}");
    assert!(
        stderr
            .lines()
            .any(|line| line.starts_with("hashsync (server): ")
                && line.contains("protocol error: a second STOP")),
        "stderr:\n{stderr}"
    );
}

#[test]
fn stdin_eof_after_end_is_the_normal_end() {
    // `serve` sends END once it needs nothing more from the client. Then it
    // waits, with every thread asleep, until its data reader reads the
    // stdin EOF that the client causes after END. It then shuts the
    // control session down, joins every thread, and exits with status 0
    // (DESIGN.md §6.9).
    let mut serve = Serve::start();
    serve.hello(&["printf", "%s", "output"]);
    let mut control = serve.attach();
    serve.send_empty_list();
    let (frames, end) = serve.result_through_end(0);
    assert_eq!(frames, [(LIT, b"output".to_vec())]);
    assert_eq!(end.outcome(), SUCCESS);
    // After the version and the features, the STOP of the command's exit,
    // which comes before END, and before the closure of the session that
    // the end of the data session allows (DESIGN.md §6.6).
    let expected = [&version_and_features(FEATURES)[..], &frame(STOP, b"")].concat();
    let mut stopped = vec![0; expected.len()];
    control.read_exact(&mut stopped).unwrap();
    assert_eq!(stopped, expected);
    wait_until_asleep(serve.child.id());
    serve.lose_data_session();
    let mut received = Vec::new();
    control.read_to_end(&mut received).unwrap();
    assert!(received.is_empty(), "the control session after STOP");
    serve.expect_success();
}

#[test]
fn a_frame_cut_short_after_end_is_the_normal_end_too() {
    // A client that fails after END, while its data writer sends a CHUNK,
    // cuts the data session inside that frame. After END, the end of the
    // session is the normal end, also inside a frame (DESIGN.md §6.9). So
    // `serve` exits with status 0, and the descendant that the command
    // left running, with its stdout and stderr closed, stays alive, where
    // a fatal end would kill the command's process group.
    let mut serve = Serve::start();
    let script = format!(
        "( exec >/dev/null 2>&1 </dev/null; exec tail --pid={} -f /dev/null ) & echo $!",
        std::process::id()
    );
    serve.hello(&["sh", "-c", &script]);
    let mut control = serve.control_session(&empty_list(), 0);
    let (frames, end) = serve.result_through_end(0);
    assert_eq!(end.outcome(), SUCCESS);
    control.stop();
    let output: Vec<u8> = frames
        .into_iter()
        .flat_map(|(_, payload)| payload)
        .collect();
    let descendant: i32 = String::from_utf8(output).unwrap().trim().parse().unwrap();
    // Nothing ends the descendant before this test does, so no other
    // process can have its PID yet.
    let descendant = Pid::from_raw(descendant).unwrap();
    let pidfd = pidfd_open(descendant, PidfdFlags::empty()).unwrap();
    // A CHUNK header that announces 100 bytes, 10 of them, then EOF.
    serve.send_raw(&[CHUNK, 100, 0, 0, 0]);
    serve.send_raw(&[0; 10]);
    serve.expect_success();
    // The normal end kills nothing, so the descendant still runs: a pidfd
    // becomes readable once its process ends, and this look does not wait.
    let mut fds = [PollFd::new(&pidfd, PollFlags::IN)];
    let ended = retry_on_intr(|| poll(&mut fds, Some(&Timespec::default()))).unwrap();
    assert_eq!(ended, 0, "the descendant was killed");
    kill_process(descendant, Signal::KILL).unwrap();
}

/// A block of 4 KiB with no repeated window, for the index of a push.
fn indexed_block() -> Vec<u8> {
    (0..4096u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect()
}

#[test]
fn a_push_writes_the_blocks_of_the_servers_index_for_its_references() {
    // In a push, `serve` receives the forward stream, deduplicated against
    // its own index, which it updates before its HELLO, and the target is
    // that index's list. The client queries the block at offset 4 on the
    // control session; `serve` pins it and answers yes, so the client sends
    // REF, and the decoder writes the pinned block to the command's stdin.
    // `cat` sends it all back in the backward stream, which is plain
    // (DESIGN.md §6.4 and §6.6). A query for a block that the index does not
    // have, as the client sends when only the weak hash of its window is
    // listed, gets a no, and so does a query for an indexed block whose
    // file changed after the update. END counts the first as a false
    // positive and the second as not found, and gives the bytes of the REF
    // frame (DESIGN.md §8).
    let block = indexed_block();
    let stale: Vec<u8> = block.iter().map(|byte| byte ^ 0x5a).collect();
    for level in [0, 3] {
        let mut serve = Serve::start();
        serve.configure_push(&[("block", &block), ("stale", &stale)]);
        let (list, mut control) = serve.push(&["cat"], level);
        let mut listed = [weak_hash(&block), weak_hash(&stale)];
        listed.sort_unstable();
        assert_eq!(list, encode_list(&listed), "the target");
        // The server sent its list after its update.
        std::fs::write(serve.home.path().join("tree/stale"), b"changed").unwrap();
        control.send_query(4, blake3::hash(&block).as_bytes());
        assert_eq!(control.recv_answer(), (4, true));
        control.send_query(8, blake3::hash(b"not indexed").as_bytes());
        assert_eq!(control.recv_answer(), (8, false));
        control.send_query(12, blake3::hash(&stale).as_bytes());
        assert_eq!(control.recv_answer(), (12, false));
        let frames = [(LIT, &b"head"[..]), (REF, b""), (LIT, b"tail")];
        serve.send_raw(&stream_of(&frames, level));
        let output: Vec<u8> = serve
            .stream(level)
            .rest()
            .into_iter()
            .flat_map(|(kind, payload)| {
                assert_eq!(kind, LIT, "a plain stream");
                payload
            })
            .collect();
        assert!(
            output == [&b"head"[..], &block, b"tail"].concat(),
            "level {level}: the output differs"
        );
        let end = serve.recv_end();
        assert_eq!(end.outcome(), SUCCESS);
        let length = block.len() as u64;
        assert_eq!(end.written, [8 + length, 8, length, 0], "level {level}");
        assert_eq!(end.frames, [5, 0], "level {level}: one REF frame");
        assert_eq!(end.misses, [1, 1], "level {level}");
        control.stop();
        serve.expect_success();
    }
}

#[test]
fn a_push_closes_the_index_at_the_stop_and_answers_no_query_after_it() {
    // The command closes its stdin, so the first write of the forward
    // stream fails with EPIPE: `serve` stops the stream, which puts its
    // receiver into Completing, and sends STOP. The lookups then end, and
    // the index closes with its lock, while the command still runs, so an
    // update could begin. A QUERY that comes after the stop gets no answer,
    // and the CHUNK and STREAM_END that the client sends before it learns
    // of the stop change nothing: the run ends normally (DESIGN.md §4.4,
    // §6.4 and §6.6). A `serve` that keeps its index open until the end of
    // the forward stream hangs the test.
    let block = indexed_block();
    let mut serve = Serve::start();
    serve.configure_push(&[("block", &block)]);
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
    let (_, mut control) = serve.push(&["sh", "-c", &script], 0);
    // The open waits for the command to open the FIFO, after it closed its
    // stdin.
    let mut message = String::new();
    std::fs::File::open(&closed)
        .unwrap()
        .read_to_string(&mut message)
        .unwrap();
    assert_eq!(message, "closed\n");
    serve.send(CHUNK, &frame(LIT, b"no process reads this"));
    control.stop();
    let lock_file = serve.index().join("hashsync.lock");
    while ofd_lock(serve.child.id(), &lock_file).is_some() {
        std::thread::sleep(Duration::from_millis(1));
    }
    control.send_query(0, blake3::hash(&block).as_bytes());
    serve.send(CHUNK, &frame(LIT, b"discarded"));
    serve.send(STREAM_END, b"");
    std::fs::write(&go, b"go\n").unwrap();
    let (frames, end) = serve.stream_through_end(0);
    assert!(frames.is_empty());
    assert_eq!(end.outcome(), SUCCESS);
    assert_eq!(end.written, [0; 4], "{end:?}");
    serve.lose_data_session();
    // No ANSWER comes before the closure of the control session.
    let error = control.next().unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    serve.expect_success();
}

#[test]
fn client_loss_during_the_index_update_of_a_push_is_fatal() {
    // This test holds a shared lock on the index of `serve`, so the update
    // that a push starts waits for good, before `serve` sends its HELLO.
    // Only the session watcher, which starts at REQUEST, can then notice
    // that the client has disappeared (DESIGN.md §4.4 and §6.6).
    let mut serve = Serve::start();
    serve.configure_push(&[("block", &indexed_block())]);
    let config = serve.home.path().join("config/hashsync/config.toml");
    let update = Command::new(env!("CARGO_BIN_EXE_hashsync"))
        .arg("--config")
        .arg(&config)
        .arg("update")
        .output()
        .unwrap();
    assert!(update.status.success(), "{update:?}");
    let _lock = shared_lock(&serve.index());
    serve.push_request(&["true"], 0);
    wait_for_lock_wait(serve.child.id());
    serve.lose_data_session();
    serve.expect_death("the client disappeared");
}

#[test]
fn client_loss_after_the_end_of_the_forward_stream_of_a_push_is_fatal_before_end() {
    // The forward stream has ended, so `serve`, its receiver, has entered
    // Completing, and its lookups have ended; but END has not come, because
    // a descendant keeps command stderr open. The closure of either session
    // is still the loss of the client then, which fails the run and kills
    // the command's process group (DESIGN.md §6.9).
    for lose_control in [true, false] {
        let mut serve = Serve::start();
        serve.configure_push(&[]);
        let script = format!(
            "cat >/dev/null; ( exec >/dev/null; exec tail --pid={} -f /dev/null ) & echo $!",
            std::process::id()
        );
        let (_, control) = serve.push(&["sh", "-c", &script], 0);
        serve.send_raw(&forward_stream(b"input", 0));
        let output: Vec<u8> = serve
            .stream(0)
            .rest()
            .into_iter()
            .flat_map(|(_, payload)| payload)
            .collect();
        let descendant: i32 = String::from_utf8(output).unwrap().trim().parse().unwrap();
        let descendant =
            pidfd_open(Pid::from_raw(descendant).unwrap(), PidfdFlags::empty()).unwrap();
        if lose_control {
            drop(control);
            serve.expect_death("the client disappeared");
        } else {
            serve.lose_data_session();
            serve.expect_death("the client disappeared");
        }
        wait_for_the_end_of(&descendant);
    }
}

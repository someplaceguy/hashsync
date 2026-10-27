//! `hashsync serve` driven directly through its standard streams and its
//! rendezvous socket, the way the client and `attach` drive it. In a pull,
//! `serve` sends the command's stdout, the backward stream, deduplicated,
//! and writes the forward stream to the command's stdin. In a push, it
//! receives the forward stream deduplicated against its own index, which
//! it updates while the command runs, and again after END, and sends the
//! backward stream plain.
//! `serve` starts the command at REQUEST, and with it both streams; the
//! list exchange of the deduplicated stream runs meanwhile on the control
//! session (DESIGN.md §5.6 and §6.2). The client's disappearance is fatal
//! at every stage before END (DESIGN.md §6.9): during rendezvous, during
//! the update of the index of a push, while the data reader waits at the
//! limit of the forward stream's spool, and while command stderr still
//! drains. After END, stdin EOF is the normal end. Queries, answers, and
//! STOP travel on the control session, inside its control stream; obsolete
//! queries still unsent at the normal end do not fail it, also inside a
//! compressed block. The stream frames of a stream form one zstd frame,
//! whose pieces travel in CHUNK frames; on the control session, the CHUNK
//! frames of each direction carry one zstd stream that no frame ends.

mod common;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{BufReader, IoSlice, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::rc::Rc;
use std::time::Duration;

use common::{
    ADVERTISE, ANSWER, CACHE_RESULT, CHUNK, END, FEATURES, HASHDATA, HASHLIST, HASHNONE, HELLO,
    INDEX, LIT, PROTOCOL_VERSION, QUERY, REF, REQUEST, RLE, STOP, STREAM_END, frame, hello_payload,
    ofd_lock, poll_syscall, proc_state, recv_frame, shared_lock, sleeping_syscall,
    sleeping_syscall_of, toml_literal, try_recv_frame, version_and_features, wait_for_lock_wait,
    wait_until_asleep,
};
use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fs::{CWD, Mode, OFlags, mkfifoat};
use rustix::io::{Errno, retry_on_intr};
use rustix::process::{Pid, PidfdFlags, Signal, kill_process, pidfd_open};
use rustix::thread::{CpuSet, sched_getaffinity, sched_setaffinity};
use tempfile::TempDir;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

/// The client's account and the ID of its index, as its INDEX names them
/// in a pull, and its account, as its ADVERTISE names it in a push.
const ACCOUNT: [u8; 16] = [0xaa; 16];
const INDEX_ID: [u8; 16] = [0xbb; 16];

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

/// The deduplicated stream, as REQUEST encodes it (DESIGN.md §6.2): the
/// forward stream in a push, the backward stream in a pull.
const FORWARD: u8 = 1;
const BACKWARD: u8 = 2;

/// The settings of a REQUEST that the tests change.
#[derive(Clone, Copy, Debug)]
struct Settings {
    /// zstd compression at this level, 0 for none.
    level: i32,
    /// The most bytes that wait in a spool.
    spool_limit: u64,
    /// The delivery of the command's stdout.
    delivery: Delivery,
    /// `--wait`: a deduplicated stream starts only once its sender has the
    /// whole list.
    wait: bool,
}

/// The client's REQUEST payload for `command`, run without a shell, as `-s`
/// sends it, with the `deduplicated` stream and `settings`. The other
/// settings have their defaults, and the server uses its profile `default`
/// (DESIGN.md §6.2).
fn request_payload(command: &[&str], deduplicated: u8, settings: Settings) -> Vec<u8> {
    let mut request = vec![1]; // the command's words, without a shell
    request.extend_from_slice(&(command.len() as u32).to_le_bytes());
    for arg in command {
        string(&mut request, arg.as_bytes());
    }
    request.push(deduplicated);
    request.push(0); // no_update
    request.push(0); // scan_immutable
    request.push(0); // compact
    request.push(1); // sender_cache
    request.extend_from_slice(&settings.level.to_le_bytes()); // compression level
    request.push(1); // compression_long
    request.extend_from_slice(&0u32.to_le_bytes()); // compression threads
    request.extend_from_slice(&0u32.to_le_bytes()); // probe threads
    request.extend_from_slice(&0u64.to_le_bytes()); // lookahead: the default
    request.extend_from_slice(&(128u64 << 20).to_le_bytes()); // negative cache
    request.extend_from_slice(&settings.spool_limit.to_le_bytes()); // spool limit
    request.push(0); // stdin delivery: eager
    request.push(settings.delivery as u8); // stdout delivery
    request.push(settings.wait.into()); // wait
    request.push(0); // quiet
    request.push(1); // human_readable
    request.push(0); // the configuration file: the profile `default`
    request
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

/// The default send buffer of a socket, `net.core.wmem_default`, in
/// bytes. A Unix socket holds at most about one and a half times as many.
fn send_buffer() -> usize {
    std::fs::read_to_string("/proc/sys/net/core/wmem_default")
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// The system call in which `UnixStream::write_vectored` sleeps on a full
/// socket, as the frame writers of `serve` write a header and a payload:
/// `writev` or `sendmsg`, by architecture and C library. The write fails
/// once the other end closes.
fn blocking_send_syscall() -> u64 {
    let size = 4 * send_buffer();
    sleeping_syscall_of(
        || UnixStream::pair().unwrap(),
        move |mut full| {
            let (header, payload) = ([0; 5], vec![0; size]);
            let parts = [IoSlice::new(&header), IoSlice::new(&payload)];
            while full.write_vectored(&parts).is_ok() {}
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
/// call `nr`, polling with no deadline. Fail when the process has ended.
fn wait_until_asleep_in(pid: u32, name: &str, nr: u64) {
    while !asleep_in(pid, name, nr) {
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Whether the thread `name` of the process `pid` sleeps in the system call
/// `nr`. Reading its `syscall` file needs the ptrace access that a parent
/// has. Fail when the process has ended.
fn asleep_in(pid: u32, name: &str, nr: u64) -> bool {
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
            Ok(sleeping) if sleeping == Some(nr) => return true,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                panic!("cannot see the system call of {name}: {e}")
            }
            _ => {}
        }
    }
    false
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

/// The incoming direction of the control session, as the client reads it:
/// its session frames. The payloads of the CHUNK frames make the control
/// stream, and the frames of the list exchange wait in their own queue
/// (DESIGN.md §6.2).
struct Incoming {
    socket: UnixStream,
    /// The bytes of the control stream that arrived and were not decoded
    /// yet.
    stream: VecDeque<u8>,
    /// The frames of the list exchange that arrived and were not read yet.
    exchange: VecDeque<(u8, Vec<u8>)>,
}

impl Incoming {
    /// Read the next session frame into its place; the error of the
    /// session, `UnexpectedEof` at its closure.
    fn read_frame(&mut self) -> std::io::Result<()> {
        let (kind, payload) = try_recv_frame(&mut self.socket)?;
        if kind == CHUNK {
            assert!(!payload.is_empty(), "an empty CHUNK");
            self.stream.extend(payload);
        } else {
            self.exchange.push_back((kind, payload));
        }
        Ok(())
    }
}

/// The control stream of the incoming direction, as bytes: the CHUNK
/// payloads, read as the decoder needs them.
struct StreamBytes(Rc<RefCell<Incoming>>);

impl Read for StreamBytes {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut incoming = self.0.borrow_mut();
        while incoming.stream.is_empty() {
            incoming.read_frame()?;
        }
        let n = buf.len().min(incoming.stream.len());
        for (slot, byte) in buf.iter_mut().zip(incoming.stream.drain(..n)) {
            *slot = byte;
        }
        Ok(n)
    }
}

/// This side of the control session after both versions and features, as
/// the client's (DESIGN.md §6.2). Each direction carries session frames:
/// the CHUNK frames of its control stream, whose control frames are plain
/// at level 0, else one zstd stream that no frame ends, and the frames of
/// the list exchange as they are. In a pull, `serve` sends queries and
/// STOP, the client answers, and the client is the receiver of the list
/// exchange; in a push, the client sends the queries, `serve` answers and
/// sends STOP, and the client is the sender.
struct Control {
    incoming: Rc<RefCell<Incoming>>,
    /// The control frames, decoded from the incoming control stream.
    frames: Box<dyn Read>,
    outgoing: UnixStream,
    /// The outgoing control stream, unless compression is disabled.
    encoder: Option<zstd::stream::write::Encoder<'static, Vec<u8>>>,
    /// STOP arrived. `serve` sends it once, at `EPIPE` or at the exit of
    /// the command, so it can come between any two queries (DESIGN.md
    /// §6.6).
    stopped: bool,
}

impl Control {
    fn new(socket: &UnixStream, level: i32) -> Self {
        let incoming = Rc::new(RefCell::new(Incoming {
            socket: socket.try_clone().unwrap(),
            stream: VecDeque::new(),
            exchange: VecDeque::new(),
        }));
        let bytes = StreamBytes(Rc::clone(&incoming));
        let frames: Box<dyn Read> = match level {
            0 => Box::new(bytes),
            _ => Box::new(zstd::stream::read::Decoder::new(bytes).unwrap()),
        };
        let encoder =
            (level != 0).then(|| zstd::stream::write::Encoder::new(Vec::new(), level).unwrap());
        Control {
            incoming,
            frames,
            outgoing: socket.try_clone().unwrap(),
            encoder,
            stopped: false,
        }
    }

    /// The next control frame from `serve` other than STOP, which this
    /// notes; else the error that ends the session first, `UnexpectedEof`
    /// at its closure.
    fn next(&mut self) -> std::io::Result<(u8, Vec<u8>)> {
        loop {
            let frame = try_recv_frame(&mut self.frames)?;
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

    /// Wait for STOP, unless it arrived already; no other control frame may
    /// come first. `serve` queues STOP before it sends END, and shuts the
    /// session down only after the EOF of the data session, so a test that
    /// waits here before it closes the data session always gets STOP
    /// (DESIGN.md §6.6).
    fn stop(&mut self) {
        if !self.stopped {
            assert_eq!(recv_frame(&mut self.frames), (STOP, Vec::new()));
            self.stopped = true;
        }
    }

    /// Control frames, `frames` with their headers, in the control stream,
    /// flushed into one CHUNK: `serve` waits for them.
    fn send_control(&mut self, frames: &[u8]) {
        let bytes = match &mut self.encoder {
            None => frames.to_vec(),
            Some(encoder) => {
                encoder.write_all(frames).unwrap();
                encoder.flush().unwrap();
                std::mem::take(encoder.get_mut())
            }
        };
        self.outgoing.write_all(&frame(CHUNK, &bytes)).unwrap();
    }

    /// ANSWER frames: `serve` waits for them.
    fn answer(&mut self, answers: &[(u64, bool)]) {
        let mut frames = Vec::new();
        for &(offset, found) in answers {
            let mut answer = offset.to_le_bytes().to_vec();
            answer.push(found.into());
            frames.extend_from_slice(&frame(ANSWER, &answer));
        }
        self.send_control(&frames);
    }

    /// STOP: the client stops the backward stream once no process reads its
    /// stdout (DESIGN.md §6.6).
    fn send_stop(&mut self) {
        self.send_control(&frame(STOP, b""));
    }

    /// In a push, a QUERY for the window at `offset` with the strong hash
    /// `digest`: the client waits for its answer.
    fn send_query(&mut self, offset: u64, digest: &[u8]) {
        let query = [&offset.to_le_bytes()[..], digest].concat();
        self.send_control(&frame(QUERY, &query));
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

    /// A frame of the list exchange, outside the control stream.
    fn send_exchange(&mut self, kind: u8, payload: &[u8]) {
        self.outgoing.write_all(&frame(kind, payload)).unwrap();
    }

    /// The next frame of the list exchange from `serve`. The control frames
    /// that come meanwhile wait for `next`.
    fn recv_exchange(&mut self) -> (u8, Vec<u8>) {
        let mut incoming = self.incoming.borrow_mut();
        loop {
            if let Some(frame) = incoming.exchange.pop_front() {
                return frame;
            }
            incoming.read_frame().unwrap();
        }
    }

    /// INDEX, as the receiver of a pull sends it once its index is ready:
    /// the client's account and the ID of its index (DESIGN.md §5.6).
    fn send_index(&mut self) {
        self.send_exchange(INDEX, &[ACCOUNT, INDEX_ID].concat());
    }

    /// HASHLIST, which names the target, `list`, by its BLAKE3 digest, and
    /// the bytes of the list in HASHDATA frames.
    fn send_list(&mut self, list: &[u8]) {
        let length = (list.len() as u64).to_le_bytes();
        self.send_exchange(
            HASHLIST,
            &[blake3::hash(list).as_bytes(), &length[..]].concat(),
        );
        for piece in list.chunks(16 << 10) {
            self.send_exchange(HASHDATA, piece);
        }
    }

    /// The client's side of the list exchange of a pull, as the receiver:
    /// INDEX, then, after `serve`'s ADVERTISE, whose payload this returns,
    /// the full target `list`, and CACHE_RESULT, which ends the exchange
    /// (DESIGN.md §5.6).
    fn give_list(&mut self, list: &[u8]) -> Vec<u8> {
        self.send_index();
        let (kind, advertise) = self.recv_exchange();
        assert_eq!(kind, ADVERTISE);
        self.send_list(list);
        assert_eq!(self.recv_exchange(), (CACHE_RESULT, Vec::new()));
        advertise
    }

    /// The client's side of the list exchange of a pull without a list:
    /// HASHNONE in place of INDEX, as a receiver sends it whose stream has
    /// ended or stopped before its index was ready. `serve`'s side ends at
    /// once, without a table (DESIGN.md §5.6).
    fn no_list(&mut self) {
        self.send_exchange(HASHNONE, b"");
    }

    /// The next `length` bytes of the lists of a target, from HASHDATA
    /// frames, as the sender of a push reads them.
    fn recv_list(&mut self, length: u64) -> Vec<u8> {
        let mut list = Vec::new();
        while (list.len() as u64) < length {
            let (kind, piece) = self.recv_exchange();
            assert_eq!(kind, HASHDATA);
            assert!(!piece.is_empty() && list.len() + piece.len() <= length as usize);
            list.extend_from_slice(&piece);
        }
        list
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
    /// The settings of REQUEST: no compression, spools of 2 GiB, eager
    /// delivery and no `--wait`, unless a test needs others.
    settings: Settings,
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
            data_out: BufReader::new(child.stdout.take().unwrap()),
            child,
            home,
            features: FEATURES,
            settings: Settings {
                level: 0,
                spool_limit: 2 << 30,
                delivery: Delivery::Eager,
                wait: false,
            },
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

    /// The start of a pull of `command`, with 4 KiB blocks and the settings
    /// of `self.settings`: the versions and the features, REQUEST, and the
    /// client's HELLO, after which `serve` starts the command (DESIGN.md
    /// §6.2).
    fn pull(&mut self, command: &[&str]) {
        self.pull_with(4096, command);
    }

    fn pull_with(&mut self, block_size: u32, command: &[&str]) {
        self.send_version_and_features();
        self.send(REQUEST, &request_payload(command, BACKWARD, self.settings));
        self.send(HELLO, &hello_payload(block_size, b"blake3"));
        assert_eq!(
            self.recv_version_and_features(),
            version_and_features(FEATURES)
        );
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

    /// Attach, and exchange the versions and the features of the control
    /// session, whose control streams use the compression of the settings.
    fn control_session(&mut self) -> Control {
        let mut socket = self.attach();
        let mut theirs = [0u8; 12];
        socket.read_exact(&mut theirs).unwrap();
        assert_eq!(theirs, version_and_features(FEATURES));
        Control::new(&socket, self.settings.level)
    }

    /// The backward stream, as it arrives: its stream frames, from its
    /// CHUNK frames through STREAM_END.
    fn stream(&mut self) -> StreamFrames<'_, BufReader<ChildStdout>> {
        let level = self.settings.level;
        StreamFrames::new(&mut self.data_out, level)
    }

    /// END, which follows the backward stream.
    fn recv_end(&mut self) -> EndFrame {
        let (kind, payload) = self.recv();
        assert_eq!(kind, END);
        EndFrame::parse(&payload)
    }

    /// The backward stream's frames through its end, and END. `serve` sends
    /// nothing after END until the client closes the data session.
    fn stream_through_end(&mut self) -> (Vec<(u8, Vec<u8>)>, EndFrame) {
        let frames = self.stream().rest();
        let end = self.recv_end();
        assert!(self.data_out.buffer().is_empty(), "bytes after END");
        (frames, end)
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
        std::fs::write(home.join("config/hashsync/default.toml"), config).unwrap();
    }

    /// The index of `serve` in a push.
    fn index(&self) -> PathBuf {
        self.home.path().join("index")
    }

    /// The start of a push to `command`: the versions, the features, and
    /// REQUEST, with the settings of `self.settings`. Then `serve`'s HELLO,
    /// as the receiver of the forward stream, from its configuration, the
    /// first frame of the data session after its features: blocks of 4 KiB
    /// (DESIGN.md §6.2).
    fn push_request(&mut self, command: &[&str]) {
        self.send_version_and_features();
        let request = request_payload(command, FORWARD, self.settings);
        self.send(REQUEST, &request);
        assert_eq!(
            self.recv_version_and_features(),
            version_and_features(FEATURES)
        );
    }

    /// `serve`'s HELLO, as the receiver of a push.
    fn recv_hello(&mut self) {
        let (kind, hello) = self.recv();
        assert_eq!(kind, HELLO);
        assert_eq!(
            hello,
            hello_payload(4096, b"blake3"),
            "the stream's parameters"
        );
    }

    /// A push to `command`, through its list exchange: `serve` sends its
    /// HELLO, starts the command, updates its index, and sends INDEX on the
    /// control session; the client answers with ADVERTISE, with no listed
    /// list; `serve` sends the full target list; the client acknowledges it
    /// with CACHE_RESULT (DESIGN.md §5.6). Returns the target list, and the
    /// control session.
    fn push(&mut self, command: &[&str]) -> (Vec<u8>, Control) {
        self.push_request(command);
        self.recv_hello();
        let mut control = self.control_session();
        let (kind, index) = control.recv_exchange();
        assert_eq!(kind, INDEX);
        assert_eq!(index.len(), 32, "the account and the index of serve");
        let advertise = [&ACCOUNT[..], &0u32.to_le_bytes()].concat(); // no list
        control.send_exchange(ADVERTISE, &advertise);
        let (kind, payload) = control.recv_exchange();
        assert_eq!(kind, HASHLIST);
        let (target, length) = payload.split_at(32);
        let list = control.recv_list(u64::from_le_bytes(length.try_into().unwrap()));
        assert_eq!(
            target,
            blake3::hash(&list).as_bytes(),
            "HASHLIST names its list"
        );
        control.send_exchange(CACHE_RESULT, b"");
        (list, control)
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
    serve.pull(&["printf", "%s", "output"]);
    let mut control = serve.control_session();
    control.no_list();
    let (frames, end) = serve.stream_through_end();
    assert_eq!(frames, [(LIT, b"output".to_vec())]);
    assert_eq!(end.outcome(), SUCCESS);
    control.stop();
    serve.expect_success();
}

#[test]
fn the_command_starts_before_the_list_exchange() {
    // `serve` starts the command at REQUEST and the client's HELLO, and the
    // backward stream flows at once, with an empty table: here the whole
    // output arrives before the client has even attached the control
    // session, which carries the list exchange (DESIGN.md §3 and §6.2).
    // Only the end of the run waits for the exchange: END comes once its
    // side of it has ended.
    let mut serve = Serve::start();
    serve.pull(&["printf", "%s", "at once"]);
    let frames = serve.stream().rest();
    assert_eq!(frames, [(LIT, b"at once".to_vec())]);
    let mut control = serve.control_session();
    control.no_list();
    assert_eq!(serve.recv_end().outcome(), SUCCESS);
    control.stop();
    serve.expect_success();
}

#[test]
fn with_wait_the_backward_stream_waits_for_the_whole_list() {
    // With `--wait`, the scanner scans only once its table is whole. The
    // command has written its output and ended, and every thread of `serve`
    // sleeps, but no frame of the stream has come: the bytes wait for the
    // list. Then the block that the list holds becomes a reference
    // (DESIGN.md §6.3).
    let block: Vec<u8> = (0..4096u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let mut serve = Serve::start();
    let file = serve.home.path().join("block");
    std::fs::write(&file, &block).unwrap();
    serve.settings.wait = true;
    serve.pull(&["cat", file.to_str().unwrap()]);
    let mut control = serve.control_session();
    // STOP shows that the command has exited.
    control.stop();
    wait_until_asleep(serve.child.id());
    assert!(
        serve.data_out.buffer().is_empty(),
        "a frame before the list"
    );
    let mut fds = [PollFd::new(serve.data_out.get_ref(), PollFlags::IN)];
    let ready = retry_on_intr(|| poll(&mut fds, Some(&Timespec::default()))).unwrap();
    assert_eq!(ready, 0, "a frame before the list");
    control.give_list(&encode_list(&[weak_hash(&block)]));
    let digest = blake3::hash(&block).as_bytes().to_vec();
    assert_eq!(control.query(), (0, digest));
    control.answer(&[(0, true)]);
    let (frames, end) = serve.stream_through_end();
    assert_eq!(frames, [(REF, Vec::new())]);
    assert_eq!(end.outcome(), SUCCESS);
    serve.expect_success();
}

#[test]
fn serve_advertises_its_listed_lists_and_removes_only_listed_files() {
    // At INDEX, `serve` lists its directory for this client and index, and
    // advertises every listed list of the run's strong hash. After it
    // published the target, it removes the listed files except the target:
    // here an older list, a list of another strong hash, and an abandoned
    // temporary. A file that appears after the listing stays, because a
    // later run can own it (DESIGN.md §5.6).
    let hex = |bytes: &[u8]| -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() };
    let mut serve = Serve::start();
    let dir = serve.home.path().join(format!(
        "cache/hashsync/remote-hashlists/{}-{}",
        hex(&ACCOUNT),
        hex(&INDEX_ID)
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

    serve.pull(&["true"]);
    let mut control = serve.control_session();
    control.send_index();
    // The account ID, then the advertised lists: a count and the digests.
    let (kind, advertise) = control.recv_exchange();
    assert_eq!(kind, ADVERTISE);
    assert_eq!(advertise[16..], [&1u32.to_le_bytes()[..], &older].concat());
    let later = format!("blake3-{}.cache", hex(&[9; 32]));
    std::fs::write(dir.join(&later), b"").unwrap();
    control.send_list(&empty_list());
    assert_eq!(control.recv_exchange(), (CACHE_RESULT, Vec::new()));

    let (frames, end) = serve.stream_through_end();
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
fn an_unknown_strong_hash_is_rejected_at_hello() {
    // The strong hash sets the length of every list ID of the exchange, so
    // `serve` rejects an unknown one with `die` as soon as it reads its
    // name, as it would for a newer client's algorithm. It reads the
    // client's HELLO before it starts the command (DESIGN.md §6.2 and
    // §11).
    let mut serve = Serve::start();
    serve.send_version_and_features();
    let request = request_payload(&["true"], BACKWARD, serve.settings);
    serve.send(REQUEST, &request);
    serve.send(HELLO, &hello_payload(4096, b"future-hash"));
    serve.expect_death("incompatible receiver: strong hash 'future-hash'");
}

#[test]
fn an_unsupported_compression_level_is_rejected() {
    // REQUEST sets the run's settings; the server ends the run before
    // command startup if it does not support them (DESIGN.md §6.2).
    let mut serve = Serve::start();
    serve.send_version_and_features();
    serve.settings.level = 20;
    let request = request_payload(&["true"], BACKWARD, serve.settings);
    serve.send(REQUEST, &request);
    serve.expect_death("incompatible client: compression level 20");
}

#[test]
fn client_loss_during_rendezvous_is_fatal() {
    // `attach` never connects here, so the main thread of `serve` keeps
    // waiting for it. The data reader sees that the data session is gone
    // (DESIGN.md §6.9).
    let mut serve = Serve::start();
    serve.pull(&["true"]);
    serve.lose_data_session();
    serve.expect_death("the client disappeared");
}

#[test]
fn control_loss_before_the_list_is_fatal() {
    let mut serve = Serve::start();
    serve.pull(&["true"]);
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
fn client_loss_while_the_data_reader_waits_at_the_spool_limit_is_fatal() {
    // With a spool limit of 0, the data reader appends one CHUNK payload to
    // the forward stream's spool, then waits until the decoder takes it.
    // The command reads none of its stdin, so the decoder waits for room in
    // the full pipe of the command's stdin, and the data reader waits for
    // good: it reads nothing, so it cannot see the loss of the data session.
    // The session watcher sees it instead (DESIGN.md §6.6). The control
    // session stays open, so only the data session shows the loss. The
    // stream is 4 MiB, more than the pipe of the command's stdin holds,
    // without STREAM_END. What the data reader does not read stays in the
    // pipe of the data session, so this test writes the stream without
    // waiting, as far as that pipe takes it, until both threads wait.
    let mut serve = Serve::start();
    let pid = serve.child.id();
    // The waits for the blocked threads need ptrace access to `serve`.
    if let Err(e) = std::fs::read_to_string(format!("/proc/{pid}/syscall")) {
        eprintln!("skipped: cannot see the system calls of serve: {e}");
        return;
    }
    serve.settings.spool_limit = 0;
    let until_the_test_ends = format!("--pid={}", std::process::id());
    serve.pull(&["tail", &until_the_test_ends, "-f", "/dev/null"]);
    let mut control = serve.control_session();
    control.no_list();
    let stream = forward_stream(&vec![b'x'; 4 << 20], 0);
    let mut rest = &stream[..stream.len() - 5];
    let data_in = serve.data_in.as_ref().unwrap();
    rustix::fs::fcntl_setfl(data_in, OFlags::NONBLOCK).unwrap();
    let poll = poll_syscall();
    while !(asleep_in(pid, "decoder", poll)
        && asleep_in(pid, "data-reader", libc::SYS_futex as u64))
    {
        match rustix::io::write(data_in, rest) {
            Ok(written) => rest = &rest[written..],
            Err(Errno::AGAIN) => {}
            Err(e) => panic!("cannot write the stream: {e}"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    serve.lose_data_session();
    serve.expect_death("the client disappeared");
}

/// `serve` runs a command whose descendant keeps command stderr open, and
/// writes its PID to the command's stdout: the stderr relay does not
/// finish, so END does not come. The descendant runs until this test
/// process ends: it does not end by itself, however slow the machine is,
/// and a failed test does not leave it behind. Returns the control session,
/// after both versions and features and the client's HASHNONE, and the
/// descendant, as a pidfd, once the backward stream has ended.
fn run_with_a_descendant_that_holds_stderr(
    serve: &mut Serve,
) -> (UnixStream, std::os::fd::OwnedFd) {
    let script = format!(
        "( exec >/dev/null; exec tail --pid={} -f /dev/null ) & echo $!",
        std::process::id()
    );
    serve.pull(&["sh", "-c", &script]);
    let mut control = serve.attach();
    let mut theirs = [0u8; 12];
    control.read_exact(&mut theirs).unwrap();
    control.write_all(&frame(HASHNONE, b"")).unwrap();
    let frames = serve.stream().rest();
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
    let (control, descendant) = run_with_a_descendant_that_holds_stderr(&mut serve);
    drop(control);
    serve.expect_death("the client disappeared");
    wait_for_the_end_of(&descendant);
}

#[test]
fn an_answer_decides_between_a_reference_and_literals() {
    // The client's list holds the weak hash of the command's only window,
    // so the scanner queries it on the control session. A yes makes it a
    // REF; a no makes the scanner rescan, and the window's bytes go out as
    // literals (DESIGN.md §6.3). With `--wait`, the scanner waits for the
    // list. With compression, each control stream is one zstd stream at
    // the level of the data session, and no frame ends it: the closure of
    // the session ends it, after END (DESIGN.md §6.2 and §6.9). The exit of
    // the command stops the forward stream, which STOP tells before END,
    // between any two control frames (DESIGN.md §6.6).
    let block: Vec<u8> = (0..4096u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let list = encode_list(&[weak_hash(&block)]);
    for level in [0, 3] {
        for found in [true, false] {
            let mut serve = Serve::start();
            let file = serve.home.path().join("block");
            std::fs::write(&file, &block).unwrap();
            serve.settings.level = level;
            serve.settings.wait = true;
            serve.pull(&["cat", file.to_str().unwrap()]);
            let mut control = serve.control_session();
            control.give_list(&list);
            let digest = blake3::hash(&block).as_bytes().to_vec();
            assert_eq!(control.query(), (0, digest), "the window at offset 0");
            control.answer(&[(0, found)]);

            let (frames, end) = serve.stream_through_end();
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
    // holds, at any level, besides the CHUNK of up to 256 KiB that holds
    // query 20, which this test reads whole.
    let chain = ((3 * send_buffer() as u64 + (256 << 10)) / 32).max(8000);
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
    // With `--wait`, the scan starts with the whole list.
    serve.settings.delivery = Delivery::Batch;
    serve.settings.wait = true;
    serve.settings.level = level;
    serve.pull_with(B as u32, &["cat", file.to_str().unwrap()]);
    let mut control = serve.control_session();
    control.give_list(&list);
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

    let (frames, end) = serve.stream_through_end();
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
    serve.settings.level = 3;
    serve.pull(&["printf", "%s", "compressed"]);
    let mut control = serve.control_session();
    control.no_list();
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
        serve.settings.level = level;
        serve.pull(&["sh", "-c", &script]);
        let mut control = serve.control_session();
        control.no_list();
        {
            let mut stream = serve.stream();
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
        serve.settings.delivery = Delivery::Batch;
        serve.settings.level = level;
        serve.pull(&["sh", "-c", &script]);
        let mut control = serve.control_session();
        control.no_list();
        // The open waits for the command to open the FIFO, after it wrote
        // its first bytes. Then `serve` has nothing more to do until the
        // command goes on.
        let mut fifo = std::fs::File::options().write(true).open(&go).unwrap();
        wait_until_asleep(serve.child.id());
        fifo.write_all(b"go\n").unwrap();
        drop(fifo);
        let (frames, end) = serve.stream_through_end();
        assert_eq!(frames, [(LIT, b"partialmore".to_vec())]);
        assert_eq!(end.outcome(), SUCCESS);
        control.stop();
        serve.expect_success();
    }
}

#[test]
fn the_forward_stream_reaches_the_command() {
    // The client sends the forward stream from the start: the command,
    // `cat`, writes no output before it reads its stdin. The payloads of
    // the CHUNK frames are pieces of the stream's LIT frames: at level 3,
    // one zstd frame from an independent encoder, and at level 0, the
    // frames themselves. STREAM_END ends the stream, so the command reads
    // EOF after it, and the run ends normally (DESIGN.md §6.2 and §6.6).
    // END gives the client the server's counts of the stream: the whole
    // input, all literal, and every CHUNK byte decoded (DESIGN.md §8).
    let input = b"the stdin of the command\n".repeat(10_000);
    for level in [0, 3] {
        let mut serve = Serve::start();
        serve.settings.level = level;
        serve.pull(&["cat"]);
        let stream = forward_stream(&input, level);
        serve.send_raw(&stream);
        let mut control = serve.control_session();
        control.no_list();
        let output: Vec<u8> = serve
            .stream()
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
    serve.pull(&["sh", "-c", &script]);
    let mut control = serve.control_session();
    control.no_list();
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
    let (frames, end) = serve.stream_through_end();
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
    serve.pull(&["sh", "-c", &script]);
    let mut control = serve.control_session();
    control.no_list();
    control.stop();
    std::fs::write(&go, b"go\n").unwrap();
    let (frames, end) = serve.stream_through_end();
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
    serve.pull(&["sh", "-c", &script]);
    let mut control = serve.control_session();
    control.no_list();
    // 4 MiB, more than a pipe holds by default: the forward stream's spool
    // takes them all, and the decoder fills the command's stdin.
    let input = vec![b'x'; 4 << 20];
    let stream = forward_stream(&input, 0);
    serve.send_raw(&stream[..stream.len() - 5]);
    wait_until_asleep_in(serve.child.id(), "decoder", poll_syscall());
    std::fs::write(&go, b"go\n").unwrap();
    let (frames, end) = serve.stream_through_end();
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
    let (_, mut control) = serve.push(&["sh", "-c", &script]);
    let run = [&[0][..], &(1u64 << 40).to_le_bytes()].concat();
    let stream = stream_of(&[(RLE, &run)], 0);
    // Without STREAM_END: only the stop can end the decoder.
    serve.send_raw(&stream[..stream.len() - 5]);
    wait_until_asleep_in(serve.child.id(), "decoder", poll_syscall());
    std::fs::write(&go, b"go\n").unwrap();
    let (frames, end) = serve.stream_through_end();
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
    serve.settings.spool_limit = 64 << 10;
    // `yes` writes its first two bytes alone, and only then the rest in
    // large writes. In eager delivery, an idle point after those two bytes
    // can send them in a CHUNK of their own, ahead of the CHUNK that this
    // test waits for. In batch delivery, every CHUNK is full.
    serve.settings.delivery = Delivery::Batch;
    serve.push_request(&["yes"]);
    serve.recv_hello();
    let mut control = serve.control_session();
    // The forward stream is empty: without compression, STREAM_END alone.
    serve.send(STREAM_END, b"");
    wait_until_asleep_in(pid, "data-writer", libc::SYS_writev as u64);
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
    // The forward stream has ended, so `serve`'s side of the list exchange
    // ends with HASHNONE once its index is ready (DESIGN.md §5.6).
    assert_eq!(control.recv_exchange(), (HASHNONE, Vec::new()));
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
    serve.pull(&["tail", &until_the_test_ends, "-f", "/dev/null"]);
    let mut control = serve.control_session();
    control.no_list();
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
fn a_fatal_end_after_end_spares_the_commands_process_group() {
    // Once END has given the client the command's status, a fatal end of
    // `serve` leaves the command's process group alone, as its normal end
    // does (DESIGN.md §6.9). The command leaves a process in its group,
    // which reads a FIFO, and exits. After END, this test sends a frame
    // that cannot come then, a bug of the client, so `serve` aborts. The
    // process that the command left still reads the FIFO: had `serve`
    // killed the group, the write to the FIFO would wait for a reader
    // forever, and the test would hang.
    let mut serve = Serve::start();
    // The abort must leave no core file behind.
    let pid = Pid::from_raw(serve.child.id() as i32).unwrap();
    let no_core = rustix::process::Rlimit {
        current: Some(0),
        maximum: Some(0),
    };
    rustix::process::prlimit(Some(pid), rustix::process::Resource::Core, no_core).unwrap();
    let go = serve.home.path().join("go");
    mkfifoat(CWD, &go, Mode::RUSR | Mode::WUSR).unwrap();
    let script = format!("(read go <'{}') </dev/null >/dev/null 2>&1 &", go.display());
    serve.pull(&["sh", "-c", &script]);
    let mut control = serve.attach();
    control.write_all(&frame(HASHNONE, b"")).unwrap();
    let (_, end) = serve.stream_through_end();
    assert_eq!(end.outcome(), SUCCESS);
    // The main thread of `serve` spares the group right after its write of
    // END, without a wait in between, and then waits for the data reader
    // (step F1). So once every thread sleeps, the group is spared: a frame
    // sent before could make `serve` abort while the group is still
    // registered for the kill.
    wait_until_asleep(serve.child.id());
    serve.send(END, b"");
    let (status, stderr) = serve.finish();
    assert_eq!(status.signal(), Some(libc::SIGABRT), "stderr:\n{stderr}");
    assert!(
        stderr.contains("protocol error: unexpected data frame type"),
        "stderr:\n{stderr}"
    );
    std::fs::write(&go, b"go\n").unwrap();
}

#[test]
fn stdin_eof_after_end_is_the_normal_end() {
    // `serve` sends END once it needs nothing more from the client. Then it
    // waits, with every thread asleep, until its data reader reads the
    // stdin EOF that the client causes after END. It then shuts the
    // control session down, joins every thread, and exits with status 0
    // (DESIGN.md §6.9).
    let mut serve = Serve::start();
    serve.pull(&["printf", "%s", "output"]);
    let mut control = serve.attach();
    control.write_all(&frame(HASHNONE, b"")).unwrap();
    let (frames, end) = serve.stream_through_end();
    assert_eq!(frames, [(LIT, b"output".to_vec())]);
    assert_eq!(end.outcome(), SUCCESS);
    // After the version and the features, the STOP of the command's exit,
    // in the control stream, which comes before END, and before the
    // closure of the session that the end of the data session allows
    // (DESIGN.md §6.6).
    let stop = frame(CHUNK, &frame(STOP, b""));
    let expected = [&version_and_features(FEATURES)[..], &stop].concat();
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
    serve.pull(&["sh", "-c", &script]);
    let mut control = serve.control_session();
    control.no_list();
    let (frames, end) = serve.stream_through_end();
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
    // its own index, which it updates while the command runs, and the
    // target is that index's list. The client queries the block at offset 4
    // on the control session; `serve` pins it and answers yes, so the client
    // sends REF, and the decoder writes the pinned block to the command's
    // stdin. `cat` sends it all back in the backward stream, which is plain
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
        serve.settings.level = level;
        let (list, mut control) = serve.push(&["cat"]);
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
            .stream()
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
    let (_, mut control) = serve.push(&["sh", "-c", &script]);
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
    let (frames, end) = serve.stream_through_end();
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
fn a_push_whose_stream_ends_during_the_update_sends_no_list() {
    // This test holds a shared lock on the index of `serve`, so the update
    // of a push waits, while the command runs. The forward stream ends
    // meanwhile, which puts the receiver into Completing: once its index is
    // ready, `serve` has no use for the list, so its side of the exchange
    // ends with HASHNONE in place of INDEX, and END follows (DESIGN.md
    // §5.6). The client's side ends with it, and sends nothing.
    let mut serve = Serve::start();
    serve.configure_push(&[("block", &indexed_block())]);
    let config = serve.home.path().join("config/hashsync/default.toml");
    let update = Command::new(env!("CARGO_BIN_EXE_hashsync"))
        .arg("--config")
        .arg(&config)
        .arg("update")
        .output()
        .unwrap();
    assert!(update.status.success(), "{update:?}");
    let lock = shared_lock(&serve.index());
    serve.push_request(&["cat"]);
    serve.recv_hello();
    let mut control = serve.control_session();
    wait_for_lock_wait(serve.child.id());
    serve.send_raw(&forward_stream(b"input", 0));
    // `cat` copies its stdin, and ends with it.
    assert_eq!(serve.stream().rest(), [(LIT, b"input".to_vec())]);
    drop(lock);
    assert_eq!(control.recv_exchange(), (HASHNONE, Vec::new()));
    assert_eq!(serve.recv_end().outcome(), SUCCESS);
    control.stop();
    serve.expect_success();
}

#[test]
fn client_loss_during_the_index_update_of_a_push_is_fatal() {
    // This test holds a shared lock on the index of `serve`, so the update
    // that a push starts waits for good. The data reader sees that the
    // client has disappeared, and so does the session watcher, which
    // starts at REQUEST (DESIGN.md §4.4 and §6.6).
    let mut serve = Serve::start();
    serve.configure_push(&[("block", &indexed_block())]);
    let config = serve.home.path().join("config/hashsync/default.toml");
    let update = Command::new(env!("CARGO_BIN_EXE_hashsync"))
        .arg("--config")
        .arg(&config)
        .arg("update")
        .output()
        .unwrap();
    assert!(update.status.success(), "{update:?}");
    let _lock = shared_lock(&serve.index());
    serve.push_request(&["true"]);
    serve.recv_hello();
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
        let (_, control) = serve.push(&["sh", "-c", &script]);
        serve.send_raw(&forward_stream(b"input", 0));
        let output: Vec<u8> = serve
            .stream()
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

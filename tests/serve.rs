//! `hashsync serve` driven directly through its standard streams and its
//! rendezvous socket, the way the receiver and `attach` drive it. The
//! receiver's disappearance is fatal at every stage before the normal
//! exit (DESIGN.md §6.7): during rendezvous, while `serve` waits for the
//! control session, and while command stderr still drains after END_ACK.
//! Queries and answers travel on the control session (DESIGN.md §6.2);
//! obsolete queries still unsent at the normal end do not fail it. After
//! HELLO, the data frames of `serve` form one zstd frame.

mod common;

use std::io::{BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::time::Duration;

use rustix::event::{PollFd, PollFlags, poll};
use rustix::io::retry_on_intr;
use rustix::process::{Pid, PidfdFlags, pidfd_open};
use rustix::thread::{CpuSet, sched_getaffinity, sched_setaffinity};
use tempfile::TempDir;

// The protocol constants and frame types of DESIGN.md §6.2, spelled out
// because the tests reach only the binary.
const PROTOCOL_VERSION: u32 = 1;
const WEAK_HASH_ID: &[u8] = b"poly64-9e3779b97f4a7c15-splitmix64";
const HELLO: u8 = 1;
const HASHLIST: u8 = 2;
const CACHE_RESULT: u8 = 6;
const LIT: u8 = 7;
const REF: u8 = 8;
const END: u8 = 10;
const END_ACK: u8 = 11;
const QUERY: u8 = 12;
const ANSWER: u8 = 13;

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

fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![kind];
    string(&mut out, payload);
    out
}

/// The first bytes of each session, in both directions.
fn version() -> [u8; 4] {
    PROTOCOL_VERSION.to_le_bytes()
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

/// One frame from `reader`.
fn recv_frame(reader: &mut impl Read) -> (u8, Vec<u8>) {
    let mut header = [0u8; 5];
    reader.read_exact(&mut header).unwrap();
    let length = u32::from_le_bytes(header[1..].try_into().unwrap());
    let mut payload = vec![0; length as usize];
    reader.read_exact(&mut payload).unwrap();
    (header[0], payload)
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

    /// Require the normal exit, with status 0.
    fn expect_success(mut self) {
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
    // finished when END_ACK arrives. The receiver's disappearance then
    // fails the run and kills the command's process group, descendant
    // included. The descendant runs until this test process ends: it does
    // not end by itself, however slow the machine is, and a failed test
    // does not leave it behind.
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
    // bytes go out as literals (DESIGN.md §6.3).
    for found in [true, false] {
        let mut serve = Serve::start();
        let block: Vec<u8> = (0..4096u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let file = serve.home.path().join("block");
        std::fs::write(&file, &block).unwrap();
        let list = encode_list(&[weak_hash(&block)]);
        serve.hello_with(4096, &list, &["cat", file.to_str().unwrap()], 0);
        let mut control = serve.attach();
        control.write_all(&version()).unwrap();
        serve.send_list(&list);

        let mut theirs = [0u8; 4];
        control.read_exact(&mut theirs).unwrap();
        assert_eq!(theirs, version());
        let (kind, query) = recv_frame(&mut control);
        assert_eq!(kind, QUERY);
        assert_eq!(query[..8], 0u64.to_le_bytes(), "the window at offset 0");
        assert_eq!(&query[8..], blake3::hash(&block).as_bytes());
        let mut answer = 0u64.to_le_bytes().to_vec();
        answer.push(found.into());
        control.write_all(&frame(ANSWER, &answer)).unwrap();

        serve.recv_result();
        let mut literal = Vec::new();
        let mut references = 0;
        loop {
            match serve.recv() {
                (LIT, payload) => literal.extend_from_slice(&payload),
                (REF, payload) => {
                    assert!(payload.is_empty());
                    references += 1;
                }
                (END, _) => break,
                (kind, _) => panic!("unexpected frame type {kind}"),
            }
        }
        if found {
            assert_eq!((references, literal.len()), (1, 0));
        } else {
            assert_eq!(references, 0);
            assert_eq!(literal, block);
        }
        serve.send(END_ACK, b"");
        serve.expect_success();
    }
}

#[test]
fn obsolete_queries_in_flight_do_not_fail_the_normal_end() {
    // Obsolete queries do not delay completion (DESIGN.md §6.3). Here they
    // are the last queries, and the receiver reads none of them, so the
    // control writer is still blocked on them when the normal end closes
    // the control socket. That failed write is expected closure (DESIGN.md
    // §6.7). On one CPU, the woken writer runs before the main thread can
    // exit, so a writer that took the closure as a failure would win.
    //
    // With 64-byte windows, the first scan queries 0 and the chain
    // w_k = 74 + 64k. Once 0 is rejected, the rescan queries 20 and the
    // chain q_k = 92 + 64k, which jumps over the w chain. Once 20 is
    // rejected, the next rescan follows the w chain, answered yes, and
    // jumps over the q chain: every q query is obsolete.
    const B: u64 = 64;
    const CHAIN: u64 = 8000;
    let w: Vec<u64> = (0..=CHAIN).map(|k| 74 + B * k).collect();
    let data = common::random((138 + B * CHAIN + 10) as usize, 21);
    let window = |offset: u64| &data[offset as usize..(offset + B) as usize];
    let q = (0..CHAIN).map(|k| 92 + B * k);
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
    let file = serve.home.path().join("output");
    std::fs::write(&file, &data).unwrap();
    serve.hello_with(B as u32, &list, &["cat", file.to_str().unwrap()], 0);
    let mut control = serve.attach();
    control.write_all(&version()).unwrap();
    serve.send_list(&list);
    let mut theirs = [0u8; 4];
    control.read_exact(&mut theirs).unwrap();
    assert_eq!(theirs, version());

    let next_query = |control: &mut UnixStream| {
        let (kind, query) = recv_frame(control);
        assert_eq!(kind, QUERY);
        u64::from_le_bytes(query[..8].try_into().unwrap())
    };
    let answer = |offset: u64, found: bool| {
        let mut payload = offset.to_le_bytes().to_vec();
        payload.push(found.into());
        frame(ANSWER, &payload)
    };
    assert_eq!(next_query(&mut control), 0);
    for &offset in &w {
        assert_eq!(next_query(&mut control), offset);
    }
    let mut answers: Vec<u8> = w.iter().flat_map(|&offset| answer(offset, true)).collect();
    answers.extend(answer(0, false));
    control.write_all(&answers).unwrap();
    assert_eq!(next_query(&mut control), 20);
    // Give the rescan time to queue the whole q chain, more than the
    // socket holds. A slower rescan only makes the test weaker. No check
    // can replace the pause: the only sign of the queued chain is its
    // queries, which must stay unread.
    std::thread::sleep(Duration::from_millis(300));
    control.write_all(&answer(20, false)).unwrap();

    serve.recv_result();
    let mut output = Vec::new();
    loop {
        match serve.recv() {
            (LIT, payload) => output.extend_from_slice(&payload),
            (REF, _) => output.extend_from_slice(window(output.len() as u64)),
            (END, _) => break,
            (kind, _) => panic!("unexpected frame type {kind}"),
        }
    }
    assert!(output == data, "the output differs");
    serve.send(END_ACK, b"");
    serve.expect_success();
}

#[test]
fn compressed_frames_form_one_zstd_frame_that_ends_with_end() {
    // After HELLO, the data frames share one zstd frame with a checksum,
    // which ends right after END, and nothing follows it (DESIGN.md §6.2).
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
    let mut rest = Vec::new();
    transport.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty(), "bytes after the zstd frame");
    serve.expect_success();
}

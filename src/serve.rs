//! `hashsync serve --token T` (DESIGN.md §3, §5.6, §6): the sender.
//!
//! `serve` first listens on the rendezvous socket, exchanges protocol
//! versions and data HELLOs, accepts `attach`, and completes the list
//! exchange and CACHE_RESULT before starting the command. The scanner then
//! turns the command's stdout into literals, runs, and references. After
//! its HELLO, this side's data frames share one zstd stream unless
//! compression is disabled (DESIGN.md §6.2).
//!
//! Each incoming direction has one owner for the whole run, so session
//! loss is fatal at any moment — during rendezvous, setup, startup
//! failure, and transfer alike (DESIGN.md §6.7). The data reader owns
//! stdin and walks the receiver's fixed sequence: the protocol version,
//! HELLO, HASHLIST and the list bytes, END_ACK, closure. The control reader
//! owns the control socket from acceptance: the protocol version, then
//! answers. The main thread writes the setup frames, then gives the data
//! session to the data writer and runs the scanner (DESIGN.md §6.6): it
//! takes command output from the output reader and answers from the
//! control reader, lends long stretches of windows to the probe threads,
//! and queues queries for the control writer and frames for the data
//! writer. Command success and startup failure share one completion path.

use std::io::{BufReader, BufWriter, Read, Write};
use std::net::Shutdown;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

use bstr::ByteSlice;
use crossbeam_channel::{Receiver, Select, Sender, TryRecvError, bounded, unbounded};
use rustix::process::Pid;

use crate::cache::{self, ListStore, PublishedList};
use crate::die::{self, die};
use crate::fsops::{self, FdWriter, retry_interrupted};
use crate::hash;
use crate::hashlist::{self, ListId};
use crate::hashtable::HashTable;
use crate::protocol::{
    self, Answer, COMPRESSION_LEVELS, Compressor, End, FrameReader, FrameWriter, HashList, Query,
    ReceiverHello, Rle, SenderHello, frame,
};
use crate::rendezvous;
use crate::scanner::{Frame, Scanner};
use crate::sys;

/// The read size of the output reader.
const READ_CHUNK: usize = 256 << 10;

/// The size of the data session's write buffer.
const WRITE_BUFFER: usize = 256 << 10;

/// The data queue holds `max(1, QUEUED_BYTES / B)` frames; each frame
/// holds at most `B` bytes.
const QUEUED_BYTES: usize = 4 << 20;

/// The data session after HELLO: frames through the compression stream.
type DataWriter = FrameWriter<Compressor<BufWriter<FdWriter>>>;

/// What the data writer sends, in order.
enum Output {
    Scanned(Frame),
    End(End),
}

pub fn run(token: &str) -> ! {
    // Listen before anything else, so `attach`'s retries can end.
    let listener = rendezvous::listen(token);

    let completing = Arc::new(AtomicBool::new(false));
    let end_sent = Arc::new(AtomicBool::new(false));
    let (hello_tx, hello_rx) = bounded(1);
    let (list_tx, list_rx) = bounded(1);
    let (ack_tx, ack_rx) = bounded(1);
    {
        let end_sent = Arc::clone(&end_sent);
        let completing = Arc::clone(&completing);
        std::thread::Builder::new()
            .name("data-reader".into())
            .spawn(move || {
                let data_in = FrameReader::new(BufReader::new(fsops::stdin()));
                data_reader(data_in, hello_tx, list_tx, ack_tx, &end_sent, &completing)
            })
            .expect("cannot start the data reader");
    }
    let mut data_out = FrameWriter::new(BufWriter::with_capacity(WRITE_BUFFER, fsops::stdout()));

    // This side's protocol version goes first, without waiting for the
    // receiver's, which the data reader checks (DESIGN.md §6.2).
    if let Err(e) = data_out.send_version().and_then(|()| data_out.flush()) {
        die(format!("data session: {e}"));
    }

    // Data HELLOs: the receiver speaks first, this side replies only
    // after it accepted the receiver's parameters.
    let hello: ReceiverHello = hello_rx.recv().expect("the data reader ended before HELLO");
    validate_hello(&hello);
    let reply = SenderHello {
        account_id: cache::account_id(),
        // Cached-list reuse arrives with PLAN.md step 8: advertise
        // explicit absence and expect a full list.
        base: None,
    };
    // The receiver needs HELLO to send the list.
    send_data(&mut data_out, frame::HELLO, &reply.encode());
    flush_data(&mut data_out);
    // After HELLO, this side's data frames share one zstd stream unless
    // compression is disabled. Output latency does not matter, so the
    // stream is flushed only before this side waits for a reply, and
    // finished after END (DESIGN.md §6.2).
    let mut data_out = FrameWriter::new(Compressor::new(
        data_out.into_inner(),
        hello.compression_level,
        hello.compression_long,
        thread_count(hello.compression_threads),
    ));

    // The control session: accept `attach`, then both sides send their
    // protocol version immediately. The control reader owns the incoming
    // direction; it reports readiness once the receiver's version matched,
    // then forwards answers.
    let control = listener.accept();
    {
        let mut control_out = FrameWriter::new(&control);
        if let Err(e) = control_out
            .send_version()
            .and_then(|()| control_out.flush())
        {
            die(format!("control session: {e}"));
        }
    }
    let (control_ready_tx, control_ready_rx) = bounded(1);
    let (answer_tx, answer_rx) = unbounded();
    {
        let completing = Arc::clone(&completing);
        let control_in = clone_control(&control);
        std::thread::Builder::new()
            .name("control-reader".into())
            .spawn(move || read_answers(control_in, control_ready_tx, answer_tx, &completing))
            .expect("cannot start the control reader");
    }

    // The list, assembled and digest-verified by the data reader; then the
    // canonical decoding and the table (DESIGN.md §5.6).
    let encoded: Vec<u8> = list_rx
        .recv()
        .expect("the data reader ended before the hash list");
    let values = hashlist::decode(&encoded)
        .unwrap_or_else(|e| panic!("protocol error: bad full hash list: {e}"));
    let table = HashTable::build(&values);
    drop(values);

    // Durable publication, or cleanup with caching disabled; failures
    // are fatal inside the cache module (DESIGN.md §5.6). Acknowledge
    // the accepted target only afterwards, then renew and expire.
    let store = ListStore::sender_side(&hello.account_id, &hello.index_id);
    let published: Option<PublishedList> = if hello.sender_cache {
        let target = ListId {
            algorithm: hello.strong_hash,
            digest: hello.target,
        };
        Some(store.publish(&target, &encoded))
    } else {
        store.remove_all();
        None
    };
    drop(encoded);
    // The receiver does not reply to CACHE_RESULT, so it can wait in the
    // stream for the command's output.
    send_data(&mut data_out, frame::CACHE_RESULT, b"");
    if let Some(list) = &published {
        list.renew();
    }
    cache::expire(hello.hashlist_max_age);

    // The receiver's versions on both sessions must match before the
    // command starts (DESIGN.md §6.2). Both sessions stay monitored
    // during this wait: the data reader awaits END_ACK on stdin and the
    // control reader holds the socket.
    control_ready_rx
        .recv()
        .expect("the control reader ended before the protocol version");

    // From here on, the data writer owns the data session's outgoing
    // direction and the control writer the control session's.
    let queued = (QUEUED_BYTES / hello.block_size as usize).max(1);
    let (output_tx, output_rx) = bounded(queued);
    std::thread::Builder::new()
        .name("data-writer".into())
        .spawn(move || write_output(data_out, &output_rx))
        .expect("cannot start the data writer");
    let (query_tx, query_rx) = unbounded();
    {
        let completing = Arc::clone(&completing);
        let control_out = clone_control(&control);
        std::thread::Builder::new()
            .name("control-writer".into())
            .spawn(move || write_queries(control_out, &query_rx, &completing))
            .expect("cannot start the control writer");
    }

    // The scanner's probe threads (DESIGN.md §6.3) start before the
    // command.
    let probe_pool = probe_pool(hello.probe_threads);

    // The command. Keep it unreaped and in its own process group
    // (DESIGN.md §6.6); a fatal exit SIGKILLs the group, and the spawn
    // is atomic with that registration.
    sys::reset_sigchld();
    let argv: Vec<&std::ffi::OsStr> = hello
        .command
        .iter()
        .map(|arg| std::ffi::OsStr::from_bytes(arg))
        .collect();
    let mut command = Command::new(argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let (status, error, stderr_relay) =
        match die::spawn_guarded(die::KillGuard::ProcessGroup, &mut command) {
            Ok(mut child) => {
                // Command stderr to this process's stderr; SSH forwards
                // it outside the protocol. The relay must finish before
                // successful exit.
                let mut command_stderr = child.stderr.take().unwrap();
                let relay = std::thread::Builder::new()
                    .name("stderr-relay".into())
                    .spawn(move || relay_stderr(&mut command_stderr))
                    .expect("cannot start the stderr relay");

                // Stdout EOF and the exit status are separate conditions;
                // the scan needs the first, END the second (DESIGN.md
                // §6.3).
                let command_stdout = child.stdout.take().unwrap();
                let (chunk_tx, chunk_rx) = bounded(1);
                std::thread::Builder::new()
                    .name("output-reader".into())
                    .spawn(move || read_output(command_stdout, &chunk_tx))
                    .expect("cannot start the output reader");
                let mut scanner = Scanner::new(
                    table,
                    hello.block_size,
                    hello.strong_hash,
                    hello.lookahead,
                    hello.negative_cache,
                    probe_pool.as_ref(),
                );
                transfer(&mut scanner, &chunk_rx, &answer_rx, &output_tx, &query_tx);
                let status = sys::wait_exit_status(Pid::from_child(&child));
                (status, String::new(), Some(relay))
            }
            // Startup failure (DESIGN.md §6.2): status 127 and the error
            // text; there are no command streams to drain.
            Err(e) => (127, format!("{}: {e}", argv[0].as_bytes().as_bstr()), None),
        };
    // No more queries: the control writer can end.
    drop(query_tx);

    // The completion conditions of DESIGN.md §6.7, one path for command
    // success and startup failure: END, one valid END_ACK, and a
    // finished stderr relay when command streams exist. Set `end_sent`
    // first so an immediate acknowledgement is valid.
    end_sent.store(true, SeqCst);
    output_tx
        .send(Output::End(End { status, error }))
        .expect("the data writer ended before END");
    ack_rx
        .recv()
        .expect("the data reader ended without acknowledgement");
    if let Some(relay) = stderr_relay {
        relay.join().expect("the stderr relay panicked");
    }

    // Normal cleanup once (DESIGN.md §6.7): close the control socket so
    // `attach` flushes and exits zero, then end normally, which removes
    // the rendezvous socket. Sender stdout stays open until process exit.
    // From here on, both control threads expect the closure: the writer
    // can still have obsolete queries to send.
    completing.store(true, SeqCst);
    let _ = control.shutdown(Shutdown::Both);
    die::finish(0)
}

/// `requested` threads, or half the cores, at least one, for 0: the
/// count of probe threads and of compression threads (DESIGN.md §7).
fn thread_count(requested: u32) -> u32 {
    match requested {
        0 => std::thread::available_parallelism()
            .map_or(1, |n| u32::try_from(n.get() / 2).unwrap_or(u32::MAX).max(1)),
        n => n,
    }
}

/// The threads that probe the scanner's long stretches of windows
/// (DESIGN.md §6.3). One thread needs no pool: the scanner probes on its
/// own thread.
fn probe_pool(requested: u32) -> Option<rayon::ThreadPool> {
    let threads = thread_count(requested) as usize;
    (threads > 1).then(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("probe{i}"))
            .build()
            .expect("cannot start the probe pool")
    })
}

/// Run the scanner until the command's output is emitted (DESIGN.md
/// §6.3). Each round takes the waiting answers first, then command output
/// while the scanner's buffer has room, then scans and queues ready frames
/// while the data queue has room. A round that moves nothing waits for an
/// answer, command output, or room in the data queue.
fn transfer(
    scanner: &mut Scanner,
    chunks: &Receiver<Vec<u8>>,
    answers: &Receiver<Answer>,
    output: &Sender<Output>,
    queries: &Sender<Query>,
) {
    let mut reading = true;
    loop {
        let mut progress = false;
        for answer in answers.try_iter() {
            scanner.answer(answer.offset, answer.found);
            progress = true;
        }
        while reading && scanner.has_room() {
            match chunks.try_recv() {
                Ok(chunk) => scanner.push(&chunk),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    scanner.end_input();
                    reading = false;
                }
            }
            progress = true;
        }
        progress |= scanner.scan(&mut |offset, strong| {
            // The control writer ends only after the scanner.
            let _ = queries.send(Query { offset, strong });
        });
        // This thread is the queue's only producer, so a queue with room
        // keeps it. Only a queue that is full before this attempt can hold
        // back a ready frame, so only then does the wait include room.
        let full = output.is_full();
        progress |= queue_frames(scanner, output);
        if progress {
            continue;
        }
        if scanner.finished() {
            return;
        }
        let mut select = Select::new();
        select.recv(answers);
        if reading && scanner.has_room() {
            select.recv(chunks);
        }
        if full {
            select.send(output);
        }
        select.ready();
    }
}

/// Queue the scanner's ready frames while the data queue has room. A frame
/// leaves the scanner, which advances `e`, only when the queue can take it
/// (DESIGN.md §6.3). True when a frame was queued.
fn queue_frames(scanner: &mut Scanner, output: &Sender<Output>) -> bool {
    let mut queued = false;
    while !output.is_full()
        && let Some(frame) = scanner.next_frame()
    {
        // The only producer found room.
        output
            .try_send(Output::Scanned(frame))
            .expect("the data queue refused a frame");
        queued = true;
    }
    queued
}

/// Read the command's stdout into chunks for the scanner, until EOF closes
/// the queue. The bounded queue pauses reading while the scanner's buffer
/// is full.
fn read_output(mut stdout: ChildStdout, chunks: &Sender<Vec<u8>>) {
    let mut buf = vec![0u8; READ_CHUNK];
    loop {
        match retry_interrupted(|| stdout.read(&mut buf)) {
            Ok(0) => return,
            Ok(n) => {
                if chunks.send(buf[..n].to_vec()).is_err() {
                    return;
                }
            }
            Err(e) => die(format!("cannot read the command's output: {e}")),
        }
    }
}

/// The data session's outgoing direction after setup: frames in order,
/// then END, which finishes the stream (DESIGN.md §6.2). Nothing flushes
/// the stream before END: the receiver replies to no frame before it.
fn write_output(mut data_out: DataWriter, output: &Receiver<Output>) {
    loop {
        match output.recv().expect("the scanner ended before END") {
            Output::Scanned(Frame::Literal(bytes)) => send_data(&mut data_out, frame::LIT, &bytes),
            Output::Scanned(Frame::Reference) => send_data(&mut data_out, frame::REF, b""),
            Output::Scanned(Frame::Run { byte, length }) => {
                send_data(&mut data_out, frame::RLE, &Rle { byte, length }.encode());
            }
            Output::End(end) => {
                send_data(&mut data_out, frame::END, &end.encode());
                // Sender stdout stays open until process exit.
                if let Err(e) = data_out.into_inner().finish() {
                    die(format!("data session: {e}"));
                }
                return;
            }
        }
    }
}

/// The control session's outgoing direction during the transfer: queries
/// in order, flushed whenever the queue runs empty. Obsolete queries do
/// not delay completion (DESIGN.md §6.3), so normal cleanup can close the
/// socket while this thread still has some to send. A failed write is
/// then expected closure, not a failure.
fn write_queries(stream: UnixStream, queries: &Receiver<Query>, completing: &AtomicBool) {
    let mut control_out = FrameWriter::new(BufWriter::new(stream));
    loop {
        let result = match control_out.next_from(queries) {
            Ok(Some(query)) => control_out.send(frame::QUERY, &query.encode()),
            Ok(None) => return,
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            if !completing.load(SeqCst) {
                die(format!("control session: {e}"));
            }
            return;
        }
    }
}

/// The single owner of the data session's incoming direction, for the
/// whole run: the protocol version, HELLO, the list, END_ACK, then
/// closure. EOF before the normal exit is the receiver's disappearance
/// and is fatal (DESIGN.md §6.7).
fn data_reader<R: Read>(
    mut data_in: FrameReader<R>,
    hello_tx: Sender<ReceiverHello>,
    list_tx: Sender<Vec<u8>>,
    ack_tx: Sender<()>,
    end_sent: &AtomicBool,
    completing: &AtomicBool,
) {
    match data_in.version() {
        Ok(Some(version)) => protocol::check_version(version, "receiver"),
        Ok(None) => die("the receiver disappeared"),
        Err(e) => die_data(&e),
    }
    let payload = data_in
        .expect(frame::HELLO, "HELLO")
        .unwrap_or_else(|e| die_data(&e));
    let hello = ReceiverHello::decode(payload);
    let target = ListId {
        algorithm: hello.strong_hash,
        digest: hello.target,
    };
    let _ = hello_tx.send(hello);

    // The receiver sends the list only after this side's HELLO reply;
    // reading ahead of the main thread is safe because this thread only
    // validates and never replies.
    let encoded = receive_list(&mut data_in, target);
    let _ = list_tx.send(encoded);

    // END_ACK is the receiver's only remaining data-session frame
    // (DESIGN.md §6.2), and it must answer a sent END.
    match data_in.next() {
        Ok(Some(received)) => {
            assert!(
                received.kind == frame::END_ACK,
                "protocol error: expected END_ACK, received frame type {}",
                received.kind
            );
            assert!(
                received.payload.is_empty(),
                "protocol error: END_ACK with a payload"
            );
            assert!(end_sent.load(SeqCst), "protocol error: unsolicited END_ACK");
        }
        Ok(None) => die("the receiver disappeared"),
        Err(e) => die_data(&e),
    }
    let _ = ack_tx.send(());

    // Nothing else may arrive, and the receiver keeps the session open
    // until this process exits: closure before the normal exit remains
    // fatal, so a receiver lost while command stderr still drains kills
    // the command instead of leaving the run half-alive.
    match data_in.next() {
        Ok(Some(extra)) => {
            panic!("protocol error: frame type {} after END_ACK", extra.kind)
        }
        Ok(None) | Err(_) => {
            if !completing.load(SeqCst) {
                die("the receiver disappeared after END_ACK");
            }
        }
    }
}

/// The owner of the control session's incoming direction: the protocol
/// version, whose check gates command startup, then answers for the
/// scanner until closure, which is fatal before the normal exit.
fn read_answers(
    stream: UnixStream,
    ready_tx: Sender<()>,
    answers: Sender<Answer>,
    completing: &AtomicBool,
) {
    let mut control_in = FrameReader::new(BufReader::new(stream));
    match control_in.version() {
        Ok(Some(version)) => protocol::check_version(version, "receiver's control session"),
        Ok(None) => die("the control session closed before the protocol version"),
        Err(e) => die(format!("control session: {e}")),
    }
    let _ = ready_tx.send(());
    loop {
        match control_in.next() {
            Ok(Some(received)) => {
                assert!(
                    received.kind == frame::ANSWER,
                    "protocol error: unexpected control frame type {}",
                    received.kind
                );
                // Answers that arrive after the scanner finished change
                // nothing.
                let _ = answers.send(Answer::decode(received.payload));
            }
            Ok(None) | Err(_) => {
                if !completing.load(SeqCst) {
                    die("the control session closed unexpectedly");
                }
                return;
            }
        }
    }
}

/// Copy command stderr to this process's stderr through EOF, with
/// prompt writes. Failures are fatal (DESIGN.md §6.5).
fn relay_stderr(command_stderr: &mut std::process::ChildStderr) {
    let mut stderr = fsops::stderr();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match retry_interrupted(|| command_stderr.read(&mut buf)) {
            Ok(0) => return,
            Ok(n) => {
                if let Err(e) = stderr.write_all(&buf[..n]) {
                    die(format!("stderr relay: write: {e}"));
                }
            }
            Err(e) => die(format!("stderr relay: read: {e}")),
        }
    }
}

fn die_data(e: &std::io::Error) -> ! {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        die("the receiver disappeared");
    }
    die(format!("data session: {e}"))
}

fn send_data<W: Write>(out: &mut FrameWriter<W>, kind: u8, payload: &[u8]) {
    if let Err(e) = out.send(kind, payload) {
        die(format!("data session: {e}"));
    }
}

fn flush_data<W: Write>(out: &mut FrameWriter<W>) {
    if let Err(e) = out.flush() {
        die(format!("data session: {e}"));
    }
}

fn clone_control(control: &UnixStream) -> UnixStream {
    match control.try_clone() {
        Ok(clone) => clone,
        Err(e) => die(format!("cannot clone the control socket: {e}")),
    }
}

/// The receiver's HELLO parameters are the run's: use them, or end the
/// run before command startup (DESIGN.md §6.2, §6.5). Decoding already
/// checked the strong hash.
fn validate_hello(hello: &ReceiverHello) {
    if hello.weak_hash_id != hash::weak::ID {
        die(format!(
            "incompatible receiver: weak hash {} (this sender: {})",
            hello.weak_hash_id,
            hash::weak::ID
        ));
    }
    if !COMPRESSION_LEVELS.contains(&hello.compression_level) {
        die(format!(
            "incompatible receiver: compression level {} (this sender supports {} to {})",
            hello.compression_level,
            COMPRESSION_LEVELS.start(),
            COMPRESSION_LEVELS.end()
        ));
    }
}

/// Receive the full canonical list: HASHLIST, then the list bytes that
/// follow it directly, verified against the target digest
/// (DESIGN.md §5.6).
fn receive_list<R: Read>(data_in: &mut FrameReader<R>, target: ListId) -> Vec<u8> {
    let payload = data_in
        .expect(frame::HASHLIST, "HASHLIST")
        .unwrap_or_else(|e| die_data(&e));
    let header = HashList::decode(payload);
    let encoded = data_in
        .bytes(header.length)
        .unwrap_or_else(|e| die_data(&e));
    assert!(
        target.algorithm.digest(&encoded) == target.digest,
        "protocol error: the full hash list does not match {target}"
    );
    encoded
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::hash::weak::WeakHasher;
    use crate::hash::{Strong, StrongHash};
    use crate::scanner::tests::noise;

    const BS: u32 = 64;

    /// `transfer` with a data queue of one frame, which the data writer
    /// drains slowly, while the lookups answer from another thread and
    /// reject the stale blocks. The output stays exact and in order, and
    /// the transfer ends (DESIGN.md §6.3). The wait has no deadline: a
    /// transfer that stalls hangs the test, and a debugger can inspect it.
    #[test]
    fn a_full_data_queue_keeps_the_output_exact() {
        let valid: Vec<Vec<u8>> = (0..4).map(|i| noise(BS as usize, i)).collect();
        let stale: Vec<Vec<u8>> = (4..6).map(|i| noise(BS as usize, i)).collect();
        let listed: Vec<&Vec<u8>> = valid.iter().chain(&stale).collect();
        let mut input = Vec::new();
        for i in 0..1000 {
            input.extend_from_slice(&noise(i % 11, 100 + i as u64));
            input.extend_from_slice(listed[i % listed.len()]);
            // Equal bytes, now and then enough for runs.
            input.extend(std::iter::repeat_n(i as u8, i % 7 * 50));
        }
        let weak = WeakHasher::new(BS as usize);
        let mut values: Vec<u64> = listed.iter().map(|b| weak.hash(b)).collect();
        values.sort_unstable();
        values.dedup();
        let blocks: HashMap<Strong, Vec<u8>> = valid
            .iter()
            .map(|b| (StrongHash::Blake3.digest(b), b.clone()))
            .collect();

        for lookahead in [u64::from(BS), 64 * u64::from(BS)] {
            let (chunk_tx, chunk_rx) = bounded(1);
            let (answer_tx, answer_rx) = unbounded();
            let (output_tx, output_rx) = bounded(1);
            let (query_tx, query_rx) = unbounded::<Query>();
            let (done_tx, done_rx) = bounded(1);
            let pins = Arc::new(Mutex::new(HashMap::new()));

            let command = input.clone();
            std::thread::spawn(move || {
                for chunk in command.chunks(100) {
                    chunk_tx.send(chunk.to_vec()).unwrap();
                }
            });
            // A lookup publishes the pin before it answers yes.
            let (lookup_pins, blocks) = (Arc::clone(&pins), blocks.clone());
            std::thread::spawn(move || {
                for (i, query) in query_rx.iter().enumerate() {
                    if i % 3 == 0 {
                        std::thread::sleep(Duration::from_micros(50));
                    }
                    let found = match blocks.get(&query.strong) {
                        Some(block) => {
                            let mut pins = lookup_pins.lock().unwrap();
                            pins.insert(query.offset, block.clone());
                            true
                        }
                        None => false,
                    };
                    let _ = answer_tx.send(Answer {
                        offset: query.offset,
                        found,
                    });
                }
            });
            std::thread::spawn(move || {
                let mut output = Vec::new();
                for (i, next) in output_rx.iter().enumerate() {
                    if i % 7 == 0 {
                        std::thread::sleep(Duration::from_micros(200));
                    }
                    match next {
                        Output::Scanned(Frame::Literal(bytes)) => output.extend_from_slice(&bytes),
                        Output::Scanned(Frame::Reference) => {
                            let c = output.len() as u64;
                            output.extend_from_slice(&pins.lock().unwrap()[&c]);
                        }
                        Output::Scanned(Frame::Run { byte, length }) => {
                            output.extend(std::iter::repeat_n(byte, length as usize));
                        }
                        Output::End(_) => break,
                    }
                }
                done_tx.send(output).unwrap();
            });
            let table = HashTable::build(&values);
            std::thread::spawn(move || {
                let mut scanner =
                    Scanner::new(table, BS, StrongHash::Blake3, lookahead, 1 << 20, None);
                transfer(&mut scanner, &chunk_rx, &answer_rx, &output_tx, &query_tx);
                let end = End {
                    status: 0,
                    error: String::new(),
                };
                output_tx.send(Output::End(end)).unwrap();
            });
            let output = done_rx
                .recv()
                .expect("the data writer ended without the output");
            assert!(output == input, "lookahead {lookahead}: the output differs");
        }
    }
}

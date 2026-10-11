//! `hashsync serve --token T` (DESIGN.md §3, §5.6, §6): the sender.
//!
//! `serve` first listens on the rendezvous socket, exchanges protocol
//! versions and data HELLOs, accepts `attach`, and completes the list
//! exchange and CACHE_RESULT before starting the command. The scanner then
//! turns the command's stdout into literals, runs, and references. After
//! its HELLO, this side's data frames share one zstd stream unless
//! compression is disabled, and so do the queries and the answers on the
//! control session after the features (DESIGN.md §6.2).
//!
//! Each incoming direction has one owner for the whole run, so session
//! loss is fatal at any moment — during rendezvous, setup, startup
//! failure, and transfer alike (DESIGN.md §6.9). The data reader owns
//! stdin and walks the receiver's fixed sequence: the protocol version and
//! the features, HELLO, HASHLIST and the list bytes, the input, END_ACK,
//! closure. From acceptance, the control reader owns the incoming
//! direction of the control socket: the protocol version and the features,
//! then answers. The control writer owns the outgoing one: the protocol
//! version and the features, then queries, and INPUT_STOP. The main
//! thread writes the setup frames, then gives the data session to the data
//! writer and runs the scanner (DESIGN.md §6.8): it takes command output
//! from the output reader and answers from the control reader, lends long
//! stretches of windows to the probe threads, and queues queries for the
//! control writer and frames for the data writer. The command's stdout
//! goes through a spool, so the command waits for the scanner only when
//! `sender_output_spool_limit` bytes wait for it (DESIGN.md §6.5). In eager
//! delivery of the output, the spool also marks the idle points of the
//! command's stdout, the last one at its EOF, which the output reader
//! passes to the scanner: the bytes before each one then reach the receiver
//! without waiting for more output, because the data writer flushes the
//! stream behind their frames (DESIGN.md §6.2 and §6.3). The receiver's
//! stdin arrives in INPUT frames, which the data reader appends to the
//! input spool, and the stdin writer decompresses that spool into the
//! command's stdin (DESIGN.md §6.6). Command success and startup failure
//! share one completion path.
//!
//! The normal end joins every thread (DESIGN.md §6.9). After the cleanup,
//! DONE tells the receiver that this side needs neither session any more,
//! and the receiver then closes both stdin pipes. The data reader monitors
//! the receiver until that EOF, and the main thread joins it last. Until
//! DONE, stdin EOF means that the receiver disappeared. While the data
//! reader waits at the limit of the input spool, it reads nothing, so
//! until the stop of the input, the session watcher of the input monitors
//! stdin too. The input has no use any more once no process reads the
//! command's stdin, or once the command has exited. Then the input stops,
//! which INPUT_STOP tells the receiver, and the data reader discards the
//! INPUT frames that still arrive before END_ACK.

use std::io::{BufReader, BufWriter, Read, Write};
use std::net::Shutdown;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU8, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use bstr::ByteSlice;
use crossbeam_channel::{Receiver, Select, Sender, TryRecvError, bounded, unbounded};
use rustix::process::Pid;

use crate::cache::{self, ListStore, PublishedList};
use crate::config::{self, thread_count};
use crate::die::{self, die};
use crate::fsops::{self, IdlePointReader, retry_interrupted};
use crate::hash;
use crate::hashlist::{self, ListId};
use crate::hashtable::HashTable;
use crate::input::SenderInput;
use crate::pool::Pool;
use crate::protocol::{
    self, Answer, COMPRESSION_LEVELS, Compressor, Decompressor, Delivery, End, FrameReader,
    FrameWriter, HashList, Query, ReceiverHello, Rle, SenderControl, SenderHello, flush_data,
    frame, send_data,
};
use crate::rendezvous;
use crate::scanner::{Frame, Scanner};
use crate::segments::Directory;
use crate::spool::{Spool, SpoolWriter, Taken};
use crate::sys;

/// The read size of the output reader.
const READ_CHUNK: usize = 256 << 10;

/// The size of the data session's write buffer.
const WRITE_BUFFER: usize = 256 << 10;

/// The data queue holds `max(1, QUEUED_BYTES / B)` frames; each frame
/// holds at most `B` bytes.
const QUEUED_BYTES: usize = 4 << 20;

/// What the data writer sends, in order.
enum Output {
    Scanned(Frame),
    End(End),
}

/// What the output reader passes to the scanner, in order: the bytes of
/// the command's stdout, and its idle points (DESIGN.md §6.3).
enum Stdout {
    Bytes(Vec<u8>),
    Idle,
}

/// A new data queue of `capacity` items, from the scanner's driver to the
/// data writer.
fn data_queue(capacity: usize) -> (DataQueue, DataItems) {
    let (items_tx, items_rx) = bounded(capacity);
    let (wakes_tx, wakes_rx) = bounded(1);
    let idle_at = Arc::new(Mutex::new(0));
    let queue = DataQueue {
        items: items_tx,
        queued: 0,
        idle_at: Arc::clone(&idle_at),
        wakes: wakes_rx,
    };
    let items = DataItems {
        items: items_rx,
        taken: 0,
        idle_at,
        wakes: wakes_tx,
    };
    (queue, items)
}

/// The driver's end of the data queue. It queues the scanner's frames for
/// the data writer, and tells whether the writer is idle (DESIGN.md
/// §6.3): the writer waits for items, with every queued item written, and
/// the flush that an idle point asked for made. The tail before an idle
/// point leaves only then, when it can leave at once.
struct DataQueue {
    items: Sender<Output>,
    /// The items queued so far.
    queued: u64,
    /// The items that the writer had taken when it last became idle. A
    /// lock, not an atomic, holds the count: see `writer_idle`.
    idle_at: Arc<Mutex<u64>>,
    /// A token each time the writer becomes idle, so that the driver can
    /// wait for that.
    wakes: Receiver<()>,
}

impl DataQueue {
    fn is_full(&self) -> bool {
        self.items.is_full()
    }

    /// Queue `frame`. The queue must have room: the driver is its only
    /// producer, and found room.
    fn queue(&mut self, frame: Frame) {
        self.items
            .try_send(Output::Scanned(frame))
            .expect("the data queue refused a frame");
        self.queued += 1;
    }

    /// True when the writer is idle. A token in `wakes` only makes the
    /// driver look again: the count decides, so an old token does no harm.
    ///
    /// The look must not miss that the writer became idle, also when the
    /// writer's token found an old token in the channel and nothing else
    /// wakes the driver. The tokens are taken before the look, and the
    /// lock puts the look either after the writer's last store, which it
    /// then sees, or before it. In the second case, the writer's token, or
    /// an older one that the take missed, is still in the channel, and
    /// wakes the next wait. An atomic count gives no such order: a failed
    /// `try_send` only reads the channel, so it does not order the store
    /// before the look.
    fn writer_idle(&self) -> bool {
        while self.wakes.try_recv().is_ok() {}
        *self.idle_at.lock().unwrap() == self.queued
    }

    /// END, after the scanner's last frame.
    fn end(self, end: End) {
        self.items
            .send(Output::End(end))
            .expect("the data writer ended before END");
    }
}

/// The data writer's end of the data queue.
struct DataItems {
    items: Receiver<Output>,
    /// The items taken so far.
    taken: u64,
    idle_at: Arc<Mutex<u64>>,
    wakes: Sender<()>,
}

impl DataItems {
    /// The next item, if one waits.
    fn try_next(&mut self) -> Option<Output> {
        let item = self.items.try_recv().ok()?;
        self.taken += 1;
        Some(item)
    }

    /// Tell the driver that the writer is idle: it has written every item
    /// that it took, and made the flush that they asked for. Then wait for
    /// the next item.
    fn wait_next(&mut self) -> Output {
        *self.idle_at.lock().unwrap() = self.taken;
        // A full channel already holds a token for the driver.
        let _ = self.wakes.try_send(());
        let item = self.items.recv().expect("the scanner ended before END");
        self.taken += 1;
        item
    }
}

/// How far the end of the run has come (DESIGN.md §6.9). The main thread
/// moves it forward, one phase after the other. The reader and writer
/// threads read it to tell the frames and closures that the end expects
/// from failures.
#[derive(Clone, Copy)]
enum Phase {
    /// Before END: every closure is a failure.
    Running,
    /// END is on its way: one END_ACK can arrive.
    EndSent,
    /// The normal cleanup has begun: the closure of the control session is
    /// expected.
    Completing,
    /// DONE is on its way: stdin EOF is the normal end.
    DoneSent,
}

/// The phase that the threads share.
struct Progress(AtomicU8);

impl Progress {
    fn new() -> Self {
        Progress(AtomicU8::new(Phase::Running as u8))
    }

    /// Move on to `phase`, which must come after the current one.
    fn advance(&self, phase: Phase) {
        let previous = self.0.swap(phase as u8, SeqCst);
        assert!(previous < phase as u8, "the phase did not advance");
    }

    /// True when the run is in `phase` or in a later one.
    fn reached(&self, phase: Phase) -> bool {
        self.0.load(SeqCst) >= phase as u8
    }
}

pub fn run(token: &str) -> ! {
    // Listen before anything else, so `attach`'s retries can end.
    let listener = rendezvous::listen(token);

    let progress = Arc::new(Progress::new());
    let (hello_tx, hello_rx) = bounded(1);
    let (list_tx, list_rx) = bounded(1);
    let (input_tx, input_rx) = bounded(1);
    let (ack_tx, ack_rx) = bounded(1);
    // The first thread, and the last that the normal end joins: it reads
    // stdin until the EOF that the receiver causes after DONE.
    let data_reader = {
        let progress = Arc::clone(&progress);
        std::thread::Builder::new()
            .name("data-reader".into())
            .spawn(move || {
                let data_in = FrameReader::new(BufReader::new(fsops::stdin()));
                data_reader(data_in, hello_tx, list_tx, input_rx, ack_tx, &progress)
            })
            .expect("cannot start the data reader")
    };
    let mut data_out = FrameWriter::new(BufWriter::with_capacity(WRITE_BUFFER, fsops::stdout()));

    // This side's protocol version and features go first, without waiting
    // for the receiver's, which the data reader reads (DESIGN.md §6.2).
    if let Err(e) = data_out
        .send_version_and_features()
        .and_then(|()| data_out.flush())
    {
        die(format!("data session: {e}"));
    }

    // Data HELLOs: the receiver speaks first, this side replies only
    // after it accepted the receiver's parameters.
    let hello: ReceiverHello = hello_rx.recv().expect("the data reader ended before HELLO");
    validate_hello(&hello);
    // This side's tmp_data directory, for the spool of the command's
    // output. An unusable directory ends the run when it opens: here,
    // before the list exchange and command startup (DESIGN.md §6.5).
    let tmp_data = Arc::new(Directory::open(&config::default_tmp_data_dir()));
    // The queue of the control writer: the scanner's queries, and the
    // INPUT_STOP of the stop of the input (DESIGN.md §6.2).
    let (control_tx, control_rx) = unbounded();
    // The input spool, which the data reader fills from the INPUT frames
    // that follow the list exchange. Until the stop of the input, the
    // session watcher monitors stdin, the incoming direction of the data
    // session, which the data reader does not read while it waits at the
    // limit of the input spool (DESIGN.md §6.6).
    let (input_writer, mut input) = SenderInput::new(
        Arc::clone(&tmp_data),
        hello.sender_input_spool_limit,
        hello.compression_level != 0,
        rustix::stdio::stdin(),
        control_tx.clone(),
    );
    let _ = input_tx.send(input_writer);
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
    // compression is disabled. The stream is flushed before this side waits
    // for a reply, and in eager delivery of the output at the idle points
    // of the command's stdout, and finished after END (DESIGN.md §6.2).
    let mut data_out = FrameWriter::new(Compressor::new(
        data_out.into_inner(),
        hello.compression_level,
        hello.compression_long,
        thread_count(hello.compression_threads),
    ));

    // The control session: accept `attach`. Each direction has one thread,
    // from the protocol version on, and both sides send their version and
    // features at once (DESIGN.md §6.2). The control writer sends the
    // scanner's queries, and INPUT_STOP; the control reader reports
    // readiness once the receiver's version matched and its features
    // arrived, then forwards answers. After the features, each direction
    // is one zstd stream at the data session's level.
    let control = listener.accept();
    let control_writer = {
        let progress = Arc::clone(&progress);
        let control_out = clone_control(&control);
        let level = hello.compression_level;
        std::thread::Builder::new()
            .name("control-writer".into())
            .spawn(move || write_control(control_out, level, &control_rx, &progress))
            .expect("cannot start the control writer")
    };
    let (control_ready_tx, control_ready_rx) = bounded(1);
    let (answer_tx, answer_rx) = unbounded();
    let control_reader = {
        let progress = Arc::clone(&progress);
        let control_in = clone_control(&control);
        let compressed = hello.compression_level != 0;
        std::thread::Builder::new()
            .name("control-reader".into())
            .spawn(move || {
                read_answers(
                    control_in,
                    compressed,
                    control_ready_tx,
                    answer_tx,
                    &progress,
                )
            })
            .expect("cannot start the control reader")
    };

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
    // The receiver sends the input only after CACHE_RESULT, and the
    // command can wait for the input before it writes any output: so
    // CACHE_RESULT cannot wait in the stream for that output (DESIGN.md
    // §6.2).
    send_data(&mut data_out, frame::CACHE_RESULT, b"");
    flush_data(&mut data_out);
    if let Some(list) = &published {
        list.renew();
    }
    cache::expire(hello.hashlist_max_age);

    // The receiver's versions on both sessions must match before the
    // command starts (DESIGN.md §6.2). Both sessions stay monitored
    // during this wait: the control reader holds the socket, and the data
    // reader takes the input on stdin. While it waits at the limit of the
    // input spool, the session watcher monitors stdin.
    control_ready_rx
        .recv()
        .expect("the control reader ended before the protocol version");

    // From here on, the data writer owns the data session's outgoing
    // direction.
    let (mut data, items) = data_queue((QUEUED_BYTES / hello.block_size as usize).max(1));
    let data_writer = std::thread::Builder::new()
        .name("data-writer".into())
        .spawn(move || write_output(data_out, items))
        .expect("cannot start the data writer");

    // The scanner's probe threads (DESIGN.md §6.3) start before the
    // command.
    let probe_pool = start_probe_pool(hello.probe_threads);

    // The command. Keep it unreaped and in its own process group
    // (DESIGN.md §6.8); a fatal exit SIGKILLs the group, and the spawn
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
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let (status, error, stderr_relay) =
        match die::spawn_guarded(die::KillGuard::ProcessGroup, &mut command) {
            Ok(mut child) => {
                // The input to the command's stdin, until the command exits
                // at the latest (DESIGN.md §6.6).
                input.start(child.stdin.take().unwrap(), Pid::from_child(&child));
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
                // §6.3). Stdout goes through a spool, so the command does
                // not wait for the scanner until
                // `sender_output_spool_limit` bytes wait in the spool. In
                // eager delivery, the spool marks the idle points of
                // stdout (DESIGN.md §6.5).
                let command_stdout = child.stdout.take().unwrap();
                let source: Box<dyn Read + Send> = match hello.output_delivery {
                    Delivery::Eager => Box::new(IdlePointReader::new(command_stdout)),
                    Delivery::Batch => Box::new(command_stdout),
                };
                let output = Spool::start(
                    "the command's output",
                    source,
                    tmp_data,
                    hello.sender_output_spool_limit,
                );
                let (chunk_tx, chunk_rx) = bounded(1);
                let output_reader = std::thread::Builder::new()
                    .name("output-reader".into())
                    .spawn(move || read_output(output, &chunk_tx))
                    .expect("cannot start the output reader");
                let mut scanner = Scanner::new(
                    table,
                    hello.block_size,
                    hello.strong_hash,
                    hello.lookahead,
                    hello.negative_cache,
                    probe_pool.as_deref(),
                );
                transfer(&mut scanner, &chunk_rx, &answer_rx, &mut data, &control_tx);
                // The scan ended after the output reader closed its queue,
                // at the end of the spool, which it closed too.
                output_reader.join().expect("the output reader panicked");
                let status = sys::wait_exit_status(Pid::from_child(&child));
                (status, String::new(), Some(relay))
            }
            // Startup failure (DESIGN.md §6.2): status 127 and the error
            // text; there are no command streams to drain.
            Err(e) => (127, format!("{}: {e}", argv[0].as_bytes().as_bstr()), None),
        };
    // No more queries. The control writer can end once the stop of the
    // input has queued INPUT_STOP.
    drop(control_tx);
    // The input stops before END (DESIGN.md §6.6). After a startup
    // failure, this is the first stop. Else the command has exited, and the
    // exit watcher, or the stdin writer at `EPIPE`, stopped the input, or
    // is still at it: this stop then waits until the input is stopped. So
    // INPUT_STOP is queued before END, and the data reader discards the
    // INPUT frames that still arrive before END_ACK.
    let input = input.stop();

    // The completion conditions of DESIGN.md §6.9, one path for command
    // success and startup failure: END, one valid END_ACK, and a
    // finished stderr relay when command streams exist. Enter `EndSent`
    // first so an immediate acknowledgement is valid.
    progress.advance(Phase::EndSent);
    data.end(End { status, error });
    ack_rx
        .recv()
        .expect("the data reader ended without acknowledgement");
    // END_ACK follows the end of the stream: the data writer has no more
    // work. It gives back the data session for DONE.
    let mut data_out = data_writer.join().expect("the data writer panicked");
    // Like the stderr relay, the stdin writer of the input can wait for a
    // descendant of the command that holds one of its streams, so the
    // threads of the input are joined only now: END does not wait for that
    // descendant (DESIGN.md §6.9).
    input.join();
    if let Some(relay) = stderr_relay {
        relay.join().expect("the stderr relay panicked");
    }

    // Normal cleanup once (DESIGN.md §6.9): shut down the control socket,
    // join the control threads and the probe threads, send DONE, join the
    // data reader, then end normally, which removes the rendezvous socket.
    // Sender stdout stays open until process exit. From here on, both
    // control threads expect the closure: the writer can still have
    // obsolete queries to send. The reader reads EOF after the shutdown.
    // The writer ends when its queue is closed and empty or a write
    // fails.
    progress.advance(Phase::Completing);
    let _ = control.shutdown(Shutdown::Both);
    control_reader.join().expect("the control reader panicked");
    control_writer.join().expect("the control writer panicked");
    drop(control);
    drop(probe_pool);
    // DONE follows the zstd frame, outside it: this side needs neither
    // session any more, so the receiver closes both stdin pipes, and the
    // data reader reads EOF. Enter `DoneSent` first, so that an immediate
    // EOF is the normal end.
    progress.advance(Phase::DoneSent);
    send_data(&mut data_out, frame::DONE, b"");
    flush_data(&mut data_out);
    data_reader.join().expect("the data reader panicked");
    die::finish(0)
}

/// Start the threads that probe the scanner's long stretches of windows
/// (DESIGN.md §6.3): `requested` threads, or half the cores for 0. One
/// thread needs no pool: the scanner probes on its own thread.
fn start_probe_pool(requested: u32) -> Option<Pool> {
    let count = thread_count(requested) as usize;
    (count > 1).then(|| Pool::start("probe", count))
}

/// Run the scanner until the command's output is emitted (DESIGN.md
/// §6.3). Each round takes the waiting answers first, then command output
/// and its idle points while the scanner's buffer has room, then scans,
/// sends the tail before an idle point when the data writer is idle, and
/// queues ready frames while the data queue has room. A round that moves
/// nothing waits for an answer, command output, room in the data queue, or,
/// while a tail waits, an idle data writer. The queries go to `control`,
/// the queue of the control writer.
fn transfer(
    scanner: &mut Scanner,
    chunks: &Receiver<Stdout>,
    answers: &Receiver<Answer>,
    data: &mut DataQueue,
    control: &Sender<SenderControl>,
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
                Ok(Stdout::Bytes(chunk)) => scanner.push(&chunk),
                Ok(Stdout::Idle) => scanner.idle(),
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
            let _ = control.send(SenderControl::Query(Query { offset, strong }));
        });
        // The tail before an idle point leaves only when it can leave at
        // once. While the data writer is busy, as on a slow link, the tail
        // waits, and output that arrives meanwhile can fill its windows
        // (DESIGN.md §6.3).
        if scanner.tail_waits() && data.writer_idle() {
            scanner.send_tail();
            progress = true;
        }
        // This thread is the queue's only producer, so a queue with room
        // keeps it. Only a queue that is full before this attempt can hold
        // back a ready frame, so only then does the wait include room.
        let full = data.is_full();
        progress |= queue_frames(scanner, data);
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
            select.send(&data.items);
        }
        if scanner.tail_waits() {
            select.recv(&data.wakes);
        }
        select.ready();
    }
}

/// Queue the scanner's ready frames while the data queue has room. A frame
/// leaves the scanner, which advances `e`, only when the queue can take it
/// (DESIGN.md §6.3). True when a frame was queued.
fn queue_frames(scanner: &mut Scanner, data: &mut DataQueue) -> bool {
    let mut queued = false;
    while !data.is_full()
        && let Some(frame) = scanner.next_frame()
    {
        data.queue(frame);
        queued = true;
    }
    queued
}

/// Read the command's output from its spool into chunks for the scanner,
/// with its idle points in order, until the EOF of the spool. Then close
/// the spool, which joins its thread (DESIGN.md §6.9), and return, which
/// closes the queue. The bounded queue pauses reading while the scanner's
/// buffer is full; meanwhile, the output waits in the spool (DESIGN.md
/// §6.3).
fn read_output(mut output: Spool, chunks: &Sender<Stdout>) {
    let mut buf = vec![0u8; READ_CHUNK];
    loop {
        // A take never fails: the spool itself ends the run when it cannot
        // read the command's output or a segment.
        let next = match output.take_next(&mut buf) {
            Taken::Bytes(n) => Stdout::Bytes(buf[..n].to_vec()),
            Taken::Idle => Stdout::Idle,
            Taken::End => break,
        };
        // The scanner takes chunks until this thread closes the queue.
        chunks
            .send(next)
            .expect("the scanner ended before the command's output");
    }
    output.close();
}

/// The data session's outgoing direction after setup: frames in order,
/// then END, which finishes the stream (DESIGN.md §6.2). The receiver
/// replies to no frame before END, so only the idle points of eager
/// delivery flush the stream before it: once their frames are written, and
/// the writer has nothing left to write. Until then, the flush has no use,
/// because the bytes before it cannot leave sooner, and it would cost
/// compression. Returns the session after the stream, for DONE.
fn write_output<W: Write>(
    mut data_out: FrameWriter<Compressor<W>>,
    mut items: DataItems,
) -> FrameWriter<W> {
    // A flush that an idle point asked for.
    let mut flush = false;
    loop {
        let item = match items.try_next() {
            Some(item) => item,
            None => {
                if flush {
                    flush_data(&mut data_out);
                    flush = false;
                }
                items.wait_next()
            }
        };
        match item {
            Output::Scanned(Frame::Literal(bytes)) => send_data(&mut data_out, frame::LIT, &bytes),
            Output::Scanned(Frame::Reference) => send_data(&mut data_out, frame::REF, b""),
            Output::Scanned(Frame::Run { byte, length }) => {
                send_data(&mut data_out, frame::RLE, &Rle { byte, length }.encode());
            }
            Output::Scanned(Frame::Flush) => flush = true,
            Output::End(end) => {
                send_data(&mut data_out, frame::END, &end.encode());
                // Sender stdout stays open until process exit.
                let transport = data_out
                    .into_inner()
                    .finish()
                    .unwrap_or_else(|e| die(format!("data session: {e}")));
                return FrameWriter::new(transport);
            }
        }
    }
}

/// The owner of the control session's outgoing direction: this side's
/// protocol version and features, sent at once, without waiting for the
/// receiver's, then the frames of `queue` in order, queries and
/// INPUT_STOP, one zstd stream at `level`, 0 for none, flushed whenever
/// the queue runs empty, until the queue is closed and empty (DESIGN.md
/// §6.2). Obsolete queries do not delay completion (DESIGN.md §6.3), so
/// normal cleanup can close the socket while this thread still has some to
/// send. In Completing, a failed write is expected closure, not a failure
/// (DESIGN.md §6.9).
fn write_control(
    stream: UnixStream,
    level: i32,
    queue: &Receiver<SenderControl>,
    progress: &Progress,
) {
    let mut control_out = FrameWriter::new(BufWriter::new(stream));
    let mut result = control_out
        .send_version_and_features()
        .and_then(|()| control_out.flush());
    let mut control_out = FrameWriter::new(Compressor::control(control_out.into_inner(), level));
    while result.is_ok() {
        result = match control_out.next_from(queue) {
            Ok(Some(SenderControl::Query(query))) => {
                control_out.send(frame::QUERY, &query.encode())
            }
            Ok(Some(SenderControl::InputStop)) => control_out.send(frame::INPUT_STOP, b""),
            Ok(None) => break,
            Err(e) => Err(e),
        };
    }
    if let Err(e) = result
        && !progress.reached(Phase::Completing)
    {
        die(format!("control session: {e}"));
    }
}

/// The single owner of the data session's incoming direction, for the
/// whole run: the protocol version and the features, HELLO, the list, the
/// input, END_ACK, then closure. EOF before DONE is the receiver's
/// disappearance and is fatal (DESIGN.md §6.9). The receiver closes the
/// session after DONE, so EOF then is the normal end of this thread, and
/// the main thread joins it. The payloads of INPUT frames go to the writer
/// of the input spool, from `input_rx` (DESIGN.md §6.6).
fn data_reader<R: Read>(
    mut data_in: FrameReader<R>,
    hello_tx: Sender<ReceiverHello>,
    list_tx: Sender<Vec<u8>>,
    input_rx: Receiver<SpoolWriter>,
    ack_tx: Sender<()>,
    progress: &Progress,
) {
    match data_in.read_version_and_features("receiver") {
        Ok(true) => {}
        Ok(false) => die("the receiver disappeared"),
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

    // The input, then END_ACK, which must answer a sent END, are the
    // receiver's remaining data-session frames (DESIGN.md §6.2). A write
    // to the input spool waits while the spool holds its limit, and after
    // the stop it discards the payload. The receiver stops sending the
    // input at END, so END_ACK can come without INPUT_END.
    let mut input = Some(
        input_rx
            .recv()
            .expect("the main thread ended before the input spool"),
    );
    loop {
        let received = match data_in.next() {
            Ok(Some(received)) => received,
            Ok(None) => die("the receiver disappeared"),
            Err(e) => die_data(&e),
        };
        match received.kind {
            frame::INPUT => {
                assert!(!received.payload.is_empty(), "protocol error: empty INPUT");
                let writer = input
                    .as_mut()
                    .expect("protocol error: INPUT after INPUT_END");
                writer
                    .write_all(received.payload)
                    .expect("a spool write failed");
            }
            frame::INPUT_END => {
                assert!(
                    received.payload.is_empty(),
                    "protocol error: INPUT_END with a payload"
                );
                // The drop of the writer ends the input spool.
                let writer = input.take().expect("protocol error: a second INPUT_END");
                drop(writer);
            }
            frame::END_ACK => {
                assert!(
                    received.payload.is_empty(),
                    "protocol error: END_ACK with a payload"
                );
                assert!(
                    progress.reached(Phase::EndSent),
                    "protocol error: unsolicited END_ACK"
                );
                break;
            }
            kind => panic!("protocol error: unexpected data frame type {kind}"),
        }
    }
    drop(input);
    let _ = ack_tx.send(());

    // Nothing else may arrive: the first byte after END_ACK is a protocol
    // error, also when no whole frame follows it. Only EOF after DONE is
    // the normal end. The receiver keeps the session open until DONE:
    // closure before DONE remains fatal, so a receiver lost while command
    // stderr still drains kills the command instead of leaving the run
    // half-alive.
    match protocol::drain_to_eof(data_in.into_inner()) {
        Ok(true) => {
            if !progress.reached(Phase::DoneSent) {
                die("the receiver disappeared after END_ACK");
            }
        }
        Ok(false) => panic!("protocol error: data after END_ACK"),
        Err(e) => die_data(&e),
    }
}

/// The owner of the control session's incoming direction: the protocol
/// version, whose check gates command startup, and the features, then
/// answers for the scanner until closure, which is fatal before the normal
/// cleanup (DESIGN.md §6.9). The answers are one zstd stream when
/// `compressed` (DESIGN.md §6.2).
fn read_answers(
    stream: UnixStream,
    compressed: bool,
    ready_tx: Sender<()>,
    answers: Sender<Answer>,
    progress: &Progress,
) {
    let mut control_in = FrameReader::new(BufReader::new(stream));
    match control_in.read_version_and_features("receiver's control session") {
        Ok(true) => {}
        Ok(false) => die("the control session closed before the protocol version"),
        Err(e) => die(format!("control session: {e}")),
    }
    let _ = ready_tx.send(());
    let mut control_in = FrameReader::new(Decompressor::new(control_in.into_inner(), compressed));
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
                if !progress.reached(Phase::Completing) {
                    die("the control session closed unexpectedly");
                }
                return;
            }
        }
    }
}

/// Copy command stderr to this process's stderr through EOF, with
/// prompt writes. Failures are fatal (DESIGN.md §6.7).
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

fn clone_control(control: &UnixStream) -> UnixStream {
    match control.try_clone() {
        Ok(clone) => clone,
        Err(e) => die(format!("cannot clone the control socket: {e}")),
    }
}

/// The receiver's HELLO parameters are the run's: use them, or end the
/// run before command startup (DESIGN.md §6.2, §6.7). Decoding already
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
    use std::time::Duration;

    use super::*;
    use crate::hash::weak::WeakHasher;
    use crate::hash::{Strong, StrongHash};
    use crate::scanner::tests::noise;

    const BS: u32 = 64;

    /// `transfer` with a data queue of one frame, which the data writer
    /// drains slowly, while the lookups answer from another thread and
    /// reject the stale blocks, and idle points come now and then. The
    /// output stays exact and in order, flushes follow the idle points, and
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
            let (mut data, mut items) = data_queue(1);
            let (query_tx, query_rx) = unbounded();
            let (done_tx, done_rx) = bounded(1);
            let pins = Arc::new(Mutex::new(HashMap::new()));

            let command = input.clone();
            std::thread::spawn(move || {
                for (i, chunk) in command.chunks(100).enumerate() {
                    chunk_tx.send(Stdout::Bytes(chunk.to_vec())).unwrap();
                    if i % 13 == 0 {
                        chunk_tx.send(Stdout::Idle).unwrap();
                    }
                }
            });
            // A lookup publishes the pin before it answers yes.
            let (lookup_pins, blocks) = (Arc::clone(&pins), blocks.clone());
            std::thread::spawn(move || {
                for (i, control) in query_rx.iter().enumerate() {
                    let SenderControl::Query(query) = control else {
                        panic!("the scanner queued INPUT_STOP");
                    };
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
                let (mut output, mut flushes) = (Vec::new(), 0);
                for i in 0.. {
                    let next = items.try_next().unwrap_or_else(|| items.wait_next());
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
                        Output::Scanned(Frame::Flush) => flushes += 1,
                        Output::End(_) => break,
                    }
                }
                done_tx.send((output, flushes)).unwrap();
            });
            let table = HashTable::build(&values);
            std::thread::spawn(move || {
                let mut scanner =
                    Scanner::new(table, BS, StrongHash::Blake3, lookahead, 1 << 20, None);
                transfer(&mut scanner, &chunk_rx, &answer_rx, &mut data, &query_tx);
                data.end(End {
                    status: 0,
                    error: String::new(),
                });
            });
            let (output, flushes) = done_rx
                .recv()
                .expect("the data writer ended without the output");
            assert!(output == input, "lookahead {lookahead}: the output differs");
            assert!(flushes > 0, "lookahead {lookahead}: no flush");
        }
    }

    /// The data writer is idle only while it waits for items, after it has
    /// taken and written every queued item (DESIGN.md §6.3). Its token
    /// wakes the driver, which then finds the writer idle.
    #[test]
    fn the_writer_is_idle_only_after_every_queued_item() {
        let (mut data, mut items) = data_queue(4);
        assert!(data.writer_idle(), "nothing to write yet");
        data.queue(Frame::Reference);
        assert!(!data.writer_idle(), "an item waits");
        assert!(items.try_next().is_some());
        assert!(!data.writer_idle(), "the writer holds an item");
        let writer = std::thread::spawn(move || {
            let item = items.wait_next();
            (items, item)
        });
        // No deadline: a writer that does not say that it is idle hangs the
        // test.
        data.wakes.recv().unwrap();
        assert!(data.writer_idle());
        data.queue(Frame::Flush);
        let (_items, item) = writer.join().unwrap();
        assert!(matches!(item, Output::Scanned(Frame::Flush)));
        assert!(!data.writer_idle(), "the writer holds an item");
    }

    /// What a transport saw: the bytes of each write call, or a flush.
    #[derive(Debug, PartialEq, Eq)]
    enum Seen {
        Write(Vec<u8>),
        Flush,
    }

    /// A transport that records what it sees.
    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<Seen>>>);

    impl Write for Recorder {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().push(Seen::Write(buf.to_vec()));
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.0.lock().unwrap().push(Seen::Flush);
            Ok(())
        }
    }

    /// The data writer makes the flush that an idle point asks for only
    /// once it has nothing left to write: frames queued behind the flush
    /// come first, because the bytes before it could not leave sooner
    /// (DESIGN.md §6.2). Without compression, a flush reaches the transport
    /// as a flush.
    #[test]
    fn the_data_writer_flushes_only_when_it_has_nothing_left_to_write() {
        let recorder = Recorder::default();
        let (mut data, items) = data_queue(8);
        data.queue(Frame::Literal(b"before".to_vec()));
        data.queue(Frame::Flush);
        data.queue(Frame::Literal(b"after".to_vec()));
        let writer = {
            let transport = Compressor::new(recorder.clone(), 0, false, 0);
            std::thread::spawn(move || write_output(FrameWriter::new(transport), items))
        };
        // No deadline: a writer that never becomes idle hangs the test.
        data.wakes.recv().unwrap();
        assert!(data.writer_idle());
        let seen = std::mem::take(&mut *recorder.0.lock().unwrap());
        let (flush, writes) = seen.split_last().unwrap();
        assert_eq!(*flush, Seen::Flush, "{seen:?}");
        let written: Vec<u8> = writes
            .iter()
            .flat_map(|seen| match seen {
                Seen::Write(bytes) => bytes.clone(),
                Seen::Flush => panic!("a flush before the last frame: {seen:?}"),
            })
            .collect();
        let lit =
            |payload: &[u8]| [&[frame::LIT, payload.len() as u8, 0, 0, 0][..], payload].concat();
        assert_eq!(written, [lit(b"before"), lit(b"after")].concat());
        data.end(End {
            status: 0,
            error: String::new(),
        });
        writer.join().unwrap();
    }

    /// The receiver's side of the data session, for the empty list, then
    /// `frames`, as frames, then `trailing`, as bytes.
    fn receiver_session(frames: &[(u8, &[u8])], trailing: &[u8]) -> Vec<u8> {
        let list = hashlist::encode(&[]);
        let hello = ReceiverHello {
            block_size: BS,
            weak_hash_id: hash::weak::ID.to_string(),
            strong_hash: StrongHash::Blake3,
            target: StrongHash::Blake3.digest(&list),
            account_id: [1; 16],
            index_id: [2; 16],
            sender_cache: false,
            hashlist_max_age: 0,
            compression_level: 0,
            compression_long: false,
            compression_threads: 0,
            probe_threads: 0,
            lookahead: u64::from(BS),
            negative_cache: 0,
            sender_output_spool_limit: 0,
            sender_input_spool_limit: 0,
            output_delivery: Delivery::Eager,
            command: vec![b"true".to_vec()],
        };
        let header = HashList {
            length: list.len() as u64,
        };
        let mut session = FrameWriter::new(Vec::new());
        session.send_version_and_features().unwrap();
        session.send(frame::HELLO, &hello.encode()).unwrap();
        session.send(frame::HASHLIST, &header.encode()).unwrap();
        session.send_bytes(&list).unwrap();
        for &(kind, payload) in frames {
            session.send(kind, payload).unwrap();
        }
        let mut bytes = session.into_inner();
        bytes.extend_from_slice(trailing);
        bytes
    }

    /// Run the data reader on `session` after DONE, as the sender's main
    /// thread runs it, with an input spool without a limit. Returns the
    /// bytes that the data reader appended to the spool, which must end.
    fn read_session(session: &[u8]) -> Vec<u8> {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(Directory::open(path.as_os_str().as_bytes()));
        let (writer, mut input) = Spool::pipe(dir, u64::MAX);
        let (hello_tx, _hello_rx) = bounded(1);
        let (list_tx, _list_rx) = bounded(1);
        let (input_tx, input_rx) = bounded(1);
        let (ack_tx, _ack_rx) = bounded(1);
        input_tx.send(writer).unwrap();
        let progress = Progress::new();
        progress.advance(Phase::DoneSent);
        data_reader(
            FrameReader::new(session),
            hello_tx,
            list_tx,
            input_rx,
            ack_tx,
            &progress,
        );
        let mut appended = Vec::new();
        input.read_to_end(&mut appended).unwrap();
        appended
    }

    /// A byte after END_ACK is a protocol error, also after DONE, and also
    /// when no whole frame follows it: here the first byte of a second
    /// END_ACK, then EOF. Only EOF after DONE is the normal end
    /// (DESIGN.md §6.9).
    #[test]
    #[should_panic(expected = "protocol error: data after END_ACK")]
    fn a_byte_after_end_ack_is_a_protocol_error() {
        read_session(&receiver_session(
            &[(frame::END_ACK, b"")],
            &[frame::END_ACK],
        ));
    }

    /// The payloads of INPUT frames go to the input spool, in order, and
    /// INPUT_END ends the spool. The receiver stops the input at END, so
    /// END_ACK can also come without INPUT_END (DESIGN.md §6.6).
    #[test]
    fn input_frames_fill_the_input_spool() {
        let frames = [
            (frame::INPUT, &b"first "[..]),
            (frame::INPUT, b"second"),
            (frame::INPUT_END, b""),
            (frame::END_ACK, b""),
        ];
        assert_eq!(
            read_session(&receiver_session(&frames, b"")),
            b"first second"
        );
        let stopped = [(frame::INPUT, &b"cut"[..]), (frame::END_ACK, b"")];
        assert_eq!(read_session(&receiver_session(&stopped, b"")), b"cut");
    }

    #[test]
    #[should_panic(expected = "protocol error: INPUT after INPUT_END")]
    fn input_after_input_end_is_a_protocol_error() {
        let frames = [
            (frame::INPUT_END, &b""[..]),
            (frame::INPUT, b"late"),
            (frame::END_ACK, b""),
        ];
        read_session(&receiver_session(&frames, b""));
    }
}

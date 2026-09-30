//! `hashsync run [--no-update] [--ssh CMD] [USER@]HOST -- COMMAND ...`
//! (DESIGN.md §3, §5.6, §6, §8): the receiver.
//!
//! The run updates the index (or opens it read-only), exports and
//! publishes the hash list, prepares the pin file and the lookup workers,
//! starts both SSH sessions, and completes the exchange. It then
//! reconstructs stdout from literals, runs, and references, which arrive
//! through the compression stream, while the lookup workers answer the
//! sender's queries (DESIGN.md §6.4). Queries and answers have compression
//! streams of their own (DESIGN.md §6.2). It returns the command's status
//! once both sessions exited successfully, after a summary.
//!
//! The end is orderly (DESIGN.md §6.7). After END_ACK, DONE tells the
//! receiver that the sender needs neither session any more. The main
//! thread then closes both stdin pipes, without waiting for the EOF of
//! either stdout, and joins each thread of the transfer once its work
//! ends. Then the pins and the index close, and the summary gives exact
//! counts.

use std::ffi::OsStr;
use std::io::{BufReader, BufWriter, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::process::{ChildStdin, ChildStdout};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use bstr::ByteSlice;
use crossbeam_channel::{Receiver, Sender, unbounded};

use crate::cache::{self, ListStore};
use crate::config::Config;
use crate::die::{self, die};
use crate::fsid::FsCache;
use crate::fsops;
use crate::hash::weak;
use crate::hashlist::{self, ListId};
use crate::index::{self, Settings};
use crate::lookup;
use crate::pins::{PinFile, Pins};
use crate::protocol::{
    self, Answer, Compressor, Decompressor, End, FrameReader, FrameWriter, HashList, Query,
    ReceiverHello, Rle, SenderHello, frame,
};
use crate::rendezvous;
use crate::roots::{self, Location};
use crate::ssh;
use crate::sys;
use crate::units::Bytes;
use crate::update;

/// The size of the data session's read buffer.
const READ_BUFFER: usize = 256 << 10;

/// The largest write of a run's bytes to stdout.
const RUN_CHUNK: u64 = 256 << 10;

/// The bytes that the receiver wrote to both sessions, and that it read
/// from each session, for the summary.
static SENT: AtomicU64 = AtomicU64::new(0);
static DATA_RECEIVED: AtomicU64 = AtomicU64::new(0);
static CONTROL_RECEIVED: AtomicU64 = AtomicU64::new(0);

pub fn run(
    config: &Config,
    no_update: bool,
    cli_ssh: Option<&OsStr>,
    host: &OsStr,
    command: &[std::ffi::OsString],
) -> ! {
    let start = Instant::now();
    // Before any other thread starts: SIGINT and SIGTERM end the run with
    // `128 + signal`, and the end kills the SSH children (DESIGN.md §6.6).
    die::handle_termination_signals();

    let options = config.transfer_options(host.as_bytes(), cli_ssh.map(OsStr::as_bytes));

    // Hold index access through the whole run (DESIGN.md §3): the index
    // stays open, with its lock, until the lookup workers have ended.
    let (index, locations) = if no_update {
        open_read_only(config)
    } else {
        update::perform(config, false, None)
    };
    let meta = index.read_meta().expect("the index lost its metadata");

    // Export and hash the target from one database snapshot, before the
    // sessions start (DESIGN.md §3 and §5.6).
    let encoded = hashlist::encode(&index.export_weak_hashes());
    let target = ListId {
        algorithm: config.strong_hash,
        digest: config.strong_hash.digest(&encoded),
    };

    // The pin file and the lookup workers, which answer queries as soon as
    // they arrive (DESIGN.md §6.4).
    let (answer_tx, answer_rx) = unbounded();
    let pin_file = PinFile::create(&config.pin_directory, config.block_size);
    let pins = Arc::new(Pins::new(
        pin_file,
        config.strong_hash,
        config.block_size,
        answer_tx,
    ));
    let (queries, lookups) = lookup::start(&index, locations, &pins, config);

    let account_id = cache::account_id();
    let token = rendezvous::new_token();
    sys::reset_sigchld();
    let (data, control) = ssh::start_sessions(
        &options.ssh,
        &options.sender_command,
        host.as_bytes(),
        &token,
    );
    let sessions = [(data.pid(), "data"), (control.pid(), "control")];

    // Each direction of the control session has one thread, from the
    // protocol version on (DESIGN.md §6.2 and §6.6). The control writer
    // gives the session's stdin back when it ends: the pipe must stay open
    // until DONE (DESIGN.md §6.7).
    let control_writer = {
        let pins = Arc::clone(&pins);
        let control_stdin = control.stdin;
        let level = config.compression_level;
        std::thread::Builder::new()
            .name("control-writer".into())
            .spawn(move || write_answers(control_stdin, level, &answer_rx, &pins))
            .expect("cannot start the control writer")
    };
    let control_reader = {
        let pins = Arc::clone(&pins);
        let control_stdout = control.stdout;
        let compressed = config.compression_level != 0;
        std::thread::Builder::new()
            .name("control-reader".into())
            .spawn(move || read_queries(control_stdout, compressed, &pins, queries))
            .expect("cannot start the control reader")
    };

    // The data session, with the bytes of both directions counted for the
    // summary.
    let mut data_out = FrameWriter::new(Counting {
        inner: data.stdin,
        count: &SENT,
    });
    let mut data_in = FrameReader::new(BufReader::with_capacity(
        READ_BUFFER,
        Counting {
            inner: data.stdout,
            count: &DATA_RECEIVED,
        },
    ));

    let hello = ReceiverHello {
        block_size: config.block_size,
        weak_hash_id: weak::ID.to_string(),
        strong_hash: config.strong_hash,
        target: target.digest,
        account_id,
        index_id: meta.index_id,
        sender_cache: options.sender_cache,
        hashlist_max_age: config.hashlist_max_age,
        compression_level: config.compression_level,
        compression_long: config.compression_long,
        compression_threads: config.compression_threads,
        probe_threads: config.probe_threads,
        lookahead: config.lookahead,
        negative_cache: config.negative_cache,
        command: command.iter().map(|arg| arg.as_bytes().to_vec()).collect(),
    };
    // HELLO follows the version at once. The sender uses its parameters
    // or ends the run, so its reply does not repeat them.
    if let Err(e) = data_out.send_version() {
        die(format!("data session: {e}"));
    }
    send_data(&mut data_out, frame::HELLO, &hello.encode());
    flush_data(&mut data_out);

    check_sender_version(&mut data_in, host);
    let sender = SenderHello::decode(expect_data(&mut data_in, frame::HELLO, "HELLO"));
    // After its HELLO, the sender's data frames share one zstd stream
    // unless compression is disabled (DESIGN.md §6.2).
    let compressed = hello.compression_level != 0;
    let mut data_in = FrameReader::new(Decompressor::new(data_in.into_inner(), compressed));

    // Durably publish the target in the sender's peer directory before
    // sending list data; a publication failure is fatal and ends setup
    // (DESIGN.md §5.6).
    let store = ListStore::receiver_side(&sender.account_id, &meta.index_id);
    let published = store.publish(&target, &encoded);

    // The full list, whose bytes follow HASHLIST directly; cached bases
    // and deltas arrive with PLAN.md step 8.
    let header = HashList {
        length: encoded.len() as u64,
    };
    send_data(&mut data_out, frame::HASHLIST, &header.encode());
    if let Err(e) = data_out.send_bytes(&encoded) {
        die(format!("data session: {e}"));
    }
    flush_data(&mut data_out);
    let hash_list_bytes = encoded.len() as u64;
    drop(encoded);

    // The run has one target, so the acknowledgement names none.
    let payload = expect_data(&mut data_in, frame::CACHE_RESULT, "CACHE_RESULT");
    assert!(
        payload.is_empty(),
        "protocol error: CACHE_RESULT with a payload"
    );
    // Renew the target after CACHE_RESULT; with a base this would renew
    // it too (DESIGN.md §5.6). Then expire both cache trees.
    published.renew();
    drop(published);
    cache::expire(config.hashlist_max_age);

    let mut stdout = fsops::stdout();
    let (end, counts) = reconstruct(&mut data_in, &pins, &mut stdout, config.block_size);

    // Completion (DESIGN.md §6.7): the compression stream must end right
    // after END. Then flush reconstructed stdout, enter Completing, which
    // closes the answer queue, and send the single END_ACK.
    let transport = data_in
        .into_inner()
        .finish()
        .unwrap_or_else(|e| die_data(&e));
    if let Err(e) = stdout.flush() {
        die(format!("cannot write to stdout: {e}"));
    }
    pins.complete();
    send_data(&mut data_out, frame::END_ACK, b"");
    flush_data(&mut data_out);

    // DONE follows the zstd frame, outside it: the sender needs neither
    // session any more, and waits for the EOF of each stdin. Close the
    // data session's stdin, then the control session's once the control
    // writer has ended: it ends once Completing has closed its queue, or
    // when a write fails. Neither close waits for the EOF of a stdout:
    // through an SSH multiplexing client, or a wrapper that waits for SSH,
    // that EOF comes only after SSH has exited.
    let mut data_in = FrameReader::new(transport);
    let payload = expect_data(&mut data_in, frame::DONE, "DONE");
    assert!(payload.is_empty(), "protocol error: DONE with a payload");
    drop(data_out);
    let control_stdin = control_writer.join().expect("the control writer panicked");
    drop(control_stdin);

    // Nothing follows DONE. Then join the other threads in order
    // (DESIGN.md §6.7). The control reader ends at the EOF of its session,
    // and its end closes the query queue: the lookup workers skip the
    // queries that remain.
    match protocol::drain_to_eof(data_in.into_inner()) {
        Ok(true) => {}
        Ok(false) => panic!("protocol error: data after DONE"),
        Err(e) => die_data(&e),
    }
    control_reader.join().expect("the control reader panicked");
    for worker in lookups {
        worker.join().expect("a lookup worker panicked");
    }

    // Both sessions must exit successfully before the command's status
    // is returned, without reaping (DESIGN.md §6.6–6.7). Then nothing uses
    // the pins or the index any more.
    for (pid, name) in sessions {
        let status = sys::wait_exit_status(pid);
        if status != 0 {
            die(format!("the {name} session exited with status {status}"));
        }
    }
    Arc::into_inner(pins)
        .expect("the pins are still in use")
        .close();
    index.close();

    if end.status == 127 && !end.error.is_empty() {
        die::error(format!("remote command failed to start: {}", end.error));
    }
    print_summary(start, &counts, hash_list_bytes);
    die::finish(end.status)
}

/// The summary on stderr (DESIGN.md §8). The first line gives the bytes
/// of output, the wire bytes that the data session received, and the
/// bytes of the hash list and the delta. The second line gives, as rsync
/// does, the bytes sent and received on both sessions, the elapsed time
/// of the run with the rate of that traffic, and the speedup: the output
/// bytes over the traffic. The threads that count the traffic have ended,
/// so the counts are exact.
fn print_summary(start: Instant, counts: &Counts, hash_list_bytes: u64) {
    let elapsed = start.elapsed().as_secs_f64();
    let sent = SENT.load(Relaxed);
    let wire = DATA_RECEIVED.load(Relaxed);
    let received = wire + CONTROL_RECEIVED.load(Relaxed);
    let traffic = sent + received;
    die::note(format_args!(
        "{} output ({} literal, {} reference, {} run), {} wire, {} hash list, {} delta",
        Bytes(counts.output),
        Bytes(counts.literal),
        Bytes(counts.reference),
        Bytes(counts.run),
        Bytes(wire),
        Bytes(hash_list_bytes),
        Bytes(0),
    ));
    die::note(format_args!(
        "sent {} and received {} in {elapsed:.1}s ({}/s), speedup {:.2}",
        Bytes(sent),
        Bytes(received),
        Bytes((traffic as f64 / elapsed).round() as u64),
        counts.output as f64 / traffic as f64,
    ));
}

/// The bytes of reconstructed output, for the summary.
#[derive(Default)]
struct Counts {
    output: u64,
    literal: u64,
    reference: u64,
    run: u64,
}

/// Reconstruct stdout in order from the data frames until END, and return
/// END with the counts (DESIGN.md §6.4). Offset overflow is checked before
/// a frame's output is written. Once it is written, the pins learn the new
/// offset, which releases the pins below it.
fn reconstruct<R: Read>(
    data_in: &mut FrameReader<R>,
    pins: &Pins,
    stdout: &mut fsops::FdWriter,
    block_size: u32,
) -> (End, Counts) {
    let mut counts = Counts::default();
    let mut block = vec![0u8; block_size as usize];
    // Repeated bytes for runs.
    let mut run = Vec::new();
    loop {
        let received = match data_in.next() {
            Ok(Some(received)) => received,
            Ok(None) => die("the data session ended before END"),
            Err(e) => die_data(&e),
        };
        match received.kind {
            frame::LIT => {
                assert!(!received.payload.is_empty(), "protocol error: empty LIT");
                let length = received.payload.len() as u64;
                counts.output = advance(counts.output, length);
                write_output(stdout, received.payload);
                counts.literal += length;
            }
            frame::REF => {
                assert!(
                    received.payload.is_empty(),
                    "protocol error: REF with a payload"
                );
                counts.output = advance(counts.output, block_size.into());
                pins.read_reference(&mut block);
                write_output(stdout, &block);
                counts.reference += u64::from(block_size);
            }
            frame::RLE => {
                // Accept any positive length, though the sender emits runs
                // of at least `B` bytes (DESIGN.md §6.4).
                let rle = Rle::decode(received.payload);
                counts.output = advance(counts.output, rle.length);
                run.clear();
                run.resize(rle.length.min(RUN_CHUNK) as usize, rle.byte);
                let mut remaining = rle.length;
                while remaining > 0 {
                    let n = remaining.min(run.len() as u64) as usize;
                    write_output(stdout, &run[..n]);
                    remaining -= n as u64;
                }
                counts.run += rle.length;
            }
            frame::END => return (End::decode(received.payload), counts),
            kind => panic!("protocol error: unexpected data frame type {kind}"),
        }
        pins.reached(counts.output);
    }
}

/// Read-only index access for `--no-update` (DESIGN.md §4.4 and §8):
/// resolve current roots, then require a compatible index. Returns the
/// index with the current root locations, from which candidates are read.
fn open_read_only(config: &Config) -> (index::Index, Vec<Location>) {
    fsops::raise_descriptor_limit();
    let fs_cache = FsCache::new();
    let locations = roots::expand(config, &fs_cache);

    let path = config.index_path.as_bstr();
    let Some(index) = index::open_read_only(&config.index_path) else {
        die(format!("no index at {path}; run `hashsync update`"));
    };
    let Some(meta) = index.read_meta() else {
        die(format!("index {path} has no schema; run `hashsync update`"));
    };
    if !meta.compatible(&Settings::from_config(config)) {
        die(format!(
            "index {path} does not match the configuration; run `hashsync update`"
        ));
    }
    (index, locations)
}

/// Counts the bytes that pass through one direction of a session, for
/// the summary.
struct Counting<T> {
    inner: T,
    count: &'static AtomicU64,
}

impl<R: Read> Read for Counting<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.fetch_add(n as u64, Relaxed);
        Ok(n)
    }
}

impl<W: Write> Write for Counting<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.count.fetch_add(n as u64, Relaxed);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn die_data(e: &std::io::Error) -> ! {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        die("the data session ended unexpectedly");
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

fn expect_data<'a, R: Read>(
    data_in: &'a mut FrameReader<R>,
    kind: u8,
    what: &'static str,
) -> &'a [u8] {
    match data_in.expect(kind, what) {
        Ok(payload) => payload,
        Err(e) => die_data(&e),
    }
}

/// The sender's protocol version, which it sends before anything else
/// (DESIGN.md §6.2). EOF before it means that no usable `hashsync` ran.
fn check_sender_version<R: Read>(data_in: &mut FrameReader<R>, host: &OsStr) {
    match data_in.version() {
        Ok(Some(version)) => protocol::check_version(version, "sender"),
        Ok(None) => die(format!(
            "the data session ended before the sender's protocol version; \
             is a compatible hashsync installed on {}?",
            host.as_bytes().as_bstr()
        )),
        Err(e) => die_data(&e),
    }
}

fn write_output(stdout: &mut fsops::FdWriter, bytes: &[u8]) {
    if let Err(e) = stdout.write_all(bytes) {
        die(format!("cannot write to stdout: {e}"));
    }
}

/// Advance the output offset; overflow is a protocol error
/// (DESIGN.md §6.2).
fn advance(offset: u64, length: u64) -> u64 {
    offset
        .checked_add(length)
        .expect("protocol error: output offset overflow")
}

/// The owner of the control session's incoming direction: the sender's
/// protocol version, then queries for the lookup workers, one zstd stream
/// when `compressed` (DESIGN.md §6.2). Loss before Completing is fatal
/// (DESIGN.md §6.7); in Completing, EOF, errors, and a partial frame are
/// expected closure. Also in Completing, reading goes on until closure. So
/// the sender's relay never blocks on a full pipe, and the pipe closes
/// only at EOF, when SSH cannot write to it any more: a write to a closed
/// pipe would make SSH fail. The end of this thread closes the query
/// queue.
fn read_queries(stdout: ChildStdout, compressed: bool, pins: &Pins, lookups: Sender<Query>) {
    let mut control_in = FrameReader::new(BufReader::new(Counting {
        inner: stdout,
        count: &CONTROL_RECEIVED,
    }));
    let closed = || {
        if !pins.is_completing() {
            die("the control session closed unexpectedly");
        }
    };
    match control_in.version() {
        Ok(Some(version)) => protocol::check_version(version, "sender's control session"),
        Ok(None) | Err(_) => return closed(),
    }
    let mut control_in = FrameReader::new(Decompressor::new(control_in.into_inner(), compressed));
    loop {
        match control_in.next() {
            Ok(Some(received)) => {
                assert!(
                    received.kind == frame::QUERY,
                    "protocol error: unexpected control frame type {}",
                    received.kind
                );
                // The workers end only after this thread closes the queue.
                let _ = lookups.send(Query::decode(received.payload));
            }
            Ok(None) | Err(_) => return closed(),
        }
    }
}

/// The owner of the control session's outgoing direction: this side's
/// protocol version, sent at once, without waiting for the sender's, then
/// answers in the order the workers queue them, one zstd stream at
/// `level`, 0 for none, flushed whenever the queue runs empty (DESIGN.md
/// §6.2). It ends in Completing: once the queue is closed and empty, or
/// when a write fails, which is then expected closure; before Completing,
/// a failed write is fatal (DESIGN.md §6.7). It gives back the session's
/// stdin without closing it or ending the stream, and discards the answers
/// that a failed write left in the buffer.
fn write_answers(
    stdin: ChildStdin,
    level: i32,
    answers: &Receiver<Answer>,
    pins: &Pins,
) -> ChildStdin {
    let mut control_out = FrameWriter::new(BufWriter::new(Counting {
        inner: stdin,
        count: &SENT,
    }));
    let mut result = control_out
        .send_version()
        .and_then(|()| control_out.flush());
    let mut control_out = FrameWriter::new(Compressor::control(control_out.into_inner(), level));
    while result.is_ok() {
        match control_out.next_from(answers) {
            Ok(Some(answer)) => result = control_out.send(frame::ANSWER, &answer.encode()),
            Ok(None) => break,
            Err(e) => result = Err(e),
        }
    }
    if let Err(e) = result
        && !pins.is_completing()
    {
        die(format!("control session: {e}"));
    }
    let (stdin, _) = control_out.into_inner().into_transport().into_parts();
    stdin.inner
}

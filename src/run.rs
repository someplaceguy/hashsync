//! `hashsync run [--stdin] [--batch-stdin] [--batch-stdout] [--no-update]
//! [--ssh CMD] [USER@]HOST -- COMMAND ...` (DESIGN.md §3, §5.6, §6, §8):
//! the receiver.
//!
//! The run updates the index (or opens it read-only), exports and publishes
//! the hash list, prepares the pins and the lookup workers, starts both SSH
//! sessions, and completes the list exchange, which gives the sender the
//! target in the cheapest form available: a confirmation that it already
//! has the list, a delta from a list both sides have, or the full list
//! (DESIGN.md §5.6). It then
//! reconstructs stdout from literals, runs, and references, which arrive
//! through the compression stream, while the lookup workers answer the
//! sender's queries (DESIGN.md §6.4). The data session goes through a
//! spool, so that a blocked stdout does not stop the transfer until
//! `receiver_output_spool_limit` bytes wait in the spool (DESIGN.md §6.5).
//! Queries and answers have compression streams of their own (DESIGN.md
//! §6.2). Meanwhile, the input goes to the command's stdin, in INPUT frames
//! on the data session, until END: with `--stdin` or `--batch-stdin`, the
//! receiver's stdin, and else no bytes (DESIGN.md §6.6). The output and the
//! input each have a delivery: eager, which sends their bytes on at each
//! idle point of their source, or batch (DESIGN.md §6.2). It returns the
//! command's status once both sessions exited successfully, after a
//! summary.
//!
//! The end is orderly (DESIGN.md §6.9). After END_ACK, DONE tells the
//! receiver that the sender needs neither session any more. The main
//! thread then closes both stdin pipes, without waiting for the EOF of
//! either stdout, and joins each thread of the transfer once its work
//! ends. Then the pins and the index close, and the summary gives exact
//! counts.

use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, BufWriter, IoSlice, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::ffi::OsStrExt;
use std::process::{ChildStdin, ChildStdout};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use bstr::ByteSlice;
use crossbeam_channel::{Receiver, Sender, unbounded};

use crate::cache::{self, Base, Exchange};
use crate::config::Config;
use crate::die::{self, die};
use crate::fsid::FsCache;
use crate::fsops;
use crate::hash::{Strong, weak};
use crate::hashlist::{self, ListId};
use crate::index::{self, Settings};
use crate::input::{InputStop, ReceiverInput};
use crate::lookup;
use crate::pins::Pins;
use crate::protocol::{
    self, Answer, Compressor, Decompressor, Delivery, End, FrameReader, FrameWriter, Query,
    ReceiverHello, Rle, SenderHello, TargetList, flush_data, frame, send_data,
};
use crate::rendezvous;
use crate::roots::{self, Location};
use crate::segments::Directory;
use crate::spool::Spool;
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

/// `input_delivery` is the delivery of this process's stdin to the
/// command, or `None` for no input, and `output_delivery` the delivery of
/// the command's stdout (DESIGN.md §6.2 and §8).
pub fn run(
    config: &Config,
    no_update: bool,
    input_delivery: Option<Delivery>,
    output_delivery: Delivery,
    cli_ssh: Option<&OsStr>,
    host: &OsStr,
    command: &[std::ffi::OsString],
) -> ! {
    let start = Instant::now();
    // Before any other thread starts: SIGINT and SIGTERM end the run with
    // `128 + signal`, and the end kills the SSH children (DESIGN.md §6.8).
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
    // sessions start (DESIGN.md §3 and §5.6). The values are kept until the
    // receiver knows whether it can send the target as a delta.
    let values = index.export_weak_hashes();
    let encoded = hashlist::encode(&values);
    let target = ListId {
        algorithm: config.strong_hash,
        digest: config.strong_hash.digest(&encoded),
    };

    // The tmp_data directory holds the segments of the pins and of the
    // spools of the data session and of the input (DESIGN.md §6.5). An
    // unusable directory ends the run when it opens, before the sessions
    // start. The lookup workers answer queries as soon as they arrive
    // (DESIGN.md §6.4).
    let tmp_data = Arc::new(Directory::open(&config.tmp_data_dir));
    let (answer_tx, answer_rx) = unbounded();
    let pins = Arc::new(Pins::new(
        Arc::clone(&tmp_data),
        config.strong_hash,
        config.block_size,
        answer_tx,
    ));
    let (queries, lookups) = lookup::start(&index, locations, &pins, config);
    // The input spool and the stop of the input exist before the sessions
    // start: INPUT_STOP on the control session can stop the input at any
    // time, also before CACHE_RESULT (DESIGN.md §6.6).
    let input = ReceiverInput::new(Arc::clone(&tmp_data), config.receiver_input_spool_limit);

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
    // protocol version on (DESIGN.md §6.2 and §6.8). The control writer
    // gives the session's stdin back when it ends: the pipe must stay open
    // until DONE (DESIGN.md §6.9). The control reader stops the input at
    // INPUT_STOP.
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
        let input_stop = input.stopper();
        std::thread::Builder::new()
            .name("control-reader".into())
            .spawn(move || read_control(control_stdout, compressed, &pins, queries, &input_stop))
            .expect("cannot start the control reader")
    };

    // The data session, with the bytes of both directions counted for the
    // summary. Its incoming bytes go through a spool: while stdout is
    // blocked, the receiver still reads the session, so the sender keeps
    // sending, and the bytes wait in the spool, until
    // `receiver_output_spool_limit` bytes wait (DESIGN.md §6.4).
    let mut data_out = FrameWriter::new(Counting {
        inner: data.stdin,
        count: &SENT,
    });
    let received = Counting {
        inner: data.stdout,
        count: &DATA_RECEIVED,
    };
    let spool = Spool::start(
        "the data session",
        received,
        tmp_data,
        config.receiver_output_spool_limit,
    );
    let mut data_in = FrameReader::new(BufReader::with_capacity(READ_BUFFER, spool));

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
        sender_output_spool_limit: config.sender_output_spool_limit,
        sender_input_spool_limit: config.sender_input_spool_limit,
        output_delivery,
        command: command.iter().map(|arg| arg.as_bytes().to_vec()).collect(),
    };
    // HELLO follows the version and the features at once. It does not
    // wait for the sender's features: this build supports none, so HELLO
    // cannot depend on them (DESIGN.md §6.2). The sender uses HELLO's
    // parameters or ends the run, so its reply does not repeat them.
    if let Err(e) = data_out.send_version_and_features() {
        die(format!("data session: {e}"));
    }
    send_data(&mut data_out, frame::HELLO, &hello.encode());
    flush_data(&mut data_out);

    read_sender_version_and_features(&mut data_in, host);
    let sender = SenderHello::decode(expect_data(&mut data_in, frame::HELLO, "HELLO"));
    // After its HELLO, the sender's data frames share one zstd stream
    // unless compression is disabled (DESIGN.md §6.2).
    let compressed = hello.compression_level != 0;
    let mut data_in = FrameReader::new(Decompressor::new(data_in.into_inner(), compressed));

    // This side's part of the exchange lists its directory for the sender
    // and the index. With sender caching, it durably publishes the target
    // there before sending list data, so that this side has every list
    // that the sender can advertise later; a publication failure is fatal
    // and ends setup. Without sender caching, the sender keeps no list,
    // so this side keeps none either (DESIGN.md §5.6).
    let mut exchange = Exchange::receiver(&sender.account_id, &meta.index_id);
    if hello.sender_cache {
        exchange.publish(&target, &encoded);
    }

    // Send the target in the smallest form that the sender's lists allow:
    // none of its bytes if the sender has it, otherwise a delta from a list
    // both sides have, otherwise the full list (DESIGN.md §5.6).
    let Offer {
        message,
        base,
        fallback,
    } = Offer::choose(&exchange, &sender.lists, &target, values, encoded);
    let list_bytes = give_target(&mut data_out, &mut data_in, message, fallback);
    // After CACHE_RESULT the sender holds the target, or no list at all if
    // caching is disabled, and the end of the receiver's exchange matches
    // that: it renews the published target and the base of HASHDELTA (even
    // if the sender asked for the full list instead), removes the other
    // listed files, and expires both cache trees (DESIGN.md §5.6).
    exchange.finish(base, config.hashlist_max_age);

    // The list exchange is complete: from here on until END, the input
    // writer owns the outgoing direction of the data session, and sends
    // the input to the command, at the level of the data stream, until the
    // stop of the input (DESIGN.md §6.6). With `--stdin` or `--batch-stdin`,
    // the stdin reader reads this process's stdin. Without them, the stdin
    // reader reads `/dev/null` in its place, as `ssh -n` does: the command
    // reads EOF at once, and this process reads nothing from its own stdin
    // (DESIGN.md §8). `/dev/null` ends at once, so its delivery changes
    // nothing.
    let (stdin, delivery): (Box<dyn AsFd + Send>, _) = match input_delivery {
        Some(delivery) => (Box::new(rustix::stdio::stdin()), delivery),
        None => match File::open("/dev/null") {
            Ok(null) => (Box::new(null), Delivery::Eager),
            Err(e) => die(format!("cannot open /dev/null: {e}")),
        },
    };
    let input = input.start(
        stdin,
        data_out,
        delivery,
        config.compression_level,
        config.compression_long,
        config.compression_threads,
    );

    let mut stdout = fsops::stdout();
    let (end, counts) = reconstruct(&mut data_in, &pins, &mut stdout, config.block_size);

    // The command has exited, so the input has no use any more: stop it,
    // unless INPUT_STOP has stopped it, which gives back the outgoing
    // direction of the data session.
    let mut data_out = input.stop();

    // Completion (DESIGN.md §6.9): the compression stream must end right
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
    // (DESIGN.md §6.9): the thread of the spool, which ended at the EOF of
    // the data session, then the control reader. The control reader ends
    // at the EOF of its session, and its end closes the query queue: the
    // lookup workers skip the queries that remain. A spool read never
    // fails: the spool itself ends the run when it cannot read the session
    // or a segment.
    let mut transport = data_in.into_inner();
    let drained = protocol::drain_to_eof(&mut transport).expect("a spool read failed");
    assert!(drained, "protocol error: data after DONE");
    transport.into_inner().close();
    control_reader.join().expect("the control reader panicked");
    for worker in lookups {
        worker.join().expect("a lookup worker panicked");
    }

    // Both sessions must exit successfully before the command's status
    // is returned, without reaping (DESIGN.md §6.8–6.9). Then nothing uses
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
    print_summary(start, &counts, &list_bytes);
    die::finish(end.status)
}

/// The summary on stderr (DESIGN.md §8). The first line gives the bytes
/// of output, the wire bytes that the data session received, and the bytes
/// of the full lists and of the delta that the receiver sent. The second
/// line gives, as rsync does, the bytes sent and received on both
/// sessions, the elapsed time of the run with the rate of that traffic,
/// and the speedup: the output bytes over the traffic. The threads that
/// count the traffic have ended, so the counts are exact.
fn print_summary(start: Instant, counts: &Counts, lists: &ListBytes) {
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
        Bytes(lists.full),
        Bytes(lists.delta),
    ));
    die::note(format_args!(
        "sent {} and received {} in {elapsed:.1}s ({}/s), speedup {:.2}",
        Bytes(sent),
        Bytes(received),
        Bytes((traffic as f64 / elapsed).round() as u64),
        counts.output as f64 / traffic as f64,
    ));
}

/// How the receiver hands the sender its target (DESIGN.md §5.6).
struct Offer {
    /// The first message: HASHKEEP, HASHDELTA, or HASHLIST.
    message: TargetList,
    /// The base of HASHDELTA. The end of the exchange renews it, even if
    /// the sender asks for the full list instead of using the delta.
    base: Option<Base>,
    /// The full list, which the sender may ask for once with HASHFULL
    /// after HASHKEEP or HASHDELTA.
    fallback: Option<TargetList>,
}

impl Offer {
    /// Choose how to send the target, whose values and encoding are
    /// `values` and `encoded`: HASHKEEP if the sender advertised the
    /// target; otherwise HASHDELTA from the base that the receiver finds
    /// among the `advertised` lists, if the delta takes fewer bytes than
    /// the full list; otherwise HASHLIST, which is also the choice when the
    /// first advertised list that the receiver finds is damaged.
    fn choose(
        exchange: &Exchange,
        advertised: &[Strong],
        target: &ListId,
        values: Vec<u64>,
        encoded: Vec<u8>,
    ) -> Offer {
        let full = TargetList::Full(encoded);
        if advertised.contains(&target.digest) {
            return Offer {
                message: TargetList::Keep,
                base: None,
                fallback: Some(full),
            };
        }
        if let Some((base, base_values)) = exchange.find_base(advertised, target.algorithm) {
            let (removed, added) = hashlist::delta(&base_values, &values);
            let delta = TargetList::Delta {
                base: base.digest,
                removed: hashlist::encode(&removed),
                added: hashlist::encode(&added),
            };
            if delta.size() < full.size() {
                return Offer {
                    message: delta,
                    base: Some(base),
                    fallback: Some(full),
                };
            }
        }
        Offer {
            message: full,
            base: None,
            fallback: None,
        }
    }
}

/// The bytes of the hash lists that the receiver sent, for the summary
/// (DESIGN.md §8).
#[derive(Default)]
struct ListBytes {
    /// Full lists, sent after HASHLIST.
    full: u64,
    /// The removed and added lists of a delta, sent after HASHDELTA.
    delta: u64,
}

impl ListBytes {
    /// Count the lists that `message` carries.
    fn count(&mut self, message: &TargetList) {
        match message {
            TargetList::Full(list) => self.full += list.len() as u64,
            TargetList::Keep => {}
            TargetList::Delta { removed, added, .. } => {
                self.delta += (removed.len() + added.len()) as u64;
            }
        }
    }
}

/// Send the sender its target as `message`, then wait for CACHE_RESULT.
/// After HASHKEEP or HASHDELTA, the sender asks once for the full list with
/// HASHFULL if it cannot use the list that the message names, or if the
/// delta does not produce the target (DESIGN.md §5.6). This function then
/// sends `fallback`, the full list. Returns the bytes of the lists sent.
fn give_target<R: Read, W: Write>(
    data_out: &mut FrameWriter<W>,
    data_in: &mut FrameReader<R>,
    message: TargetList,
    mut fallback: Option<TargetList>,
) -> ListBytes {
    let mut sent = ListBytes::default();
    send_target(data_out, &message, &mut sent);
    loop {
        let (kind, empty) = match data_in.next() {
            Ok(Some(reply)) => (reply.kind, reply.payload.is_empty()),
            Ok(None) => die_data(&std::io::ErrorKind::UnexpectedEof.into()),
            Err(e) => die_data(&e),
        };
        match kind {
            // The run has one target, so the acknowledgement names none.
            frame::CACHE_RESULT => {
                assert!(empty, "protocol error: CACHE_RESULT with a payload");
                return sent;
            }
            frame::HASHFULL => {
                assert!(empty, "protocol error: HASHFULL with a payload");
                let Some(full) = fallback.take() else {
                    panic!("protocol error: HASHFULL after the full list");
                };
                send_target(data_out, &full, &mut sent);
            }
            kind => panic!("protocol error: expected CACHE_RESULT, received frame type {kind}"),
        }
    }
}

/// Send the target list `message` and count its lists in `sent`. The
/// sender replies to it, so it must not stay in a buffer.
fn send_target<W: Write>(
    data_out: &mut FrameWriter<W>,
    message: &TargetList,
    sent: &mut ListBytes,
) {
    if let Err(e) = message.send(data_out) {
        die(format!("data session: {e}"));
    }
    flush_data(data_out);
    sent.count(message);
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
/// offset, which detaches the pins below it.
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

    /// One vectored write of the inner writer, which `FrameWriter::send`
    /// needs: the default would write only the first buffer, and a frame
    /// header would go out alone.
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        let n = self.inner.write_vectored(bufs)?;
        self.count.fetch_add(n as u64, Relaxed);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// End the run after the data session ended inside a frame or inside the
/// compressed stream, because the sender or SSH went away. A read of the
/// session fails in no other way: the spool itself ends the run when it
/// cannot read the session.
fn die_data(e: &std::io::Error) -> ! {
    assert_eq!(
        e.kind(),
        std::io::ErrorKind::UnexpectedEof,
        "a read of the data session failed: {e}"
    );
    die("the data session ended unexpectedly")
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

/// The sender's protocol version, which it sends before anything else,
/// then its features (DESIGN.md §6.2). EOF before the version means that
/// no usable `hashsync` ran.
fn read_sender_version_and_features<R: Read>(data_in: &mut FrameReader<R>, host: &OsStr) {
    match data_in.read_version_and_features("sender") {
        Ok(true) => {}
        Ok(false) => die(format!(
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
/// protocol version and features, then queries for the lookup workers, and
/// at most one INPUT_STOP, which stops the input with `input`, one zstd
/// stream when `compressed` (DESIGN.md §6.2 and §6.6). Loss before
/// Completing is fatal (DESIGN.md §6.9); in Completing, EOF, errors, and a
/// partial frame are expected closure. Also in Completing, reading goes on
/// until closure. So the sender's relay never blocks on a full pipe, and
/// the pipe closes only at EOF, when SSH cannot write to it any more: a
/// write to a closed pipe would make SSH fail. The end of this thread
/// closes the query queue.
fn read_control(
    stdout: ChildStdout,
    compressed: bool,
    pins: &Pins,
    lookups: Sender<Query>,
    input: &InputStop,
) {
    let mut control_in = FrameReader::new(BufReader::new(Counting {
        inner: stdout,
        count: &CONTROL_RECEIVED,
    }));
    let closed = || {
        if !pins.is_completing() {
            die("the control session closed unexpectedly");
        }
    };
    match control_in.read_version_and_features("sender's control session") {
        Ok(true) => {}
        Ok(false) | Err(_) => return closed(),
    }
    let mut control_in = FrameReader::new(Decompressor::new(control_in.into_inner(), compressed));
    let mut input_stopped = false;
    loop {
        match control_in.next() {
            Ok(Some(received)) => match received.kind {
                frame::QUERY => {
                    // The workers end only after this thread closes the
                    // queue.
                    let _ = lookups.send(Query::decode(received.payload));
                }
                frame::INPUT_STOP => {
                    assert!(
                        received.payload.is_empty(),
                        "protocol error: INPUT_STOP with a payload"
                    );
                    assert!(!input_stopped, "protocol error: a second INPUT_STOP");
                    input_stopped = true;
                    // Before END, or after it in Completing: a stop at END
                    // can come first, and then this one does nothing.
                    input.stop();
                }
                kind => panic!("protocol error: unexpected control frame type {kind}"),
            },
            Ok(None) | Err(_) => return closed(),
        }
    }
}

/// The owner of the control session's outgoing direction: this side's
/// protocol version and features, sent at once, without waiting for the
/// sender's, then answers in the order the workers queue them, one zstd
/// stream at `level`, 0 for none, flushed whenever the queue runs empty
/// (DESIGN.md §6.2). It ends in Completing: once the queue is closed and
/// empty, or when a write fails, which is then expected closure; before
/// Completing, a failed write is fatal (DESIGN.md §6.9). It gives back the
/// session's stdin without closing it or ending the stream, and discards
/// the answers that a failed write left in the buffer.
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
        .send_version_and_features()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::tests::with_home;
    use crate::hashlist::tests::list;

    /// What the receiver offers for a target with `values`, when the sender
    /// advertised `advertised` and the receiver's directory for the sender
    /// holds `cached`.
    fn offer(values: &[u64], advertised: &[&ListId], cached: &[(&ListId, &[u8])]) -> Offer {
        let (peer, index) = ([5; 16], [6; 16]);
        let mut earlier = Exchange::receiver(&peer, &index);
        for &(id, encoded) in cached {
            earlier.publish(id, encoded);
        }
        let exchange = Exchange::receiver(&peer, &index);
        let (target, encoded) = list(values);
        let advertised: Vec<Strong> = advertised.iter().map(|id| id.digest).collect();
        Offer::choose(&exchange, &advertised, &target, values.to_vec(), encoded)
    }

    /// The receiver sends no list bytes for a target that the sender has, a
    /// delta from a list both sides have when that is smaller than the full
    /// list, and the full list otherwise. The sender can ask for the full
    /// list only after HASHKEEP or HASHDELTA (DESIGN.md §5.6).
    #[test]
    fn the_receiver_offers_the_smallest_form_of_the_target() {
        with_home(|_| {
            let wide: Vec<u64> = (0..100).map(|i| i << 56).collect();
            let (target, encoded) = list(&wide);
            let full = Some(TargetList::Full(encoded));
            let keep = offer(&wide, &[&target], &[]);
            assert_eq!((keep.message, keep.fallback), (TargetList::Keep, full));

            // If the receiver has none of the advertised lists, it sends
            // the full list.
            let (base, base_bytes) = list(&wide);
            let mut changed = wide.clone();
            changed[50] += 1;
            let (_, changed_bytes) = list(&changed);
            let changed_full = TargetList::Full(changed_bytes);
            for advertised in [&[&base][..], &[]] {
                let full = offer(&changed, advertised, &[]);
                assert_eq!((&full.message, &full.fallback), (&changed_full, &None));
                assert!(full.base.is_none());
            }

            // With one value changed in a list of 100, the delta is smaller.
            let delta = offer(&changed, &[&base], &[(&base, &base_bytes)]);
            let expected = TargetList::Delta {
                base: base.digest,
                removed: hashlist::encode(&[wide[50]]),
                added: hashlist::encode(&[changed[50]]),
            };
            assert_eq!(delta.message, expected);
            assert_eq!(delta.base.unwrap().digest, base.digest);
            assert_eq!(delta.fallback, Some(changed_full));

            // When every value of a short list changes, the delta takes
            // more bytes than the full list.
            let (small, small_bytes) = list(&[1]);
            let full = offer(&[2], &[&small], &[(&small, &small_bytes)]);
            assert_eq!(full.message, TargetList::Full(hashlist::encode(&[2])));
        });
    }

    /// Run `give_target` with `message` and `fallback` against `replies`,
    /// the sender's frames, uncompressed. Returns what the receiver sent and
    /// the list bytes that it counted.
    fn give(
        message: TargetList,
        fallback: Option<TargetList>,
        replies: &[u8],
    ) -> (Vec<u8>, ListBytes) {
        let mut data_out = FrameWriter::new(Vec::new());
        let mut data_in = FrameReader::new(replies);
        let sent = give_target(&mut data_out, &mut data_in, message, fallback);
        (data_out.into_inner(), sent)
    }

    /// One uncompressed frame.
    fn frame_bytes(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = FrameWriter::new(Vec::new());
        out.send(kind, payload).unwrap();
        out.into_inner()
    }

    /// The frame of `message` followed by its lists.
    fn message_bytes(message: &TargetList) -> Vec<u8> {
        let mut out = FrameWriter::new(Vec::new());
        message.send(&mut out).unwrap();
        out.into_inner()
    }

    /// After HASHKEEP or HASHDELTA, the sender can ask for the full list once
    /// with HASHFULL: the receiver sends it and waits for CACHE_RESULT
    /// again. After a full list, only CACHE_RESULT can follow (DESIGN.md
    /// §5.6).
    #[test]
    fn hashfull_gets_the_full_list_once() {
        let list = hashlist::encode(&[1, 2, 3]);
        let full = || TargetList::Full(list.clone());
        let result = frame_bytes(frame::CACHE_RESULT, b"");
        let replies = [frame_bytes(frame::HASHFULL, b""), result.clone()].concat();
        let (sent, bytes) = give(TargetList::Keep, Some(full()), &replies);
        let keep_then_full = [message_bytes(&TargetList::Keep), message_bytes(&full())];
        assert_eq!(sent, keep_then_full.concat());
        assert_eq!((bytes.full, bytes.delta), (list.len() as u64, 0));

        let (sent, bytes) = give(full(), None, &result);
        assert_eq!(sent, message_bytes(&full()));
        assert_eq!((bytes.full, bytes.delta), (list.len() as u64, 0));
    }

    #[test]
    #[should_panic(expected = "protocol error: HASHFULL after the full list")]
    fn hashfull_after_the_full_list_is_a_protocol_error() {
        let full = TargetList::Full(hashlist::encode(&[1]));
        give(full, None, &frame_bytes(frame::HASHFULL, b""));
    }

    #[test]
    #[should_panic(expected = "protocol error: HASHFULL after the full list")]
    fn a_second_hashfull_is_a_protocol_error() {
        let full = TargetList::Full(hashlist::encode(&[1]));
        let hashfull = frame_bytes(frame::HASHFULL, b"");
        give(
            TargetList::Keep,
            Some(full),
            &[hashfull.clone(), hashfull].concat(),
        );
    }
}

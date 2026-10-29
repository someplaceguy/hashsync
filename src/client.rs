// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! `hashsync ssh [OPTIONS] [-l USER] [USER@]HOST [--] COMMAND [ARGS...]`
//! (DESIGN.md §3, §5.6, §6, §8): the client, which starts both SSH
//! sessions, and has the user's stdin, stdout, and exit status.
//!
//! The client sends its stdin to the command, the forward stream, and
//! receives the command's stdout, the backward stream. One or both of them
//! are deduplicated, each against the index of the host that receives it,
//! and a stream that is not is plain. In a pull, the default, the backward
//! stream is deduplicated against the client's index; in a push, with
//! `--dedup-stdin`, the forward stream against the server's, and the client
//! needs no index; with `--dedup-stdin --dedup-stdout`, both are.
//!
//! The client starts the sessions at once, and sends REQUEST, with the
//! command and every setting of the run, so that the server starts the
//! command at once too. Both streams start then. The data reader takes the
//! backward stream's CHUNK frames into that stream's spool, so that a
//! blocked stdout does not stop the transfer until `--spool-limit` bytes
//! wait there, and the decoder writes the stream to stdout, with the
//! references that the pins hold when the stream is deduplicated. The stdin
//! reader copies stdin into the forward stream's spool, which is empty with
//! `-n`, and the forward data writer sends it, through the scanner when the
//! stream is deduplicated, which starts at the server's HELLO, with an empty
//! table.
//!
//! The list exchanges run while the streams run, each side on a thread of
//! its own (DESIGN.md §5.6). As the receiver of a deduplicated backward
//! stream, this side sends its HELLO right after REQUEST, so that the
//! server's scanner can start, then updates its index (or opens it
//! read-only), starts its lookup workers, and runs its side of the
//! exchange. With both streams deduplicated, the server updates its own
//! index at the same time, and each side keeps its index closed from the
//! end of its update until the other's update is done too (DESIGN.md §4.4).
//! The forward stream stops at STOP, or at END. The backward stream
//! stops once no process reads stdout any more: this side then sends STOP,
//! and the server closes the command's stdout, as the end of the reader of
//! a local pipeline does.
//!
//! This side lets go of stdout once the decoder has written the backward
//! stream there, and of stdin once the stdin reader has ended, at its EOF
//! or the stop of the forward stream, or at once with `-n`: it replaces
//! each with `/dev/null`, so that the process at the other end sees what it
//! would see at this side's exit, as at the end of a local pipeline. That
//! process can hold a lock of an index that this run waits for, such as
//! another run whose stdin is this side's stdout (DESIGN.md §4.4).
//!
//! The end is orderly (DESIGN.md §6.9, steps N1 to N6). At END, the main
//! thread stops the forward stream, closes the data session's stdin once
//! the forward data writer has ended, and the control session's once the
//! control writer has ended, without waiting for the EOF of either stdout.
//! Then the decoder writes the rest of the backward stream, and checks its
//! end, and the main thread joins the threads. As the receiver of a
//! deduplicated backward stream, this side then updates its index again,
//! unless `--no-update`, while the files that the run wrote here are still
//! in the page cache. Only then does it wait for the EOF of the data
//! session, which comes once the server has exited, after its own update.
//! Both sessions must exit successfully before the client returns the
//! command's status, after a summary.

use std::io::{BufReader, IoSlice, Read, Write};
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use crossbeam_channel::{Sender, bounded, unbounded};
use rustix::fs::{Mode, OFlags};
use rustix::io::retry_on_intr;

use crate::config::Config;
use crate::control::{self, Dispatch, EndFlag};
use crate::dedup::{Receiving, Sending};
use crate::die::{self, die};
use crate::escapes::quoted;
use crate::list_exchange::{self, ListBytes};
use crate::protocol::{
    self, Counts, End, FrameReader, FrameWriter, Request, SourceRead, die_data, flush_data, frame,
    lost, send_data,
};
use crate::rendezvous;
use crate::report;
use crate::segments::Directory;
use crate::spool::Spool;
use crate::ssh;
use crate::stream::{self, Frames, Inlet, PlainFrames, Stdout, Stop};
use crate::sys;
use crate::units::Bytes;

/// The size of the data session's read buffer.
const READ_BUFFER: usize = 256 << 10;

/// The other host, as the messages about it name it.
const PEER: &str = "server";

/// The bytes that the client wrote to both sessions, and that it read
/// from both, for the summary.
static SENT: AtomicU64 = AtomicU64::new(0);
static RECEIVED: AtomicU64 = AtomicU64::new(0);

/// Run `request` on `remote`, which the client reaches through SSH, with
/// an empty forward stream in place of stdin for `no_stdin` (`-n`,
/// DESIGN.md §8).
/// `config` is the client's configuration, whose index a deduplicated
/// backward stream needs.
pub fn run(config: Config, remote: &ssh::Remote, request: &Request, no_stdin: bool) -> ! {
    let start = Instant::now();
    // Before any other thread starts: SIGINT and SIGTERM end the run with
    // `128 + signal`, and the end kills the SSH children (DESIGN.md §6.8).
    die::handle_termination_signals();
    let config = Arc::new(config);
    let settings = &request.settings;
    let compressed = settings.compression_level != 0;
    let deduplicated = request.deduplicated;
    // When the backward stream is deduplicated, a `--lookahead` below the
    // block size of this side's index is a user error, reported before
    // anything else (DESIGN.md §8).
    if deduplicated.stdout()
        && let Err(e) = settings.lookahead(config.block_size, "the index")
    {
        die(e);
    }

    // The tmp_data directory holds the segments of the pins and of the
    // spools of both streams (DESIGN.md §6.5). An unusable directory ends
    // the run when it opens, before the sessions start. The segment files
    // that failed runs left there go once they are older than
    // `hashlist_max_age`.
    let tmp_data = Arc::new(Directory::open(&config.tmp_data_dir));
    tmp_data.expire_temporaries(config.hashlist_max_age);
    // This side's roles in the deduplicated streams: the receiver of a
    // deduplicated backward stream, and the sender of a deduplicated forward
    // stream. With the stop of the backward stream, they are the producers
    // of the control writer's queue (see `control`).
    // Each side has its own queue in the control writer for the frames of
    // its list exchange, so that the frames of one never wait behind the
    // list of the other.
    let (control_tx, control_rx) = unbounded();
    let (receiver_out, receiver_frames) = bounded(1);
    let (sender_out, sender_frames) = bounded(1);
    let backward = deduplicated
        .stdout()
        .then(|| Receiving::new(&config, &tmp_data, &control_tx, receiver_out));
    let forward = deduplicated
        .stdin()
        .then(|| Sending::new("stdin", &control_tx, sender_out));

    // The spools of both streams, and their stops, which exist before the
    // sessions start: STOP can come at any time, and the stop of the
    // backward stream, which this side decides, puts the receiver of a
    // deduplicated backward stream into Completing (DESIGN.md §6.6).
    let (forward_writer, forward_spool) = Spool::pipe(Arc::clone(&tmp_data), settings.spool_limit);
    // With `-n`, the forward stream is empty, as `ssh -n` reads `/dev/null`
    // in place of stdin: its spool ends here, before any stop can come, no
    // stdin reader runs (DESIGN.md §8), and this side has no use for stdin.
    let forward_writer = match no_stdin {
        false => Some(forward_writer),
        true => {
            drop(forward_writer);
            release_stdin();
            None
        }
    };
    let forward_stop = Stop::sending(forward_spool.stopper());
    let (backward_writer, backward_spool) = Spool::pipe(tmp_data, settings.spool_limit);
    let backward_stop = Stop::receiving(
        backward_spool.stopper(),
        backward.as_ref().map(Receiving::pins),
        control_tx,
    );
    // The control reader passes the queries to the lookup workers of a
    // deduplicated backward stream, the answers to the scanner of a
    // deduplicated forward stream, STOP to the forward stream, and the
    // frames of the exchanges to their sides.
    let dispatch = Dispatch {
        queries: backward.as_ref().map(Receiving::queries),
        answers: forward.as_ref().map(Sending::answers),
        stop: forward_stop.clone(),
        receiver: backward.as_ref().map(Receiving::frames),
        sender: forward.as_ref().map(Sending::frames),
    };

    // The sessions start at once: the update of a deduplicated backward
    // stream's index runs while the command runs (DESIGN.md §3).
    let token = rendezvous::new_token();
    sys::reset_sigchld();
    let (data, control) = ssh::start_sessions(remote, &token);
    let sessions = [(data.pid(), "data"), (control.pid(), "control")];

    // Each direction of the control session has one thread, from the
    // protocol version on (DESIGN.md §6.2 and §6.8). The control writer
    // gives the session's stdin back when it ends: the pipe must stay open
    // until END (DESIGN.md §6.9).
    let end = Arc::new(EndFlag::new());
    let control_writer = {
        let end = Arc::clone(&end);
        let control_stdin = Counting {
            inner: control.stdin,
            count: &SENT,
        };
        let level = settings.compression_level;
        let exchanges = [receiver_frames, sender_frames];
        std::thread::Builder::new()
            .name("control-writer".into())
            .spawn(move || {
                control::write(
                    control_stdin,
                    level,
                    &control_rx,
                    &exchanges,
                    None,
                    &end,
                    PEER,
                )
            })
            .expect("cannot start the control writer")
    };
    let control_reader = {
        let end = Arc::clone(&end);
        let control_stdout = Counting {
            inner: control.stdout,
            count: &RECEIVED,
        };
        let level = settings.compression_level;
        std::thread::Builder::new()
            .name("control-reader".into())
            .spawn(move || control::read(control_stdout, level, &dispatch, &end, PEER))
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
            count: &RECEIVED,
        },
    ));
    // REQUEST follows the version and the features at once, without
    // waiting for the server's features: this build supports none, so it
    // cannot depend on them (DESIGN.md §6.2). Then the HELLO of a
    // deduplicated backward stream, from this side's configuration: the
    // server's scanner needs it before it scans a byte, and it reads it
    // before it starts the command.
    if let Err(e) = data_out.send_version_and_features() {
        die_data(PEER, &e);
    }
    send_data(&mut data_out, frame::REQUEST, &request.encode(), PEER);
    if backward.is_some() {
        let hello = list_exchange::hello(&config);
        send_data(&mut data_out, frame::HELLO, &hello, PEER);
    }
    flush_data(&mut data_out, PEER);

    // The receiver's role of a deduplicated backward stream, while the
    // streams run: the index, which gives the hash list and the candidates,
    // and this side of the list exchange. With both streams deduplicated,
    // the server updates its own index at the same time, and the run must
    // hold no lock while the server can still wait for the exclusive lock of
    // its update: this side releases its index after its update, and opens
    // it again only once the server's update is done (DESIGN.md §4.4).
    let backward_pins = backward
        .as_ref()
        .map(|receiving| Arc::clone(receiving.pins()));
    let receiving =
        backward.map(|receiving| receiving.start(Arc::clone(&config), settings, deduplicated));
    // The stdin reader reads this process's stdin, as ssh does, from the
    // start: its bytes wait in the spool until the forward data writer
    // takes them. At its EOF or the stop, stdin has no more use.
    let stdin_reader = forward_writer.map(|writer| {
        let (stop, delivery) = (forward_stop.clone(), settings.stdin_delivery());
        std::thread::Builder::new()
            .name("stdin-reader".into())
            .spawn(move || {
                let read = stream::read_stdin(rustix::stdio::stdin(), writer, &stop, delivery);
                release_stdin();
                read
            })
            .expect("cannot start the stdin reader")
    });
    read_server_version_and_features(&mut data_in, &remote.host);

    // The encoder of the forward stream. For a deduplicated forward stream,
    // the scanner, which starts at the server's HELLO, the server's first
    // frame: its block size and hashes are the stream's. A `--lookahead`
    // below that block size is a user error (DESIGN.md §8). The scanner
    // stops with the stream: it can wait for answers that the server no
    // longer sends after the stop. Its sender's side of the list exchange
    // then fills its table.
    let (frames, sending): (Box<dyn Frames + Send>, _) = match forward {
        Some(sending) => {
            let hello = data_in
                .expect(frame::HELLO, "HELLO")
                .unwrap_or_else(|e| die_data(PEER, &e));
            let lookahead = |block_size| {
                let lookahead = settings.lookahead(block_size, "the server's index");
                lookahead.unwrap_or_else(|e| die(e))
            };
            let (items, threads) = sending.start(
                hello,
                forward_spool,
                forward_stop.clone(),
                settings,
                config.hashlist_max_age,
                lookahead,
            );
            (Box::new(items), Some(threads))
        }
        None => (Box::new(PlainFrames::new(forward_spool)), None),
    };
    let forward_data_writer = {
        let (settings, stop) = (settings.clone(), forward_stop.clone());
        std::thread::Builder::new()
            .name("data-writer".into())
            .spawn(move || stream::write_stream(data_out, frames, &settings, PEER, &stop))
            .expect("cannot start the data writer")
    };

    // The data reader takes the backward stream into its spool until
    // STREAM_END, and the decoder writes it to stdout, until the stop of
    // the backward stream once no process reads stdout. Either way, stdout
    // has no more use then.
    let (end_tx, end_rx) = bounded(1);
    let data_reader = {
        let (end, stop) = (Arc::clone(&end), backward_stop.clone());
        let inlet = Inlet::new(backward_writer, backward_pins.clone());
        std::thread::Builder::new()
            .name("data-reader".into())
            .spawn(move || read_data(data_in, inlet, &end_tx, &end, &stop))
            .expect("cannot start the data reader")
    };
    let decoder = {
        let mut stdout = Stdout::new(backward_stop.clone());
        std::thread::Builder::new()
            .name("decoder".into())
            .spawn(move || {
                let pins = backward_pins.as_deref();
                let counts = stream::decode(backward_spool, compressed, &mut stdout, pins);
                release_stdout();
                counts
            })
            .expect("cannot start the decoder")
    };

    // END (DESIGN.md §6.9). The data reader ends the run if the data
    // session ends before it.
    let end_frame = end_rx.recv().expect("the data reader ended before END");

    // N1. The forward stream has no use any more, so stop it, unless STOP
    // has stopped it. And the server, which has ended the backward stream,
    // needs no STOP any more: the stop of the backward stream lets go of its
    // place in the control writer's queue. A broken stdout from now on
    // still stops the stream on this side, so that its decoder ends.
    forward_stop.raise();
    backward_stop.release_control();
    // N2. Close the data session's stdin once the data writer has ended,
    // then the control session's once the control writer has ended: it
    // ends once its producers are gone, the pins at their Completing, the
    // scanner at the end of its scan, the stop of the backward stream at
    // its raise or at step N1, and the sides of the list exchanges at their
    // end, or when a write fails. The server ended its sides of the
    // exchanges before END, so this side's end at once, or once the last
    // frame of the server's side, which the control session carries,
    // arrives. Neither close waits for the EOF of a stdout: through an SSH
    // multiplexing client, or a wrapper that waits for SSH, that EOF comes
    // only after SSH has exited.
    let (data_out, stdin_sent) = forward_data_writer
        .join()
        .expect("the data writer panicked");
    drop(data_out);
    let control_stdin = control_writer.join().expect("the control writer panicked");
    drop(control_stdin);
    // N3. The decoder writes the rest of the backward stream and checks its
    // end, or ends at the stop. Then join the other threads but the data
    // reader: the control reader reads the control session to its EOF; the
    // stdin reader and a scan ended at the stop or at the EOF of stdin, and
    // the sides of the list exchanges at their end.
    let mut stdout_counts = decoder.join().expect("the decoder panicked");
    control_reader.join().expect("the control reader panicked");
    let stdin_read = match stdin_reader {
        Some(reader) => reader.join().expect("the stdin reader panicked"),
        // The empty stream of `-n` is its whole source.
        None => SourceRead {
            bytes: 0,
            eof: true,
        },
    };
    let forward_lists = sending.map(|threads| threads.join());
    let backward = receiving.map(|thread| thread.join());

    // N4. Nothing uses the pins any more, and the index closed when the
    // lookups ended. They answered nothing from Completing on, which came
    // before the decoder ended, so their counts of misses are final. Then
    // the index is updated again, now that the command has finished
    // (DESIGN.md §3); with both streams deduplicated, the server updates
    // its own at the same time.
    let backward_lists = backward.map(|(lookups, lists)| {
        stdout_counts.misses = lookups.pins().misses();
        lookups.end();
        lists
    });
    // N5. The data reader reads the data session to its EOF, which comes
    // only once the server has exited, after its own update: SSH passes
    // the EOF of stdout on only once stderr has ended too (DESIGN.md §6.9).
    // Both sessions must exit successfully before the command's status is
    // returned, without reaping (DESIGN.md §6.8–6.9).
    let received = data_reader.join().expect("the data reader panicked");
    for (pid, name) in sessions {
        let status = sys::wait_exit_status(pid);
        if status != 0 {
            die(format!("the {name} session exited with status {status}"));
        }
    }
    // N6.
    if end_frame.status == 127 && !end_frame.error.is_empty() {
        report::error(format!(
            "remote command failed to start: {}",
            end_frame.error
        ));
    }
    // This side runs the forward stream's source reader and the backward
    // stream's decoder, and END brings the server's counts of the other
    // two. The CHUNK bytes that this side sent of the forward stream are
    // all of them, also those of the CHUNK frames that reached the server
    // only after it counted at END. Every CHUNK frame of the backward
    // stream came before END.
    let forward = Line::of(
        &stdin_read,
        &end_frame.forward,
        stdin_sent,
        forward_lists.as_ref(),
    );
    let backward = Line::of(
        &end_frame.backward,
        &stdout_counts,
        received,
        backward_lists.as_ref(),
    );
    print_summary(
        start,
        compressed,
        &[("stdin", forward), ("stdout", backward)],
    );
    die::finish(end_frame.status)
}

/// The summary line of one stream (DESIGN.md §8). A stopped stream's line
/// shows everything that a whole stream's line shows, so that a stream
/// that always stops, such as the forward stream of `rsync -e`, still shows
/// what deduplication did.
#[derive(Debug, PartialEq, Eq)]
struct Line<'a> {
    /// The bytes that the stream's source reader read.
    read: u64,
    /// What the stream's receiver counted: the bytes that its destination
    /// took, and in a deduplicated stream, the queries that its lookups
    /// answered no. The wire bytes are those of all the stream's CHUNK
    /// payloads, also the ones that the receiver dropped undecoded.
    counts: Counts,
    /// In a deduplicated stream, the lists that its exchange sent.
    lists: Option<&'a ListBytes>,
    /// For a stream that stopped before its destination took all of it, the
    /// CHUNK bytes that its receiver dropped undecoded. `None` when the
    /// stream is whole.
    dropped: Option<u64>,
}

impl<'a> Line<'a> {
    /// The line of a stream, from what its source reader read, the counts
    /// of its receiver, `chunks`, the bytes of all its CHUNK payloads,
    /// which become its wire bytes, and in a deduplicated stream, the lists
    /// of its exchange. A stream is whole when its source reader reached
    /// the EOF of its source (`SourceRead::eof`) and its destination took
    /// every byte that the source reader read, whether or not a stop came:
    /// the CHUNK bytes that a stop after its last byte left undecoded, such
    /// as the end of its zstd frame, don't show as dropped. Otherwise the
    /// stream stopped first, and its receiver dropped the CHUNK bytes that
    /// it did not decode.
    ///
    /// One of the counts of each stream comes from the server's END, so a
    /// count that cannot be right is a protocol error: a destination that
    /// took more bytes than the sender read, or a decoder that decoded more
    /// CHUNK bytes than the stream had (DESIGN.md §6.7).
    fn of(
        source: &SourceRead,
        decoded: &Counts,
        chunks: u64,
        lists: Option<&'a ListBytes>,
    ) -> Self {
        assert!(
            decoded.bytes <= source.bytes,
            "protocol error: a destination took {} bytes of a stream whose source gave {}",
            decoded.bytes,
            source.bytes
        );
        let dropped = chunks.checked_sub(decoded.wire).unwrap_or_else(|| {
            panic!(
                "protocol error: a decoder decoded {} CHUNK bytes of a stream that had {chunks}",
                decoded.wire
            )
        });
        let whole = source.eof && decoded.bytes == source.bytes;
        Line {
            read: source.bytes,
            counts: Counts {
                wire: chunks,
                ..decoded.clone()
            },
            lists,
            dropped: (!whole).then_some(dropped),
        }
    }
}

/// The owner of the incoming direction of the data session once the list
/// exchange is done: the backward stream's CHUNK frames and STREAM_END go
/// to `inlet`, then END, which this thread passes on to `end_tx` after it
/// set `end`. END comes after the backward stream's STREAM_END, unless
/// `stop`, the stop of the backward stream, came first: the server sends
/// no STREAM_END after its stop, and it can have stopped only after this
/// side's stop sent STOP. Then the session reaches EOF once the server has
/// exited: nothing may follow END, and the first byte after it is a
/// protocol error, also when no whole frame follows it. Reading on until
/// EOF also keeps SSH from blocking on a full pipe. EOF before END is fatal
/// (DESIGN.md §6.9). Returns the bytes of all the CHUNK payloads that came,
/// for the summary.
fn read_data<R: Read>(
    mut data_in: FrameReader<R>,
    mut inlet: Inlet,
    end_tx: &Sender<End>,
    end: &EndFlag,
    stop: &Stop,
) -> u64 {
    let end_frame = loop {
        let received = match data_in.next() {
            Ok(Some(received)) => received,
            Ok(None) => lost(PEER),
            Err(e) => die_data(PEER, &e),
        };
        match received.kind {
            frame::CHUNK => inlet.chunk(received.payload),
            frame::STREAM_END => inlet.end(received.payload),
            frame::END => {
                assert!(
                    inlet.has_ended() || stop.is_raised(),
                    "protocol error: END before the end of the backward stream"
                );
                break End::decode(received.payload);
            }
            kind => panic!("protocol error: unexpected data frame type {kind}"),
        }
    };
    // The inlet goes, and the pins with it, before END reaches the main
    // thread, which closes them while this thread waits for EOF.
    let received = inlet.received();
    drop(inlet);
    end.set();
    // The main thread waits for END.
    let _ = end_tx.send(end_frame);
    match protocol::drain_to_eof(data_in.into_inner()) {
        Ok(true) => received,
        Ok(false) => panic!("protocol error: data after END"),
        Err(e) => die(format!("data session: {e}")),
    }
}

/// Let go of stdin, which the run has no more use for: a process that
/// writes it then gets `EPIPE` (`release`).
fn release_stdin() {
    release("stdin", |null| rustix::stdio::dup2_stdin(null));
}

/// Let go of stdout, which the run has no more use for: a process that
/// reads it then reads its EOF (`release`).
fn release_stdout() {
    release("stdout", |null| rustix::stdio::dup2_stdout(null));
}

/// Replace the standard descriptor `name` with `/dev/null`, through `dup2`,
/// so that the process at its other end sees now what it would see at this
/// process's exit. That process can hold what the run still waits for,
/// such as a lock of an index (DESIGN.md §4.4). `/dev/null` keeps the
/// descriptor in use, so that no file that the run opens later takes its
/// number; it was in use before too, since Rust's runtime opens `/dev/null`
/// on a closed standard descriptor at startup. `dup2` clears close-on-exec
/// on its copy, as on any standard descriptor.
fn release(name: &str, dup2: impl Fn(&OwnedFd) -> rustix::io::Result<()>) {
    let null = retry_on_intr(|| {
        rustix::fs::open("/dev/null", OFlags::RDWR | OFlags::CLOEXEC, Mode::empty())
    });
    let replaced = null.and_then(|null| retry_on_intr(|| dup2(&null)));
    if let Err(e) = replaced {
        die(format!("cannot replace {name} with /dev/null: {e}"));
    }
}

/// The summary on stderr (DESIGN.md §8). One line for each stream, which
/// names it. The line gives the bytes that the stream's source reader
/// read, its wire bytes, and how it went over the wire: deduplicated,
/// compressed, both, or neither, which shows nothing. The wire bytes are
/// those of all its CHUNK payloads, and in a deduplicated stream also those
/// of the lists that its exchange sent, which the line shows before them.
/// The line of a deduplicated stream also splits the bytes that its
/// destination took into literal, reference, and RLE bytes, the last two
/// with the bytes of the frames that carried them, and gives the queries
/// that its receiver answered no after a lookup in its index: those
/// without a candidate, false positives of the weak hash, and those whose
/// candidates did not verify, not found. The line of a stopped stream adds,
/// before how the stream went over the wire, the CHUNK bytes that its
/// receiver dropped and the bytes that its destination took. The last line
/// gives, as rsync does, the bytes sent and received on both sessions, and
/// the elapsed time of the run with the rate of that traffic; then the
/// data shrinkage factor, the bytes that reached the destinations of both
/// streams over the traffic, and the bytes saved, the difference. The
/// threads that count have ended, so the counts are exact.
fn print_summary(start: Instant, compressed: bool, streams: &[(&str, Line)]) {
    let elapsed = start.elapsed().as_secs_f64();
    for (name, line) in streams {
        let counts = &line.counts;
        let (deduplication, list_bytes) = match line.lists {
            Some(lists) => (
                format!(
                    " ({} literal, {} -> {} refs, {} -> {} RLE, \
                     {} false positives, {} not found){}",
                    Bytes(counts.literal),
                    Bytes(counts.reference),
                    Bytes(counts.reference_frames),
                    Bytes(counts.run),
                    Bytes(counts.run_frames),
                    counts.misses.false_positives,
                    counts.misses.not_found,
                    sent_lists(lists),
                ),
                lists.full + lists.delta,
            ),
            None => (String::new(), 0),
        };
        let stop = match line.dropped {
            Some(dropped) => format!(
                ", pipe closed, {} dropped, {} written",
                Bytes(dropped),
                Bytes(counts.bytes)
            ),
            None => String::new(),
        };
        report::note(format_args!(
            "{name} -- {}{deduplication} --> {} wire{stop}{}",
            Bytes(line.read),
            Bytes(counts.wire + list_bytes),
            how_sent(line.lists.is_some(), compressed),
        ));
    }
    let sent = SENT.load(Relaxed);
    let received = RECEIVED.load(Relaxed);
    let traffic = sent + received;
    let carried: u64 = streams.iter().map(|(_, line)| line.counts.bytes).sum();
    // A run that sends more than it delivers, such as a short one whose
    // hash list outweighs its streams, saves a negative amount.
    let saved = match carried.checked_sub(traffic) {
        Some(saved) => Bytes(saved).to_string(),
        None => format!("-{}", Bytes(traffic - carried)),
    };
    report::note(format_args!(
        "total -- sent {} and received {} in {elapsed:.1}s ({}/s), \
         data shrinkage factor: {:.2}, saved {saved} of data transfer",
        Bytes(sent),
        Bytes(received),
        Bytes((traffic as f64 / elapsed).round() as u64),
        carried as f64 / traffic as f64,
    ));
}

/// The lists that the exchange of a deduplicated stream sent, as the
/// stream's summary line adds them to its bytes: ` + 12.00 KiB hashlist` or
/// ` + 1.50 KiB hashlist delta`, both when the sender could not use the
/// delta and asked for the full list, and nothing after HASHKEEP. Every
/// list that goes out takes at least the 8 bytes of its count (DESIGN.md
/// §5.4), so the lists with bytes are those that the exchange sent.
fn sent_lists(lists: &ListBytes) -> String {
    let mut sent = String::new();
    if lists.full > 0 {
        sent += &format!(" + {} hashlist", Bytes(lists.full));
    }
    if lists.delta > 0 {
        sent += &format!(" + {} hashlist delta", Bytes(lists.delta));
    }
    sent
}

/// How a stream went over the wire, at the end of its summary line.
fn how_sent(deduplicated: bool, compressed: bool) -> &'static str {
    match (deduplicated, compressed) {
        (true, true) => " (deduplicated, compressed)",
        (true, false) => " (deduplicated)",
        (false, true) => " (compressed)",
        (false, false) => "",
    }
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

/// The server's protocol version, which it sends before anything else,
/// then its features (DESIGN.md §6.2). EOF before the version means that
/// no usable `hashsync` ran.
fn read_server_version_and_features<R: Read>(data_in: &mut FrameReader<R>, host: &[u8]) {
    match data_in.read_version_and_features(PEER) {
        Ok(true) => {}
        Ok(false) => die(format!(
            "the data session ended before the server's protocol version; \
             is a compatible hashsync installed on {}?",
            quoted(host)
        )),
        Err(e) => die_data(PEER, &e),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt;

    use super::*;
    use crate::segments::Directory;

    /// The server's side of the data session after its version and
    /// features: `frames` as frames, then `trailing` as raw bytes.
    fn server_session(frames: &[(u8, &[u8])], trailing: &[u8]) -> Vec<u8> {
        let mut session = FrameWriter::new(Vec::new());
        for &(kind, payload) in frames {
            session.send(kind, payload).unwrap();
        }
        let mut bytes = session.into_inner();
        bytes.extend_from_slice(trailing);
        bytes
    }

    /// Run the data reader on `session`, with a spool of the backward
    /// stream without a limit, whose stop is raised first when `stopped`.
    /// Returns the status that END carried, the bytes that the data reader
    /// appended to the spool, and the bytes of the CHUNK payloads that it
    /// received.
    fn read_session(session: &[u8], stopped: bool) -> (i32, Vec<u8>, u64) {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(Directory::open(path.as_os_str().as_bytes()));
        let (writer, mut spool) = Spool::pipe(dir, u64::MAX);
        let (control, _queue) = unbounded();
        let stop = Stop::receiving(spool.stopper(), None, control);
        if stopped {
            stop.raise();
        }
        let (end_tx, end_rx) = bounded(1);
        let end = EndFlag::new();
        let received = read_data(
            FrameReader::new(session),
            Inlet::new(writer, None),
            &end_tx,
            &end,
            &stop,
        );
        assert!(end.is_set(), "END did not set the flag");
        let mut appended = Vec::new();
        spool.read_to_end(&mut appended).unwrap();
        (end_rx.try_recv().unwrap().status, appended, received)
    }

    fn end_payload(status: i32) -> Vec<u8> {
        End {
            status,
            error: String::new(),
            forward: Counts::default(),
            backward: SourceRead::default(),
        }
        .encode()
    }

    /// The backward stream's CHUNK frames go to its spool, STREAM_END ends
    /// it, and END follows, which the main thread gets (DESIGN.md §6.9).
    #[test]
    fn end_follows_the_end_of_the_backward_stream() {
        let end = end_payload(3);
        let frames = [
            (frame::CHUNK, &b"backward"[..]),
            (frame::STREAM_END, b""),
            (frame::END, &end),
        ];
        let (status, appended, received) = read_session(&server_session(&frames, b""), false);
        assert_eq!((status, &appended[..], received), (3, &b"backward"[..], 8));
    }

    /// END comes only after the end of the backward stream (DESIGN.md
    /// §6.2).
    #[test]
    #[should_panic(expected = "protocol error: END before the end of the backward stream")]
    fn end_before_stream_end_is_a_protocol_error() {
        let end = end_payload(0);
        let frames = [(frame::CHUNK, &b"backward"[..]), (frame::END, &end)];
        read_session(&server_session(&frames, b""), false);
    }

    /// After this side stopped the backward stream, the server sends no
    /// STREAM_END once STOP reached it, so END can follow the CHUNK frames
    /// directly. The spool discards their payloads, and they still count as
    /// received, for the bytes that the summary shows as dropped (DESIGN.md
    /// §6.6 and §8).
    #[test]
    fn after_the_stop_end_can_come_without_stream_end() {
        let end = end_payload(141);
        let frames = [(frame::CHUNK, &b"discarded"[..]), (frame::END, &end)];
        let (status, appended, received) = read_session(&server_session(&frames, b""), true);
        assert_eq!((status, &appended[..], received), (141, &b""[..], 9));
    }

    /// The server can send STREAM_END before STOP reaches it, when the
    /// command's stdout ends while STOP is on its way. After this side's
    /// stop, the data reader takes it as it takes the CHUNK frames before
    /// it, and the spool discards their payloads (DESIGN.md §6.6).
    #[test]
    fn after_the_stop_stream_end_can_still_come() {
        let end = end_payload(0);
        let frames = [
            (frame::CHUNK, &b"late"[..]),
            (frame::STREAM_END, b""),
            (frame::END, &end),
        ];
        let (status, appended, received) = read_session(&server_session(&frames, b""), true);
        assert_eq!((status, &appended[..], received), (0, &b""[..], 4));
    }

    /// Nothing follows END: the first byte after it is a protocol error,
    /// also when no whole frame follows it (DESIGN.md §6.9).
    #[test]
    #[should_panic(expected = "protocol error: data after END")]
    fn a_byte_after_end_is_a_protocol_error() {
        let end = end_payload(0);
        let frames = [(frame::STREAM_END, &b""[..]), (frame::END, &end)];
        read_session(&server_session(&frames, &[frame::CHUNK]), false);
    }

    /// What a source reader read, `bytes`, and whether it reached the EOF
    /// of its source, `eof`.
    fn read(bytes: u64, eof: bool) -> SourceRead {
        SourceRead { bytes, eof }
    }

    /// The counts of a decoder that wrote `bytes`, all literal, and decoded
    /// `wire` CHUNK bytes.
    fn decoded(bytes: u64, wire: u64) -> Counts {
        Counts {
            bytes,
            literal: bytes,
            wire,
            ..Counts::default()
        }
    }

    /// A stream whose destination took every byte of its source is whole,
    /// also when a stop after its last byte left the end of its zstd frame
    /// undecoded: its wire bytes are all its CHUNK bytes (DESIGN.md §8).
    #[test]
    fn a_stream_that_reached_its_destination_whole_shows_all_its_chunk_bytes() {
        let lists = ListBytes { full: 5, delta: 0 };
        let counts = Counts {
            bytes: 10,
            literal: 4,
            reference: 6,
            reference_frames: 5,
            wire: 7,
            ..Counts::default()
        };
        let line = Line::of(&read(10, true), &counts, 9, Some(&lists));
        let whole = Line {
            read: 10,
            counts: Counts { wire: 9, ..counts },
            lists: Some(&lists),
            dropped: None,
        };
        assert_eq!(line, whole);
    }

    /// The line of a stopped stream keeps everything that the line of a
    /// whole stream has, here the counts of a deduplicated stream, which
    /// split what its destination took, and the lists of its exchange. Its
    /// wire bytes are all its CHUNK bytes, and it adds the CHUNK bytes that
    /// its receiver dropped (DESIGN.md §8).
    #[test]
    fn a_stopped_stream_shows_the_chunk_bytes_that_its_receiver_dropped() {
        let lists = ListBytes { full: 0, delta: 3 };
        let counts = Counts {
            bytes: 40,
            literal: 8,
            reference: 32,
            reference_frames: 10,
            wire: 20,
            ..Counts::default()
        };
        let line = Line::of(&read(100, false), &counts, 50, Some(&lists));
        let stopped = Line {
            read: 100,
            counts: Counts { wire: 50, ..counts },
            lists: Some(&lists),
            dropped: Some(30),
        };
        assert_eq!(line, stopped);
    }

    /// A source read to its EOF does not make a stream whole: its
    /// destination must also have taken every byte of it.
    #[test]
    fn a_stream_stopped_after_the_eof_of_its_source_is_not_whole() {
        let line = Line::of(&read(100, true), &decoded(60, 30), 45, None);
        let stopped = Line {
            read: 100,
            counts: Counts {
                wire: 45,
                ..decoded(60, 30)
            },
            lists: None,
            dropped: Some(15),
        };
        assert_eq!(line, stopped);
    }

    /// A decoder cannot decode more CHUNK bytes than the stream had: when
    /// END says so of the forward stream, the server is wrong (DESIGN.md
    /// §6.7).
    #[test]
    #[should_panic(expected = "protocol error: a decoder decoded 51 CHUNK bytes")]
    fn more_decoded_chunk_bytes_than_sent_is_a_protocol_error() {
        Line::of(&read(100, false), &decoded(40, 51), 50, None);
    }

    /// A destination cannot take more bytes than the source gave: when END
    /// says that the server's stdout reader read fewer bytes than this side
    /// wrote, the server is wrong (DESIGN.md §6.7).
    #[test]
    #[should_panic(expected = "protocol error: a destination took 41 bytes")]
    fn more_written_than_read_is_a_protocol_error() {
        Line::of(&read(40, true), &decoded(41, 20), 50, None);
    }
}

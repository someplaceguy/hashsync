//! `hashsync ssh [OPTIONS] [-l USER] [USER@]HOST [--] COMMAND [ARGS...]`
//! (DESIGN.md §3, §5.6, §6, §8): the client, which starts both SSH
//! sessions, and has the user's stdin, stdout, and exit status.
//!
//! The client sends its stdin to the command, the forward stream, and
//! receives the command's stdout, the backward stream. One or both of them
//! are deduplicated, each against the index of the host that receives it,
//! and a stream that is not is plain. In a pull, the default, the backward
//! stream is deduplicated against the client's index: before the sessions
//! start, the client updates that index (or opens it read-only), exports
//! its hash list, and starts its pins and lookup workers. In a push, with
//! `--dedup-stdin`, the forward stream is deduplicated against the server's
//! index, and the client needs no index. With `--dedup-stdin
//! --dedup-stdout`, both are: the client updates its index before the
//! sessions start, releases it while the server updates its own, and opens
//! it again for the backward stream's list exchange, which follows the
//! forward stream's. The client sends REQUEST, with the command and every
//! setting of the run, then runs its side of each list exchange (DESIGN.md
//! §4.4 and §5.6).
//!
//! Once the list exchanges are done, both streams run. The data reader
//! takes the backward stream's CHUNK frames into that stream's spool, so
//! that a blocked stdout does not stop the transfer until `--spool-limit`
//! bytes wait there, and the decoder writes the stream to stdout, with the
//! references that the pins hold when the stream is deduplicated. The
//! stdin reader copies stdin into the forward stream's spool, which is
//! empty with `-n`, and the forward data writer sends it, through the
//! scanner when the stream is deduplicated. The forward stream
//! stops at STOP, or at END. The backward stream stops once no process
//! reads stdout any more: this side then sends STOP, and the server closes
//! the command's stdout, as the end of the reader of a local pipeline does.
//!
//! The end is orderly (DESIGN.md §6.9, steps N1 to N6). At END, the main
//! thread stops the forward stream, closes the data session's stdin once
//! the forward data writer has ended, and the control session's once the
//! control writer has ended, without waiting for the EOF of either stdout.
//! Then the decoder writes the rest of the backward stream, and checks its
//! end, and the main thread joins every thread. Both sessions must exit
//! successfully before the client returns the command's status, after a
//! summary.

use std::io::{BufReader, IoSlice, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use bstr::ByteSlice;
use crossbeam_channel::{Sender, bounded, unbounded};

use crate::config::Config;
use crate::control::{self, Dispatch, EndFlag};
use crate::dedup::{self, Receiving, Sending};
use crate::die::{self, die};
use crate::list_exchange::ListBytes;
use crate::protocol::{
    self, Counts, End, FrameReader, FrameWriter, Request, SourceRead, die_data, flush_data, frame,
    lost, send_data,
};
use crate::rendezvous;
use crate::segments::Directory;
use crate::spool::Spool;
use crate::ssh;
use crate::stream::{self, Inlet, Stdout, Stop};
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
pub fn run(config: &Config, remote: &ssh::Remote, request: &Request, no_stdin: bool) -> ! {
    let start = Instant::now();
    // Before any other thread starts: SIGINT and SIGTERM end the run with
    // `128 + signal`, and the end kills the SSH children (DESIGN.md §6.8).
    die::handle_termination_signals();
    let settings = &request.settings;
    let compressed = settings.compression_level != 0;
    let deduplicated = request.deduplicated;
    // When the backward stream is deduplicated, a `--lookahead` below the
    // block size of this side's index is a user error, reported before
    // anything else, the update included (DESIGN.md §8).
    if deduplicated.stdout()
        && let Err(e) = settings.lookahead(config.block_size, "the index")
    {
        die(e);
    }

    // The tmp_data directory holds the segments of the pins and of the
    // spools of both streams (DESIGN.md §6.5). An unusable directory ends
    // the run when it opens, before the sessions start.
    let tmp_data = Arc::new(Directory::open(&config.tmp_data_dir));
    // This side's roles in the deduplicated streams: the receiver of a
    // deduplicated backward stream, and the sender of a deduplicated forward
    // stream. With the stop of the backward stream, they are the producers
    // of the control writer's queue (see `control`).
    let (control_tx, control_rx) = unbounded();
    let backward = deduplicated
        .stdout()
        .then(|| Receiving::new(config, &tmp_data, &control_tx, PEER));
    let forward = deduplicated
        .stdin()
        .then(|| Sending::new(&control_tx, PEER));

    // The spools of both streams, and their stops, which exist before the
    // sessions start: STOP can come at any time, also before CACHE_RESULT,
    // and the stop of the backward stream, which this side decides, puts
    // the receiver of a deduplicated backward stream into Completing
    // (DESIGN.md §6.6).
    let (forward_writer, forward_spool) = Spool::pipe(Arc::clone(&tmp_data), settings.spool_limit);
    // With `-n`, the forward stream is empty, as `ssh -n` reads `/dev/null`
    // in place of stdin: its spool ends here, before any stop can come, and
    // no stdin reader runs (DESIGN.md §8).
    let forward_writer = match no_stdin {
        false => Some(forward_writer),
        true => {
            drop(forward_writer);
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
    // deduplicated forward stream, and STOP to the forward stream.
    let dispatch = Dispatch {
        queries: backward.as_ref().map(Receiving::queries),
        answers: forward.as_ref().map(Sending::answers),
        stop: forward_stop.clone(),
    };

    // The index of a deduplicated backward stream, before the sessions
    // start: a failed update starts no remote process, and no session waits
    // idle during the update (DESIGN.md §3 and §4.4). The index, updated and
    // then read-only, or read-only with `--no-update`, gives the hash list,
    // and the lookup workers own it from then on: they answer queries as
    // soon as they arrive, and the index closes once the backward stream
    // has ended (DESIGN.md §6.4).
    //
    // With a deduplicated forward stream too, this side's HELLO comes after
    // the forward stream's list exchange, for two reasons: the exchanges
    // come one after the other, so that their frames need no stream ID
    // (DESIGN.md §6.2), and the server updates its index before its HELLO
    // of the forward exchange. The run must hold no lock while the server
    // waits for the exclusive lock of that update (DESIGN.md §4.4, step L1),
    // so then this side's update releases its index, and its late HELLO
    // opens it again.
    let hello_after_forward = deduplicated.stdin();
    let backward = backward.map(
        |receiving| match hello_after_forward && !settings.no_update {
            true => receiving.update_and_release(config),
            false => receiving.prepare(config, settings.no_update),
        },
    );

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
        std::thread::Builder::new()
            .name("control-writer".into())
            .spawn(move || control::write(control_stdin, level, &control_rx, &end, PEER))
            .expect("cannot start the control writer")
    };
    let control_reader = {
        let end = Arc::clone(&end);
        let control_stdout = Counting {
            inner: control.stdout,
            count: &RECEIVED,
        };
        std::thread::Builder::new()
            .name("control-reader".into())
            .spawn(move || control::read(control_stdout, compressed, &dispatch, None, &end, PEER))
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
    // cannot depend on them (DESIGN.md §6.2). Without a forward exchange,
    // this side's HELLO, as the backward stream's receiver, follows REQUEST
    // at once too.
    if let Err(e) = data_out.send_version_and_features() {
        die_data(PEER, &e);
    }
    send_data(&mut data_out, frame::REQUEST, &request.encode(), PEER);
    let (backward_index, backward) = match hello_after_forward {
        true => (backward, None),
        false => (
            None,
            backward.map(|index| index.hello(&mut data_out, config)),
        ),
    };
    flush_data(&mut data_out, PEER);
    read_server_version_and_features(&mut data_in, &remote.host);

    // The list exchange of a deduplicated forward stream, with this side
    // as its sender: the server's HELLO sets the block size and the hashes
    // of the stream. A `--lookahead` below that block size is a user error,
    // which ends the run before this side's HELLO, and so before the
    // command starts (DESIGN.md §8).
    let forward = forward.map(|sending| {
        let lookahead = |block_size| {
            let lookahead = settings.lookahead(block_size, "the server's index");
            lookahead.unwrap_or_else(|e| die(e))
        };
        sending
            .start(&mut data_in, &mut data_out, settings, lookahead)
            .finish(
                &mut data_in,
                &mut data_out,
                settings,
                config.hashlist_max_age,
            )
    });
    // The rest of the backward stream's list exchange. With both streams
    // deduplicated, this side's HELLO goes out only now. The server's HELLO
    // came before the forward exchange, after the server's update, so the
    // HELLO here can open this side's index again (DESIGN.md §4.4, step
    // L2).
    let backward =
        backward.or_else(|| backward_index.map(|index| index.hello(&mut data_out, config)));
    let backward = backward.map(|exchange| {
        exchange.finish(
            &mut data_in,
            &mut data_out,
            settings,
            config.hashlist_max_age,
        )
    });

    // The exchange is complete, and both streams start. The data reader
    // takes the backward stream into its spool until STREAM_END, and the
    // decoder writes it to stdout, until the stop of the backward stream
    // once no process reads stdout.
    let backward_pins = backward
        .as_ref()
        .map(|(lookups, _)| Arc::clone(lookups.pins()));
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
                stream::decode(backward_spool, compressed, &mut stdout, pins)
            })
            .expect("cannot start the decoder")
    };
    // The stdin reader reads this process's stdin, as ssh does.
    let stdin_reader = forward_writer.map(|writer| {
        let (stop, delivery) = (forward_stop.clone(), settings.stdin_delivery());
        std::thread::Builder::new()
            .name("stdin-reader".into())
            .spawn(move || stream::read_stdin(rustix::stdio::stdin(), writer, &stop, delivery))
            .expect("cannot start the stdin reader")
    });
    // The forward data writer sends the spool's bytes as they are, or for a
    // deduplicated forward stream, the frames of the scanner, which stops
    // with the stream: it can wait for answers that the server no longer
    // sends after the stop.
    let (scan, forward_lists) = forward.unzip();
    let (frames, scanning) = dedup::encoder(scan, forward_spool, forward_stop.clone());
    let forward_data_writer = {
        let (settings, stop) = (settings.clone(), forward_stop.clone());
        std::thread::Builder::new()
            .name("data-writer".into())
            .spawn(move || stream::write_stream(data_out, frames, &settings, PEER, &stop))
            .expect("cannot start the data writer")
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
    // scanner at the end of its scan, and the stop of the backward stream
    // at its raise or at step N1, or when a write fails. Neither close
    // waits for the EOF of a stdout: through an SSH multiplexing client, or
    // a wrapper that waits for SSH, that EOF comes only after SSH has
    // exited.
    let (data_out, stdin_sent) = forward_data_writer
        .join()
        .expect("the data writer panicked");
    drop(data_out);
    let control_stdin = control_writer.join().expect("the control writer panicked");
    drop(control_stdin);
    // N3. The decoder writes the rest of the backward stream and checks its
    // end, or ends at the stop. Then join the other threads: the data
    // reader reads the data session to its EOF, and the control reader the
    // control session; the stdin reader and a scan ended at the stop or at
    // the EOF of stdin.
    let stdout_counts = decoder.join().expect("the decoder panicked");
    let received = data_reader.join().expect("the data reader panicked");
    control_reader.join().expect("the control reader panicked");
    let stdin_read = match stdin_reader {
        Some(reader) => reader.join().expect("the stdin reader panicked"),
        // The empty stream of `-n` is its whole source.
        None => SourceRead {
            bytes: 0,
            eof: true,
        },
    };
    if let Some(scanning) = scanning {
        scanning.join();
    }

    // N4. Both sessions must exit successfully before the command's status
    // is returned, without reaping (DESIGN.md §6.8–6.9).
    for (pid, name) in sessions {
        let status = sys::wait_exit_status(pid);
        if status != 0 {
            die(format!("the {name} session exited with status {status}"));
        }
    }
    // N5. Nothing uses the pins any more, and the index closed when the
    // lookups ended.
    let backward_lists = backward.map(|(lookups, lists)| {
        lookups.close();
        lists
    });

    // N6.
    if end_frame.status == 127 && !end_frame.error.is_empty() {
        die::error(format!(
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
    print_summary(start, &[("stdout", backward), ("stdin", forward)]);
    die::finish(end_frame.status)
}

/// The summary line of one stream (DESIGN.md §8).
#[derive(Debug, PartialEq, Eq)]
enum Line<'a> {
    /// A stream that its receiver wrote whole: its counts, and in a
    /// deduplicated stream, the lists of its exchange.
    Whole(Counts, Option<&'a ListBytes>),
    /// A stream that stopped before its receiver wrote all of it: the bytes
    /// that its sender read from its source, the bytes of the CHUNK payloads
    /// that its receiver decoded and that it dropped undecoded, and the
    /// bytes that its destination took.
    Stopped {
        read: u64,
        decoded: u64,
        dropped: u64,
        written: u64,
    },
}

impl<'a> Line<'a> {
    /// The line of a stream, from what its sender read of its source, the
    /// counts of its receiver's decoder, `chunks`, the bytes of all its
    /// CHUNK payloads, and in a deduplicated stream, the lists of its
    /// exchange. A stream is whole when its source reader reached the EOF
    /// of its source (`SourceRead::eof`) and its destination took every
    /// byte that the sender read, whether or not a stop came. Its wire
    /// bytes are then all its CHUNK bytes, also those that a stop after its
    /// last byte left undecoded. Else the stream stopped first, and its
    /// receiver dropped the CHUNK bytes that it did not decode.
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
        if source.eof && decoded.bytes == source.bytes {
            let counts = Counts {
                wire: chunks,
                ..decoded.clone()
            };
            Line::Whole(counts, lists)
        } else {
            Line::Stopped {
                read: source.bytes,
                decoded: decoded.wire,
                dropped,
                written: decoded.bytes,
            }
        }
    }

    /// The bytes of the stream that reached its destination.
    fn written(&self) -> u64 {
        match self {
            Line::Whole(counts, _) => counts.bytes,
            Line::Stopped { written, .. } => *written,
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
    end.set();
    // The main thread waits for END.
    let _ = end_tx.send(end_frame);
    match protocol::drain_to_eof(data_in.into_inner()) {
        Ok(true) => inlet.received(),
        Ok(false) => panic!("protocol error: data after END"),
        Err(e) => die(format!("data session: {e}")),
    }
}

/// The summary on stderr (DESIGN.md §8). One line for each stream, which
/// names it. A whole stream's line gives the bytes that it carried, and the
/// bytes of its CHUNK payloads, its wire bytes; the line of the
/// deduplicated stream also gives the bytes of its literals, references and
/// runs, and the bytes of the full lists and of the delta that its list
/// exchange carried. A stopped stream's line gives the bytes that its
/// sender read, the CHUNK bytes that its receiver decoded and that it
/// dropped, and the bytes that its destination took. The last line gives,
/// as rsync does, the bytes sent and received on both sessions, the elapsed
/// time of the run with the rate of that traffic, and the speedup: the
/// bytes that reached the destinations of both streams over the traffic.
/// The threads that count have ended, so the counts are exact.
fn print_summary(start: Instant, streams: &[(&str, Line)]) {
    let elapsed = start.elapsed().as_secs_f64();
    for (name, line) in streams {
        match *line {
            Line::Whole(ref counts, Some(lists)) => die::note(format_args!(
                "{name} -- {} ({} literal, {} reference, {} run), {} wire, \
                 {} hash list, {} delta",
                Bytes(counts.bytes),
                Bytes(counts.literal),
                Bytes(counts.reference),
                Bytes(counts.run),
                Bytes(counts.wire),
                Bytes(lists.full),
                Bytes(lists.delta),
            )),
            Line::Whole(ref counts, None) => die::note(format_args!(
                "{name} -- {}, {} wire",
                Bytes(counts.bytes),
                Bytes(counts.wire)
            )),
            Line::Stopped {
                read,
                decoded,
                dropped,
                written,
            } => die::note(format_args!(
                "{name} -- {} read, pipe closed, {} wire, {} dropped, {} written",
                Bytes(read),
                Bytes(decoded),
                Bytes(dropped),
                Bytes(written)
            )),
        }
    }
    let sent = SENT.load(Relaxed);
    let received = RECEIVED.load(Relaxed);
    let traffic = sent + received;
    let carried: u64 = streams.iter().map(|(_, line)| line.written()).sum();
    die::note(format_args!(
        "sent {} and received {} in {elapsed:.1}s ({}/s), speedup {:.2}",
        Bytes(sent),
        Bytes(received),
        Bytes((traffic as f64 / elapsed).round() as u64),
        carried as f64 / traffic as f64,
    ));
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
            host.as_bstr()
        )),
        Err(e) => die_data(PEER, &e),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt;

    use super::*;
    use crate::segments::Directory;

    /// The server's side of the data session after the list exchange:
    /// `frames` as frames, then `trailing` as raw bytes.
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
            run: 0,
            wire: 7,
        };
        let line = Line::of(&read(10, true), &counts, 9, Some(&lists));
        let whole = Counts { wire: 9, ..counts };
        assert_eq!(line, Line::Whole(whole, Some(&lists)));
    }

    /// The line of a stopped stream: the bytes that its sender read, the
    /// CHUNK bytes that its receiver decoded and those that it dropped, and
    /// the bytes that its destination took (DESIGN.md §8).
    #[test]
    fn a_stopped_stream_shows_the_chunk_bytes_that_its_receiver_dropped() {
        let line = Line::of(&read(100, false), &decoded(40, 20), 50, None);
        let stopped = Line::Stopped {
            read: 100,
            decoded: 20,
            dropped: 30,
            written: 40,
        };
        assert_eq!(line, stopped);
    }

    /// A source read to its EOF does not make a stream whole: its
    /// destination must also have taken every byte of it.
    #[test]
    fn a_stream_stopped_after_the_eof_of_its_source_is_not_whole() {
        let line = Line::of(&read(100, true), &decoded(60, 30), 45, None);
        let stopped = Line::Stopped {
            read: 100,
            decoded: 30,
            dropped: 15,
            written: 60,
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

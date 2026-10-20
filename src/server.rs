//! `hashsync serve --token T` (DESIGN.md §3, §6): the server, which runs
//! the command.
//!
//! `serve` first listens on the rendezvous socket, exchanges protocol
//! versions, reads REQUEST, with the command and the settings of the run,
//! and reads its own configuration. It receives the client's stdin, the
//! forward stream, and sends the command's stdout, the backward stream.
//! In a pull, the backward stream is deduplicated against the client's
//! index, so this side is the sender of that stream; in a push, the forward
//! stream is deduplicated against this side's index, and this side is its
//! receiver; with both streams deduplicated, it is both. This side starts
//! the command as soon as it has what the streams need: its HELLO as the
//! receiver of a deduplicated forward stream, which goes out at once, and
//! the client's HELLO, which follows REQUEST, as the sender of a
//! deduplicated backward stream, whose scanner starts at once, with an
//! empty table. Then it accepts `attach` for the control session.
//!
//! Each incoming direction has one owner at a time. The main thread reads
//! the data session until the client's HELLO, then the data reader owns
//! it: it takes the forward stream's CHUNK frames into that stream's spool,
//! and the decoder writes the stream to the command's stdin, with the
//! references that the pins hold when the stream is deduplicated. The
//! control reader owns the incoming direction of the control socket, and
//! the control writer the outgoing one. The data writer of the backward
//! stream writes the data session, through the scanner when the stream is
//! deduplicated, and then the main thread sends END.
//!
//! The list exchanges run while the streams run, each side on a thread of
//! its own (DESIGN.md §5.6). As the receiver of a deduplicated forward
//! stream, this side updates its index (or opens it read-only), starts its
//! lookup workers, and runs its side of the exchange; with both streams
//! deduplicated, the client updates its own index at the same time, and
//! each side keeps its index closed from the end of its update until the
//! other's update is done too (DESIGN.md §4.4).
//!
//! The forward stream stops when the command closes its stdin, when the
//! command exits, or when it fails to start; the stop queues STOP for the
//! client (DESIGN.md §6.6). Until the stop, the data reader can wait at the
//! limit of the forward stream's spool, where it reads nothing, so the
//! session watcher waits for the session to hang up meanwhile, and ends the
//! run if it does. The backward stream stops at the client's STOP, once no
//! process reads the client's stdout: the stdout reader then closes the
//! command's stdout, whose next write gets `EPIPE` or SIGPIPE, as in a
//! local pipeline.
//!
//! The run ends with END, which this side sends once it needs nothing more
//! from the client: the command has exited, or failed to start, the
//! backward stream has ended, the forward stream has ended or stopped, the
//! stderr relay has finished, and the sides of the list exchanges have
//! ended (DESIGN.md §6.9). Before END, the closure of either session means
//! that the client disappeared, which is fatal and kills the command's
//! process group. After END, the client closes both sessions, and this side
//! joins every thread and exits (steps F1 to F3).

use std::io::{BufReader, Read, Write};
use std::net::Shutdown;
use std::os::fd::AsFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::sync::Arc;
use std::thread::JoinHandle;

use bstr::ByteSlice;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use rustix::event::{PollFd, PollFlags};
use rustix::process::Pid;

use crate::config;
use crate::control::{self, Dispatch, EndFlag};
use crate::dedup::{Receiving, Sending};
use crate::die::{self, die};
use crate::fsops::{self, poll_fds, retry_interrupted};
use crate::list_exchange;
use crate::pins::Pins;
use crate::protocol::{
    COMPRESSION_LEVELS, ControlFrame, Counts, End, ExchangeFrame, FrameReader, FrameWriter,
    Request, SourceRead, die_data, flush_data, frame, lost, send_data,
};
use crate::rendezvous;
use crate::report;
use crate::segments::Directory;
use crate::settings::RunSettings;
use crate::spool::Spool;
use crate::stream::{self, CommandStdin, Frames, Inlet, PlainFrames, Stop};
use crate::sys;
use crate::units;

/// The other host, as the messages about it name it.
const PEER: &str = "client";

/// Serve one run. `source` is the configuration file that this process's
/// own options select, which a `--hashsync-path` wrapper can pass; without
/// them, the file of the default profile, which the server needs only when
/// it receives a deduplicated forward stream (DESIGN.md §7).
pub fn run(source: config::Source, token: &str) -> ! {
    // SSH carries this process's stderr to the client's: mark its lines as
    // the server's before anything can report one (DESIGN.md §8).
    report::mark_as_server();
    // Listen before anything else, so `attach`'s retries can end.
    let listener = rendezvous::listen(token);

    // This thread reads the data session itself until the client's HELLO,
    // and then hands it to the data reader. Each frame goes out in one
    // write, so the session needs no buffer of its own.
    let mut data_in = FrameReader::new(BufReader::new(fsops::stdin()));
    let mut data_out = FrameWriter::new(fsops::stdout());

    // The server's protocol version and features go first, without waiting
    // for the client's (DESIGN.md §6.2).
    if let Err(e) = data_out.send_version_and_features() {
        die_data(PEER, &e);
    }
    match data_in.read_version_and_features(PEER) {
        Ok(true) => {}
        Ok(false) => lost(PEER),
        Err(e) => die_data(PEER, &e),
    }

    // REQUEST, with the command and the settings of the run, which this
    // side applies to its roles (DESIGN.md §6.2). Its notes follow the
    // client's `-q` and `--no-human-readable`. Then this host's own
    // configuration. The receiver of a deduplicated forward stream needs
    // the file, for its roots and its index; otherwise this side uses only
    // its `tmp_data_dir` and `hashlist_max_age`, so it needs none
    // (DESIGN.md §7).
    let payload = data_in
        .expect(frame::REQUEST, "REQUEST")
        .unwrap_or_else(|e| die_data(PEER, &e));
    let request = Request::decode(payload);
    validate_request(&request);
    let settings = &request.settings;
    let deduplicated = request.deduplicated;
    if request.quiet {
        report::disable_notes();
    }
    if !request.human_readable {
        units::disable_human_readable();
    }
    let config = Arc::new(config::load_for_run(source, deduplicated.stdin()));
    let compressed = settings.compression_level != 0;

    // This side's tmp_data directory, for the pins and the spools of both
    // streams. An unusable directory ends the run when it opens: here,
    // before command startup (DESIGN.md §6.5).
    let tmp_data = Arc::new(Directory::open(&config.tmp_data_dir));

    // This side's roles in the deduplicated streams: the receiver of a
    // deduplicated forward stream, and the sender of a deduplicated backward
    // stream. With the stop of the forward stream, they are the producers
    // of the control writer's queue (see `control`).
    // Each side has its own queue in the control writer for the frames of
    // its list exchange, so that the frames of one never wait behind the
    // list of the other.
    let (control_tx, control_rx) = unbounded();
    let (receiver_out, receiver_frames) = bounded(1);
    let (sender_out, sender_frames) = bounded(1);
    let forward = deduplicated
        .stdin()
        .then(|| Receiving::new(&config, &tmp_data, &control_tx, receiver_out));
    let backward = deduplicated
        .stdout()
        .then(|| Sending::new("stdout", &control_tx, sender_out));
    // The spools of both streams and their stops exist from here on: STOP
    // can come once the control session runs. The stop of the forward
    // stream, which this side decides, holds the pins of a deduplicated
    // forward stream, so that it can put its receiver into Completing.
    // Until that stop, the session watcher monitors the incoming direction
    // of the data session.
    let (forward_writer, forward_spool) = Spool::pipe(Arc::clone(&tmp_data), settings.spool_limit);
    let forward_stop = Stop::receiving(
        forward_spool.stopper(),
        forward.as_ref().map(Receiving::pins),
        control_tx,
    );
    let (backward_writer, backward_spool) = Spool::pipe(tmp_data, settings.spool_limit);
    let backward_stop = Stop::sending(backward_spool.stopper());
    let session_watcher = {
        let stop = forward_stop.clone();
        std::thread::Builder::new()
            .name("session-watcher".into())
            .spawn(move || watch_session(rustix::stdio::stdin(), &stop))
            .expect("cannot start the session watcher")
    };
    // The control reader forwards the queries of a deduplicated forward
    // stream to its lookup workers, the answers for a deduplicated backward
    // stream to its scanner, STOP to the backward stream, and the frames of
    // the exchanges to their sides.
    let dispatch = Dispatch {
        queries: forward.as_ref().map(Receiving::queries),
        answers: backward.as_ref().map(Sending::answers),
        stop: backward_stop.clone(),
        receiver: forward.as_ref().map(Receiving::frames),
        sender: backward.as_ref().map(Sending::frames),
    };

    // The HELLO of a deduplicated forward stream, from this side's
    // configuration, at once: the client's scanner needs it before it scans
    // a byte of stdin (DESIGN.md §6.2).
    if forward.is_some() {
        let hello = list_exchange::hello(&config);
        send_data(&mut data_out, frame::HELLO, &hello, PEER);
        flush_data(&mut data_out, PEER);
    }
    // The encoder of the backward stream. For a deduplicated backward
    // stream, the scanner, which starts at the client's HELLO, the frame
    // after REQUEST: its block size and hashes are the stream's. So a hash
    // that this side does not support, and a lookahead buffer or probe
    // threads that it cannot provide, end the run before the command
    // starts. The client checked `--lookahead` against its own block size
    // before SSH started (DESIGN.md §8), so a lookahead below it is a
    // protocol error. The sender's side of the list exchange then fills the
    // scanner's table.
    let (frames, sending): (Box<dyn Frames + Send>, _) = match backward {
        Some(sending) => {
            let hello = data_in
                .expect(frame::HELLO, "HELLO")
                .unwrap_or_else(|e| die_data(PEER, &e));
            let lookahead = |block_size| {
                let lookahead = settings.lookahead(block_size, "the index");
                lookahead.unwrap_or_else(|e| panic!("protocol error: {e}"))
            };
            let (items, threads) = sending.start(
                hello,
                backward_spool,
                backward_stop.clone(),
                settings,
                config.hashlist_max_age,
                lookahead,
            );
            (Box::new(items), Some(threads))
        }
        None => (Box::new(PlainFrames::new(backward_spool)), None),
    };

    // The command, run as sshd runs it: a command line with the user's
    // shell, the words of `-s` without a shell (DESIGN.md §6.1). Keep it
    // unreaped and in its own process group (DESIGN.md §6.8); a fatal exit
    // SIGKILLs the group, and the spawn is atomic with that registration.
    sys::reset_sigchld();
    let mut command = request.command.process();
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let forward_pins = forward
        .as_ref()
        .map(|receiving| Arc::clone(receiving.pins()));
    let running = match die::spawn_guarded(die::KillGuard::ProcessGroup, &mut command) {
        Ok(mut child) => {
            let pid = Pid::from_child(&child);
            // The forward stream's decoder writes the command's stdin, until
            // the end of the stream or the stop, and its end closes that
            // stdin.
            let decoder = {
                let stdin = child.stdin.take().unwrap();
                let mut stdin = CommandStdin::new(stdin, forward_stop.clone());
                let pins = forward_pins.clone();
                std::thread::Builder::new()
                    .name("decoder".into())
                    .spawn(move || {
                        let pins = pins.as_deref();
                        stream::decode(forward_spool, compressed, &mut stdin, pins)
                    })
                    .expect("cannot start the decoder")
            };
            // The exit of the command stops the forward stream: a descendant
            // that reads the command's stdin then reads EOF, as under SSH
            // (DESIGN.md §6.6).
            let exit_watcher = {
                let stop = forward_stop.clone();
                std::thread::Builder::new()
                    .name("exit-watcher".into())
                    .spawn(move || {
                        sys::wait_exit_status(pid);
                        stop.raise();
                    })
                    .expect("cannot start the exit watcher")
            };
            // Command stderr to this process's stderr; SSH forwards it
            // outside the protocol. The relay must finish before END.
            let mut command_stderr = child.stderr.take().unwrap();
            let relay = std::thread::Builder::new()
                .name("stderr-relay".into())
                .spawn(move || relay_stderr(&mut command_stderr))
                .expect("cannot start the stderr relay");
            // The backward stream's source: the stdout reader copies the
            // command's stdout into the stream's spool, so the command waits
            // for the data writer only when `--spool-limit` bytes wait in
            // the spool. In eager delivery, it marks the idle points of
            // stdout (DESIGN.md §6.5). At the stop of the stream, it closes
            // the command's stdout.
            let stdout_reader = {
                let stdout = child.stdout.take().unwrap();
                let (stop, delivery) = (backward_stop.clone(), settings.stdout_delivery());
                std::thread::Builder::new()
                    .name("stdout-reader".into())
                    .spawn(move || {
                        stream::read_command_stdout(stdout, backward_writer, &stop, delivery)
                    })
                    .expect("cannot start the stdout reader")
            };
            Ok(Command {
                pid,
                decoder,
                exit_watcher,
                relay,
                stdout_reader,
            })
        }
        // A startup failure (DESIGN.md §6.2): the forward stream stops, the
        // backward stream is empty, and END carries status 127 and the
        // error text.
        Err(e) => {
            forward_stop.raise();
            drop((forward_spool, backward_writer));
            let program = command.get_program().as_bytes().as_bstr();
            Err(format!("{program}: {e}"))
        }
    };

    // The data reader takes over the incoming direction of the data session
    // for the rest of the run: the forward stream, and the EOF that the
    // client causes after END. The data writer sends the backward stream,
    // until its end or its stop.
    let end = Arc::new(EndFlag::new());
    let data_reader = {
        let end = Arc::clone(&end);
        let inlet = Inlet::new(forward_writer, forward_pins.clone());
        std::thread::Builder::new()
            .name("data-reader".into())
            .spawn(move || read_data(data_in, inlet, &end))
            .expect("cannot start the data reader")
    };
    let data_writer = {
        let (settings, stop) = (settings.clone(), backward_stop.clone());
        std::thread::Builder::new()
            .name("data-writer".into())
            .spawn(move || stream::write_stream(data_out, frames, &settings, PEER, &stop))
            .expect("cannot start the data writer")
    };

    // The receiver's role of a deduplicated forward stream, while the
    // streams run: the index, updated and then read-only, or read-only with
    // `--no-update`, gives the hash list, and the lookup workers own it
    // (DESIGN.md §4.4 and §6.4). The update's notes go to stderr, which SSH
    // forwards to the client. With both streams deduplicated, the client
    // updates its own index at the same time, and the run must hold no lock
    // while the client can still wait for the exclusive lock of its update:
    // this side releases its index after its update, and opens it again only
    // once the client's update is done.
    let receiving =
        forward.map(|receiving| receiving.start(Arc::clone(&config), settings, deduplicated));

    // The control session. Each direction has one thread, from the
    // protocol version on, and both sides send their version and features
    // at once (DESIGN.md §6.2). Its frames wait in their queues until then:
    // the client's STOP, its answers, and the frames of the list exchanges.
    let session = listener.accept();
    let exchanges = [receiver_frames, sender_frames];
    let (exchanges_written, exchanges_out) = bounded::<()>(0);
    let (control_reader, control_writer) = start_control(
        &session,
        settings,
        dispatch,
        control_rx,
        exchanges,
        exchanges_written,
        &end,
    );

    // The END conditions (DESIGN.md §6.9). The exit watcher and this thread
    // both wait for the exit, so only the join of the watcher puts its stop,
    // and so STOP, before END. The decoder ends at the end of the forward
    // stream or at that stop. The stdout reader ended at the EOF of the
    // command's stdout, or at the stop of the backward stream, and so did
    // the data writer.
    let end_frame = match running {
        Ok(command) => {
            let status = sys::wait_exit_status(command.pid);
            command
                .exit_watcher
                .join()
                .expect("the exit watcher panicked");
            let decoded = command.decoder.join().expect("the decoder panicked");
            command.relay.join().expect("the stderr relay panicked");
            let backward = command
                .stdout_reader
                .join()
                .expect("the stdout reader panicked");
            End {
                status,
                error: String::new(),
                forward: decoded,
                backward,
            }
        }
        Err(error) => End {
            status: 127,
            error,
            forward: Counts::default(),
            // No command, so no stdout: an empty source, read whole.
            backward: SourceRead {
                bytes: 0,
                eof: true,
            },
        },
    };
    let (mut data_out, _) = data_writer.join().expect("the data writer panicked");
    // The sides of the list exchanges end before END, which is the last
    // frame of the data session: an exchange that the receiver began with
    // its target ends with its CACHE_RESULT, and one that ended before
    // with HASHNONE.
    if let Some(threads) = sending {
        threads.join();
    }
    let forward = receiving.map(|thread| thread.join());
    // After END, this side shuts the control session down once the data
    // session has ended, which the client can cause at once: so the last
    // frames of the exchanges, such as CACHE_RESULT, for which the client's
    // sides wait, must be out before END. They usually are long before; a
    // frame that comes last waits only behind the control frames queued
    // before it, which go first (DESIGN.md §6.9).
    let _ = exchanges_out.recv();
    // The forward stream has ended or stopped, and either put its receiver
    // into Completing before the decoder or the exit watcher ended. The
    // lookups answer nothing from then on, so their counts of misses are
    // final, although a lookup can still run.
    let misses = forward_pins.as_deref().map(Pins::misses);
    let end_frame = End {
        forward: Counts {
            misses: misses.unwrap_or_default(),
            ..end_frame.forward
        },
        ..end_frame
    };

    // END, after which the closure of a session is the normal end.
    end.set();
    send_data(&mut data_out, frame::END, &end_frame.encode(), PEER);
    flush_data(&mut data_out, PEER);

    // F1. The client closes the data session's stdin once its data writer
    // has ended, and the data reader then reads EOF.
    data_reader.join().expect("the data reader panicked");
    // F2. Only then does this side shut down the control socket, so the
    // client never sees the control session close before END. `attach`
    // then relays the rest of the socket to its stdout, and the control
    // reader reads EOF. The control writer ends once its producers are
    // gone, or when a write fails.
    let _ = session.shutdown(Shutdown::Both);
    control_reader.join().expect("the control reader panicked");
    control_writer.join().expect("the control writer panicked");
    drop(session);
    // F3. The session watcher ended at the stop of the forward stream,
    // which came before END. The lookup workers of a deduplicated forward
    // stream ended at Completing, at its end or at that stop, and the
    // decoder and the data reader, which read the pins, have ended: close
    // the pins.
    session_watcher
        .join()
        .expect("the session watcher panicked");
    drop(forward_pins);
    if let Some((lookups, _)) = forward {
        lookups.close();
    }
    die::finish(0)
}

/// The threads of a command that started, for the END conditions.
struct Command {
    pid: Pid,
    decoder: JoinHandle<Counts>,
    exit_watcher: JoinHandle<()>,
    relay: JoinHandle<()>,
    stdout_reader: JoinHandle<SourceRead>,
}

/// The owner of the incoming direction of the data session once the
/// client's HELLO is in, for the rest of the run: the forward stream's
/// CHUNK frames and its STREAM_END go to `inlet`, and after END, the EOF
/// that the client causes ends this thread. EOF before END means that the client
/// disappeared, which is fatal (DESIGN.md §6.9). After the stop of the
/// forward stream, the spool discards the CHUNK payloads, and STREAM_END
/// can still come, sent before the client learned of the stop.
fn read_data<R: Read>(mut data_in: FrameReader<R>, mut inlet: Inlet, end: &EndFlag) {
    loop {
        let received = match data_in.next() {
            Ok(Some(received)) => received,
            // After END, the end of the session is the normal end, also
            // inside a frame: a client that fails after END, while its data
            // writer sends a CHUNK, ends this side normally, and the
            // command's process group, with the descendants that it left
            // running, stays alive (DESIGN.md §6.9).
            Ok(None) | Err(_) if end.is_set() => return,
            Ok(None) => lost(PEER),
            Err(e) => die_data(PEER, &e),
        };
        match received.kind {
            frame::CHUNK => inlet.chunk(received.payload),
            frame::STREAM_END => inlet.end(received.payload),
            kind => panic!("protocol error: unexpected data frame type {kind}"),
        }
    }
}

/// The session watcher (DESIGN.md §6.6, step M1): wait until the data
/// session hangs up, unless the stop comes first. This thread exists
/// because until the stop, the data reader can wait at the limit of the
/// forward stream's spool, where it reads nothing, and so would not see
/// the EOF that the loss of the session causes (see the module doc). A
/// hangup before the stop therefore ends the run: the client disappeared.
fn watch_session(session: impl AsFd, stop: &Stop) {
    // A pipe tells its hangup with `POLLHUP`, which `poll` always reports,
    // and a socket whose peer shut down its writes tells it with
    // `POLLRDHUP`. Bytes that arrive wake this thread for neither.
    let stopped = stop.fd();
    let mut fds = [
        PollFd::new(&session, PollFlags::RDHUP),
        PollFd::new(&stopped, PollFlags::IN),
    ];
    // Ends at the hangup or at the stop; no timeout (DESIGN.md §3). After
    // the stop, the data reader no longer waits at the limit of the
    // spool, which discards what arrives, so the stop wins when both are
    // ready.
    poll_fds(&mut fds);
    if fds[1].revents().is_empty() {
        lost(PEER);
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

/// Start the threads of the control session `session`, which this side
/// accepted (DESIGN.md §6.2 and §6.8): the control writer, which sends this
/// side's version and features, then the control frames of `queue` at the
/// compression level of `settings`, and the frames of `exchanges`, the
/// queues of the sides of the list exchanges, after whose last frames it
/// drops `exchanges_written`; and the control reader, which takes the
/// client's version and features, then passes the client's frames to
/// `dispatch`. Returns the reader, then the writer.
fn start_control(
    session: &UnixStream,
    settings: &RunSettings,
    dispatch: Dispatch,
    queue: Receiver<ControlFrame>,
    exchanges: [Receiver<ExchangeFrame>; 2],
    exchanges_written: Sender<()>,
    end: &Arc<EndFlag>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let level = settings.compression_level;
    let writer = {
        let end = Arc::clone(end);
        let control_out = clone_control(session);
        std::thread::Builder::new()
            .name("control-writer".into())
            .spawn(move || {
                drop(control::write(
                    control_out,
                    level,
                    &queue,
                    &exchanges,
                    Some(exchanges_written),
                    &end,
                    PEER,
                ));
            })
            .expect("cannot start the control writer")
    };
    let reader = {
        let end = Arc::clone(end);
        let control_in = clone_control(session);
        std::thread::Builder::new()
            .name("control-reader".into())
            .spawn(move || control::read(control_in, level, &dispatch, &end, PEER))
            .expect("cannot start the control reader")
    };
    (reader, writer)
}

fn clone_control(control: &UnixStream) -> UnixStream {
    match control.try_clone() {
        Ok(clone) => clone,
        Err(e) => die(format!("cannot clone the control socket: {e}")),
    }
}

/// The settings of REQUEST are the run's: use them, or end the run before
/// command startup (DESIGN.md §6.2, §6.7).
fn validate_request(request: &Request) {
    let level = request.settings.compression_level;
    if !COMPRESSION_LEVELS.contains(&level) {
        die(format!(
            "incompatible client: compression level {level} (this server supports {} to {})",
            COMPRESSION_LEVELS.start(),
            COMPRESSION_LEVELS.end()
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt;

    use super::*;

    /// The client's side of the data session after its HELLO: `frames` as
    /// frames, then `trailing` as raw bytes.
    fn client_session(frames: &[(u8, &[u8])], trailing: &[u8]) -> Vec<u8> {
        let mut session = FrameWriter::new(Vec::new());
        for &(kind, payload) in frames {
            session.send(kind, payload).unwrap();
        }
        let mut bytes = session.into_inner();
        bytes.extend_from_slice(trailing);
        bytes
    }

    /// Run the data reader on `session` after END, with a forward spool
    /// without a limit. Returns the bytes that the data reader appended to
    /// the spool.
    fn read_session(session: &[u8]) -> Vec<u8> {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(Directory::open(path.as_os_str().as_bytes()));
        let (writer, mut spool) = Spool::pipe(dir, u64::MAX);
        let end = EndFlag::new();
        end.set();
        read_data(FrameReader::new(session), Inlet::new(writer, None), &end);
        let mut appended = Vec::new();
        spool.read_to_end(&mut appended).unwrap();
        appended
    }

    /// The payloads of CHUNK frames go to the forward stream's spool, in
    /// order, and STREAM_END ends the spool (DESIGN.md §6.6).
    #[test]
    fn chunks_fill_the_spool_of_the_forward_stream() {
        let frames = [
            (frame::CHUNK, &b"first "[..]),
            (frame::CHUNK, b"second"),
            (frame::STREAM_END, b""),
        ];
        assert_eq!(read_session(&client_session(&frames, b"")), b"first second");
    }

    /// The list exchanges run on the control session, so a frame of one on
    /// the data session is a protocol error (DESIGN.md §6.2).
    #[test]
    #[should_panic(expected = "protocol error: unexpected data frame type 8")]
    fn a_list_on_the_data_session_is_a_protocol_error() {
        read_session(&client_session(&[(frame::HASHLIST, &[0; 40][..])], b""));
    }

    /// After END, a client that fails while its data writer sends a CHUNK
    /// cuts the session inside that frame, which is the normal end of the
    /// data reader too (DESIGN.md §6.9).
    #[test]
    fn a_frame_cut_short_after_end_ends_the_data_reader_normally() {
        let frames = [(frame::CHUNK, &b"forward"[..])];
        let cut = [frame::CHUNK, 100, 0, 0, 0, 1, 2];
        assert_eq!(read_session(&client_session(&frames, &cut)), b"forward");
    }

    #[test]
    #[should_panic(expected = "protocol error: STREAM_END with a payload")]
    fn stream_end_has_no_payload() {
        read_session(&client_session(&[(frame::STREAM_END, &b"x"[..])], b""));
    }

    #[test]
    #[should_panic(expected = "protocol error: CHUNK after STREAM_END")]
    fn a_chunk_after_stream_end_is_a_protocol_error() {
        let frames = [(frame::STREAM_END, &b""[..]), (frame::CHUNK, b"late")];
        read_session(&client_session(&frames, b""));
    }
}

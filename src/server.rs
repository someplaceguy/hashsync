//! `hashsync serve --token T` (DESIGN.md §3, §6): the server, which runs
//! the command.
//!
//! `serve` first listens on the rendezvous socket, exchanges protocol
//! versions, reads REQUEST, with the command and the settings of the run,
//! and reads its own configuration. It receives the client's stdin, the
//! forward stream, and sends the command's stdout, the backward stream.
//! In a pull, the backward stream is deduplicated against the client's
//! index, so this side runs the sender's side of its list exchange. In a
//! push, the forward stream is deduplicated against this side's index:
//! this side updates the index (or opens it read-only), starts its pins and
//! lookup workers, and runs the receiver's side of the list exchange. With
//! both streams deduplicated, it does both, the forward exchange first.
//! Either way, it accepts `attach` for the control session after its first
//! HELLO, and starts the command after the last CACHE_RESULT, once the
//! client's versions on both sessions matched (DESIGN.md §6.2).
//!
//! Each incoming direction has one owner at a time. The main thread reads
//! the data session until the list exchanges are done, then the data
//! reader owns it: it takes the forward stream's CHUNK frames into that
//! stream's spool, and the decoder writes the stream to the command's
//! stdin, with the references that the pins hold when the stream is
//! deduplicated. The control reader owns the incoming direction of the
//! control socket, and the control writer the outgoing one. The main
//! thread writes the setup frames, then sends the backward stream as its
//! data writer, through the scanner when the stream is deduplicated, and
//! END.
//!
//! The forward stream stops when the command closes its stdin, when the
//! command exits, or when it fails to start; the stop queues STOP for the
//! client (DESIGN.md §6.6). Until the stop, long stretches pass in which no
//! thread reads the data session: while the main thread updates the index
//! of a deduplicated forward stream, waits for `attach`, builds the table
//! and ends its exchange, and while the data reader waits at the limit of
//! the forward stream's spool. So the session watcher waits for the session
//! to hang up meanwhile, and ends the run if it does. The backward stream
//! stops at the client's STOP, once no process reads the client's stdout:
//! the stdout reader then closes the command's stdout, whose next write
//! gets `EPIPE` or SIGPIPE, as in a local pipeline.
//!
//! The run ends with END, which this side sends once it needs nothing more
//! from the client: the command has exited, or failed to start, the
//! backward stream has ended, the forward stream has ended or stopped, and
//! the stderr relay has finished (DESIGN.md §6.9). Before END, the closure
//! of either session means that the client disappeared, which is fatal and
//! kills the command's process group. After END, the client closes both
//! sessions, and this side joins every thread and exits (steps F1 to F3).

use std::ffi::OsStr;
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
use crate::dedup::{self, Receiving, Sending};
use crate::die::{self, die};
use crate::fsops::{self, poll_fds, retry_interrupted};
use crate::protocol::{
    COMPRESSION_LEVELS, ControlFrame, Counts, End, FrameReader, FrameWriter, Request, SourceRead,
    die_data, flush_data, frame, lost, send_data,
};
use crate::rendezvous;
use crate::segments::Directory;
use crate::settings::RunSettings;
use crate::spool::Spool;
use crate::stream::{self, CommandStdin, Frames, Inlet, Item, Stop};
use crate::sys;
use crate::units;

/// The other host, as the messages about it name it.
const PEER: &str = "client";

/// Serve one run. `config_path` is `--config`, which a `--hashsync-path`
/// wrapper can pass; without it, the default configuration file, which the
/// server needs only when it receives a deduplicated forward stream
/// (DESIGN.md §7).
pub fn run(config_path: Option<&OsStr>, token: &str) -> ! {
    // SSH carries this process's stderr to the client's: mark its lines as
    // the server's before anything can report one (DESIGN.md §8).
    die::report_as_server();
    // Listen before anything else, so `attach`'s retries can end.
    let listener = rendezvous::listen(token);

    // This thread reads the data session itself until the list exchange is
    // done, and then hands it to the data reader. Each frame goes out in
    // one write, so the session needs no buffer of its own.
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
        die::disable_notes();
    }
    if !request.human_readable {
        units::disable_human_readable();
    }
    let config = config::load_for_run(config_path, deduplicated.stdin());
    let compressed = settings.compression_level != 0;

    // This side's tmp_data directory, for the pins and the spools of both
    // streams. An unusable directory ends the run when it opens: here,
    // before the list exchange and command startup (DESIGN.md §6.5).
    let tmp_data = Arc::new(Directory::open(&config.tmp_data_dir));

    // This side's roles in the deduplicated streams: the receiver of a
    // deduplicated forward stream, and the sender of a deduplicated backward
    // stream. With the stop of the forward stream, they are the producers
    // of the control writer's queue (see `control`).
    let (control_tx, control_rx) = unbounded();
    let forward = deduplicated
        .stdin()
        .then(|| Receiving::new(&config, &tmp_data, &control_tx, PEER));
    let backward = deduplicated
        .stdout()
        .then(|| Sending::new(&control_tx, PEER));
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
    // stream to its scanner, and STOP to the backward stream.
    let dispatch = Dispatch {
        queries: forward.as_ref().map(Receiving::queries),
        answers: backward.as_ref().map(Sending::answers),
        stop: backward_stop.clone(),
    };

    // The list exchanges, the forward one whole before the backward one
    // starts (DESIGN.md §6.2), and the control session. This side accepts
    // `attach` once its first HELLO is out, so that the control session
    // connects while the client prepares its next step of the exchange.
    // Each direction of the control session has one thread, from the
    // protocol version on, and both sides send their version and features
    // at once (DESIGN.md §6.2). The control reader reports readiness once
    // the client's version matched and its features arrived.
    let end = Arc::new(EndFlag::new());
    let (ready_tx, ready_rx) = bounded(1);
    let mut pending = Some((dispatch, control_rx, ready_tx));
    let mut control = None;
    let mut accept_control = || {
        if let Some((dispatch, queue, ready)) = pending.take() {
            let session = listener.accept();
            let threads = start_control(&session, settings, dispatch, queue, ready, &end);
            control = Some((session, threads));
        }
    };
    // The list exchange of a deduplicated forward stream, with this side as
    // its receiver: the index, updated and then read-only, or read-only
    // with `--no-update`, gives the hash list, and the lookup workers own
    // it from here on (DESIGN.md §4.4 and §6.4). The update's notes go to
    // stderr, which SSH forwards to the client. With both streams
    // deduplicated, the client holds no lock of its own index during this
    // update. This side's HELLO sets the block size and the hashes of the
    // stream (DESIGN.md §6.2).
    let forward = forward.map(|receiving| {
        let exchange = receiving
            .prepare(&config, settings.no_update)
            .hello(&mut data_out, &config);
        accept_control();
        let (lookups, _) = exchange.finish(
            &mut data_in,
            &mut data_out,
            settings,
            config.hashlist_max_age,
        );
        lookups
    });
    // The list exchange of a deduplicated backward stream, with this side
    // as its sender: the client's HELLO sets the block size and the hashes
    // of the stream, and this side's HELLO advertises the lists of its
    // cache. The client checked `--lookahead` against its own block size
    // before SSH started (DESIGN.md §8), so a lookahead below it is a
    // protocol error. With both streams deduplicated, the client opens its
    // index again before its HELLO (DESIGN.md §4.4); an index that no
    // longer matches its configuration ends the client, and this side then
    // reads the end of the session here, before the command starts.
    let backward = backward.map(|sending| {
        let lookahead = |block_size| {
            let lookahead = settings.lookahead(block_size, "the index");
            lookahead.unwrap_or_else(|e| panic!("protocol error: {e}"))
        };
        let exchange = sending.start(&mut data_in, &mut data_out, settings, lookahead);
        accept_control();
        let (scan, _) = exchange.finish(
            &mut data_in,
            &mut data_out,
            settings,
            config.hashlist_max_age,
        );
        scan
    });
    let (control, (control_reader, control_writer)) =
        control.expect("no stream is deduplicated, so this side sent no HELLO");

    // The data reader then takes over the incoming direction of the data
    // session for the rest of the run: the forward stream, and the EOF that
    // the client causes after END.
    let forward_pins = forward.as_ref().map(|lookups| Arc::clone(lookups.pins()));
    let data_reader = {
        let end = Arc::clone(&end);
        let inlet = Inlet::new(forward_writer, forward_pins.clone());
        std::thread::Builder::new()
            .name("data-reader".into())
            .spawn(move || read_data(data_in, inlet, &end))
            .expect("cannot start the data reader")
    };

    // The client's versions on both sessions must match before the command
    // starts (DESIGN.md §6.2). The control reader holds the socket, and the
    // data reader and the session watcher monitor stdin during this wait.
    ready_rx
        .recv()
        .expect("the control reader ended before the protocol version");

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
    let (end_frame, after_stream) =
        match die::spawn_guarded(die::KillGuard::ProcessGroup, &mut command) {
            Ok(mut child) => {
                let pid = Pid::from_child(&child);
                // The forward stream's decoder writes the command's stdin,
                // until the end of the stream or the stop, and its end closes
                // that stdin.
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
                // The exit of the command stops the forward stream: a
                // descendant that reads the command's stdin then reads EOF, as
                // under SSH (DESIGN.md §6.6).
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

                // The backward stream: the stdout reader copies the command's
                // stdout into the stream's spool, so the command waits for the
                // data writer only when `--spool-limit` bytes wait in the
                // spool. In eager delivery, it marks the idle points of stdout
                // (DESIGN.md §6.5). At the stop of the stream, it closes the
                // command's stdout. The scanner deduplicates the stream when
                // it is deduplicated; else it goes as it is. This thread is
                // its data writer, which ends at the end of the stream or at
                // the stop.
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
                let (frames, scanning) =
                    dedup::encoder(backward, backward_spool, backward_stop.clone());
                let (data_out, _) =
                    stream::write_stream(data_out, frames, settings, PEER, &backward_stop);

                // The END conditions (DESIGN.md §6.9). The exit watcher and
                // this thread both wait for the exit, so only the join of the
                // watcher puts its stop, and so STOP, before END. The decoder
                // ends at the end of the forward stream or at that stop. The
                // stdout reader ended at the EOF of the command's stdout, or
                // at the stop of the backward stream.
                let status = sys::wait_exit_status(pid);
                exit_watcher.join().expect("the exit watcher panicked");
                let forward = decoder.join().expect("the decoder panicked");
                relay.join().expect("the stderr relay panicked");
                if let Some(scanning) = scanning {
                    scanning.join();
                }
                let backward = stdout_reader.join().expect("the stdout reader panicked");
                let end_frame = End {
                    status,
                    error: String::new(),
                    forward,
                    backward,
                };
                (end_frame, data_out)
            }
            // A startup failure (DESIGN.md §6.2): the forward stream stops, the
            // backward stream is empty, and END carries status 127 and the
            // error text.
            Err(e) => {
                forward_stop.raise();
                // Nothing scans, so the scanner's sender of the control
                // queue goes now: the control writer ends only once every
                // sender is gone, and the end of the run joins it.
                drop((forward_spool, backward, backward_writer, backward_spool));
                let (data_out, _) =
                    stream::write_stream(data_out, Empty, settings, PEER, &backward_stop);
                let program = command.get_program().as_bytes().as_bstr();
                let end_frame = End {
                    status: 127,
                    error: format!("{program}: {e}"),
                    forward: Counts::default(),
                    // No command, so no stdout: an empty source, read whole.
                    backward: SourceRead {
                        bytes: 0,
                        eof: true,
                    },
                };
                (end_frame, data_out)
            }
        };

    // END, after which the closure of a session is the normal end.
    let mut data_out = after_stream;
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
    let _ = control.shutdown(Shutdown::Both);
    control_reader.join().expect("the control reader panicked");
    control_writer.join().expect("the control writer panicked");
    drop(control);
    // F3. The session watcher ended at the stop of the forward stream,
    // which came before END. The lookup workers of a deduplicated forward
    // stream ended at Completing, at its end or at that stop, and the
    // decoder and the data reader, which read the pins, have ended: close
    // the pins.
    session_watcher
        .join()
        .expect("the session watcher panicked");
    drop(forward_pins);
    if let Some(lookups) = forward {
        lookups.close();
    }
    die::finish(0)
}

/// The empty backward stream of a command that failed to start.
struct Empty;

impl Frames for Empty {
    fn next(&mut self) -> Item<'_> {
        Item::End
    }
}

/// The owner of the incoming direction of the data session once the list
/// exchange is done, for the rest of the run: the forward stream's CHUNK
/// frames and its STREAM_END go to `inlet`, and after END, the EOF that the
/// client causes ends this thread. EOF before END means that the client
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
/// because until the stop, there are long stretches in which no other
/// thread reads the data session, and so none would see the EOF that its
/// loss causes (see the module doc). A hangup before the stop therefore
/// ends the run: the client disappeared.
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
/// side's version and features, then the frames of `queue` at the
/// compression level of `settings`, and the control reader, which takes
/// the client's version and features, tells `ready`, then passes the
/// client's frames to `dispatch`. Returns the reader, then the writer.
fn start_control(
    session: &UnixStream,
    settings: &RunSettings,
    dispatch: Dispatch,
    queue: Receiver<ControlFrame>,
    ready: Sender<()>,
    end: &Arc<EndFlag>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let compressed = settings.compression_level != 0;
    let writer = {
        let end = Arc::clone(end);
        let control_out = clone_control(session);
        let level = settings.compression_level;
        std::thread::Builder::new()
            .name("control-writer".into())
            .spawn(move || drop(control::write(control_out, level, &queue, &end, PEER)))
            .expect("cannot start the control writer")
    };
    let reader = {
        let end = Arc::clone(end);
        let control_in = clone_control(session);
        std::thread::Builder::new()
            .name("control-reader".into())
            .spawn(move || {
                control::read(control_in, compressed, &dispatch, Some(ready), &end, PEER);
            })
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
    use crate::hashlist;
    use crate::protocol::TargetList;

    /// The client's side of the data session after the list exchange:
    /// `frames` as frames, then `trailing` as raw bytes.
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

    /// The data reader starts after the list exchange, so a full list that
    /// arrives then, without a HASHFULL that asked for it, is a protocol
    /// error (DESIGN.md §5.6).
    #[test]
    #[should_panic(expected = "protocol error: unexpected data frame type 3")]
    fn a_full_list_that_was_not_asked_for_is_a_protocol_error() {
        let mut session = FrameWriter::new(Vec::new());
        let full = TargetList::Full(hashlist::encode(&[]));
        full.send(&mut session).unwrap();
        read_session(&session.into_inner());
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

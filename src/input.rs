//! The input (DESIGN.md §6.6): the bytes of the receiver's stdin, which the
//! command reads as its stdin.
//!
//! On the receiver, the stdin reader reads stdin, compresses it into the
//! input stream, and appends the stream to the receiver's input spool. The
//! input writer sends the spool in INPUT frames on the data session. On the
//! sender, the data reader appends the payloads of those frames to the
//! sender's input spool, and the stdin writer decompresses that spool into
//! the command's stdin. So the producer of the receiver's stdin waits for a
//! command that reads slowly, or late, only when both input spools hold
//! their limits.
//!
//! The input stream is one zstd frame at the level and long-distance
//! matching of the data stream, or the bytes of stdin themselves when
//! compression is disabled. The command can wait for its input before it
//! writes output, so the stdin reader flushes the stream before each wait
//! for stdin, and the EOF of stdin ends the stream. It compresses on its
//! own thread: with worker threads, each flush would end a zstd job, which
//! costs much more than the end of a block. The pieces of the stream travel
//! in frames that are not compressed, so each role can stop the input after
//! any frame.
//!
//! Each role stops its input once, when the input has no use any more
//! (`InputStop`). The sender stops it when no process reads the command's
//! stdin any more, or when the command exits, and tells the receiver with
//! INPUT_STOP on the control session. The receiver stops it at INPUT_STOP,
//! or at END when END comes first. A stop discards the bytes that wait in
//! the input spool, and ends the threads of the input, which the main
//! thread then joins (DESIGN.md §6.9).
//!
//! The sender's data reader waits while the input spool holds its limit,
//! and then it reads nothing from the data session: it cannot see the loss
//! of the session. So until the stop, the session watcher sees it instead,
//! and ends the run.

use std::io::{ErrorKind, IsTerminal, PipeReader, PipeWriter, Read, Write};
use std::os::fd::AsFd;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam_channel::Sender;
use rustix::event::{PollFd, PollFlags};
use rustix::fs::OFlags;
use rustix::io::{Errno, retry_on_intr};
use rustix::process::Pid;

use crate::die::{self, die};
use crate::fsops::{any_ready, poll_fds};
use crate::protocol::{Compressor, FrameWriter, SenderControl, flush_data, frame, send_data};
use crate::segments::Directory;
use crate::spool::{Spool, SpoolWriter, Stopper};
use crate::sys::{self, ZstdDCtx};

/// The most bytes of one read of stdin, and of one INPUT frame.
const CHUNK: usize = 256 << 10;

/// The stop of the input of one role (DESIGN.md §6.6). The threads that
/// can stop the input share it, and only the first stop does something:
/// it stops the input spool, then closes the write end of the stop pipe,
/// whose EOF ends the wait of the thread that polls the pipe. On the
/// sender, it also queues INPUT_STOP for the control writer. Every stop
/// returns only once the input is stopped, also when the first stop runs
/// on another thread.
#[derive(Clone)]
pub struct InputStop(Arc<Mutex<Option<Stopping>>>);

/// What the first stop of the input does. The stop takes it, so a later
/// stop finds nothing to do.
struct Stopping {
    spool: Stopper,
    /// The write end of the stop pipe.
    pipe: PipeWriter,
    /// The queue of the sender's control writer, for INPUT_STOP; `None` on
    /// the receiver.
    control: Option<Sender<SenderControl>>,
}

impl InputStop {
    fn new(spool: Stopper, pipe: PipeWriter, control: Option<Sender<SenderControl>>) -> Self {
        let stopping = Stopping {
            spool,
            pipe,
            control,
        };
        InputStop(Arc::new(Mutex::new(Some(stopping))))
    }

    /// Stop the input, unless it is stopped already.
    pub fn stop(&self) {
        // The lock stays held until the stop is done, so that a stop on
        // another thread at the same time returns only after it: the
        // sender sends END only after the stop of its input, and the
        // receiver joins the threads of its input only after their stop
        // (DESIGN.md §6.6).
        let mut pending = self.0.lock().unwrap();
        let Some(stopping) = pending.take() else {
            return;
        };
        // The spool stops first. Then the receiver's input writer reads the
        // EOF of the stop, and not the end of the stream: the writer of the
        // spool ends the stream when the closure of the stop pipe ends the
        // stdin reader. So no INPUT_END follows the stop.
        stopping.spool.stop();
        drop(stopping.pipe);
        if let Some(control) = stopping.control {
            // The control writer ends only after its queue closes, and this
            // sender keeps the queue open until now.
            control
                .send(SenderControl::InputStop)
                .expect("the control writer ended before INPUT_STOP");
        }
    }
}

/// A new stop pipe (DESIGN.md §6.6): its read end for the thread that polls
/// it, and its write end for the stop.
fn stop_pipe() -> (PipeReader, PipeWriter) {
    std::io::pipe()
        .unwrap_or_else(|e| die(format!("cannot create the stop pipe of the input: {e}")))
}

/// The receiver's side of the input before CACHE_RESULT. The input spool
/// and the stop pipe exist from before the sessions start, so that
/// INPUT_STOP can stop the input at any time, also before CACHE_RESULT
/// (DESIGN.md §6.6).
pub struct ReceiverInput {
    stop: InputStop,
    writer: SpoolWriter,
    spool: Spool,
    /// The read end of the stop pipe, for the stdin reader.
    stopped: PipeReader,
}

impl ReceiverInput {
    /// The input spool, in the tmp_data directory `dir`, which holds at
    /// most about `limit` bytes.
    pub fn new(dir: Arc<Directory>, limit: u64) -> Self {
        let (writer, spool) = Spool::pipe(dir, limit);
        let (stopped, pipe) = stop_pipe();
        ReceiverInput {
            stop: InputStop::new(spool.stopper(), pipe, None),
            writer,
            spool,
            stopped,
        }
    }

    /// The stop of the input, for the control reader: INPUT_STOP stops the
    /// input.
    pub fn stopper(&self) -> InputStop {
        self.stop.clone()
    }

    /// Start to send `stdin` to the command, after CACHE_RESULT: the stdin
    /// reader compresses it at zstd `level`, with long-distance matching
    /// when `long`, into the input spool, and the input writer sends the
    /// spool in INPUT frames on `session`, the outgoing direction of the
    /// data session. When INPUT_STOP came first, both threads end at once.
    pub fn start<W: Write + Send + 'static>(
        self,
        stdin: impl AsFd + Send + 'static,
        session: FrameWriter<W>,
        level: i32,
        long: bool,
    ) -> RunningInput<W> {
        let ReceiverInput {
            stop,
            writer,
            spool,
            stopped,
        } = self;
        // No worker threads: the stream flushes often (`read_stdin`).
        let input = Compressor::new(writer, level, long, 0);
        let stdin_reader = std::thread::Builder::new()
            .name("stdin-reader".into())
            .spawn(move || read_stdin(stdin, &stopped, input))
            .expect("cannot start the stdin reader");
        let input_writer = std::thread::Builder::new()
            .name("input-writer".into())
            .spawn(move || write_input(spool, session))
            .expect("cannot start the input writer");
        RunningInput {
            stop,
            stdin_reader,
            input_writer,
        }
    }
}

/// The receiver's side of the input from CACHE_RESULT: the stdin reader,
/// and the input writer, which owns the outgoing direction of the data
/// session until END.
pub struct RunningInput<W: Write> {
    stop: InputStop,
    stdin_reader: JoinHandle<()>,
    input_writer: JoinHandle<FrameWriter<W>>,
}

impl<W: Write> RunningInput<W> {
    /// At END, after the command has exited: stop the input, unless
    /// INPUT_STOP has stopped it, join both threads, and give back the
    /// outgoing direction of the data session, for END_ACK.
    pub fn stop(self) -> FrameWriter<W> {
        self.stop.stop();
        let session = self.input_writer.join().expect("the input writer panicked");
        self.stdin_reader.join().expect("the stdin reader panicked");
        session
    }
}

/// The stdin reader (DESIGN.md §6.6, steps I1 to I3): read stdin, and
/// append its bytes to the input stream, until the EOF of stdin, which
/// ends the stream: its zstd frame, and then the input spool. Before each
/// wait for stdin, flush the stream, because the command can wait for the
/// bytes that the stream holds. The stop ends this function at once. A
/// failed read ends the run.
///
/// As ssh does, a stdin that is not a terminal is non-blocking while this
/// function reads it, so that no read waits: when another process takes
/// the bytes that `poll` reported, the read fails with `EAGAIN`, and the
/// next wait follows a flush, and the stop can end it. A terminal stays as
/// it is, because the shell of the user shares it. Other processes can
/// share stdin, so its flags go back to what they were when this function
/// stops to read it, and at every end of the process, also a fatal one.
fn read_stdin(stdin: impl AsFd, stopped: &PipeReader, mut input: Compressor<SpoolWriter>) {
    let non_blocking = if stdin.as_fd().is_terminal() {
        None
    } else {
        die::add_flags(stdin.as_fd(), OFlags::NONBLOCK)
    };
    let mut buf = vec![0u8; CHUNK];
    let mut fds = [
        PollFd::new(&stdin, PollFlags::IN),
        PollFd::new(stopped, PollFlags::IN),
    ];
    let eof = loop {
        // I1. When a look shows that neither stdin nor the stop is ready,
        // flush, and only then wait, without a timeout (DESIGN.md §3). So
        // every wait follows a flush, also when another process that
        // shares stdin took the bytes that an earlier look reported.
        if !any_ready(&mut fds) {
            input.flush().expect("a spool write failed");
            poll_fds(&mut fds);
        }
        if !fds[1].revents().is_empty() {
            break false;
        }
        // I2.
        let n = match retry_on_intr(|| rustix::io::read(&stdin, &mut buf[..])) {
            Ok(n) => n,
            // Another process that shares stdin took the bytes that `poll`
            // reported. Look again: so the flush comes before the next
            // wait, and the stop can end that wait.
            Err(Errno::AGAIN) => continue,
            Err(e) => die(format!("cannot read stdin: {e}")),
        };
        if n == 0 {
            break true;
        }
        // I3. The write waits while the input spool holds its limit, or
        // until the stop.
        input.write_all(&buf[..n]).expect("a spool write failed");
    };
    // This function reads stdin no more, so the drop of the guard gives
    // stdin its flags back now, as ssh restores them when it stops to read
    // its stdin, and not only at the end of the process. Not later: the end
    // of the zstd frame below can wait while the input spool holds its
    // limit.
    drop(non_blocking);
    if eof {
        // The end of the zstd frame, then the drop of the spool's writer,
        // which ends the input spool.
        let writer = input.finish().expect("a spool write failed");
        drop(writer);
    }
}

/// The input writer (DESIGN.md §6.6, steps S1 and S2): send the bytes of
/// the input spool in INPUT frames on `session`, each at once, and
/// INPUT_END at the end of the input. The stop ends it without INPUT_END.
/// A failure to send is the loss of the session before END_ACK, which
/// ends the run (DESIGN.md §6.9). Returns the session.
fn write_input<W: Write>(mut input: Spool, mut session: FrameWriter<W>) -> FrameWriter<W> {
    let mut buf = vec![0u8; CHUNK];
    loop {
        // A spool read never fails: the spool itself ends the run when it
        // cannot read a segment.
        let n = input.read(&mut buf).expect("a spool read failed");
        if n == 0 {
            if !input.is_stopped() {
                send_data(&mut session, frame::INPUT_END, b"");
                flush_data(&mut session);
            }
            return session;
        }
        send_data(&mut session, frame::INPUT, &buf[..n]);
        flush_data(&mut session);
    }
}

/// The sender's side of the input: the input spool, which the data reader
/// fills, the stdin writer, which empties it into the command's stdin, the
/// exit watcher, which stops the input when the command exits, and the
/// session watcher.
pub struct SenderInput {
    stop: InputStop,
    /// The reading end of the input spool, until the command starts.
    unread: Option<Spool>,
    /// The input stream is one zstd frame, not the bytes themselves.
    compressed: bool,
    /// The threads of the input: the session watcher, and from the start
    /// of the command, the stdin writer and the exit watcher.
    threads: Vec<JoinHandle<()>>,
}

impl SenderInput {
    /// The sender's input spool, in the tmp_data directory `dir`, with the
    /// limit `limit`, from the sender's HELLO on. The returned writer is the
    /// data reader's: it appends the payload of each INPUT frame, and its
    /// drop at INPUT_END ends the input. `compressed` tells that the input
    /// stream is a zstd frame. The session watcher watches `session`, the
    /// incoming direction of the data session, until the stop, and the stop
    /// queues INPUT_STOP on `control`, the queue of the control writer.
    pub fn new(
        dir: Arc<Directory>,
        limit: u64,
        compressed: bool,
        session: impl AsFd + Send + 'static,
        control: Sender<SenderControl>,
    ) -> (SpoolWriter, SenderInput) {
        let (writer, spool) = Spool::pipe(dir, limit);
        let (stopped, pipe) = stop_pipe();
        let session_watcher = std::thread::Builder::new()
            .name("session-watcher".into())
            .spawn(move || watch_session(session, &stopped))
            .expect("cannot start the session watcher");
        let input = SenderInput {
            stop: InputStop::new(spool.stopper(), pipe, Some(control)),
            unread: Some(spool),
            compressed,
            threads: vec![session_watcher],
        };
        (writer, input)
    }

    /// Start the input to the command `command`, which has started: the
    /// stdin writer writes the input to `stdin`, the command's stdin, and
    /// the exit watcher stops the input when the command exits.
    pub fn start(&mut self, stdin: impl Write + Send + 'static, command: Pid) {
        let spool = self.unread.take().expect("the stdin writer started twice");
        let (stop, compressed) = (self.stop.clone(), self.compressed);
        let stdin_writer = std::thread::Builder::new()
            .name("stdin-writer".into())
            .spawn(move || write_stdin(spool, stdin, compressed, &stop))
            .expect("cannot start the stdin writer");
        let stop = self.stop.clone();
        let exit_watcher = std::thread::Builder::new()
            .name("exit-watcher".into())
            .spawn(move || watch_exit(command, &stop))
            .expect("cannot start the exit watcher");
        self.threads.extend([stdin_writer, exit_watcher]);
    }

    /// Stop the input before END: after the command failed to start, or
    /// once its exit status is available. In the second case, the exit
    /// watcher, or the stdin writer at `EPIPE`, stopped the input first, or
    /// is still at it, and this stop waits until the input is stopped.
    /// From then on, the data reader discards the INPUT payloads that are
    /// still on their way, and the receiver stops sending them at
    /// INPUT_STOP. Returns the threads of the input, for their join.
    pub fn stop(self) -> StoppedInput {
        self.stop.stop();
        StoppedInput(self.threads)
    }
}

/// The threads of the sender's input after its stop, which the sender
/// joins after END_ACK, with the stderr relay (DESIGN.md §6.9). The session
/// watcher ends at the stop, and the exit watcher at the exit of the
/// command, but the stdin writer can still wait in a write to the
/// command's stdin while a descendant of the command holds it. So END does
/// not wait for that descendant.
pub struct StoppedInput(Vec<JoinHandle<()>>);

impl StoppedInput {
    pub fn join(self) {
        for thread in self.0 {
            thread.join().expect("a thread of the input panicked");
        }
    }
}

/// The session watcher (DESIGN.md §6.6, step M1): wait until the data
/// session hangs up, unless the stop pipe closes first. Until the stop, the
/// data reader can wait at the limit of the input spool, and then it does
/// not read the EOF that the loss of the session causes. So a hangup before
/// the stop ends the run: the receiver disappeared.
fn watch_session(session: impl AsFd, stopped: &PipeReader) {
    // A pipe tells its hangup with `POLLHUP`, which `poll` always reports,
    // and a socket whose peer shut down its writes tells it with
    // `POLLRDHUP`. Bytes that arrive wake this thread for neither.
    let mut fds = [
        PollFd::new(&session, PollFlags::RDHUP),
        PollFd::new(stopped, PollFlags::IN),
    ];
    // Ends at the hangup or at the stop; no timeout (DESIGN.md §3). After
    // the stop, the data reader reads the session again, so the stop wins
    // when both are ready.
    poll_fds(&mut fds);
    if fds[1].revents().is_empty() {
        die("the receiver disappeared");
    }
}

/// The exit watcher (DESIGN.md §6.6, step E1): wait until the command
/// exits, without reaping it (DESIGN.md §6.8), then stop the input. This
/// function exists because the exit of the command can come long before
/// the EOF of its stdout, which a descendant can hold open. A descendant
/// that reads the command's stdin then reads EOF, as under SSH, and does
/// not keep the run open while it waits for more input.
fn watch_exit(command: Pid, stop: &InputStop) {
    sys::wait_exit_status(command);
    stop.stop();
}

/// The stdin writer (DESIGN.md §6.6, steps C1 and C2): take the input
/// stream from the input spool, decompress it when `compressed`, and write
/// the input to `stdin`, the command's stdin, until the end of the input or
/// the stop. Then return, which closes the command's stdin. A compressed
/// stream must end its zstd frame exactly where the input ends.
///
/// The stop ends the writes after the write in progress, also inside the
/// output of one read of the spool: a few bytes of the stream can
/// decompress to gigabytes, which have no use any more.
///
/// A failed write with `EPIPE` means that no process reads the command's
/// stdin any more, so the input has no use any more: this thread stops it
/// with `stop`, and returns.
fn write_stdin(mut input: Spool, mut stdin: impl Write, compressed: bool, stop: &InputStop) {
    let mut decoder = compressed.then(Decoder::new);
    let mut buf = vec![0u8; CHUNK];
    loop {
        // A spool read never fails: the spool itself ends the run when it
        // cannot read a segment.
        let n = input.read(&mut buf).expect("a spool read failed");
        if n == 0 {
            if let Some(decoder) = &decoder
                && !input.is_stopped()
            {
                assert!(
                    decoder.ended,
                    "protocol error: the input ended inside its zstd frame"
                );
            }
            return;
        }
        let written = match &mut decoder {
            Some(decoder) => decoder.decode(&buf[..n], &mut stdin, || input.is_stopped()),
            None => stdin.write_all(&buf[..n]),
        };
        match written {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                // No process will read the rest of the input, so the stop
                // discards it, and INPUT_STOP makes the receiver send no
                // more of it (DESIGN.md §6.6, step C2).
                stop.stop();
                return;
            }
            Err(e) => die(format!("cannot write to the command's stdin: {e}")),
        }
    }
}

/// The decoding of a compressed input stream: one zstd frame, whose end
/// must be the end of the input.
struct Decoder {
    zstd: ZstdDCtx,
    /// Decoded bytes on their way to the command.
    buffer: Box<[u8]>,
    /// zstd decoded the whole frame and verified its checksum.
    ended: bool,
}

impl Decoder {
    fn new() -> Self {
        Decoder {
            zstd: ZstdDCtx::new(),
            buffer: vec![0; ZstdDCtx::out_size()].into_boxed_slice(),
            ended: false,
        }
    }

    /// Decode `input`, the next bytes of the stream, into `output`, in
    /// writes of at most `ZstdDCtx::out_size()` bytes. After a write, return
    /// early when `stopped` tells that the input stopped: the rest of the
    /// output has no use any more. Invalid compressed data, a wrong checksum
    /// included, and bytes after the end of the frame are protocol errors
    /// and panic (DESIGN.md §6.7). Write failures are `io::Error`s for the
    /// caller.
    fn decode(
        &mut self,
        mut input: &[u8],
        output: &mut impl Write,
        stopped: impl Fn() -> bool,
    ) -> std::io::Result<()> {
        assert!(
            !self.ended,
            "protocol error: input after the end of its zstd frame"
        );
        loop {
            let status = self
                .zstd
                .decompress_stream(input, &mut self.buffer)
                .unwrap_or_else(|e| panic!("protocol error: invalid compressed input: {e}"));
            input = &input[status.read..];
            output.write_all(&self.buffer[..status.written])?;
            if status.remaining == 0 {
                self.ended = true;
                assert!(
                    input.is_empty(),
                    "protocol error: input after the end of its zstd frame"
                );
                return Ok(());
            }
            // zstd needs more input once it has taken all of it without
            // filling the buffer.
            if input.is_empty() && status.written < self.buffer.len() {
                return Ok(());
            }
            if stopped() {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{PipeReader, Read};
    use std::os::unix::ffi::OsStrExt;
    use std::process::{Command, Stdio};

    use crossbeam_channel::{Receiver, unbounded};

    use super::*;
    use crate::protocol::FrameReader;
    use crate::scanner::tests::noise;

    /// A tmp_data directory, which lives as long as the returned guard.
    fn tmp_data() -> (Arc<Directory>, tempfile::TempDir) {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(Directory::open(path.as_os_str().as_bytes()));
        (dir, tempdir)
    }

    /// A data session that never hangs up, for the session watcher of a
    /// test: `/dev/null`. The hangup of a pipe would make the watcher end
    /// the test process.
    fn quiet_session() -> File {
        File::open("/dev/null").unwrap()
    }

    /// Give the payloads of the INPUT frames of `session` to `writer`, as
    /// the sender's data reader does, until INPUT_END, which ends the
    /// input.
    fn deliver(session: &mut FrameReader<PipeReader>, mut writer: SpoolWriter) {
        loop {
            let received = session.next().unwrap().expect("EOF before INPUT_END");
            match received.kind {
                frame::INPUT => writer.write_all(received.payload).unwrap(),
                frame::INPUT_END => return,
                kind => panic!("unexpected frame type {kind}"),
            }
        }
    }

    #[test]
    fn the_input_reaches_the_command_through_both_roles() {
        // The receiver's side reads a pipe as its stdin, and sends INPUT
        // frames on a second pipe. The sender's side gets their payloads,
        // as its data reader does, and writes the input to the stdin of
        // `cat`, the command. The command reads exactly the input, then
        // EOF: with compression and without, also for an empty input. Both
        // spools hold at most about 1 MiB, less than the input. The exit of
        // the command stops the input, which queues INPUT_STOP for the
        // control writer, once.
        let (dir, _tempdir) = tmp_data();
        let inputs = [
            [noise(3 << 20, 1), b"text ".repeat(200_000)].concat(),
            Vec::new(),
        ];
        for input in inputs {
            for level in [0, 3] {
                let (stdin, mut producer) = std::io::pipe().unwrap();
                let (session_in, session_out) = std::io::pipe().unwrap();
                let receiver = ReceiverInput::new(Arc::clone(&dir), 1 << 20).start(
                    stdin,
                    FrameWriter::new(session_out),
                    level,
                    false,
                );
                let (control_tx, control_rx) = unbounded();
                let (writer, mut sender) = SenderInput::new(
                    Arc::clone(&dir),
                    1 << 20,
                    level != 0,
                    quiet_session(),
                    control_tx,
                );
                let mut cat = Command::new("cat")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();
                sender.start(cat.stdin.take().unwrap(), Pid::from_child(&cat));
                let mut output = cat.stdout.take().unwrap();
                let command = std::thread::spawn(move || {
                    let mut read = Vec::new();
                    output.read_to_end(&mut read).unwrap();
                    read
                });
                let produced = {
                    let input = input.clone();
                    std::thread::spawn(move || producer.write_all(&input).unwrap())
                };
                deliver(&mut FrameReader::new(session_in), writer);
                produced.join().unwrap();
                let read = command.join().unwrap();
                assert!(read == input, "level {level}: the input differs");
                drop(receiver.stop());
                sender.stop().join();
                // The exit watcher has returned, so this wait can reap.
                assert!(cat.wait().unwrap().success());
                assert!(matches!(
                    control_rx.try_iter().collect::<Vec<_>>()[..],
                    [SenderControl::InputStop]
                ));
            }
        }
    }

    #[test]
    fn a_stop_ends_the_input_without_input_end() {
        // The producer writes to stdin, but does not end it. The stdin
        // reader then waits for stdin, and the stop at END ends it, and the
        // input writer, without INPUT_END.
        let (dir, _tempdir) = tmp_data();
        let (stdin, mut producer) = std::io::pipe().unwrap();
        let (session_in, session_out) = std::io::pipe().unwrap();
        let receiver =
            ReceiverInput::new(dir, u64::MAX).start(stdin, FrameWriter::new(session_out), 0, false);
        producer.write_all(b"partial").unwrap();
        let mut session = FrameReader::new(session_in);
        let received = session.next().unwrap().unwrap();
        assert_eq!(
            (received.kind, received.payload),
            (frame::INPUT, &b"partial"[..])
        );
        drop(receiver.stop());
        assert!(session.next().unwrap().is_none(), "a frame after the stop");
    }

    #[test]
    fn input_stop_before_cache_result_ends_the_input_at_once() {
        // INPUT_STOP can come before the receiver starts its input. Then the
        // stdin reader ends without a read of stdin, and the input writer
        // without INPUT_END, before the stop at END: both threads end, and
        // the bytes of stdin stay for its next reader (DESIGN.md §6.6).
        let (dir, _tempdir) = tmp_data();
        let (stdin, mut producer) = std::io::pipe().unwrap();
        let mut unread = stdin.try_clone().unwrap();
        producer.write_all(b"unread").unwrap();
        drop(producer);
        let (session_in, session_out) = std::io::pipe().unwrap();
        let input = ReceiverInput::new(dir, u64::MAX);
        input.stopper().stop();
        let running = input.start(stdin, FrameWriter::new(session_out), 3, false);
        // Ends when both threads have ended; no timeout.
        while !(running.stdin_reader.is_finished() && running.input_writer.is_finished()) {
            std::thread::yield_now();
        }
        drop(running.stop());
        let mut session = FrameReader::new(session_in);
        assert!(session.next().unwrap().is_none(), "a frame after the stop");
        let mut left = Vec::new();
        unread.read_to_end(&mut left).unwrap();
        assert_eq!(left, b"unread");
    }

    #[test]
    fn the_input_stream_flushes_when_stdin_waits() {
        // The producer writes to stdin, then waits, without EOF. The
        // command can wait for these bytes, so the stdin reader flushes the
        // stream before it waits for more: the bytes reach the session in
        // INPUT frames that decode before the zstd frame ends.
        let (dir, _tempdir) = tmp_data();
        let (stdin, mut producer) = std::io::pipe().unwrap();
        let (session_in, session_out) = std::io::pipe().unwrap();
        let receiver =
            ReceiverInput::new(dir, u64::MAX).start(stdin, FrameWriter::new(session_out), 3, false);
        producer.write_all(b"partial").unwrap();
        let mut session = FrameReader::new(session_in);
        let mut decoder = Decoder::new();
        let mut decoded = Vec::new();
        // No deadline: without the flush, the test hangs here.
        while decoded.len() < b"partial".len() {
            let received = session.next().unwrap().expect("EOF before the input");
            assert_eq!(received.kind, frame::INPUT);
            decoder
                .decode(received.payload, &mut decoded, || false)
                .unwrap();
        }
        assert_eq!(decoded, b"partial");
        assert!(!decoder.ended, "the zstd frame ended before EOF");
        drop(receiver.stop());
    }

    /// The sender's input spool with `stream`, and the end of the input.
    fn ended_spool(stream: &[u8]) -> (Spool, tempfile::TempDir) {
        let (dir, tempdir) = tmp_data();
        let (mut writer, spool) = Spool::pipe(dir, u64::MAX);
        writer.write_all(stream).unwrap();
        drop(writer);
        (spool, tempdir)
    }

    /// The stop of the sender's input for `spool`: the queue of the
    /// control writer receives its INPUT_STOP, and the read end of its stop
    /// pipe reads EOF after it.
    fn sender_stop(spool: &Spool) -> (InputStop, Receiver<SenderControl>, PipeReader) {
        let (control_tx, control_rx) = unbounded();
        let (stopped, pipe) = stop_pipe();
        let stop = InputStop::new(spool.stopper(), pipe, Some(control_tx));
        (stop, control_rx, stopped)
    }

    /// `input` as one zstd frame, as the stdin reader compresses it.
    fn compressed(input: &[u8]) -> Vec<u8> {
        let mut compressor = Compressor::new(Vec::new(), 3, false, 0);
        compressor.write_all(input).unwrap();
        compressor.finish().unwrap()
    }

    #[test]
    #[should_panic(expected = "protocol error: the input ended inside its zstd frame")]
    fn the_input_ends_with_its_zstd_frame() {
        // The last byte of the checksum is missing.
        let stream = compressed(&noise(10_000, 3));
        let (spool, _tempdir) = ended_spool(&stream[..stream.len() - 1]);
        let (stop, _control, _stopped) = sender_stop(&spool);
        write_stdin(spool, std::io::sink(), true, &stop);
    }

    #[test]
    #[should_panic(expected = "protocol error: input after the end of its zstd frame")]
    fn nothing_follows_the_zstd_frame_of_the_input() {
        let mut stream = compressed(b"input");
        stream.push(0);
        let (spool, _tempdir) = ended_spool(&stream);
        let (stop, _control, _stopped) = sender_stop(&spool);
        write_stdin(spool, std::io::sink(), true, &stop);
    }

    #[test]
    fn a_command_that_closes_its_stdin_stops_the_input() {
        // No process reads the command's stdin: the write fails with
        // EPIPE, which ends the stdin writer without a failure of the run.
        // It stops the input: the stop pipe closes, and INPUT_STOP waits in
        // the queue of the control writer (DESIGN.md §6.6, step C2).
        let (reader, command_stdin) = std::io::pipe().unwrap();
        drop(reader);
        let (spool, _tempdir) = ended_spool(&compressed(b"input that no one reads"));
        let (stop, control, mut stopped) = sender_stop(&spool);
        write_stdin(spool, command_stdin, true, &stop);
        assert!(matches!(control.try_recv(), Ok(SenderControl::InputStop)));
        assert_eq!(stopped.read(&mut [0]).unwrap(), 0, "the stop pipe is open");
        // Only the first stop does something.
        stop.stop();
        assert!(control.try_recv().is_err(), "a second INPUT_STOP");
    }

    /// The stdin of a command that stops the input during the first write
    /// to it, as the exit of the command does while a descendant holds that
    /// stdin. It counts the bytes of the writes.
    struct StopsTheInput {
        stop: InputStop,
        written: usize,
    }

    impl Write for StopsTheInput {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.stop.stop();
            self.written += buf.len();
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_stop_ends_the_writes_inside_the_output_of_one_read() {
        // 16 MiB of zeros compress to less than a kilobyte, so one read of
        // the spool takes the whole stream. The input stops during the
        // first write, so the stdin writer writes no more of the output of
        // that read: it has no use any more (DESIGN.md §6.6, step C2).
        let (spool, _tempdir) = ended_spool(&compressed(&vec![0; 16 << 20]));
        let (stop, _control, _stopped) = sender_stop(&spool);
        let mut stdin = StopsTheInput {
            stop: stop.clone(),
            written: 0,
        };
        write_stdin(spool, &mut stdin, true, &stop);
        assert!(
            stdin.written <= ZstdDCtx::out_size(),
            "{} bytes written",
            stdin.written
        );
    }
}

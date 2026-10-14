//! The two ends of a stream (DESIGN.md §6.6), which both hosts build the
//! same way, and the stop of a stream.
//!
//! The sending end has a source reader, which copies the stream's source
//! into the stream's spool (`read_stdin`, `read_command_stdout`), an
//! encoder, which turns the bytes of the spool into stream frames, and a
//! data writer, which compresses the frames into the stream's zstd frame
//! and sends that in CHUNK frames, then STREAM_END. The scanner is the
//! encoder of a deduplicated stream (`dedup`); a plain stream has no
//! encoder of its own, because its data writer takes the spool directly,
//! one LIT frame per take (`PlainFrames`).
//!
//! The receiving end has the data reader of its session, which appends the
//! payload of each CHUNK to the stream's spool (`Inlet`), and a decoder,
//! which decompresses the spool, reads the stream frames, and writes their
//! bytes to the stream's destination (`decode`).
//!
//! Either stream can stop, and the host that receives it decides: the
//! server stops the forward stream once no process reads the command's
//! stdin, and the client stops the backward stream once no process reads
//! its stdout. A stop is one event on each host (`Stop`), which happens at
//! most once and wakes every thread of that end of the stream: a stopped
//! end gives up at once, without finishing the stream.

use std::io::{BufReader, ErrorKind, IsTerminal, PipeReader, PipeWriter, Write};
use std::os::fd::{AsFd, BorrowedFd};
use std::process::ChildStdin;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, Weak};

use crossbeam_channel::{Receiver, Sender, bounded};
use rustix::event::{PollFd, PollFlags};
use rustix::fs::OFlags;
use rustix::io::{Errno, retry_on_intr};

use crate::config::thread_count;
use crate::die::{self, die};
use crate::fsops::{self, any_ready, poll_fds};
use crate::pins::Pins;
use crate::protocol::{
    CHUNK_BYTES, Compressor, ControlFrame, Counts, Delivery, FrameReader, FrameWriter, Rle,
    SourceRead, StreamReader, die_data, flush_data, frame, send_data,
};
use crate::settings::RunSettings;
use crate::spool::{Spool, SpoolWriter, Stopper, Taken};

/// The most bytes of one read of a source, and of one take of a stream's
/// bytes from its spool on the sending host: by a plain stream's encoder,
/// so of one LIT frame of a plain stream, or by the lookahead reader of a
/// deduplicated one.
pub const READ_SIZE: usize = 256 << 10;

/// The size of the decoder's read buffer over its spool.
const DECODE_BUFFER: usize = 256 << 10;

/// The largest write of the bytes of an RLE frame to the destination.
const RUN_PIECE: u64 = 256 << 10;

/// The stop of one stream on one host (DESIGN.md §6.6). The threads that
/// can stop the stream share it, and only the first raise does something:
/// it stops the stream's spool on this host, so that its reader reads the
/// EOF of the stop and its writer discards what it appends; on the host
/// that receives a deduplicated stream, it puts the stream's receiver into
/// Completing; it closes the write end of the stop pipe, whose EOF ends the
/// waits in `poll`, and disconnects the stop channel, which ends the waits
/// in a `Select`. On the host that receives the stream, which decides the
/// stop, it also queues STOP for the control writer, so that the sending
/// host stops too. Every raise returns only once the stream is stopped,
/// also when the first raise runs on another thread.
///
/// Each host makes the stops of both streams before anything can raise
/// them: the client before the sessions start, and the server at REQUEST.
#[derive(Clone)]
pub struct Stop(Arc<StopState>);

struct StopState {
    /// What the first raise does; `None` once it has begun.
    pending: Mutex<Option<Stopping>>,
    /// The first raise has begun. A look at it never waits for the lock.
    raised: AtomicBool,
    /// The read end of the stop pipe, which reads EOF after the stop.
    pipe: PipeReader,
    /// Disconnected at the stop. Nothing is ever sent on it.
    signal: Receiver<()>,
}

struct Stopping {
    spool: Stopper,
    /// The pins of this host's receiver when the stream is deduplicated:
    /// its lookups have no use after the stop. The stop only tells them;
    /// the threads of the receiver own them.
    pins: Option<Weak<Pins>>,
    /// The write end of the stop pipe.
    pipe: PipeWriter,
    /// The only sender of the stop channel.
    signal: Sender<()>,
    /// The queue of the control writer, for STOP, on the host that
    /// receives the stream; `None` on the host that sends it, and once the
    /// peer needs no STOP any more.
    control: Option<Sender<ControlFrame>>,
}

impl Stop {
    /// The stop of a stream on the host that sends it, whose spool on this
    /// host `spool` stops. The host that receives the stream decides the
    /// stop and tells this host with STOP, so this stop holds neither pins
    /// nor a sender of the control writer's queue.
    pub fn sending(spool: Stopper) -> Stop {
        Stop::new(spool, None, None)
    }

    /// The stop of a stream on the host that receives it, which decides
    /// the stop: `spool` is the stream's spool on this host, `pins` are
    /// those of this host's receiver when the stream is deduplicated, and
    /// `control` is the queue of the control writer, which gets STOP. This
    /// stop is one of the producers of that queue (see `control`).
    pub fn receiving(
        spool: Stopper,
        pins: Option<&Arc<Pins>>,
        control: Sender<ControlFrame>,
    ) -> Stop {
        Stop::new(spool, pins, Some(control))
    }

    fn new(
        spool: Stopper,
        pins: Option<&Arc<Pins>>,
        control: Option<Sender<ControlFrame>>,
    ) -> Stop {
        let (read, write) =
            std::io::pipe().unwrap_or_else(|e| die(format!("cannot create the stop pipe: {e}")));
        let (signal_tx, signal) = bounded(0);
        let stopping = Stopping {
            spool,
            pins: pins.map(Arc::downgrade),
            pipe: write,
            signal: signal_tx,
            control,
        };
        Stop(Arc::new(StopState {
            pending: Mutex::new(Some(stopping)),
            raised: AtomicBool::new(false),
            pipe: read,
            signal,
        }))
    }

    /// Stop the stream, unless it is stopped already.
    pub fn raise(&self) {
        // The lock stays held until the stop is done, so that a raise on
        // another thread at the same time returns only after it: the server
        // sends END only after the stop, which queues STOP first.
        let mut pending = self.0.pending.lock().unwrap();
        let Some(stopping) = pending.take() else {
            return;
        };
        self.0.raised.store(true, SeqCst);
        // S1.
        stopping.spool.stop();
        // S2. The lookups end, and the index closes, also while the run
        // goes on: the decoder reads no pin after the stop (DESIGN.md
        // §6.4). The pins themselves stay until the decoder has ended. Pins
        // that are gone already have no lookups to end.
        if let Some(pins) = stopping.pins.and_then(|pins| pins.upgrade()) {
            pins.complete();
        }
        // S3.
        drop(stopping.pipe);
        drop(stopping.signal);
        // S4. While this sender is here, the control writer ends only after
        // a failed write. Before END, that write ends the run with `die`,
        // and the control writer never returns. After END, it returns, and
        // the send fails, but the peer needs no STOP after END: on the
        // client, the stop of the backward stream keeps its sender until
        // step N1, and stdout can break before that.
        if let Some(control) = stopping.control {
            let _ = control.send(ControlFrame::Stop);
        }
    }

    /// The peer needs no STOP for this stream any more, because END has
    /// passed: drop this stop's sender of the control writer's queue, so
    /// that the control writer can end. A raise after this one sends no
    /// STOP; it still stops the stream on this host. This function exists
    /// for the client's stop of the backward stream, which nothing needs
    /// to raise before the end of the run, while the server raises its own
    /// stop before END (DESIGN.md §6.9, step N1).
    pub fn release_control(&self) {
        if let Some(stopping) = self.0.pending.lock().unwrap().as_mut() {
            stopping.control = None;
        }
    }

    /// True once a raise has begun: the stream is stopped, or is being
    /// stopped. It takes no lock, so a writer can look before each write.
    pub fn is_raised(&self) -> bool {
        self.0.raised.load(SeqCst)
    }

    /// The read end of the stop pipe, for a wait in `poll`: it is readable,
    /// with EOF, once the stream is stopped.
    pub fn fd(&self) -> BorrowedFd<'_> {
        self.0.pipe.as_fd()
    }

    /// A channel for a wait in a `Select`: it disconnects at the stop.
    pub fn signal(&self) -> &Receiver<()> {
        &self.0.signal
    }
}

/// What the data writer of a stream takes next from the stream's encoder.
/// A literal borrows the encoder's buffer, so that a plain stream's take
/// from its spool goes to the compressor without a copy.
pub enum Item<'a> {
    /// LIT: literal bytes.
    Literal(&'a [u8]),
    /// REF: the pin at the receiver's current output offset.
    Reference,
    /// RLE: `length` copies of `byte`.
    Run { byte: u8, length: u64 },
    /// Flush the stream. The encoder gives a flush only when nothing is
    /// left to write: at an idle point of the source, once the frames of the
    /// bytes before it are all out, and in a deduplicated stream also after
    /// every `lookahead` bytes (DESIGN.md §6.2 and §6.3).
    Flush,
    /// The end of the stream: end its zstd frame, and send STREAM_END.
    End,
    /// The stop of the stream: end without STREAM_END.
    Stopped,
}

/// The encoder of a stream, as its data writer sees it (DESIGN.md §6.6).
pub trait Frames {
    /// The next item, after a wait without a timeout if none is ready.
    fn next(&mut self) -> Item<'_>;
}

/// The encoder of a sending end that is plain or deduplicated, as the
/// run decides: the data writer takes either one the same way.
impl<F: Frames + ?Sized> Frames for Box<F> {
    fn next(&mut self) -> Item<'_> {
        (**self).next()
    }
}

/// The data writer of a stream (DESIGN.md §6.6, steps D1 to D3): it takes
/// the stream frames of `frames`, compresses them into the stream's zstd
/// frame at the settings of the run, unless compression is disabled, and
/// sends that on `session` in CHUNK frames. At each flush, the CHUNK that
/// holds the last bytes goes out. At the end of the stream, it ends the
/// zstd frame, sends the last CHUNK and STREAM_END, and flushes the session.
/// A failure to send is the loss of `peer`, the other host, or another
/// failure of the session, which ends the run. Returns the session, for
/// what follows the stream, and the bytes of the CHUNK payloads sent, which
/// the summary compares with what the receiver decoded (DESIGN.md §8).
///
/// `stop` is the stop of the stream on this host. At the stop, this
/// function ends at once, without STREAM_END: it takes no more items,
/// however many the encoder holds, and sends nothing more, also while
/// zstd ends a job or the zstd frame. Only a CHUNK that is on its way when
/// the stop comes still goes out. zstd cannot cancel a job of its worker
/// threads, so this function returns only once the workers have finished
/// the jobs that zstd holds (DESIGN.md §6.9).
pub fn write_stream<W: Write>(
    session: FrameWriter<W>,
    mut frames: impl Frames,
    settings: &RunSettings,
    peer: &str,
    stop: &Stop,
) -> (FrameWriter<W>, u64) {
    let compressor = Compressor::new(
        ChunkWriter::new(session, stop),
        settings.compression_level,
        settings.compression_long,
        thread_count(settings.compression_threads),
    );
    let mut stream = FrameWriter::new(compressor);
    loop {
        // The look at the stop takes no lock and no system call.
        let item = if stop.is_raised() {
            Item::Stopped
        } else {
            frames.next()
        };
        match item {
            // D1.
            Item::Literal(bytes) => send_data(&mut stream, frame::LIT, bytes, peer),
            Item::Reference => send_data(&mut stream, frame::REF, b"", peer),
            Item::Run { byte, length } => {
                let run = Rle { byte, length }.encode();
                send_data(&mut stream, frame::RLE, &run, peer);
            }
            // D2.
            Item::Flush => flush_data(&mut stream, peer),
            // D3.
            Item::End => {
                let chunks = stream
                    .into_inner()
                    .finish()
                    .unwrap_or_else(|e| die_data(peer, &e));
                let mut session = chunks.session;
                // A stop while zstd ended its frame left the frame cut
                // short, so the end of the stream has no use.
                if !stop.is_raised() {
                    send_data(&mut session, frame::STREAM_END, b"", peer);
                    flush_data(&mut session, peer);
                }
                return (session, chunks.sent);
            }
            Item::Stopped => {
                let chunks = stream.into_inner().into_transport();
                return (chunks.session, chunks.sent);
            }
        }
    }
}

/// The bytes of one stream on their way to the data session: its zstd
/// frame, or its stream frames when compression is disabled, in CHUNK
/// frames (DESIGN.md §6.2). A CHUNK goes out once it holds `CHUNK_BYTES`,
/// and at each flush. This type exists so that the session frames after
/// the stream, such as END, stay outside its zstd frame, where the peer
/// finds them without decoding the stream, and so that a stopped stream
/// sends nothing more: after the stop, it sends no CHUNK and drops what zstd
/// still writes into it, so a zstd job that ends after the stop costs CPU,
/// not bytes on the link (DESIGN.md §6.6).
struct ChunkWriter<'a, W: Write> {
    session: FrameWriter<W>,
    /// The bytes of the next CHUNK.
    buffer: Vec<u8>,
    /// The bytes of the CHUNK payloads sent so far, for the summary.
    sent: u64,
    /// The stop of the stream on this host.
    stop: &'a Stop,
}

impl<'a, W: Write> ChunkWriter<'a, W> {
    fn new(session: FrameWriter<W>, stop: &'a Stop) -> Self {
        ChunkWriter {
            session,
            buffer: Vec::with_capacity(CHUNK_BYTES),
            sent: 0,
            stop,
        }
    }

    fn is_stopped(&self) -> bool {
        self.stop.is_raised()
    }

    /// Send the buffer as one CHUNK.
    fn send(&mut self) -> std::io::Result<()> {
        self.session.send(frame::CHUNK, &self.buffer)?;
        self.sent += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
}

impl<W: Write> Write for ChunkWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.is_stopped() {
            self.buffer.clear();
            return Ok(buf.len());
        }
        let n = buf.len().min(CHUNK_BYTES - self.buffer.len());
        self.buffer.extend_from_slice(&buf[..n]);
        if self.buffer.len() == CHUNK_BYTES {
            self.send()?;
        }
        Ok(n)
    }

    /// Send the bytes of the buffer, then flush the session.
    fn flush(&mut self) -> std::io::Result<()> {
        if self.is_stopped() {
            self.buffer.clear();
            return Ok(());
        }
        if !self.buffer.is_empty() {
            self.send()?;
        }
        self.session.flush()
    }
}

/// The encoder of a plain stream (DESIGN.md §6.6): each take from the
/// stream's spool becomes one LIT frame, and each idle point that the spool
/// reports becomes a flush. The spool reports an idle point only once its
/// reader has taken every byte before it, and no byte follows it, so the
/// flush comes when nothing is left to write, as the data writer of a
/// deduplicated stream makes it.
pub struct PlainFrames {
    /// The spool, until its end or its stop.
    spool: Option<Spool>,
    buffer: Box<[u8]>,
}

impl PlainFrames {
    pub fn new(spool: Spool) -> Self {
        PlainFrames {
            spool: Some(spool),
            buffer: vec![0; READ_SIZE].into_boxed_slice(),
        }
    }
}

impl Frames for PlainFrames {
    fn next(&mut self) -> Item<'_> {
        let spool = self.spool.as_mut().expect("a take after the end");
        match spool.take_next(&mut self.buffer) {
            Taken::Bytes(n) => Item::Literal(&self.buffer[..n]),
            Taken::Idle => Item::Flush,
            // A stopped spool is dropped, which discards what it holds.
            Taken::End if spool.is_stopped() => {
                self.spool = None;
                Item::Stopped
            }
            // The normal end closes the spool, which frees its storage.
            Taken::End => {
                self.spool.take().unwrap().close();
                Item::End
            }
        }
    }
}

/// The source reader of the forward stream (DESIGN.md §6.6, steps I1 to
/// I3): copy `stdin` into the stream's spool through `writer`, until the
/// EOF of `stdin`, which ends the spool, or until `stop`. In eager
/// `delivery`, it marks an idle point in the spool each time `stdin` has
/// no byte ready, so that the data writer flushes the stream behind the
/// bytes before it. A failed read ends the run. Returns what it read, and
/// whether stdin was at its EOF when it stopped reading, for the summary.
///
/// As ssh does, a stdin that is not a terminal is non-blocking while this
/// function reads it, so that no read waits: when another process takes
/// the bytes that `poll` reported, the read fails with `EAGAIN`, and the
/// stop can end the next wait. A terminal stays as it is, because the
/// shell of the user shares it; a read of it can then wait for the next
/// line. Other processes can share stdin, so its flags go back to what
/// they were when this function stops to read it, and at every end of the
/// process, also a fatal one.
pub fn read_stdin(
    stdin: impl AsFd,
    mut writer: SpoolWriter,
    stop: &Stop,
    delivery: Delivery,
) -> SourceRead {
    let non_blocking = if stdin.as_fd().is_terminal() {
        None
    } else {
        die::add_flags(stdin.as_fd(), OFlags::NONBLOCK)
    };
    let stop_fd = stop.fd();
    let mut fds = [
        PollFd::new(&stdin, PollFlags::IN),
        PollFd::new(&stop_fd, PollFlags::IN),
    ];
    let mut buf = vec![0u8; READ_SIZE];
    let mut read = SourceRead::default();
    loop {
        // I1. In eager delivery, a look without a wait first: when neither
        // stdin nor the stop is ready, an idle point comes before the wait.
        // No timeout (DESIGN.md §3).
        if delivery == Delivery::Batch || !any_ready(&mut fds) {
            if delivery == Delivery::Eager {
                writer.mark_idle();
            }
            poll_fds(&mut fds);
        }
        if !fds[1].revents().is_empty() {
            break;
        }
        // I2.
        let n = match retry_on_intr(|| rustix::io::read(&stdin, &mut buf[..])) {
            Ok(0) => {
                read.eof = true;
                break;
            }
            Ok(n) => n,
            // Another process that shares stdin took the bytes that `poll`
            // reported. Look again: so the idle point comes before the next
            // wait, and the stop can end that wait.
            Err(Errno::AGAIN) => continue,
            Err(e) => die(format!("cannot read stdin: {e}")),
        };
        // I3. The append waits while the spool holds its limit, or until
        // the stop.
        read.bytes += n as u64;
        writer.write_all(&buf[..n]).expect("a spool write failed");
    }
    // The stop can come while stdin is at its EOF already: before the first
    // read of an empty stdin, or between the read of its last bytes and the
    // read of its EOF. Then the stream lost nothing, and a look at stdin that
    // reads nothing keeps its summary line from depending on which came
    // first (DESIGN.md §8). The server's stdout reader needs no such look:
    // the client stops the backward stream only once a write to its stdout
    // has failed, which lost bytes, so that stream's line shows the stop
    // anyway.
    if !read.eof {
        read.eof = fsops::at_eof(stdin.as_fd());
    }
    // This function reads stdin no more, so the drop of the guard gives
    // stdin its flags back now, as ssh restores them when it stops to read
    // its stdin, and not only at the end of the process. Then the drop of
    // the writer ends the spool, which after a stop changes nothing.
    drop(non_blocking);
    read
}

/// The source reader of the backward stream (DESIGN.md §6.6, steps O1 to
/// O3): copy the command's `stdout` into the stream's spool through
/// `writer`, until the EOF of `stdout`, which ends the spool, or until
/// `stop`. In eager `delivery`, it marks an idle point in the spool each
/// time `stdout` has no byte ready. A failed read ends the run. Returns
/// what it read, for the summary.
///
/// This function exists so that the stop ends the reads of the command's
/// stdout, and closes it, as the end of the reader of a local `cmd | head`
/// does: the command's next write then fails with `EPIPE`, or kills it
/// with SIGPIPE. No other process has hashsync's end of the pipe, so it is
/// non-blocking for good: a read takes what the pipe holds, and costs no
/// other system call, and the waits for bytes are in `poll`, together with
/// the stop pipe.
pub fn read_command_stdout(
    stdout: impl AsFd,
    mut writer: SpoolWriter,
    stop: &Stop,
    delivery: Delivery,
) -> SourceRead {
    let flags = rustix::fs::fcntl_getfl(&stdout).expect("fcntl F_GETFL on a pipe failed");
    rustix::fs::fcntl_setfl(&stdout, flags | OFlags::NONBLOCK)
        .expect("fcntl F_SETFL on a pipe failed");
    let stop_fd = stop.fd();
    let mut buf = vec![0u8; READ_SIZE];
    let mut read = SourceRead::default();
    // An idle point is marked, and no byte has come after it.
    let mut idle = false;
    loop {
        // O1. The look at the stop takes no system call.
        if stop.is_raised() {
            break;
        }
        match retry_on_intr(|| rustix::io::read(&stdout, &mut buf[..])) {
            Ok(0) => {
                read.eof = true;
                break;
            }
            // O3. The append waits while the spool holds its limit, or
            // until the stop.
            Ok(n) => {
                read.bytes += n as u64;
                idle = false;
                writer.write_all(&buf[..n]).expect("a spool write failed");
            }
            // O2. No byte is ready: in eager delivery, an idle point, once
            // for each stretch without bytes. Then wait for bytes, EOF, or
            // the stop; no timeout (DESIGN.md §3).
            Err(Errno::AGAIN) => {
                if delivery == Delivery::Eager && !idle {
                    writer.mark_idle();
                    idle = true;
                }
                poll_fds(&mut [
                    PollFd::new(&stdout, PollFlags::IN),
                    PollFd::new(&stop_fd, PollFlags::IN),
                ]);
            }
            Err(e) => die(format!("cannot read the command's output: {e}")),
        }
    }
    // The drop of the writer ends the spool, which after a stop changes
    // nothing. Then the drop of `stdout` closes this end of the pipe.
    drop(writer);
    drop(stdout);
    read
}

/// The data reader's part of a receiving end (DESIGN.md §6.6): it appends
/// the payload of each CHUNK to the stream's spool, which waits while the
/// spool holds its limit, and ends the spool at STREAM_END. After a stop,
/// the spool discards what arrives, so the data reader takes CHUNK and
/// STREAM_END the same way before and after the stop. A deduplicated
/// stream's receiver enters Completing at STREAM_END: the lookups have no
/// use any more, because the sender emits a frame only after its answer
/// (DESIGN.md §6.4).
pub struct Inlet {
    /// The writer of the spool, until STREAM_END.
    writer: Option<SpoolWriter>,
    /// The pins of a deduplicated stream's receiver.
    pins: Option<Arc<Pins>>,
    /// The bytes of the CHUNK payloads so far, also those that a stopped
    /// spool discarded, for the summary.
    received: u64,
}

impl Inlet {
    /// The data reader's part of the receiving end whose spool `writer`
    /// fills; `pins` for a deduplicated stream.
    pub fn new(writer: SpoolWriter, pins: Option<Arc<Pins>>) -> Self {
        Inlet {
            writer: Some(writer),
            pins,
            received: 0,
        }
    }

    /// A CHUNK with `payload`, the next bytes of the stream.
    pub fn chunk(&mut self, payload: &[u8]) {
        assert!(!payload.is_empty(), "protocol error: empty CHUNK");
        let writer = self
            .writer
            .as_mut()
            .expect("protocol error: CHUNK after STREAM_END");
        self.received += payload.len() as u64;
        writer.write_all(payload).expect("a spool write failed");
    }

    /// The bytes of all the CHUNK payloads that came, also after a stop:
    /// those that the decoder did not decode, the spool dropped (DESIGN.md
    /// §8).
    pub fn received(&self) -> u64 {
        self.received
    }

    /// STREAM_END with `payload`, which must be empty: the drop of the
    /// writer ends the spool. Completing comes first, so that the lookup
    /// workers begin to end, and with the last of them the index closes,
    /// before the destination can see the end of the stream. A command that
    /// updates the index once its stdin has ended then waits at most for
    /// the lookups that are under way.
    pub fn end(&mut self, payload: &[u8]) {
        assert!(
            payload.is_empty(),
            "protocol error: STREAM_END with a payload"
        );
        let writer = self
            .writer
            .take()
            .expect("protocol error: a second STREAM_END");
        if let Some(pins) = &self.pins {
            pins.complete();
        }
        drop(writer);
    }

    /// True after STREAM_END.
    pub fn has_ended(&self) -> bool {
        self.writer.is_none()
    }
}

/// The stream stopped after `written` bytes of a write had gone out: the
/// rest of its bytes have no use any more.
pub struct Stopped {
    pub written: usize,
}

/// Where a decoder writes the bytes of its stream.
pub trait Destination {
    /// Write all of `bytes`, unless the stream stops first. Then report
    /// how many of them the destination took, which the summary counts
    /// (DESIGN.md §8).
    fn write(&mut self, bytes: &[u8]) -> Result<(), Stopped>;
}

/// The decoder of a receiving end (DESIGN.md §6.6, steps C1 to C3): read
/// the stream frames of `spool`, one zstd frame when `compressed`, and
/// write their bytes to `destination`, until the end of the stream, or
/// until a stop. A deduplicated stream's receiver gives its `pins`: REF
/// reads the pin at the output offset, and after each frame the pins learn
/// the new offset (DESIGN.md §6.4). Returns the counts of the bytes that
/// the destination took, and of the bytes of the spool that it decoded.
///
/// The normal end is the end of the zstd frame, with its checksum, where a
/// stream frame ends, with the end of the spool right after it; without
/// compression, the end of the spool where a stream frame ends. A spool
/// that ends inside the zstd frame or inside a stream frame, and bytes
/// after the zstd frame, are protocol errors. After a stop, nothing is
/// checked: the spool gives EOF at once, and the destination writes no
/// more.
pub fn decode(
    spool: Spool,
    compressed: bool,
    destination: &mut impl Destination,
    pins: Option<&Pins>,
) -> Counts {
    let stopper = spool.stopper();
    let transport = BufReader::with_capacity(DECODE_BUFFER, spool);
    let mut frames = FrameReader::new(StreamReader::new(transport, compressed));
    let (mut counts, ended) = decode_frames(&mut frames, &stopper, destination, pins);
    let reader = frames.into_inner();
    counts.wire = reader.consumed();
    if ended {
        reader.finish().expect("a spool read failed");
    }
    counts
}

/// The loop of `decode`, over the stream frames of the spool of `stopper`.
/// The counts take in the whole of each frame before its bytes go out, so
/// that an output offset that overflows is a protocol error whatever part
/// of the frame the destination takes; at a stop, they give back the bytes
/// that did not go out. Returns the counts, and whether the stream frames
/// reached their normal end, which `decode` then checks.
fn decode_frames<R: std::io::BufRead>(
    frames: &mut FrameReader<StreamReader<R>>,
    stopper: &Stopper,
    destination: &mut impl Destination,
    pins: Option<&Pins>,
) -> (Counts, bool) {
    let mut counts = Counts::default();
    let mut block = pins.map(|pins| vec![0u8; pins.block_size()]);
    let mut run = Vec::new();
    loop {
        // C1. A spool read never fails, so an error is the end of the spool
        // inside a frame or inside the zstd frame.
        let received = match frames.next() {
            Ok(Some(received)) => received,
            Ok(None) => return (counts, !stopper.is_stopped()),
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::UnexpectedEof, "a spool read failed");
                assert!(
                    stopper.is_stopped(),
                    "protocol error: a stream ended inside a frame or inside its zstd frame"
                );
                return (counts, false);
            }
        };
        // C2.
        let length;
        let written = match received.kind {
            frame::LIT => {
                assert!(!received.payload.is_empty(), "protocol error: empty LIT");
                length = received.payload.len() as u64;
                counts.bytes = advance(counts.bytes, length);
                counts.literal += length;
                taken(destination.write(received.payload))
            }
            frame::REF => {
                assert!(
                    received.payload.is_empty(),
                    "protocol error: REF with a payload"
                );
                let (Some(pins), Some(block)) = (pins, &mut block) else {
                    panic!("protocol error: REF in a stream without deduplication");
                };
                length = block.len() as u64;
                counts.bytes = advance(counts.bytes, length);
                counts.reference += length;
                pins.read_reference(block);
                taken(destination.write(block))
            }
            frame::RLE => {
                // Any positive length, though a sender emits runs of at
                // least `B` bytes (DESIGN.md §6.4). The pieces keep a stop
                // inside one run prompt: 14 bytes of RLE can stand for
                // terabytes.
                let rle = Rle::decode(received.payload);
                length = rle.length;
                counts.bytes = advance(counts.bytes, length);
                counts.run += length;
                run.clear();
                run.resize(rle.length.min(RUN_PIECE) as usize, rle.byte);
                write_run(destination, &run, length)
            }
            kind => panic!("protocol error: unexpected stream frame type {kind}"),
        };
        if let Err(written) = written {
            let rest = length - written;
            counts.bytes -= rest;
            match received.kind {
                frame::LIT => counts.literal -= rest,
                frame::REF => counts.reference -= rest,
                _ => counts.run -= rest,
            }
            return (counts, false);
        }
        // C3.
        if let Some(pins) = pins {
            pins.reached(counts.bytes);
        }
    }
}

/// The result of a write of a whole frame: at a stop, the bytes of the
/// frame that the destination took.
fn taken(written: Result<(), Stopped>) -> Result<(), u64> {
    written.map_err(|Stopped { written }| written as u64)
}

/// Write the bytes of an RLE frame of `length` bytes to `destination`, in
/// pieces of `piece`, which holds the repeated byte, until a stop. On a
/// stop, returns the bytes of the frame that the destination took.
fn write_run(destination: &mut impl Destination, piece: &[u8], length: u64) -> Result<(), u64> {
    let mut remaining = length;
    while remaining > 0 {
        let n = remaining.min(piece.len() as u64) as usize;
        if let Err(Stopped { written }) = destination.write(&piece[..n]) {
            return Err(length - remaining + written as u64);
        }
        remaining -= n as u64;
    }
    Ok(())
}

/// Advance the output offset; overflow is a protocol error
/// (DESIGN.md §6.2).
fn advance(offset: u64, length: u64) -> u64 {
    offset
        .checked_add(length)
        .expect("protocol error: output offset overflow")
}

/// The client's stdout, the destination of the backward stream on the
/// client (DESIGN.md §6.6, step C2). A write waits while stdout is full, as
/// ssh's writes do: a consumer that pauses holds the stream back, and the
/// client's spool takes the stream meanwhile. `EPIPE` means that no process
/// reads stdout any more: then this raises the stop of the backward stream,
/// as the command's stdin raises the stop of the forward stream on the
/// server. Other write failures end the run.
pub struct Stdout<W: Write = fsops::FdWriter> {
    out: W,
    stop: Stop,
}

impl Stdout {
    /// This process's stdout, of the backward stream that `stop` stops.
    pub fn new(stop: Stop) -> Self {
        Stdout {
            out: fsops::stdout(),
            stop,
        }
    }
}

impl<W: Write> Destination for Stdout<W> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), Stopped> {
        // As `write_all`, but with the count of the bytes that went out
        // before `EPIPE`, which the summary shows (DESIGN.md §8).
        let mut written = 0;
        while written < bytes.len() {
            match self.out.write(&bytes[written..]) {
                Ok(0) => die(format!("cannot write to stdout: {}", ErrorKind::WriteZero)),
                Ok(n) => written += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                    self.stop.raise();
                    return Err(Stopped { written });
                }
                Err(e) => die(format!("cannot write to stdout: {e}")),
            }
        }
        Ok(())
    }
}

/// The command's stdin, the destination of the forward stream on the
/// server (DESIGN.md §6.6, step C2). This type exists so that no write
/// keeps the decoder from the stop: hashsync's end of the pipe is
/// non-blocking, so a write takes at most the room in the pipe, and the
/// decoder looks at the stop before each one, and waits for room in `poll`
/// together with the stop pipe. Neither a descendant of the command that
/// holds its stdin without reading it, nor an RLE frame of 14 bytes that
/// stands for terabytes, keeps it writing after the stop. `EPIPE` means
/// that no process reads the command's stdin any more: then this raises
/// the stop. The drop closes the command's stdin.
pub struct CommandStdin {
    pipe: ChildStdin,
    stop: Stop,
}

impl CommandStdin {
    /// The command's stdin `pipe`, of the forward stream that `stop`
    /// stops. No other process has hashsync's end of the pipe, so its
    /// flags need no restore.
    pub fn new(pipe: ChildStdin, stop: Stop) -> Self {
        let flags = rustix::fs::fcntl_getfl(&pipe).expect("fcntl F_GETFL on a pipe failed");
        rustix::fs::fcntl_setfl(&pipe, flags | OFlags::NONBLOCK)
            .expect("fcntl F_SETFL on a pipe failed");
        CommandStdin { pipe, stop }
    }
}

impl Destination for CommandStdin {
    fn write(&mut self, bytes: &[u8]) -> Result<(), Stopped> {
        let mut written = 0;
        while written < bytes.len() {
            if self.stop.is_raised() {
                return Err(Stopped { written });
            }
            match retry_on_intr(|| rustix::io::write(&self.pipe, &bytes[written..])) {
                Ok(n) => written += n,
                // The pipe is full: wait for room, or for the stop. No
                // timeout (DESIGN.md §3).
                Err(Errno::AGAIN) => {
                    let stopped = self.stop.fd();
                    poll_fds(&mut [
                        PollFd::new(&self.pipe, PollFlags::OUT),
                        PollFd::new(&stopped, PollFlags::IN),
                    ]);
                }
                Err(Errno::PIPE) => {
                    self.stop.raise();
                    return Err(Stopped { written });
                }
                Err(e) => die(format!("cannot write to the command's stdin: {e}")),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::Read;
    use std::os::unix::ffi::OsStrExt;
    use std::sync::atomic::AtomicU64;

    use crossbeam_channel::unbounded;
    use proptest::prelude::*;

    use super::*;
    use crate::scanner::tests::noise;
    use crate::segments::Directory;

    /// A tmp_data directory, which lives as long as the returned guard.
    fn tmp_data() -> (Arc<Directory>, tempfile::TempDir) {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(Directory::open(path.as_os_str().as_bytes()));
        (dir, tempdir)
    }

    /// A stop that nothing raises, for a stream that ends normally, with
    /// the spool that it would stop and the guard of that spool's
    /// directory.
    fn unraised() -> (Stop, Spool, tempfile::TempDir) {
        let (dir, tempdir) = tmp_data();
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        (Stop::sending(spool.stopper()), spool, tempdir)
    }

    /// The settings of `hashsync ssh` with `options`.
    fn settings(options: &[&str]) -> RunSettings {
        crate::settings::tests::settings(options)
    }

    /// Items for a data writer, in order; then `End`.
    struct Script<'a>(std::vec::IntoIter<Item<'a>>);

    impl Frames for Script<'_> {
        fn next(&mut self) -> Item<'_> {
            self.0.next().unwrap_or(Item::End)
        }
    }

    /// An encoder that counts the items that a data writer takes from it,
    /// flushes included, so that a test can tell how far the writer went:
    /// what it sends does not show that, because a stopped writer drops
    /// what it takes.
    pub(crate) struct Counted<F> {
        frames: F,
        taken: Arc<AtomicU64>,
    }

    impl<F> Counted<F> {
        /// `frames`, counted, and the count, which the caller keeps.
        pub(crate) fn new(frames: F) -> (Self, Arc<AtomicU64>) {
            let taken = Arc::new(AtomicU64::new(0));
            let counted = Counted {
                frames,
                taken: Arc::clone(&taken),
            };
            (counted, taken)
        }
    }

    impl<F: Frames> Frames for Counted<F> {
        fn next(&mut self) -> Item<'_> {
            self.taken.fetch_add(1, SeqCst);
            self.frames.next()
        }
    }

    /// What a data writer sent: the session frames, without the payloads
    /// of the CHUNK frames, and the bytes of all CHUNK payloads.
    fn session_of(bytes: &[u8]) -> (Vec<(u8, usize)>, Vec<u8>) {
        let mut reader = FrameReader::new(bytes);
        let (mut frames, mut stream) = (Vec::new(), Vec::new());
        while let Some(frame) = reader.next().unwrap() {
            frames.push((frame.kind, frame.payload.len()));
            if frame.kind == frame::CHUNK {
                stream.extend_from_slice(frame.payload);
            }
        }
        (frames, stream)
    }

    /// The stream frames of `stream`, read as a receiver reads them.
    fn stream_frames(stream: &[u8], compressed: bool) -> Vec<(u8, Vec<u8>)> {
        let mut reader = FrameReader::new(StreamReader::new(stream, compressed));
        let mut frames = Vec::new();
        while let Some(frame) = reader.next().unwrap() {
            frames.push((frame.kind, frame.payload.to_vec()));
        }
        reader.into_inner().finish().unwrap();
        frames
    }

    #[test]
    fn a_stream_ends_with_its_zstd_frame_and_stream_end() {
        // The frames go out as one zstd frame in CHUNK payloads, whose end
        // is the last CHUNK; STREAM_END follows it. A flush sends the CHUNK
        // that holds the last bytes. Without compression, the CHUNK
        // payloads are the frames as they are (DESIGN.md §6.2).
        let literal = noise(300 << 10, 1);
        let (stop, _spool, _tempdir) = unraised();
        for options in [&[][..], &["--compression-level", "0"]] {
            let items = vec![
                Item::Literal(&literal),
                Item::Flush,
                Item::Reference,
                Item::Run {
                    byte: 7,
                    length: 1 << 40,
                },
            ];
            let (session, sent) = write_stream(
                FrameWriter::new(Vec::new()),
                Script(items.into_iter()),
                &settings(options),
                "client",
                &stop,
            );
            let (frames, stream) = session_of(&session.into_inner());
            assert_eq!(sent, stream.len() as u64, "{options:?}");
            assert_eq!(frames.last(), Some(&(frame::STREAM_END, 0)), "{options:?}");
            let chunks = frames
                .iter()
                .filter(|(kind, _)| *kind == frame::CHUNK)
                .count();
            assert!(chunks >= 2, "{options:?}: {chunks} chunks");
            let decoded = stream_frames(&stream, options.is_empty());
            let run = Rle {
                byte: 7,
                length: 1 << 40,
            }
            .encode();
            assert_eq!(
                decoded,
                [
                    (frame::LIT, literal.clone()),
                    (frame::REF, Vec::new()),
                    (frame::RLE, run)
                ],
                "{options:?}"
            );
        }
    }

    #[test]
    fn an_empty_stream_is_an_empty_zstd_frame_and_stream_end() {
        let (stop, _spool, _tempdir) = unraised();
        for (options, chunks) in [(&[][..], 1), (&["--compression-level", "0"], 0)] {
            let (session, sent) = write_stream(
                FrameWriter::new(Vec::new()),
                Script(Vec::new().into_iter()),
                &settings(options),
                "client",
                &stop,
            );
            let (frames, stream) = session_of(&session.into_inner());
            assert_eq!(sent, stream.len() as u64);
            assert_eq!(frames.len(), chunks + 1, "{options:?}");
            assert_eq!(frames.last(), Some(&(frame::STREAM_END, 0)));
            assert!(stream_frames(&stream, chunks == 1).is_empty());
        }
    }

    #[test]
    fn a_stopped_stream_ends_without_stream_end() {
        let items = vec![Item::Literal(b"before"), Item::Flush, Item::Stopped];
        let (stop, _spool, _tempdir) = unraised();
        let (session, sent) = write_stream(
            FrameWriter::new(Vec::new()),
            Script(items.into_iter()),
            &settings(&["--compression-level", "0"]),
            "client",
            &stop,
        );
        let (frames, stream) = session_of(&session.into_inner());
        assert_eq!(frames, [(frame::CHUNK, 11)]);
        assert_eq!(stream, [&[frame::LIT, 6, 0, 0, 0][..], b"before"].concat());
        assert_eq!(sent, 11);
    }

    /// The payloads of the CHUNK frames on `session`, which holds no other
    /// frame.
    fn chunks(session: &[u8]) -> Vec<Vec<u8>> {
        let mut reader = FrameReader::new(session);
        let mut chunks = Vec::new();
        while let Some(frame) = reader.next().unwrap() {
            assert_eq!(frame.kind, frame::CHUNK);
            chunks.push(frame.payload.to_vec());
        }
        chunks
    }

    #[test]
    fn chunks_carry_the_stream_in_pieces_of_at_most_256_kib() {
        // A CHUNK goes out once it holds 256 KiB, and at each flush, which
        // sends nothing when no byte waits. The payloads are consecutive
        // pieces of the stream (DESIGN.md §6.2).
        let bytes = noise(600 << 10, 3);
        let (stop, _spool, _tempdir) = unraised();
        let mut writer = ChunkWriter::new(FrameWriter::new(Vec::new()), &stop);
        writer.write_all(&bytes[..1000]).unwrap();
        writer.flush().unwrap();
        writer.flush().unwrap();
        writer.write_all(&bytes[1000..]).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.sent, 600 << 10);
        let session = writer.session.into_inner();
        let sizes: Vec<usize> = chunks(&session).iter().map(Vec::len).collect();
        let last = (600 << 10) - 1000 - 2 * CHUNK_BYTES;
        assert_eq!(sizes, [1000, CHUNK_BYTES, CHUNK_BYTES, last]);
        assert!(chunks(&session).concat() == bytes, "the stream differs");
    }

    #[test]
    fn a_stopped_stream_sends_no_more_chunks() {
        // After the stop, what zstd still writes has no use: no CHUNK goes
        // out, also at a flush, and the bytes sent stay as they were
        // (DESIGN.md §6.6).
        let (dir, _tempdir) = tmp_data();
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        let stop = Stop::sending(spool.stopper());
        let mut writer = ChunkWriter::new(FrameWriter::new(Vec::new()), &stop);
        writer.write_all(b"before").unwrap();
        writer.flush().unwrap();
        writer.write_all(b"held").unwrap();
        stop.raise();
        writer.write_all(&noise(300 << 10, 5)).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.sent, 6);
        assert_eq!(chunks(&writer.session.into_inner()), [b"before".to_vec()]);
    }

    /// An encoder that gives `left` literals of `size` bytes of noise, each
    /// one other bytes, then the end of the stream.
    struct Literals {
        literal: Vec<u8>,
        size: usize,
        left: u64,
    }

    impl Literals {
        fn new(left: u64, size: usize) -> Self {
            Literals {
                literal: Vec::new(),
                size,
                left,
            }
        }
    }

    impl Frames for Literals {
        fn next(&mut self) -> Item<'_> {
            if self.left == 0 {
                return Item::End;
            }
            self.left -= 1;
            self.literal = noise(self.size, self.left);
            Item::Literal(&self.literal)
        }
    }

    /// Wait until the thread `tid` of this process sleeps in the system
    /// call `nr`; no deadline.
    fn wait_for_syscall(tid: rustix::thread::Pid, nr: u64) {
        while sleeping_syscall(tid).0 != nr {
            std::thread::yield_now();
        }
    }

    /// Run the data writer of a stream over `frames`, with the settings of
    /// `options` and a stop, on a session whose pipe is full: its first
    /// CHUNK waits for room. Once the writer sleeps in that write, raise
    /// the stop, then read the pipe to its end. Returns the items that the
    /// writer took, the CHUNK bytes that it reports sent, and the session
    /// frames that came after the bytes that filled the pipe.
    fn stopped_while_sending(
        frames: impl Frames + Send + 'static,
        options: &[&str],
    ) -> (u64, u64, Vec<(u8, usize)>) {
        let (dir, _tempdir) = tmp_data();
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        let stop = Stop::sending(spool.stopper());
        let (frames, taken) = Counted::new(frames);
        let (mut reader, session) = std::io::pipe().unwrap();
        rustix::fs::fcntl_setfl(&session, OFlags::NONBLOCK).unwrap();
        let mut filled = 0;
        while let Ok(n) = rustix::io::write(&session, &[0; 4096]) {
            filled += n;
        }
        rustix::fs::fcntl_setfl(&session, OFlags::empty()).unwrap();
        let settings = settings(options);
        let (tid_tx, tid_rx) = std::sync::mpsc::channel();
        let writer = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                tid_tx.send(rustix::thread::gettid()).unwrap();
                let session = FrameWriter::new(session);
                let (session, sent) = write_stream(session, frames, &settings, "server", &stop);
                // Its end ends the read below.
                drop(session);
                sent
            })
        };
        // No deadline: a writer that never sends a CHUNK hangs the test.
        wait_for_syscall(tid_rx.recv().unwrap(), libc::SYS_writev as u64);
        stop.raise();
        let mut sent = Vec::new();
        reader.read_to_end(&mut sent).unwrap();
        let (frames, _) = session_of(&sent[filled..]);
        let sent = writer.join().unwrap();
        (taken.load(SeqCst), sent, frames)
    }

    #[test]
    fn the_stop_ends_the_data_writer_inside_the_end_of_its_zstd_frame() {
        // zstd's worker takes 2 MiB of noise, less than one of its jobs,
        // without output, so the CHUNKs go out only once the data writer has
        // taken the end of the stream and ends the zstd frame. The first one
        // waits for room, and the stop comes: that CHUNK still goes out, but
        // none of the rest of the frame, and no STREAM_END (DESIGN.md §6.6).
        let options = ["--compression-threads", "1", "--no-compression-long"];
        let (taken, sent, frames) = stopped_while_sending(Literals::new(8, 256 << 10), &options);
        assert_eq!(taken, 8 + 1, "the writer did not take the end");
        assert_eq!(frames, [(frame::CHUNK, CHUNK_BYTES)]);
        assert_eq!(sent, CHUNK_BYTES as u64);
    }

    #[test]
    fn the_stop_ends_the_data_writer_before_its_next_item() {
        // Without compression, the fourth literal of 64 KiB completes the
        // first CHUNK, which waits for room. At the stop, that CHUNK still
        // goes out, but the writer takes no more items, however many the
        // encoder holds (DESIGN.md §6.6).
        let options = ["--compression-level", "0"];
        let (taken, _, frames) = stopped_while_sending(Literals::new(16, 64 << 10), &options);
        assert_eq!(taken, 4, "the writer took more items");
        assert_eq!(frames, [(frame::CHUNK, CHUNK_BYTES)]);
    }

    #[test]
    fn a_plain_stream_takes_its_spool_and_flushes_at_its_idle_points() {
        // Each take from the spool is one LIT frame, each idle point a
        // flush, and the end of the spool the end of the stream (DESIGN.md
        // §6.6).
        let (dir, _tempdir) = tmp_data();
        let (mut writer, spool) = Spool::pipe(dir, u64::MAX);
        writer.write_all(b"first").unwrap();
        writer.mark_idle();
        let mut frames = PlainFrames::new(spool);
        assert!(matches!(frames.next(), Item::Literal(bytes) if bytes == b"first"));
        assert!(matches!(frames.next(), Item::Flush));
        writer.write_all(b"second").unwrap();
        drop(writer);
        assert!(matches!(frames.next(), Item::Literal(bytes) if bytes == b"second"));
        assert!(matches!(frames.next(), Item::End));
    }

    #[test]
    fn a_stopped_plain_stream_reports_the_stop() {
        let (dir, _tempdir) = tmp_data();
        let (mut writer, spool) = Spool::pipe(dir, u64::MAX);
        writer.write_all(b"discarded").unwrap();
        let stop = Stop::sending(spool.stopper());
        let mut frames = PlainFrames::new(spool);
        stop.raise();
        assert!(matches!(frames.next(), Item::Stopped));
        drop(writer);
    }

    #[test]
    fn the_first_raise_stops_the_stream_once() {
        // The first raise stops the spool, puts the receiver of a
        // deduplicated stream into Completing, closes the stop pipe,
        // disconnects the stop channel, and queues STOP; a later raise does
        // nothing (DESIGN.md §6.6).
        let (dir, _tempdir) = tmp_data();
        let (mut writer, mut spool) = Spool::pipe(Arc::clone(&dir), u64::MAX);
        let (control, queue) = unbounded();
        let pins = Arc::new(Pins::new(
            dir,
            crate::hash::StrongHash::Blake3,
            4096,
            control.clone(),
        ));
        let stop = Stop::receiving(spool.stopper(), Some(&pins), control);
        // The stop only tells the pins: the threads of the receiver own them.
        assert_eq!(Arc::strong_count(&pins), 1, "the stop owns the pins");
        assert!(!stop.is_raised());
        let completing = pins.completing();
        assert!(completing.try_recv().unwrap_err().is_empty());
        assert!(stop.signal().try_recv().unwrap_err().is_empty());
        stop.raise();
        stop.clone().raise();
        assert!(stop.is_raised());
        assert!(completing.recv().is_err(), "no Completing at the stop");
        assert!(stop.signal().recv().is_err(), "the stop channel is open");
        // Completing dropped the sender of the pins, and the stop its own:
        // only STOP is left in the queue.
        assert!(matches!(
            queue.try_iter().collect::<Vec<_>>()[..],
            [ControlFrame::Stop]
        ));
        assert!(queue.recv().is_err(), "the stop kept its sender");
        let mut pipe = std::fs::File::from(stop.fd().try_clone_to_owned().unwrap());
        assert_eq!(pipe.read(&mut [0]).unwrap(), 0, "the stop pipe is open");
        // The spool discards what is appended after the stop.
        writer.write_all(b"late").unwrap();
        assert_eq!(spool.read(&mut [0; 8]).unwrap(), 0);
        assert!(spool.is_stopped());
    }

    #[test]
    fn the_inlet_fills_the_spool_and_stream_end_ends_it() {
        let (dir, _tempdir) = tmp_data();
        let (writer, mut spool) = Spool::pipe(dir, u64::MAX);
        let mut inlet = Inlet::new(writer, None);
        inlet.chunk(b"first ");
        inlet.chunk(b"second");
        assert!(!inlet.has_ended());
        inlet.end(b"");
        assert!(inlet.has_ended());
        let mut read = Vec::new();
        spool.read_to_end(&mut read).unwrap();
        assert_eq!(read, b"first second");
    }

    #[test]
    #[should_panic(expected = "protocol error: CHUNK after STREAM_END")]
    fn a_chunk_after_stream_end_is_a_protocol_error() {
        let (dir, _tempdir) = tmp_data();
        let (writer, _spool) = Spool::pipe(dir, u64::MAX);
        let mut inlet = Inlet::new(writer, None);
        inlet.end(b"");
        inlet.chunk(b"late");
    }

    #[test]
    #[should_panic(expected = "protocol error: a second STREAM_END")]
    fn a_second_stream_end_is_a_protocol_error() {
        let (dir, _tempdir) = tmp_data();
        let (writer, _spool) = Spool::pipe(dir, u64::MAX);
        let mut inlet = Inlet::new(writer, None);
        inlet.end(b"");
        inlet.end(b"");
    }

    /// A destination that keeps what it gets.
    #[derive(Default)]
    struct Collect(Vec<u8>);

    impl Destination for Collect {
        fn write(&mut self, bytes: &[u8]) -> Result<(), Stopped> {
            self.0.extend_from_slice(bytes);
            Ok(())
        }
    }

    /// A spool that holds `bytes`, and ends after them.
    fn ended_spool(bytes: &[u8]) -> (Spool, tempfile::TempDir) {
        let (dir, tempdir) = tmp_data();
        let (mut writer, spool) = Spool::pipe(dir, u64::MAX);
        writer.write_all(bytes).unwrap();
        drop(writer);
        (spool, tempdir)
    }

    /// The zstd frame of a stream with `frames`, at level 3, or the frames
    /// as they are at level 0.
    fn encoded(frames: &[(u8, &[u8])], level: i32) -> Vec<u8> {
        let mut writer = FrameWriter::new(Compressor::new(Vec::new(), level, false, 0));
        for &(kind, payload) in frames {
            writer.send(kind, payload).unwrap();
        }
        writer.into_inner().finish().unwrap()
    }

    #[test]
    fn the_decoder_writes_the_bytes_of_the_stream_frames() {
        // LIT and RLE frames of a plain stream, through the end contract:
        // the zstd frame ends where a stream frame ends, and the spool ends
        // right after it (DESIGN.md §6.6).
        let run = Rle {
            byte: b'z',
            length: 600 << 10,
        }
        .encode();
        let frames = [(frame::LIT, &b"literal"[..]), (frame::RLE, &run[..])];
        let expected = [&b"literal"[..], &vec![b'z'; 600 << 10]].concat();
        for level in [0, 3] {
            let encoded = encoded(&frames, level);
            let (spool, _tempdir) = ended_spool(&encoded);
            let mut collect = Collect::default();
            let counts = decode(spool, level != 0, &mut collect, None);
            assert!(collect.0 == expected, "level {level}");
            assert_eq!(
                (counts.bytes, counts.literal, counts.run),
                (expected.len() as u64, 7, 600 << 10)
            );
            // The wire bytes are those of the spool.
            assert_eq!(counts.wire, encoded.len() as u64, "level {level}");
        }
    }

    #[test]
    #[should_panic(expected = "protocol error: REF in a stream without deduplication")]
    fn a_reference_in_a_plain_stream_is_a_protocol_error() {
        let (spool, _tempdir) = ended_spool(&encoded(&[(frame::REF, b"")], 0));
        decode(spool, false, &mut Collect::default(), None);
    }

    #[test]
    #[should_panic(expected = "protocol error: empty LIT")]
    fn an_empty_literal_is_a_protocol_error() {
        let (spool, _tempdir) = ended_spool(&encoded(&[(frame::LIT, b"")], 0));
        decode(spool, false, &mut Collect::default(), None);
    }

    #[test]
    #[should_panic(expected = "protocol error: REF with a payload")]
    fn a_reference_has_no_payload() {
        let (spool, _tempdir) = ended_spool(&encoded(&[(frame::REF, b"x")], 0));
        decode(spool, false, &mut Collect::default(), None);
    }

    /// A session frame has no place inside a stream.
    #[test]
    #[should_panic(expected = "protocol error: unexpected stream frame type 8")]
    fn another_frame_in_a_stream_is_a_protocol_error() {
        let (spool, _tempdir) = ended_spool(&encoded(&[(frame::CHUNK, b"x")], 0));
        decode(spool, false, &mut Collect::default(), None);
    }

    /// A destination that keeps at most `cap` bytes. One more stops the
    /// stream, so that a damaged RLE frame that stands for terabytes costs
    /// no more than that.
    struct Capped {
        bytes: Vec<u8>,
        cap: usize,
        overflowed: bool,
    }

    impl Destination for Capped {
        fn write(&mut self, bytes: &[u8]) -> Result<(), Stopped> {
            if self.bytes.len() + bytes.len() > self.cap {
                self.overflowed = true;
                return Err(Stopped { written: 0 });
            }
            self.bytes.extend_from_slice(bytes);
            Ok(())
        }
    }

    /// What `decode` made of a stream.
    #[derive(Debug, PartialEq)]
    enum Outcome {
        /// The normal end, with these bytes written.
        Ended(Vec<u8>),
        /// More bytes than the cap: not the original stream.
        Overflowed,
        /// A panic with this message.
        Panicked(String),
    }

    /// Decode `stream`, all of a spool in the tmp_data directory `dir`
    /// that has ended, into a destination that keeps at most `cap` bytes.
    fn decoded(dir: &Arc<Directory>, stream: &[u8], compressed: bool, cap: usize) -> Outcome {
        let (mut writer, spool) = Spool::pipe(Arc::clone(dir), u64::MAX);
        writer.write_all(stream).unwrap();
        drop(writer);
        let mut capped = Capped {
            bytes: Vec::new(),
            cap,
            overflowed: false,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decode(spool, compressed, &mut capped, None)
        }));
        match result {
            Ok(_) if capped.overflowed => Outcome::Overflowed,
            Ok(_) => Outcome::Ended(capped.bytes),
            Err(payload) => Outcome::Panicked(match payload.downcast::<String>() {
                Ok(message) => *message,
                Err(payload) => payload
                    .downcast::<&str>()
                    .map_or_else(|_| String::new(), |message| message.to_string()),
            }),
        }
    }

    /// One frame of a plain stream, for the fuzzing below: a literal, or a
    /// run of up to 5000 bytes.
    fn plain_frame() -> impl Strategy<Value = (u8, Vec<u8>)> {
        prop_oneof![
            proptest::collection::vec(any::<u8>(), 1..300).prop_map(|bytes| (frame::LIT, bytes)),
            (any::<u8>(), 1u64..5000)
                .prop_map(|(byte, length)| (frame::RLE, Rle { byte, length }.encode())),
        ]
    }

    /// The bytes that the plain frames `frames` stand for.
    fn bytes_of(frames: &[(u8, Vec<u8>)]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (kind, payload) in frames {
            match *kind {
                frame::LIT => bytes.extend_from_slice(payload),
                _ => {
                    let rle = Rle::decode(payload);
                    bytes.extend(std::iter::repeat_n(rle.byte, rle.length as usize));
                }
            }
        }
        bytes
    }

    proptest! {
        /// The end contract of a stream under damage (DESIGN.md §6.6 and
        /// §11). The decoder ends normally with the stream's bytes when the
        /// stream is intact. A zstd frame cut anywhere is a protocol error,
        /// and so is a plain stream cut inside a frame; cut between frames,
        /// a plain stream gives the bytes of the frames before the cut. A
        /// changed byte of a zstd frame never ends normally with other
        /// bytes. Without compression, nothing tells a changed byte from the
        /// original, so there the decoder only must end, or report a
        /// protocol error. No damage hangs the decoder or panics otherwise.
        #[test]
        fn the_decoder_ends_normally_only_with_an_intact_stream(
            frames in proptest::collection::vec(plain_frame(), 0..12),
            compressed in any::<bool>(),
            cut in any::<prop::sample::Index>(),
            changed in any::<prop::sample::Index>(),
            flip in 1u8..=255,
        ) {
            let (dir, _tempdir) = tmp_data();
            let parts: Vec<(u8, &[u8])> =
                frames.iter().map(|(kind, payload)| (*kind, &payload[..])).collect();
            let stream = encoded(&parts, if compressed { 3 } else { 0 });
            let original = bytes_of(&frames);
            let cap = original.len() + (64 << 10);
            prop_assert_eq!(
                decoded(&dir, &stream, compressed, cap),
                Outcome::Ended(original.clone())
            );
            if stream.is_empty() {
                return Ok(());
            }

            let at = cut.index(stream.len());
            let outcome = decoded(&dir, &stream[..at], compressed, cap);
            // The ends of the plain frames, from the start of the stream.
            let mut boundary = 0;
            let before = frames.iter().take_while(|(_, payload)| {
                let next = boundary + 5 + payload.len();
                let fits = next <= at;
                if fits {
                    boundary = next;
                }
                fits
            });
            let before: Vec<(u8, Vec<u8>)> = before.cloned().collect();
            if !compressed && boundary == at {
                prop_assert_eq!(outcome, Outcome::Ended(bytes_of(&before)));
            } else {
                let cut_short = "protocol error: a stream ended inside a frame or inside its zstd frame";
                prop_assert!(
                    matches!(&outcome, Outcome::Panicked(message) if message.contains(cut_short)),
                    "cut at {}: {:?}", at, outcome
                );
            }

            let mut damaged = stream.clone();
            damaged[changed.index(stream.len())] ^= flip;
            match decoded(&dir, &damaged, compressed, cap) {
                Outcome::Ended(bytes) => {
                    prop_assert!(!compressed || bytes == original, "damage ended normally");
                }
                Outcome::Overflowed => {}
                Outcome::Panicked(message) => {
                    prop_assert!(message.contains("protocol error"), "{}", message);
                }
            }
        }
    }

    #[test]
    #[should_panic(
        expected = "protocol error: a stream ended inside a frame or inside its zstd frame"
    )]
    fn a_zstd_frame_cut_short_is_a_protocol_error() {
        let stream = encoded(&[(frame::LIT, b"literal")], 3);
        let (spool, _tempdir) = ended_spool(&stream[..stream.len() - 1]);
        decode(spool, true, &mut Collect::default(), None);
    }

    #[test]
    #[should_panic(
        expected = "protocol error: a stream ended inside a frame or inside its zstd frame"
    )]
    fn a_plain_stream_cut_inside_a_frame_is_a_protocol_error() {
        let stream = encoded(&[(frame::LIT, b"literal")], 0);
        let (spool, _tempdir) = ended_spool(&stream[..stream.len() - 1]);
        decode(spool, false, &mut Collect::default(), None);
    }

    #[test]
    #[should_panic(expected = "protocol error: bytes after the zstd frame of a stream")]
    fn bytes_after_the_zstd_frame_are_a_protocol_error() {
        let mut stream = encoded(&[(frame::LIT, b"literal")], 3);
        stream.push(0);
        let (spool, _tempdir) = ended_spool(&stream);
        decode(spool, true, &mut Collect::default(), None);
    }

    #[test]
    fn a_stop_inside_the_zstd_frame_ends_the_decoder_without_checks() {
        // The spool holds a part of the zstd frame, and the stream stops:
        // the decoder writes what it decoded and ends, without a check of
        // the end of the stream (DESIGN.md §6.6).
        let stream = encoded(&[(frame::LIT, b"literal"), (frame::LIT, b"more")], 3);
        let (dir, _tempdir) = tmp_data();
        let (mut writer, spool) = Spool::pipe(dir, u64::MAX);
        let (control, _queue) = unbounded();
        let stop = Stop::receiving(spool.stopper(), None, control);
        writer.write_all(&stream[..stream.len() - 3]).unwrap();
        let decoder = std::thread::spawn(move || {
            let mut collect = Collect::default();
            decode(spool, true, &mut collect, None);
            collect.0
        });
        stop.raise();
        let written = decoder.join().unwrap();
        assert!(b"literalmore".starts_with(&written), "{written:?}");
        drop(writer);
    }

    /// A destination that raises the stop during its first write, as the
    /// exit of the command does while the decoder writes, and counts the
    /// bytes of its writes.
    struct StopsTheStream {
        stop: Stop,
        written: usize,
    }

    impl Destination for StopsTheStream {
        fn write(&mut self, bytes: &[u8]) -> Result<(), Stopped> {
            if self.stop.is_raised() {
                return Err(Stopped { written: 0 });
            }
            self.stop.raise();
            self.written += bytes.len();
            Ok(())
        }
    }

    /// A destination that takes at most `cap` bytes, and stops the stream
    /// in the write that reaches the cap, after the part of it that fits.
    struct TakesAtMost {
        taken: Vec<u8>,
        cap: usize,
    }

    impl Destination for TakesAtMost {
        fn write(&mut self, bytes: &[u8]) -> Result<(), Stopped> {
            let n = bytes.len().min(self.cap - self.taken.len());
            self.taken.extend_from_slice(&bytes[..n]);
            match n < bytes.len() {
                true => Err(Stopped { written: n }),
                false => Ok(()),
            }
        }
    }

    #[test]
    fn a_stop_inside_a_write_counts_only_the_bytes_that_went_out() {
        // The destination takes 600 bytes of a LIT of 1000, or 300 KiB of
        // an RLE frame of 1 MiB, which goes out in pieces of 256 KiB, and the
        // stream stops there. The counts of the decoder are the bytes that
        // the destination took, also by kind; the wire bytes are those of the
        // frames that it read, not the frame after them that waits in the
        // spool; and the decoder checks no end of the stream, which that
        // frame would fail (DESIGN.md §8).
        let literal = noise(1000, 7);
        let run = Rle {
            byte: 9,
            length: 1 << 20,
        }
        .encode();
        let cases = [
            ((frame::LIT, &literal[..]), 5 + 600, (5 + 600, 0)),
            ((frame::RLE, &run[..]), 5 + (300 << 10), (5, 300 << 10)),
        ];
        for (cut, cap, (literal, run)) in cases {
            let frames = [(frame::LIT, &b"first"[..]), cut, (frame::LIT, b"after")];
            let (spool, _tempdir) = ended_spool(&encoded(&frames, 0));
            let mut destination = TakesAtMost {
                taken: Vec::new(),
                cap,
            };
            let counts = decode(spool, false, &mut destination, None);
            assert_eq!(destination.taken.len(), cap);
            assert_eq!(counts.bytes, cap as u64);
            assert_eq!((counts.literal, counts.run), (literal, run));
            assert_eq!(counts.wire, (5 + 5 + 5 + cut.1.len()) as u64);
        }
    }

    #[test]
    fn a_stop_ends_the_writes_inside_one_run() {
        // One RLE frame of 14 bytes stands for a terabyte. The stream stops
        // during the first write of its bytes, so the decoder writes no
        // more of them (DESIGN.md §6.6).
        let run = Rle {
            byte: 0,
            length: 1 << 40,
        }
        .encode();
        let (dir, _tempdir) = tmp_data();
        let (mut writer, spool) = Spool::pipe(dir, u64::MAX);
        writer
            .write_all(&encoded(&[(frame::RLE, &run)], 0))
            .unwrap();
        drop(writer);
        let (control, _queue) = unbounded();
        let stop = Stop::receiving(spool.stopper(), None, control);
        let mut destination = StopsTheStream {
            stop: stop.clone(),
            written: 0,
        };
        decode(spool, false, &mut destination, None);
        assert_eq!(destination.written, RUN_PIECE as usize);
    }

    /// The system call in which the thread `tid` of this process sleeps,
    /// with its six arguments, once it sleeps in one; no deadline. While
    /// the thread runs, its `syscall` file says `running`, and while it
    /// sleeps outside a system call, the number is -1. A process can always
    /// read the `syscall` files of its own threads.
    fn sleeping_syscall(tid: rustix::thread::Pid) -> (u64, Vec<u64>) {
        let path = format!("/proc/self/task/{}/syscall", tid.as_raw_nonzero());
        loop {
            let syscall = std::fs::read_to_string(&path).unwrap();
            let mut fields = syscall.split_whitespace();
            if let Some(Ok(nr)) = fields.next().map(str::parse::<u64>) {
                let args = fields
                    .take(6)
                    .map(|arg| u64::from_str_radix(arg.trim_start_matches("0x"), 16).unwrap())
                    .collect();
                return (nr, args);
            }
            std::thread::yield_now();
        }
    }

    /// The system call in which `poll_fds` sleeps, `ppoll` or `poll`, by
    /// its number on this architecture. A new thread waits for a pipe that
    /// nothing writes. It tells its ID before the wait, so the wait is the
    /// only call that it can then sleep in. The drop of the write end ends
    /// the wait.
    fn poll_syscall() -> u64 {
        let (reader, writer) = std::io::pipe().unwrap();
        let (tid_tx, tid_rx) = std::sync::mpsc::channel();
        let waits = std::thread::spawn(move || {
            tid_tx.send(rustix::thread::gettid()).unwrap();
            poll_fds(&mut [PollFd::new(&reader, PollFlags::IN)]);
        });
        let (nr, _) = sleeping_syscall(tid_rx.recv().unwrap());
        drop(writer);
        waits.join().unwrap();
        nr
    }

    /// Wait until the thread `tid` of this process sleeps in a poll of
    /// `count` descriptors; no deadline.
    fn wait_for_poll(tid: rustix::thread::Pid, count: u64) {
        let poll = poll_syscall();
        loop {
            match sleeping_syscall(tid) {
                (nr, args) if nr == poll && args[1] == count => return,
                _ => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn a_full_command_stdin_waits_for_room_or_for_the_stop() {
        // The command reads nothing, so a write fills the pipe, and the
        // next one waits for room in a poll of the pipe and the stop pipe.
        // The stop ends that wait, and the write ends without the rest of
        // its bytes, reporting the bytes that the pipe took, which the
        // summary counts (DESIGN.md §6.6 and §8). A writer that waits for
        // room alone hangs the test.
        let mut command = KillOnDrop(
            std::process::Command::new("sleep")
                .arg("infinity")
                .stdin(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let pipe = command.0.stdin.take().unwrap();
        let (dir, _tempdir) = tmp_data();
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        let (control, _queue) = unbounded();
        let stop = Stop::receiving(spool.stopper(), None, control);
        let mut stdin = CommandStdin::new(pipe, stop.clone());
        let (tid_tx, tid_rx) = std::sync::mpsc::channel();
        let writes = std::thread::spawn(move || {
            tid_tx.send(rustix::thread::gettid()).unwrap();
            stdin.write(&vec![0; 16 << 20])
        });
        wait_for_poll(tid_rx.recv().unwrap(), 2);
        stop.raise();
        let Err(Stopped { written }) = writes.join().unwrap() else {
            panic!("the write did not stop");
        };
        assert_eq!(written, pipe_capacity(), "the bytes that the pipe took");
    }

    /// The bytes that a new pipe holds before a write to it waits: a
    /// non-blocking write end takes them until it fails with `EAGAIN`.
    fn pipe_capacity() -> usize {
        let (_reader, writer) = std::io::pipe().unwrap();
        let flags = rustix::fs::fcntl_getfl(&writer).unwrap();
        rustix::fs::fcntl_setfl(&writer, flags | OFlags::NONBLOCK).unwrap();
        let bytes = [0u8; 4096];
        let mut capacity = 0;
        loop {
            match rustix::io::write(&writer, &bytes) {
                Ok(n) => capacity += n,
                Err(Errno::AGAIN) => return capacity,
                Err(e) => panic!("write to a new pipe: {e}"),
            }
        }
    }

    /// A child process that the test kills and reaps when it ends, also
    /// when an assertion fails first: a child left behind would hold the
    /// test binary's stdout and stderr open.
    struct KillOnDrop(std::process::Child);

    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn epipe_on_the_command_stdin_raises_the_stop() {
        // No process reads the command's stdin any more: the write fails
        // with EPIPE, which stops the stream, and the stop queues STOP
        // (DESIGN.md §6.6).
        let mut command = std::process::Command::new("true")
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let pipe = command.stdin.take().unwrap();
        command.wait().unwrap();
        let (dir, _tempdir) = tmp_data();
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        let (control, queue) = unbounded();
        let stop = Stop::receiving(spool.stopper(), None, control);
        let mut stdin = CommandStdin::new(pipe, stop.clone());
        assert!(stdin.write(b"no process reads this").is_err());
        assert!(stop.is_raised());
        assert!(matches!(queue.try_recv(), Ok(ControlFrame::Stop)));
    }

    /// A stdout that takes `took` bytes of the first write, then fails as a
    /// pipe without a reader does.
    struct BreaksAfter {
        took: usize,
        writes: usize,
    }

    impl Write for BreaksAfter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            match self.writes {
                1 => Ok(self.took.min(bytes.len())),
                _ => Err(ErrorKind::BrokenPipe.into()),
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_broken_stdout_raises_the_stop_and_counts_what_went_out() {
        // The consumer of the client's stdout took a part of a write, then
        // exited: the rest fails with EPIPE, which stops the backward stream
        // on the client, and the stop queues STOP for the server. The write
        // reports the bytes that stdout took, which the summary counts
        // (DESIGN.md §6.6 and §8).
        let (dir, _tempdir) = tmp_data();
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        let (control, queue) = unbounded();
        let stop = Stop::receiving(spool.stopper(), None, control);
        let mut stdout = Stdout {
            out: BreaksAfter { took: 3, writes: 0 },
            stop: stop.clone(),
        };
        assert!(matches!(
            stdout.write(b"abcdef"),
            Err(Stopped { written: 3 })
        ));
        assert!(stop.is_raised());
        assert!(matches!(queue.try_recv(), Ok(ControlFrame::Stop)));
    }

    #[test]
    fn a_raise_after_the_control_writer_ended_still_stops_the_stream() {
        // After END, a failed write ends the client's control writer while
        // the stop of the backward stream still has its sender, until step
        // N1. A broken stdout then still stops the stream on this host, and
        // STOP, which the server no longer needs, goes nowhere (DESIGN.md
        // §6.9).
        let (dir, _tempdir) = tmp_data();
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        let (control, queue) = unbounded();
        drop(queue);
        let stop = Stop::receiving(spool.stopper(), None, control);
        stop.raise();
        assert!(stop.is_raised());
        assert!(spool.is_stopped());
    }

    /// Whether this process still has a descriptor of the pipe of `writer`
    /// other than `writer` itself: the read end, which only the code under
    /// test can hold. Other processes do not count: those that other tests
    /// start can hold a copy until they start their programs.
    fn reads_the_pipe_of(writer: &std::io::PipeWriter) -> bool {
        use std::os::fd::AsRawFd;
        let target = std::fs::read_link(format!("/proc/self/fd/{}", writer.as_raw_fd())).unwrap();
        std::fs::read_dir("/proc/self/fd").unwrap().any(|fd| {
            let fd = fd.unwrap();
            fd.file_name().to_string_lossy() != writer.as_raw_fd().to_string()
                && std::fs::read_link(fd.path()).is_ok_and(|link| link == target)
        })
    }

    #[test]
    fn the_stdout_reader_copies_the_command_stdout_with_its_idle_points() {
        // The stdout reader appends what the command writes, and in eager
        // delivery, an idle point each time the command pauses; in batch
        // delivery, none. At the EOF of the command's stdout, it ends the
        // spool, and reports what it read (DESIGN.md §6.6, steps O1 to O3).
        for delivery in [Delivery::Eager, Delivery::Batch] {
            let (dir, _tempdir) = tmp_data();
            let (writer, mut spool) = Spool::pipe(dir, u64::MAX);
            let stop = Stop::sending(spool.stopper());
            let (stdout, mut command) = std::io::pipe().unwrap();
            command.write_all(b"abc").unwrap();
            let reader = {
                let stop = stop.clone();
                std::thread::spawn(move || read_command_stdout(stdout, writer, &stop, delivery))
            };
            let mut buf = [0u8; 64];
            assert_eq!(spool.take_next(&mut buf), Taken::Bytes(3), "{delivery:?}");
            if delivery == Delivery::Eager {
                assert_eq!(spool.take_next(&mut buf), Taken::Idle);
            }
            command.write_all(b"de").unwrap();
            assert_eq!(spool.take_next(&mut buf), Taken::Bytes(2), "{delivery:?}");
            if delivery == Delivery::Eager {
                assert_eq!(spool.take_next(&mut buf), Taken::Idle);
            }
            drop(command);
            assert_eq!(spool.take_next(&mut buf), Taken::End, "{delivery:?}");
            let read = reader.join().unwrap();
            assert_eq!((read.bytes, read.eof), (5, true), "{delivery:?}");
        }
    }

    #[test]
    fn a_stop_ends_the_stdout_reader_and_closes_the_command_stdout() {
        // The stdout reader waits for the command's next bytes, or with a
        // spool limit of 0, for room in the spool after the bytes that it
        // appended. The stop ends either wait, and the reader closes its end
        // of the command's stdout, so the command's next write fails with
        // EPIPE, or kills it with SIGPIPE, as in a local pipeline (DESIGN.md
        // §6.6). A reader that waited for bytes or room alone would hang the
        // test.
        for (before, limit, waits) in [(&b""[..], u64::MAX, 2), (b"full", 0, 0)] {
            let (dir, _tempdir) = tmp_data();
            let (writer, spool) = Spool::pipe(dir, limit);
            let stop = Stop::sending(spool.stopper());
            let (stdout, mut command) = std::io::pipe().unwrap();
            command.write_all(before).unwrap();
            let (tid_tx, tid_rx) = std::sync::mpsc::channel();
            let reader = {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    tid_tx.send(rustix::thread::gettid()).unwrap();
                    read_command_stdout(stdout, writer, &stop, Delivery::Batch)
                })
            };
            let tid = tid_rx.recv().unwrap();
            match waits {
                // A wait in a poll of the pipe and the stop pipe.
                2 => wait_for_poll(tid, 2),
                // A wait for room in the spool, on its condition variable.
                _ => wait_for_syscall(tid, libc::SYS_futex as u64),
            }
            assert!(reads_the_pipe_of(&command), "the reader closed the pipe");
            stop.raise();
            let read = reader.join().unwrap();
            assert_eq!(
                (read.bytes, read.eof),
                (before.len() as u64, false),
                "limit {limit}"
            );
            assert!(!reads_the_pipe_of(&command), "the pipe is still open");
            drop(spool);
        }
    }

    /// The source reader over a pipe that holds `before`, in `delivery`,
    /// into a new spool: the spool, the stop, the producer's end of the
    /// pipe, and the reader's thread.
    fn source_reader(
        before: &[u8],
        delivery: Delivery,
    ) -> (
        Spool,
        Stop,
        std::io::PipeWriter,
        std::thread::JoinHandle<SourceRead>,
        tempfile::TempDir,
    ) {
        let (dir, tempdir) = tmp_data();
        let (writer, spool) = Spool::pipe(dir, u64::MAX);
        let stop = Stop::sending(spool.stopper());
        let (stdin, mut producer) = std::io::pipe().unwrap();
        producer.write_all(before).unwrap();
        let reader = {
            let stop = stop.clone();
            std::thread::spawn(move || read_stdin(stdin, writer, &stop, delivery))
        };
        (spool, stop, producer, reader, tempdir)
    }

    #[test]
    fn eager_delivery_marks_an_idle_point_when_stdin_waits() {
        // stdin holds a few bytes, then waits without EOF: the source
        // reader appends them, then marks an idle point, which the spool
        // reports after them. EOF then ends the spool (DESIGN.md §6.6).
        let (mut spool, _stop, producer, reader, _tempdir) =
            source_reader(b"partial", Delivery::Eager);
        let mut buf = [0u8; 64];
        assert_eq!(spool.take_next(&mut buf), Taken::Bytes(7));
        assert_eq!(spool.take_next(&mut buf), Taken::Idle);
        drop(producer);
        assert_eq!(spool.take_next(&mut buf), Taken::End);
        let read = reader.join().unwrap();
        assert_eq!((read.bytes, read.eof), (7, true));
    }

    #[test]
    fn batch_delivery_marks_no_idle_point() {
        let (mut spool, _stop, producer, reader, _tempdir) =
            source_reader(b"partial", Delivery::Batch);
        let mut buf = [0u8; 64];
        assert_eq!(spool.take_next(&mut buf), Taken::Bytes(7));
        drop(producer);
        assert_eq!(spool.take_next(&mut buf), Taken::End);
        reader.join().unwrap();
    }

    #[test]
    fn a_stop_ends_the_source_reader_without_a_read() {
        // A stop can come before the source reader starts, as STOP before
        // the client starts its forward stream (DESIGN.md §6.6). Then the
        // reader reads nothing: the bytes of stdin stay for its next reader.
        // It still finds an empty stdin at its EOF, without a read, so that
        // the stream counts as whole. Then a stop ends a reader that waits
        // for stdin, which is not at its EOF.
        for delivery in [Delivery::Eager, Delivery::Batch] {
            for before in [&b"unread"[..], b""] {
                let (dir, _tempdir) = tmp_data();
                let (writer, spool) = Spool::pipe(dir, u64::MAX);
                let stop = Stop::sending(spool.stopper());
                let (stdin, mut producer) = std::io::pipe().unwrap();
                let mut unread = stdin.try_clone().unwrap();
                producer.write_all(before).unwrap();
                drop(producer);
                stop.raise();
                let read = read_stdin(stdin, writer, &stop, delivery);
                let eof = before.is_empty();
                assert_eq!((read.bytes, read.eof), (0, eof), "{delivery:?}");
                let mut left = Vec::new();
                unread.read_to_end(&mut left).unwrap();
                assert_eq!(left, before, "{delivery:?}");
            }
        }
        let (_spool, stop, _producer, reader, _tempdir) = source_reader(b"", Delivery::Eager);
        stop.raise();
        let read = reader.join().unwrap();
        assert_eq!((read.bytes, read.eof), (0, false));
    }
}

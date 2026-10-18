//! The control session (DESIGN.md §6.2), on either host: one writer and one
//! reader per host.
//!
//! Each direction of the control session carries two kinds of frames. The
//! control frames, QUERY, ANSWER and STOP, are small and must go at once:
//! the scanner waits for answers, and the peer must learn of a stop at
//! once. They form one zstd stream at the run's level, unless compression
//! is disabled, the control stream, whose bytes go in CHUNK frames. The
//! frames of the list exchanges are session frames of their own, outside
//! the control stream: the hash lists are large, and zstd cannot make them
//! smaller (DESIGN.md §10). The lists travel here, and not on the data
//! session, because nothing on this session waits for a stream: a stream
//! whose spool on the peer is full stops the peer's data reader, and a
//! list behind it would never arrive.
//!
//! The writer sends this host's protocol version and features at once,
//! without waiting for the peer's. Then it sends the control frames of one
//! queue in order, and flushes the control stream whenever that queue runs
//! empty. Between them, when the queue is empty, it sends the frames of the
//! list exchanges of this host, one at a time, from one queue for each side
//! of an exchange that this host plays: a control frame that comes while a
//! list goes out waits for one HASHDATA frame at most. The control queue
//! has up to three producers on each host, each with its own sender: the
//! pins of the stream that the host receives deduplicated, with the answers
//! until Completing; the scanner of the stream that it sends deduplicated,
//! with the queries until its scan ends; and the stop of the stream that it
//! receives, with STOP until its raise, or on the client, until END (step
//! N1). Each side of an exchange is the only producer of its own queue,
//! until its end. The writer ends once every producer has dropped its
//! sender.
//!
//! The reader checks the peer's version and features, then reads the
//! session frames. It decodes the control stream from the CHUNK frames and
//! dispatches each control frame: QUERY to the lookup workers of the stream
//! that this host receives, ANSWER to the scanner of the stream that this
//! host sends, and STOP to the stop of the stream that this host sends. It
//! passes each frame of a list exchange to the side that this host plays in
//! it: the frames of a receiver to the sender of the stream that this host
//! sends, and those of a sender to the receiver of the stream that it
//! receives. Each direction carries the queries of one stream, the answers
//! and the STOP of the other, so no frame needs a stream ID. The reader
//! reads until the closure of the session, so the peer's relay never blocks
//! on a full pipe.
//!
//! Before END, the closure of the session, EOF or a failed write, is the
//! loss of the peer, which ends the run with the message of the data
//! session's loss; after END, it is the normal end (DESIGN.md §6.9).
//! `EndFlag` tells which.

use std::io::{BufRead, BufReader, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

use crossbeam_channel::{Receiver, Select, Sender, TryRecvError};

use crate::protocol::{
    Answer, CHUNK_BYTES, Compressor, ControlFrame, Decompressor, ExchangeFrame, FrameReader,
    FrameWriter, Query, die_session, frame, lost,
};
use crate::stream::Stop;

/// Whether END has passed: the server sets it before it sends END, the
/// client when its data reader reads END. This type exists so that the
/// threads that read or write the sessions can tell the normal end, which
/// closes the sessions after END, from the loss of the peer before it.
pub struct EndFlag(AtomicBool);

impl EndFlag {
    pub fn new() -> Self {
        EndFlag(AtomicBool::new(false))
    }

    pub fn set(&self) {
        self.0.store(true, SeqCst);
    }

    pub fn is_set(&self) -> bool {
        self.0.load(SeqCst)
    }
}

/// The control writer: this host's protocol version and features, then the
/// control frames of `queue` in order, in one control stream at `level`, 0
/// for none, which it flushes whenever `queue` runs empty, and between them
/// the frames of `exchanges`, the queues of the sides of the list exchanges
/// of this host, until every producer has dropped its sender and the
/// queues are empty. Once it has written the last frame of every side, it
/// drops `exchanges_written`, whose receiver gets no message: the server
/// waits for that before END, because after END it shuts the session down,
/// which would cut off the last frames of its sides, such as CACHE_RESULT,
/// for which the client's sides wait (DESIGN.md §6.9). A failed write ends
/// the run before END, a write to a session that `peer` closed as the loss
/// of `peer`, and ends this thread after it. Returns `transport` without
/// closing it: the client closes the session's stdin only once this thread
/// has ended. The bytes of a failed write are lost.
pub fn write<W: Write>(
    transport: W,
    level: i32,
    queue: &Receiver<ControlFrame>,
    exchanges: &[Receiver<ExchangeFrame>],
    exchanges_written: Option<Sender<()>>,
    end: &EndFlag,
    peer: &str,
) -> W {
    let mut writer = ControlWriter {
        session: FrameWriter::new(transport),
        stream: FrameWriter::new(Compressor::control(Vec::new(), level)),
        written: false,
        exchanges_written,
    };
    let mut result = writer.session.send_version_and_features();
    writer.stream.get_mut().prepare();
    let mut queue = Some(queue);
    let mut exchanges: Vec<Option<&Receiver<ExchangeFrame>>> = exchanges.iter().map(Some).collect();
    while result.is_ok() {
        result = writer.next(&mut queue, &mut exchanges);
        if queue.is_none() && exchanges.iter().all(Option::is_none) {
            break;
        }
    }
    if let Err(e) = result
        && !end.is_set()
    {
        die_session("control session", peer, &e);
    }
    writer.session.into_inner()
}

/// The two levels of the outgoing direction of the control session.
struct ControlWriter<W: Write> {
    session: FrameWriter<W>,
    /// The control stream, whose compressed bytes wait in a buffer until
    /// they go in a CHUNK frame.
    stream: FrameWriter<Compressor<Vec<u8>>>,
    /// Control frames have gone into the stream since its last flush.
    written: bool,
    /// Dropped once the last frame of every side of the exchanges is out.
    exchanges_written: Option<Sender<()>>,
}

impl<W: Write> ControlWriter<W> {
    /// Send the next frame: a control frame while one waits, else, after a
    /// flush of the control stream, one frame of a list exchange. When
    /// nothing waits, wait for the next frame. A queue that the last
    /// producer dropped, and that is empty, becomes `None`.
    fn next(
        &mut self,
        queue: &mut Option<&Receiver<ControlFrame>>,
        exchanges: &mut [Option<&Receiver<ExchangeFrame>>],
    ) -> std::io::Result<()> {
        if let Some(control) = queue {
            match control.try_recv() {
                Ok(frame) => return self.control(&frame),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => *queue = None,
            }
        }
        // The control queue is empty: the scanner waits for answers, and
        // the sending host of a stream must stop it as soon as the
        // receiving host has stopped it, so the control frames so far must
        // not stay in the stream or in the buffer.
        self.flush()?;
        for side in exchanges.iter_mut() {
            if let Some(frames) = side {
                match frames.try_recv() {
                    Ok(exchange) => return self.session.send(exchange.kind, &exchange.payload),
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => *side = None,
                }
            }
        }
        if exchanges.iter().all(Option::is_none) {
            drop(self.exchanges_written.take());
        }
        // Nothing waits. Wait for any queue that a producer still holds;
        // then the next call takes what came.
        let mut select = Select::new();
        if let Some(control) = queue {
            select.recv(control);
        }
        for frames in exchanges.iter().flatten() {
            select.recv(frames);
        }
        if queue.is_some() || exchanges.iter().any(Option::is_some) {
            select.ready();
        }
        Ok(())
    }

    /// Put `frame` into the control stream. A buffer that holds a whole
    /// CHUNK goes out at once.
    fn control(&mut self, frame: &ControlFrame) -> std::io::Result<()> {
        let (kind, payload) = frame.encode();
        self.stream.send(kind, &payload)?;
        self.written = true;
        if self.buffer().len() >= CHUNK_BYTES {
            self.send_chunk()?;
        }
        Ok(())
    }

    /// Make every control frame so far decodable, and send the bytes of the
    /// stream that wait in the buffer.
    fn flush(&mut self) -> std::io::Result<()> {
        if !self.written {
            return Ok(());
        }
        self.stream.flush()?;
        self.written = false;
        self.send_chunk()
    }

    fn buffer(&mut self) -> &mut Vec<u8> {
        self.stream.get_mut().transport_mut()
    }

    /// Send the bytes that wait in the buffer, if any, in one CHUNK.
    fn send_chunk(&mut self) -> std::io::Result<()> {
        let bytes = std::mem::take(self.buffer());
        if !bytes.is_empty() {
            self.session.send(frame::CHUNK, &bytes)?;
        }
        *self.buffer() = bytes;
        self.buffer().clear();
        Ok(())
    }
}

/// Where the control reader takes the frames of the peer.
pub struct Dispatch {
    /// The lookup workers of the stream that this host receives, when it is
    /// deduplicated: QUERY.
    pub queries: Option<Sender<Query>>,
    /// The scanner of the stream that this host sends, when it is
    /// deduplicated: ANSWER.
    pub answers: Option<Sender<Answer>>,
    /// The stop of the stream that this host sends, which the peer
    /// receives and stops: STOP.
    pub stop: Stop,
    /// The receiver's side of the list exchange of the stream that this host
    /// receives, when it is deduplicated: the frames of the peer's sender,
    /// ADVERTISE, HASHFULL and CACHE_RESULT, and the peer's UPDATED.
    pub receiver: Option<Sender<ExchangeFrame>>,
    /// The sender's side of the list exchange of the stream that this host
    /// sends, when it is deduplicated: the frames of the peer's receiver,
    /// INDEX, HASHLIST, HASHDELTA, HASHKEEP, HASHNONE and HASHDATA.
    pub sender: Option<Sender<ExchangeFrame>>,
}

impl Dispatch {
    /// Pass the frame `kind` with `payload`, of a list exchange, to the side
    /// of the exchange that this host plays. A frame for a stream that is
    /// not deduplicated, and a frame after the end of its side, which reads
    /// every frame of the exchange, are protocol errors.
    fn exchange(&self, kind: u8, payload: &[u8]) {
        let side = match kind {
            frame::INDEX
            | frame::HASHLIST
            | frame::HASHDELTA
            | frame::HASHKEEP
            | frame::HASHNONE
            | frame::HASHDATA => &self.sender,
            frame::ADVERTISE | frame::HASHFULL | frame::CACHE_RESULT | frame::UPDATED => {
                &self.receiver
            }
            kind => panic!("protocol error: unexpected control session frame type {kind}"),
        };
        let side = side.as_ref().unwrap_or_else(|| {
            panic!("protocol error: frame type {kind} for a stream without deduplication")
        });
        let frame = ExchangeFrame {
            kind,
            payload: payload.to_vec(),
        };
        if side.send(frame).is_err() {
            panic!("protocol error: frame type {kind} after the end of its list exchange");
        }
    }
}

/// The control stream of the incoming direction of the control session:
/// the payloads of its CHUNK frames, one after the other. This type exists
/// so that the frames of the list exchanges, which come between the CHUNK
/// frames, go to their sides as soon as they arrive, while the control
/// stream is decoded as one stream (`Decompressor`). The reader of the
/// stream asks for more bytes whenever it has decoded every whole frame,
/// so no frame of an exchange waits behind a control frame.
struct ControlStream<'a, R: Read> {
    session: FrameReader<R>,
    dispatch: &'a Dispatch,
    /// The payload of the last CHUNK, of which `consumed` bytes were read.
    chunk: Vec<u8>,
    consumed: usize,
}

impl<R: Read> BufRead for ControlStream<'_, R> {
    /// The bytes of the current CHUNK that were not read yet, after the
    /// next CHUNK when none are left; none at the end of the session.
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        while self.consumed == self.chunk.len() {
            let Some(received) = self.session.next()? else {
                return Ok(&[]);
            };
            if received.kind != frame::CHUNK {
                self.dispatch.exchange(received.kind, received.payload);
                continue;
            }
            assert!(!received.payload.is_empty(), "protocol error: empty CHUNK");
            self.chunk.clear();
            self.chunk.extend_from_slice(received.payload);
            self.consumed = 0;
        }
        Ok(&self.chunk[self.consumed..])
    }

    fn consume(&mut self, amount: usize) {
        self.consumed += amount;
    }
}

impl<R: Read> Read for ControlStream<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let available = self.fill_buf()?;
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.consume(n);
        Ok(n)
    }
}

/// The control reader: the protocol version and the features of `peer`,
/// the other host, then the session frames until the closure of the
/// session. It decodes the control stream from the CHUNK frames, one zstd
/// stream at `level`, 0 for none, and gives each control frame and each
/// frame of a list exchange to `dispatch`. Before END, the closure of the session
/// is the loss of `peer`. A frame that `dispatch` has no place for, and a
/// second STOP, are protocol errors. A QUERY or ANSWER whose receiver has
/// ended, at Completing or at the end of the scan, has no use, and is
/// dropped. So has a STOP after the stream that it stops has ended.
pub fn read<R: Read>(transport: R, level: i32, dispatch: &Dispatch, end: &EndFlag, peer: &str) {
    let compressed = level != 0;
    let closed = |error: Option<std::io::Error>| {
        if end.is_set() {
            return;
        }
        match error {
            None => lost(peer),
            Some(e) => die_session("control session", peer, &e),
        }
    };
    let mut session = FrameReader::new(BufReader::new(transport));
    match session.read_version_and_features(&format!("{peer}'s control session")) {
        Ok(true) => {}
        Ok(false) => return closed(None),
        Err(e) => return closed(Some(e)),
    }
    let stream = ControlStream {
        session,
        dispatch,
        chunk: Vec::new(),
        consumed: 0,
    };
    let mut decompressor = Decompressor::new(stream, compressed);
    decompressor.prepare(level);
    let mut control_in = FrameReader::new(decompressor);
    let mut stopped = false;
    loop {
        let received = match control_in.next() {
            Ok(Some(received)) => received,
            Ok(None) => return closed(None),
            Err(e) => return closed(Some(e)),
        };
        match received.kind {
            frame::QUERY => {
                let queries = dispatch
                    .queries
                    .as_ref()
                    .expect("protocol error: QUERY for a stream without deduplication");
                let _ = queries.send(Query::decode(received.payload));
            }
            frame::ANSWER => {
                let answers = dispatch
                    .answers
                    .as_ref()
                    .expect("protocol error: ANSWER for a stream without deduplication");
                let _ = answers.send(Answer::decode(received.payload));
            }
            frame::STOP => {
                assert!(
                    received.payload.is_empty(),
                    "protocol error: STOP with a payload"
                );
                assert!(!stopped, "protocol error: a second STOP");
                stopped = true;
                // After END, or after the end of the stream, the stop
                // changes nothing.
                dispatch.stop.raise();
            }
            kind => panic!("protocol error: unexpected control frame type {kind}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt;
    use std::sync::{Arc, Mutex};

    use crossbeam_channel::{bounded, unbounded};

    use super::*;
    use crate::segments::Directory;
    use crate::spool::Spool;

    /// The peer's side of a control session: its version and features,
    /// then `frames`, the control frames, without compression, each in a
    /// CHUNK of its own, and the frames of the list exchanges as they are,
    /// then EOF.
    fn session(frames: &[(u8, &[u8])]) -> Vec<u8> {
        let mut session = FrameWriter::new(Vec::new());
        session.send_version_and_features().unwrap();
        for &(kind, payload) in frames {
            if [frame::QUERY, frame::ANSWER, frame::STOP].contains(&kind) {
                let mut chunk = FrameWriter::new(Vec::new());
                chunk.send(kind, payload).unwrap();
                session.send(frame::CHUNK, &chunk.into_inner()).unwrap();
            } else {
                session.send(kind, payload).unwrap();
            }
        }
        session.into_inner()
    }

    /// Read `frames` with `dispatch` after END, so that the EOF after them
    /// is the normal end.
    fn read_frames(frames: &[(u8, &[u8])], dispatch: &Dispatch) {
        let end = EndFlag::new();
        end.set();
        read(&session(frames)[..], 0, dispatch, &end, "peer");
    }

    /// A stop of the stream that a host sends, with the spool that it stops
    /// and the tmp_data directory of that spool.
    fn stop() -> (Stop, Spool, tempfile::TempDir) {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(Directory::open(path.as_os_str().as_bytes()));
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        (Stop::sending(spool.stopper()), spool, tempdir)
    }

    /// The dispatch of a host that neither sends nor receives a
    /// deduplicated stream, with the stop of the stream that it sends.
    fn nowhere(stop: Stop) -> Dispatch {
        Dispatch {
            queries: None,
            answers: None,
            stop,
            receiver: None,
            sender: None,
        }
    }

    fn query(offset: u64) -> Vec<u8> {
        Query {
            offset,
            strong: [3; 32],
        }
        .encode()
    }

    fn answer(offset: u64) -> Vec<u8> {
        Answer {
            offset,
            found: true,
        }
        .encode()
    }

    /// QUERY goes to the lookups of the stream that this host receives,
    /// ANSWER to the scanner of the stream that it sends, and STOP to the
    /// stop of the stream that it sends, on either host. The frames of a
    /// sender go to the receiver's side of this host, and those of a
    /// receiver to its sender's side, in order also across CHUNK frames
    /// (DESIGN.md §6.2).
    #[test]
    fn each_frame_goes_to_its_place() {
        let (queries_tx, queries) = unbounded();
        let (answers_tx, answers) = unbounded();
        let (receiver_tx, receiver) = unbounded();
        let (sender_tx, sender) = unbounded();
        let (stop, _spool, _tempdir) = stop();
        let dispatch = Dispatch {
            queries: Some(queries_tx),
            answers: Some(answers_tx),
            stop: stop.clone(),
            receiver: Some(receiver_tx),
            sender: Some(sender_tx),
        };
        let (q, a) = (query(7), answer(9));
        read_frames(
            &[
                (frame::INDEX, b"index"),
                (frame::QUERY, &q),
                (frame::ADVERTISE, b"lists"),
                (frame::HASHDATA, b"data"),
                (frame::ANSWER, &a),
                (frame::UPDATED, b""),
                (frame::STOP, b""),
            ],
            &dispatch,
        );
        assert_eq!(queries.try_recv().unwrap().offset, 7);
        assert_eq!(answers.try_recv().unwrap().offset, 9);
        assert!(stop.is_raised());
        let kinds = |side: &Receiver<ExchangeFrame>| {
            side.try_iter()
                .map(|frame| (frame.kind, frame.payload))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            kinds(&sender),
            [
                (frame::INDEX, b"index".to_vec()),
                (frame::HASHDATA, b"data".to_vec())
            ]
        );
        assert_eq!(
            kinds(&receiver),
            [
                (frame::ADVERTISE, b"lists".to_vec()),
                (frame::UPDATED, Vec::new())
            ]
        );
    }

    #[test]
    #[should_panic(expected = "protocol error: a second STOP")]
    fn a_second_stop_is_a_protocol_error() {
        let (stop, _spool, _tempdir) = stop();
        read_frames(&[(frame::STOP, b""), (frame::STOP, b"")], &nowhere(stop));
    }

    #[test]
    #[should_panic(expected = "protocol error: STOP with a payload")]
    fn stop_has_no_payload() {
        let (stop, _spool, _tempdir) = stop();
        read_frames(&[(frame::STOP, b"x")], &nowhere(stop));
    }

    #[test]
    #[should_panic(expected = "protocol error: QUERY for a stream without deduplication")]
    fn a_query_for_a_plain_stream_is_a_protocol_error() {
        let (stop, _spool, _tempdir) = stop();
        read_frames(&[(frame::QUERY, &query(0))], &nowhere(stop));
    }

    #[test]
    #[should_panic(expected = "protocol error: ANSWER for a stream without deduplication")]
    fn an_answer_for_a_plain_stream_is_a_protocol_error() {
        let (stop, _spool, _tempdir) = stop();
        read_frames(&[(frame::ANSWER, &answer(0))], &nowhere(stop));
    }

    #[test]
    #[should_panic(expected = "protocol error: frame type 8 for a stream without deduplication")]
    fn a_list_for_a_plain_stream_is_a_protocol_error() {
        let (stop, _spool, _tempdir) = stop();
        read_frames(&[(frame::HASHLIST, b"")], &nowhere(stop));
    }

    /// A side of an exchange reads every frame of its exchange, so a frame
    /// after its end is a protocol error.
    #[test]
    #[should_panic(expected = "protocol error: frame type 14 after the end of its list exchange")]
    fn a_frame_after_the_end_of_its_exchange_is_a_protocol_error() {
        let (stop, _spool, _tempdir) = stop();
        let (receiver, ended) = unbounded();
        drop(ended);
        let dispatch = Dispatch {
            receiver: Some(receiver),
            ..nowhere(stop)
        };
        read_frames(&[(frame::CACHE_RESULT, b"")], &dispatch);
    }

    /// A data session frame has no place on the control session.
    #[test]
    #[should_panic(expected = "protocol error: unexpected control session frame type 4")]
    fn another_session_frame_is_a_protocol_error() {
        let (stop, _spool, _tempdir) = stop();
        read_frames(&[(frame::STREAM_END, b"")], &nowhere(stop));
    }

    /// And a session frame has no place inside the control stream.
    #[test]
    #[should_panic(expected = "protocol error: unexpected control frame type 8")]
    fn a_session_frame_inside_the_control_stream_is_a_protocol_error() {
        let (stop, _spool, _tempdir) = stop();
        let mut chunk = FrameWriter::new(Vec::new());
        chunk.send(frame::HASHLIST, b"").unwrap();
        read_frames(&[(frame::CHUNK, &chunk.into_inner())], &nowhere(stop));
    }

    /// The writer sends a frame of an exchange only when no control frame
    /// waits, and flushes the control stream first, so that the control
    /// frames before it decode at once. A reader on the other end gets
    /// every frame in its place, with compression too (DESIGN.md §6.2).
    #[test]
    fn control_frames_go_before_the_frames_of_an_exchange() {
        for level in [0, 9] {
            let (control_tx, control) = unbounded();
            let (lists_tx, lists) = bounded(4);
            for offset in 0..3 {
                control_tx
                    .send(ControlFrame::Answer(Answer {
                        offset,
                        found: true,
                    }))
                    .unwrap();
            }
            for piece in [b"one", b"two"] {
                lists_tx
                    .send(ExchangeFrame {
                        kind: frame::HASHDATA,
                        payload: piece.to_vec(),
                    })
                    .unwrap();
            }
            drop((control_tx, lists_tx));
            let end = EndFlag::new();
            let sent = write(Vec::new(), level, &control, &[lists], None, &end, "peer");

            // The three answers in one CHUNK, then the two HASHDATA frames.
            let mut reader = FrameReader::new(&sent[..]);
            assert!(reader.read_version_and_features("peer").unwrap());
            let kinds: Vec<u8> =
                std::iter::from_fn(|| reader.next().unwrap().map(|f| f.kind)).collect();
            assert_eq!(kinds, [frame::CHUNK, frame::HASHDATA, frame::HASHDATA]);

            let (answers_tx, answers) = unbounded();
            let (sender_tx, sender) = unbounded();
            let (stop, _spool, _tempdir) = stop();
            let dispatch = Dispatch {
                answers: Some(answers_tx),
                sender: Some(sender_tx),
                ..nowhere(stop)
            };
            end.set();
            read(&sent[..], level, &dispatch, &end, "peer");
            let offsets: Vec<u64> = answers.try_iter().map(|answer| answer.offset).collect();
            assert_eq!(offsets, [0, 1, 2], "level {level}");
            let pieces: Vec<Vec<u8>> = sender.try_iter().map(|frame| frame.payload).collect();
            assert_eq!(pieces, [b"one".to_vec(), b"two".to_vec()]);
        }
    }

    /// The writer drops its signal once it has written the last frame of
    /// every side of the exchanges, also while a producer of control frames
    /// remains: the server sends END only then, so the frames are out
    /// before it shuts the session down (DESIGN.md §6.9).
    #[test]
    fn the_last_frames_of_the_exchanges_are_out_when_the_signal_drops() {
        /// A session whose bytes this test reads while the writer runs.
        #[derive(Clone, Default)]
        struct Shared(Arc<Mutex<Vec<u8>>>);
        impl Write for Shared {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (control_tx, control) = unbounded::<ControlFrame>();
        let (receiver_tx, receiver) = bounded(1);
        let (sender_tx, sender) = bounded(1);
        let (written_tx, written) = bounded::<()>(0);
        let session = Shared::default();
        let writer = {
            let session = session.clone();
            std::thread::spawn(move || {
                let exchanges = [receiver, sender];
                let end = EndFlag::new();
                write(
                    session,
                    0,
                    &control,
                    &exchanges,
                    Some(written_tx),
                    &end,
                    "peer",
                );
            })
        };
        let result = ExchangeFrame {
            kind: frame::CACHE_RESULT,
            payload: Vec::new(),
        };
        receiver_tx.send(result).unwrap();
        drop((receiver_tx, sender_tx));
        assert!(written.recv().is_err(), "a message on the signal");
        let bytes = session.0.lock().unwrap().clone();
        let mut reader = FrameReader::new(&bytes[..]);
        assert!(reader.read_version_and_features("peer").unwrap());
        let kind = reader.next().unwrap().map(|frame| frame.kind);
        assert_eq!(kind, Some(frame::CACHE_RESULT));
        // The control queue still has its producer.
        assert!(!writer.is_finished(), "the writer ended");
        drop(control_tx);
        writer.join().unwrap();
    }
}

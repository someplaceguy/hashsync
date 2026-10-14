//! The control session (DESIGN.md §6.2), on either host: one writer and one
//! reader per host.
//!
//! The writer sends this host's protocol version and features at once,
//! without waiting for the peer's, then the frames of one queue in order:
//! the queries of the stream that this host scans, the answers for the
//! stream that it receives, and STOP, when this host stops the stream that
//! it receives. After the features, they form one zstd stream at the run's
//! level, unless compression is disabled, and the writer flushes it
//! whenever the queue runs empty: the scanner waits for answers, and the
//! peer must learn of the stop at once. The queue has up to three
//! producers on each host, each with its own sender: the pins of the
//! stream that the host receives deduplicated, with the answers until
//! Completing; the scanner of the stream that it sends deduplicated, with
//! the queries until its scan ends; and the stop of the stream that it
//! receives, with STOP until its raise, or on the client, until END (step
//! N1). The writer ends once every producer has dropped its sender of the
//! queue.
//!
//! The reader checks the peer's version and features, then dispatches each
//! frame: QUERY to the lookup workers of the stream that this host
//! receives, ANSWER to the scanner of the stream that this host sends, and
//! STOP to the stop of the stream that this host sends. Each direction
//! carries the queries of one stream, the answers and the STOP of the
//! other, so no frame needs a stream ID. The reader reads until the
//! closure of the session, so the peer's relay never blocks on a full
//! pipe.
//!
//! Before END, the closure of the session, EOF or a failed write, is the
//! loss of the peer, which ends the run with the message of the data
//! session's loss; after END, it is the normal end (DESIGN.md §6.9).
//! `EndFlag` tells which.

use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

use crossbeam_channel::{Receiver, Sender};

use crate::protocol::{
    Answer, Compressor, ControlFrame, Decompressor, FrameReader, FrameWriter, Query, die_session,
    frame, lost,
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
/// frames of `queue` in order, one zstd stream at `level`, 0 for none,
/// flushed whenever the queue runs empty, until every producer has dropped
/// its sender and the queue is empty. A failed write ends the run before
/// END, a write to a session that `peer` closed as the loss of `peer`, and
/// ends this thread after it. Returns `transport` without closing it: the
/// client closes the session's stdin only once this thread has ended
/// (DESIGN.md §6.9). The bytes that a failed write left in a buffer are
/// lost.
pub fn write<W: Write>(
    transport: W,
    level: i32,
    queue: &Receiver<ControlFrame>,
    end: &EndFlag,
    peer: &str,
) -> W {
    let mut control_out = FrameWriter::new(BufWriter::new(transport));
    let mut result = control_out
        .send_version_and_features()
        .and_then(|()| control_out.flush());
    let mut control_out = FrameWriter::new(Compressor::control(control_out.into_inner(), level));
    while result.is_ok() {
        result = match control_out.next_from(queue) {
            Ok(Some(next)) => {
                let (kind, payload) = next.encode();
                control_out.send(kind, &payload)
            }
            Ok(None) => break,
            Err(e) => Err(e),
        };
    }
    if let Err(e) = result
        && !end.is_set()
    {
        die_session("control session", peer, &e);
    }
    let (transport, _) = control_out.into_inner().into_transport().into_parts();
    transport
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
}

/// The control reader: the protocol version and the features of `peer`,
/// the other host, then the frames, one zstd stream when `compressed`,
/// which this function dispatches, until the closure of the session.
/// `ready` learns that the peer's version matched, which the server waits
/// for before it starts the command (DESIGN.md §6.2). Before END, the
/// closure of the session is the loss of `peer`. A frame that `dispatch`
/// has no place for, and a second STOP, are protocol errors. A QUERY or
/// ANSWER whose receiver has ended, at Completing or at the end of the
/// scan, has no use, and is dropped. So has a STOP after the stream that
/// it stops has ended.
pub fn read<R: Read>(
    transport: R,
    compressed: bool,
    dispatch: &Dispatch,
    ready: Option<Sender<()>>,
    end: &EndFlag,
    peer: &str,
) {
    let closed = |error: Option<std::io::Error>| {
        if end.is_set() {
            return;
        }
        match error {
            None => lost(peer),
            Some(e) => die_session("control session", peer, &e),
        }
    };
    let mut control_in = FrameReader::new(BufReader::new(transport));
    match control_in.read_version_and_features(&format!("{peer}'s control session")) {
        Ok(true) => {}
        Ok(false) => return closed(None),
        Err(e) => return closed(Some(e)),
    }
    if let Some(ready) = ready {
        // The receiver of the signal waits for it, or has ended the run.
        let _ = ready.send(());
    }
    let mut control_in = FrameReader::new(Decompressor::new(control_in.into_inner(), compressed));
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
    use std::sync::Arc;

    use crossbeam_channel::unbounded;

    use super::*;
    use crate::segments::Directory;
    use crate::spool::Spool;

    /// The peer's side of a control session: its version and features,
    /// then `frames`, without compression, then EOF.
    fn session(frames: &[(u8, &[u8])]) -> Vec<u8> {
        let mut session = FrameWriter::new(Vec::new());
        session.send_version_and_features().unwrap();
        for &(kind, payload) in frames {
            session.send(kind, payload).unwrap();
        }
        session.into_inner()
    }

    /// Read `frames` with `dispatch` after END, so that the EOF after them
    /// is the normal end.
    fn read_frames(frames: &[(u8, &[u8])], dispatch: &Dispatch) {
        let end = EndFlag::new();
        end.set();
        read(&session(frames)[..], false, dispatch, None, &end, "peer");
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
    /// stop of the stream that it sends, on either host (DESIGN.md §6.2).
    #[test]
    fn each_control_frame_goes_to_its_place() {
        let (queries_tx, queries) = unbounded();
        let (answers_tx, answers) = unbounded();
        let (stop, _spool, _tempdir) = stop();
        let dispatch = Dispatch {
            queries: Some(queries_tx),
            answers: Some(answers_tx),
            stop: stop.clone(),
        };
        let (q, a) = (query(7), answer(9));
        read_frames(
            &[(frame::QUERY, &q), (frame::ANSWER, &a), (frame::STOP, b"")],
            &dispatch,
        );
        assert_eq!(queries.try_recv().unwrap().offset, 7);
        assert_eq!(answers.try_recv().unwrap().offset, 9);
        assert!(stop.is_raised());
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

    /// A session frame has no place on the control session.
    #[test]
    #[should_panic(expected = "protocol error: unexpected control frame type 8")]
    fn another_frame_is_a_protocol_error() {
        let (stop, _spool, _tempdir) = stop();
        read_frames(&[(frame::CHUNK, b"chunk")], &nowhere(stop));
    }
}

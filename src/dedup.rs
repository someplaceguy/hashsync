//! The dedup block of a deduplicated stream (DESIGN.md §6.3 and §6.4): what
//! such a stream adds to the blocks that every stream has, and the two
//! roles that a host can play in it, from the start of the run to its end.
//! Client and server build their deduplicated ends from these roles alone,
//! and keep only the order of the steps, which the wire fixes, and their
//! own steps between them.
//!
//! The sender's role (`Sending`) holds, from the start of the run, the
//! queue of its answers, which the control reader fills, and its own
//! sender of the control writer's queue. Its list exchange gives the
//! stream's `Scan`. Then the lookahead reader takes the stream's bytes and
//! idle points from its spool into the scanner's lookahead buffer, and the
//! scanner, on a thread of its own with its probe threads, turns them into
//! literals, runs, and references, and queues QUERY for the windows that
//! the hash table finds. The scanner is the stream's encoder: its data
//! queue is what the stream's data writer takes (`DataItems`).
//!
//! The receiver's role (`Receiving`) holds, from the start of the run, its
//! pins and the queue of its lookup workers, which the control reader
//! fills. The index of the host, prepared for the run, gives the hash list
//! for the list exchange and the candidates, and the lookup workers answer
//! the queries with the pins, which the decoder reads for REF. The workers
//! own the index, which closes when they end at Completing. In a run with
//! both streams deduplicated, the client's index is released between its
//! update and its HELLO, so that the run holds no lock while the server
//! updates its own (DESIGN.md §4.4).
//!
//! At the stop of a deduplicated stream, its scanner returns at once,
//! without the end of the stream: it would otherwise wait for answers that
//! the receiver no longer sends (DESIGN.md §6.6).

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use bstr::ByteSlice;
use crossbeam_channel::{Receiver, Select, Sender, TryRecvError, TrySendError, bounded, unbounded};

use crate::config::{Config, thread_count};
use crate::die::die;
use crate::fsid::FsCache;
use crate::fsops;
use crate::hash::StrongHash;
use crate::hashtable::HashTable;
use crate::index::{self, Index, Settings};
use crate::list_exchange::{ListBytes, ReceiverExchange, SenderExchange, Target};
use crate::lookup;
use crate::pins::Pins;
use crate::pool::Pool;
use crate::protocol::{
    Answer, ControlFrame, FrameReader, FrameWriter, Query, ReceiverHello, die_data,
};
use crate::roots::{self, Location};
use crate::scanner::{Frame, Lookahead, Scanner};
use crate::segments::Directory;
use crate::settings::RunSettings;
use crate::spool::{Spool, Taken};
use crate::stream::{Frames, Item, PlainFrames, READ_SIZE, Stop};
use crate::update;

/// The data queue holds `max(1, QUEUED_BYTES / B)` frames; each frame
/// holds at most `B` bytes.
const QUEUED_BYTES: usize = 4 << 20;

/// What the scanner gives the data writer, in order.
enum Output {
    Scanned(Frame),
    End,
}

/// What the lookahead reader passes to the scanner, in order: the bytes of
/// the stream, and the idle points of its source (DESIGN.md §6.3).
enum Piece {
    Bytes(Vec<u8>),
    Idle,
}

/// The scanner of one stream, ready to start: the hash table of its
/// receiver's list, its receiver's block size and strong hash, and the
/// settings of the run (DESIGN.md §6.3), with the resources that those
/// settings size, its reserved lookahead buffer and its probe threads, and
/// with the channels of its sender role: the queue of the answers, and its
/// own sender of the control writer's queue, for its queries. That sender
/// keeps the control writer alive until the scan ends, whatever the host
/// does with its own.
pub struct Scan {
    table: HashTable,
    block_size: u32,
    strong: StrongHash,
    resources: ScanResources,
    negative_cache: u64,
    answers: Receiver<Answer>,
    control: Sender<ControlFrame>,
}

/// What the scanner of a stream holds of this host for the whole stream:
/// its lookahead buffer and its probe threads. The client sets their sizes
/// with `--lookahead` and `--probe-threads`, to any value, so the sender
/// takes them before its HELLO: a host that cannot provide them ends the
/// run there, before the command starts (DESIGN.md §6.2), and not in the
/// middle of the stream.
struct ScanResources {
    lookahead: Lookahead,
    /// The probe threads, which end with the scanner's thread, which joins
    /// them; `None` probes on the scanner's thread.
    probe_pool: Option<Pool>,
}

impl ScanResources {
    /// Reserve the buffer of a lookahead of `lookahead` bytes for blocks of
    /// `block_size` bytes, and start the probe threads of `settings`, or
    /// end the run.
    fn take(lookahead: u64, block_size: u32, settings: &RunSettings) -> ScanResources {
        let reserved = Lookahead::reserve(lookahead, block_size).unwrap_or_else(|e| {
            die(format!(
                "cannot reserve the scanner's buffer for a lookahead of {lookahead} bytes \
                 (--lookahead): {e}"
            ))
        });
        ScanResources {
            lookahead: reserved,
            probe_pool: start_probe_pool(settings.probe_threads),
        }
    }
}

/// The threads of the sender's dedup block, for their join at the normal
/// end.
pub struct Scanning {
    lookahead_reader: JoinHandle<()>,
    scanner: JoinHandle<()>,
}

impl Scanning {
    pub fn join(self) {
        self.lookahead_reader
            .join()
            .expect("the lookahead reader panicked");
        self.scanner.join().expect("the scanner panicked");
    }
}

/// The encoder of a sending end (DESIGN.md §6.6), which its data writer
/// takes: the scanner of a deduplicated stream, with `scan`, or for a plain
/// stream, the bytes of `spool` as they are. `stop` is the stop of the
/// stream on this host. Returns the scanner's threads too, for their join
/// at the end of the run.
pub fn encoder(
    scan: Option<Scan>,
    spool: Spool,
    stop: Stop,
) -> (Box<dyn Frames + Send>, Option<Scanning>) {
    match scan {
        Some(scan) => {
            let (items, scanning) = start_scan(scan, spool, stop);
            (Box::new(items), Some(scanning))
        }
        None => (Box::new(PlainFrames::new(spool)), None),
    }
}

/// Start the sender's dedup block of a stream (DESIGN.md §6.3): the
/// lookahead reader, which takes the stream's bytes and idle points from
/// `spool`, and the scanner, which turns them into frames for the stream's
/// data writer, the returned `DataItems`. The scanner takes the answers of
/// `scan`, and queues its queries on the control writer's queue. `stop` is
/// the stop of the stream on this host.
fn start_scan(scan: Scan, spool: Spool, stop: Stop) -> (DataItems, Scanning) {
    let (piece_tx, piece_rx) = bounded(1);
    let lookahead_reader = std::thread::Builder::new()
        .name("lookahead-reader".into())
        .spawn(move || read_lookahead(spool, &piece_tx))
        .expect("cannot start the lookahead reader");
    let capacity = (QUEUED_BYTES / scan.block_size as usize).max(1);
    let (mut data, items) = data_queue(capacity);
    let scanner = std::thread::Builder::new()
        .name("scanner".into())
        .spawn(move || {
            let Scan {
                table,
                block_size,
                strong,
                resources,
                negative_cache,
                answers,
                control,
            } = scan;
            let ScanResources {
                lookahead,
                probe_pool,
            } = resources;
            let mut scanner = Scanner::new(
                table,
                block_size,
                strong,
                lookahead,
                negative_cache,
                probe_pool.as_deref(),
            );
            // At the stop, this thread returns without the end: the drop of
            // the data queue then tells the data writer that the stream
            // stopped, and the drop of `control` lets the control writer
            // end.
            if transfer(
                &mut scanner,
                &piece_rx,
                &answers,
                &mut data,
                &control,
                &stop,
            ) {
                data.end();
            }
        })
        .expect("cannot start the scanner");
    let scanning = Scanning {
        lookahead_reader,
        scanner,
    };
    (items, scanning)
}

/// Start the threads that probe the scanner's long stretches of windows
/// (DESIGN.md §6.3): `requested` threads, or half the cores for 0. One
/// thread needs no pool: the scanner probes on its own thread. A thread
/// that cannot start ends the run.
fn start_probe_pool(requested: u32) -> Option<Pool> {
    let count = thread_count(requested) as usize;
    (count > 1).then(|| {
        Pool::try_start("probe", count).unwrap_or_else(|e| {
            die(format!(
                "cannot start {count} probe threads (--probe-threads): {e}"
            ))
        })
    })
}

/// A new data queue of `capacity` items, from the scanner to the data
/// writer.
fn data_queue(capacity: usize) -> (DataQueue, DataItems) {
    let (items_tx, items_rx) = bounded(capacity);
    let (wakes_tx, wakes_rx) = bounded(1);
    let idle_at = Arc::new(Mutex::new(0));
    let queue = DataQueue {
        items: items_tx,
        queued: 0,
        idle_at: Arc::clone(&idle_at),
        wakes: wakes_rx,
    };
    let items = DataItems {
        items: items_rx,
        taken: 0,
        idle_at,
        wakes: wakes_tx,
        flush: false,
        literal: Vec::new(),
    };
    (queue, items)
}

/// The scanner's end of the data queue. It queues the scanner's frames for
/// the data writer, and tells whether the writer is idle (DESIGN.md
/// §6.3): the writer waits for items, with every queued item written, and
/// every flush that the scanner asked for made. The tail before an idle
/// point leaves only then, when it can leave at once.
struct DataQueue {
    items: Sender<Output>,
    /// The items queued so far.
    queued: u64,
    /// The items that the writer had taken when it last became idle. A
    /// lock, not an atomic, holds the count: see `writer_idle`.
    idle_at: Arc<Mutex<u64>>,
    /// A token each time the writer becomes idle, so that the scanner can
    /// wait for that.
    wakes: Receiver<()>,
}

impl DataQueue {
    fn is_full(&self) -> bool {
        self.items.is_full()
    }

    /// Queue `frame`. The queue must have room: the scanner is its only
    /// producer, and found room. A data writer that has ended took no more
    /// frames at the stop, which the scanner sees at the start of its next
    /// round; until then, the frames have no use, and are dropped.
    fn queue(&mut self, frame: Frame) {
        if let Err(TrySendError::Full(_)) = self.items.try_send(Output::Scanned(frame)) {
            panic!("the data queue refused a frame");
        }
        self.queued += 1;
    }

    /// True when the writer is idle. A token in `wakes` only makes the
    /// scanner look again: the count decides, so an old token does no
    /// harm.
    ///
    /// The look must not miss that the writer became idle, also when the
    /// writer's token found an old token in the channel and nothing else
    /// wakes the scanner. The tokens are taken before the look, and the
    /// lock puts the look either after the writer's last store, which it
    /// then sees, or before it. In the second case, the writer's token, or
    /// an older one that the take missed, is still in the channel, and
    /// wakes the next wait. An atomic count gives no such order: a failed
    /// `try_send` only reads the channel, so it does not order the store
    /// before the look.
    fn writer_idle(&self) -> bool {
        while self.wakes.try_recv().is_ok() {}
        *self.idle_at.lock().unwrap() == self.queued
    }

    /// The end of the stream, after the scanner's last frame. A data writer
    /// that ended at a stop that came right after that frame needs no end.
    fn end(self) {
        let _ = self.items.send(Output::End);
    }
}

/// The data writer's end of the data queue: the encoder of a deduplicated
/// stream, as its data writer sees it. It makes a flush that the scanner
/// asked for, at an idle point or after `lookahead` bytes, only once it has
/// nothing left to write: frames queued behind the flush come first,
/// because the bytes before the flush could not leave sooner, and an
/// earlier flush would only cost compression (DESIGN.md §6.2). So a writer
/// that is busy, as on a slow link, flushes only once it has caught up. A
/// queue that the scanner dropped without the end is the stop of the
/// stream. The data writer itself looks at the stop before each item, so
/// it takes none of the frames that the queue still holds then
/// (`stream::write_stream`).
pub struct DataItems {
    items: Receiver<Output>,
    /// The items taken so far.
    taken: u64,
    idle_at: Arc<Mutex<u64>>,
    wakes: Sender<()>,
    /// A flush that the scanner asked for, which waits until the queue is
    /// empty.
    flush: bool,
    /// The bytes of the last literal, which the data writer borrows.
    literal: Vec<u8>,
}

impl Frames for DataItems {
    fn next(&mut self) -> Item<'_> {
        loop {
            let output = match self.items.try_recv() {
                Ok(output) => output,
                Err(TryRecvError::Empty) if self.flush => {
                    self.flush = false;
                    return Item::Flush;
                }
                // The writer is idle: it has written every item that it
                // took, and made the flush that they asked for. Tell the
                // scanner, then wait for the next item.
                Err(TryRecvError::Empty) => {
                    *self.idle_at.lock().unwrap() = self.taken;
                    // A full channel already holds a token for the scanner.
                    let _ = self.wakes.try_send(());
                    match self.items.recv() {
                        Ok(output) => output,
                        Err(_) => return Item::Stopped,
                    }
                }
                Err(TryRecvError::Disconnected) => return Item::Stopped,
            };
            self.taken += 1;
            return match output {
                Output::Scanned(Frame::Literal(bytes)) => {
                    self.literal = bytes;
                    Item::Literal(&self.literal)
                }
                Output::Scanned(Frame::Reference) => Item::Reference,
                Output::Scanned(Frame::Run { byte, length }) => Item::Run { byte, length },
                Output::Scanned(Frame::Flush) => {
                    self.flush = true;
                    continue;
                }
                Output::End => Item::End,
            };
        }
    }
}

/// Run the scanner until the stream is emitted (DESIGN.md §6.3). Each round
/// takes the waiting answers first, then the stream's bytes and its idle
/// points while the scanner's buffer has room, then scans, sends the tail
/// before an idle point when the data writer is idle, and queues ready
/// frames while the data queue has room. A round that moves nothing waits
/// for an answer, bytes, room in the data queue, or, while a tail waits, an
/// idle data writer, and for `stop`. The queries go to `control`, the queue
/// of the control writer. True at the end of the stream, false at the stop.
fn transfer(
    scanner: &mut Scanner,
    pieces: &Receiver<Piece>,
    answers: &Receiver<Answer>,
    data: &mut DataQueue,
    control: &Sender<ControlFrame>,
    stop: &Stop,
) -> bool {
    let mut reading = true;
    loop {
        // The stop also ends the pieces: the lookahead reader takes the EOF
        // of the stopped spool. That EOF comes after the raise began, so this
        // look, which follows each sight of it, never takes it for the end
        // of the stream.
        if stop.is_raised() {
            return false;
        }
        let mut progress = false;
        for answer in answers.try_iter() {
            scanner.answer(answer.offset, answer.found);
            progress = true;
        }
        while reading && scanner.has_room() {
            match pieces.try_recv() {
                Ok(Piece::Bytes(bytes)) => scanner.push(&bytes),
                Ok(Piece::Idle) => scanner.idle(),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    scanner.end_input();
                    reading = false;
                }
            }
            progress = true;
        }
        progress |= scanner.scan(&mut |offset, strong| {
            // The control writer ends only after this producer.
            let _ = control.send(ControlFrame::Query(Query { offset, strong }));
        });
        // The tail before an idle point leaves only when it can leave at
        // once. While the data writer is busy, as on a slow link, the tail
        // waits, and bytes that arrive meanwhile can fill its windows
        // (DESIGN.md §6.3).
        if scanner.tail_waits() && data.writer_idle() {
            scanner.send_tail();
            progress = true;
        }
        // This thread is the queue's only producer, so a queue with room
        // keeps it. Only a queue that is full before this attempt can hold
        // back a ready frame, so only then does the wait include room.
        let full = data.is_full();
        progress |= queue_frames(scanner, data);
        if progress {
            continue;
        }
        if scanner.finished() {
            return true;
        }
        let mut select = Select::new();
        select.recv(answers);
        // The receiver sends no answer after the stop, and the scanner can
        // wait for one alone: when its lookahead is full and the window at
        // its front waits for an answer.
        select.recv(stop.signal());
        if reading && scanner.has_room() {
            select.recv(pieces);
        }
        if full {
            select.send(&data.items);
        }
        if scanner.tail_waits() {
            select.recv(&data.wakes);
        }
        select.ready();
    }
}

/// Queue the scanner's ready frames while the data queue has room. A frame
/// leaves the scanner, which advances `e`, only when the queue can take it
/// (DESIGN.md §6.3). True when a frame was queued.
fn queue_frames(scanner: &mut Scanner, data: &mut DataQueue) -> bool {
    let mut queued = false;
    while !data.is_full()
        && let Some(frame) = scanner.next_frame()
    {
        data.queue(frame);
        queued = true;
    }
    queued
}

/// The lookahead reader: read the stream's bytes from its spool into
/// pieces for the scanner's lookahead buffer, with its idle points in
/// order, until the end of the spool. Then close the spool (DESIGN.md
/// §6.9), and return, which closes the queue. The bounded queue pauses
/// reading while the scanner's buffer is full; meanwhile, the bytes wait in
/// the spool (DESIGN.md §6.3). At the stop of the stream, the spool gives
/// EOF, or the scanner returns and drops the queue: then the drop of the
/// spool discards what it holds.
fn read_lookahead(mut spool: Spool, pieces: &Sender<Piece>) {
    let mut buf = vec![0u8; READ_SIZE];
    loop {
        // A take never fails: the spool itself ends the run when it cannot
        // read its source or a segment.
        let next = match spool.take_next(&mut buf) {
            Taken::Bytes(n) => Piece::Bytes(buf[..n].to_vec()),
            Taken::Idle => Piece::Idle,
            Taken::End if spool.is_stopped() => return,
            Taken::End => break,
        };
        // The scanner takes pieces until this thread closes the queue, or
        // until the stop.
        if pieces.send(next).is_err() {
            return;
        }
    }
    spool.close();
}

/// The sender's role in a deduplicated stream (DESIGN.md §5.6 and §6.3),
/// from the start of the run: the queue of the answers, which the control
/// reader fills once the stream runs, and the role's own sender of the
/// control writer's queue, for the queries of its scanner. The role's list
/// exchange gives the stream's `Scan`.
pub struct Sending {
    answers: (Sender<Answer>, Receiver<Answer>),
    control: Sender<ControlFrame>,
    /// The other host, as the messages about the data session name it.
    peer: &'static str,
}

impl Sending {
    /// The sender's role, with its own sender of `control`, the queue of
    /// this host's control writer.
    pub fn new(control: &Sender<ControlFrame>, peer: &'static str) -> Sending {
        Sending {
            answers: unbounded(),
            control: control.clone(),
            peer,
        }
    }

    /// The end of the answer queue that the control reader fills.
    pub fn answers(&self) -> Sender<Answer> {
        self.answers.0.clone()
    }

    /// The first step of the sender's list exchange (DESIGN.md §5.6): read
    /// the receiver's HELLO from `input`. `lookahead` then gives the
    /// scanner's lookahead for the receiver's block size, or ends the run.
    /// Then this side takes the scanner's lookahead buffer and probe
    /// threads, or ends the run, and only then sends its HELLO on `output`,
    /// with the lists of the cache when `settings` keep sender caching. A
    /// failure of the session ends the run.
    pub fn start<R: Read, W: Write>(
        self,
        input: &mut FrameReader<R>,
        output: &mut FrameWriter<W>,
        settings: &RunSettings,
        lookahead: impl FnOnce(u32) -> u64,
    ) -> SendingExchange {
        let accept = |receiver: &ReceiverHello| {
            let block_size = receiver.block_size;
            ScanResources::take(lookahead(block_size), block_size, settings)
        };
        let (exchange, resources) =
            SenderExchange::start(input, output, settings.sender_cache, accept)
                .unwrap_or_else(|e| die_data(self.peer, &e));
        SendingExchange {
            exchange,
            resources,
            answers: self.answers.1,
            control: self.control,
            peer: self.peer,
        }
    }
}

/// The sender's role during its list exchange, after its HELLO.
pub struct SendingExchange {
    exchange: SenderExchange,
    resources: ScanResources,
    answers: Receiver<Answer>,
    control: Sender<ControlFrame>,
    peer: &'static str,
}

impl SendingExchange {
    /// The rest of the sender's list exchange, up to CACHE_RESULT, with the
    /// list cache's `max_age` (DESIGN.md §5.6). Returns the stream's scanner
    /// with the scan settings of `settings`, and the bytes of the lists
    /// received. A failure of the session ends the run.
    pub fn finish<R: Read, W: Write>(
        self,
        input: &mut FrameReader<R>,
        output: &mut FrameWriter<W>,
        settings: &RunSettings,
        max_age: u64,
    ) -> (Scan, ListBytes) {
        let (block_size, strong) = (
            self.exchange.receiver.block_size,
            self.exchange.receiver.strong_hash,
        );
        let (table, lists) = self
            .exchange
            .finish(input, output, max_age)
            .unwrap_or_else(|e| die_data(self.peer, &e));
        let scan = Scan {
            table,
            block_size,
            strong,
            resources: self.resources,
            negative_cache: settings.negative_cache,
            answers: self.answers,
            control: self.control,
        };
        (scan, lists)
    }
}

/// The receiver's role in a deduplicated stream (DESIGN.md §4.4, §5.6 and
/// §6.4), from the start of the run: the pins, which hold the blocks that
/// the lookups find, and the queue of the lookup workers, which the control
/// reader fills once the stream runs. The stop of the stream on the
/// receiving host holds the pins from the start too, to put the receiver
/// into Completing. The index, once prepared, gives the hash list that the
/// list exchange hands the sender.
pub struct Receiving {
    pins: Arc<Pins>,
    queries: (Sender<Query>, Receiver<Query>),
    /// The other host, as the messages about the data session name it.
    peer: &'static str,
}

impl Receiving {
    /// The receiver's role for the host's index, whose block size and
    /// strong hash `config` gives, with its pins in `tmp_data`, which
    /// answer on `control`, the queue of this host's control writer.
    pub fn new(
        config: &Config,
        tmp_data: &Arc<Directory>,
        control: &Sender<ControlFrame>,
        peer: &'static str,
    ) -> Receiving {
        let pins = Pins::new(
            Arc::clone(tmp_data),
            config.strong_hash,
            config.block_size,
            control.clone(),
        );
        Receiving {
            pins: Arc::new(pins),
            queries: unbounded(),
            peer,
        }
    }

    pub fn pins(&self) -> &Arc<Pins> {
        &self.pins
    }

    /// The end of the queue of the lookup workers that the control reader
    /// fills.
    pub fn queries(&self) -> Sender<Query> {
        self.queries.0.clone()
    }

    /// Prepare the receiver's index for one run, as DESIGN.md §4.4 orders
    /// it: update it and keep it read-only with a shared lock, or with
    /// `no_update`, open it read-only; export its hash list; then start the
    /// lookups, which take over the index, and answer the queries as soon
    /// as they arrive.
    pub fn prepare(self, config: &Config, no_update: bool) -> ReceivingIndex {
        let (index, locations) = open_index(config, no_update);
        let peer = self.peer;
        ReceivingIndex {
            state: Prepared::Open(self.export(index, locations, config)),
            peer,
        }
    }

    /// Update the receiver's index for one run, then close it, which
    /// releases its lock until the receiver's HELLO opens the index again
    /// (`ReceivingIndex::hello`). This is the client's step in a run with
    /// both streams deduplicated: the run must hold no lock while the
    /// server waits for the exclusive lock of its own update, or two such
    /// runs in opposite directions between the same hosts would each hold
    /// one index and wait for the other (DESIGN.md §4.4).
    pub fn update_and_release(self, config: &Config) -> ReceivingIndex {
        let (index, locations) = update::perform(config, false, None);
        index.close();
        let peer = self.peer;
        ReceivingIndex {
            state: Prepared::Released {
                receiving: self,
                locations,
            },
            peer,
        }
    }

    /// Export the hash list of `index`, which is read-only with a shared
    /// lock, from one database snapshot, and start the lookups, which take
    /// over the index, and answer the queries as soon as they arrive.
    /// `locations` are the current locations of the roots of `config`.
    fn export(self, index: Index, locations: Vec<Location>, config: &Config) -> Exported {
        let meta = index.read_meta().expect("the index lost its metadata");
        let target = Target::export(&index, config.strong_hash);
        let workers = lookup::start(index, locations, &self.pins, config, self.queries.1);
        Exported {
            lookups: Lookups {
                pins: self.pins,
                workers,
            },
            target,
            index_id: meta.index_id,
        }
    }
}

/// The receiver's role once its index is prepared, until its HELLO.
pub struct ReceivingIndex {
    state: Prepared,
    peer: &'static str,
}

/// The index of a `ReceivingIndex`: ready for the run, or, for the client
/// of a run with both streams deduplicated, released between its update
/// and its HELLO.
enum Prepared {
    /// Open read-only with a shared lock, with its hash list exported and
    /// its lookups running.
    Open(Exported),
    /// Updated and closed, without a lock, with the root locations of the
    /// update, until the HELLO opens it again.
    Released {
        receiving: Receiving,
        locations: Vec<Location>,
    },
}

/// A receiver's index once its lookups own it, with the hash list that the
/// receiver's HELLO names.
struct Exported {
    lookups: Lookups,
    /// The hash list of the index, exported from one database snapshot.
    target: Target,
    /// The ID of the index, which partitions the list caches.
    index_id: [u8; 16],
}

impl ReceivingIndex {
    /// The first step of the receiver's list exchange (DESIGN.md §5.6):
    /// send its HELLO on `output`, with the block size and the hashes of
    /// `config`, which are the stream's, and the ID of the index's hash
    /// list. An index that `update_and_release` released opens again first,
    /// read-only with a shared lock, and only then exports its hash list:
    /// another process can have changed it meanwhile. If that process
    /// rebuilt it with other settings, so that it no longer matches
    /// `config`, the run ends with `die`, as a run with `--no-update` does
    /// (DESIGN.md §4.4). A failure of the session ends the run.
    ///
    /// For an index that `update_and_release` released, call this only
    /// once the server's update is done, which the forward stream's list
    /// exchange shows: the reopen takes this index's lock, and a run that
    /// held it while the server waits for the exclusive lock of its update
    /// could deadlock with a run in the opposite direction (DESIGN.md §4.4,
    /// step L1).
    pub fn hello<W: Write>(
        self,
        output: &mut FrameWriter<W>,
        config: &Config,
    ) -> ReceivingExchange {
        let exported = match self.state {
            Prepared::Open(exported) => exported,
            Prepared::Released {
                receiving,
                locations,
            } => receiving.export(open_read_only(config), locations, config),
        };
        let Exported {
            lookups,
            target,
            index_id,
        } = exported;
        let exchange = ReceiverExchange::start(output, config, target, index_id)
            .unwrap_or_else(|e| die_data(self.peer, &e));
        ReceivingExchange {
            lookups,
            exchange,
            peer: self.peer,
        }
    }
}

/// The receiver's role during its list exchange, after its HELLO.
pub struct ReceivingExchange {
    lookups: Lookups,
    exchange: ReceiverExchange,
    peer: &'static str,
}

impl ReceivingExchange {
    /// The rest of the receiver's list exchange, up to CACHE_RESULT, with
    /// sender caching as `settings` say, and the list cache's `max_age`
    /// (DESIGN.md §5.6). Returns the lookups, and the bytes of the lists
    /// sent. A failure of the session ends the run.
    pub fn finish<R: Read, W: Write>(
        self,
        input: &mut FrameReader<R>,
        output: &mut FrameWriter<W>,
        settings: &RunSettings,
        max_age: u64,
    ) -> (Lookups, ListBytes) {
        let lists = self
            .exchange
            .finish(input, output, settings.sender_cache, max_age)
            .unwrap_or_else(|e| die_data(self.peer, &e));
        (self.lookups, lists)
    }
}

/// The receiver's index for one run (DESIGN.md §4.4): after an update, the
/// read-only index with a shared lock, or with `no_update`, the index
/// opened read-only, which must match the configuration. Returns the index
/// with the current root locations, from which the lookups read candidate
/// blocks.
fn open_index(config: &Config, no_update: bool) -> (Index, Vec<Location>) {
    if !no_update {
        let (index, locations) = update::perform(config, false, None);
        return (index.into_read_only(&config.index_path), locations);
    }
    fsops::raise_descriptor_limit();
    let fs_cache = FsCache::new();
    let locations = roots::expand(config, &fs_cache);
    (open_read_only(config), locations)
}

/// The index of `config`, opened read-only with a shared lock: with
/// `--no-update`, or again after an update that released it. No update
/// holds this lock, so the index must exist and still match the
/// configuration, else the run ends with `die`.
fn open_read_only(config: &Config) -> Index {
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
    index
}

/// The receiver's role once its list exchange is done (DESIGN.md §6.4):
/// the pins, which the stream's data reader and decoder take, and the
/// lookup workers, which own the index until Completing.
pub struct Lookups {
    pins: Arc<Pins>,
    workers: Vec<JoinHandle<()>>,
}

impl Lookups {
    pub fn pins(&self) -> &Arc<Pins> {
        &self.pins
    }

    /// The normal end (DESIGN.md §6.9): join the workers, which ended at
    /// Completing, and close the pins, which no other thread holds any
    /// more.
    pub fn close(self) {
        for worker in self.workers {
            worker.join().expect("a lookup worker panicked");
        }
        Arc::into_inner(self.pins)
            .expect("the pins are still in use")
            .close();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::atomic::Ordering::SeqCst;
    use std::time::Duration;

    use super::*;
    use crate::hash::weak::WeakHasher;
    use crate::hash::{Strong, StrongHash};
    use crate::protocol::{FrameReader, FrameWriter, frame};
    use crate::scanner::tests::noise;
    use crate::stream::tests::Counted;
    use crate::stream::write_stream;

    const BS: u32 = 64;

    /// A stop that nothing raises, for a stream that ends normally, with
    /// the spool that it would stop and the guard of that spool's
    /// directory.
    fn unraised() -> (Stop, Spool, tempfile::TempDir) {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(crate::segments::Directory::open(
            path.as_os_str().as_encoded_bytes(),
        ));
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        (Stop::sending(spool.stopper()), spool, tempdir)
    }

    /// `transfer` with a data queue of one frame, which the data writer
    /// drains slowly, while the lookups answer from another thread and
    /// reject the stale blocks, and idle points come now and then. The
    /// output stays exact and in order, flushes follow the idle points, and
    /// the transfer ends (DESIGN.md §6.3). The wait has no deadline: a
    /// transfer that stalls hangs the test, and a debugger can inspect it.
    #[test]
    fn a_full_data_queue_keeps_the_output_exact() {
        let valid: Vec<Vec<u8>> = (0..4).map(|i| noise(BS as usize, i)).collect();
        let stale: Vec<Vec<u8>> = (4..6).map(|i| noise(BS as usize, i)).collect();
        let listed: Vec<&Vec<u8>> = valid.iter().chain(&stale).collect();
        let mut input = Vec::new();
        for i in 0..1000 {
            input.extend_from_slice(&noise(i % 11, 100 + i as u64));
            input.extend_from_slice(listed[i % listed.len()]);
            // Equal bytes, now and then enough for runs.
            input.extend(std::iter::repeat_n(i as u8, i % 7 * 50));
        }
        let weak = WeakHasher::new(BS as usize);
        let mut values: Vec<u64> = listed.iter().map(|b| weak.hash(b)).collect();
        values.sort_unstable();
        values.dedup();
        let blocks: HashMap<Strong, Vec<u8>> = valid
            .iter()
            .map(|b| (StrongHash::Blake3.digest(b), b.clone()))
            .collect();

        for lookahead in [u64::from(BS), 64 * u64::from(BS)] {
            let (piece_tx, piece_rx) = bounded(1);
            let (answer_tx, answer_rx) = crossbeam_channel::unbounded();
            let (mut data, mut items) = data_queue(1);
            let (query_tx, query_rx) = crossbeam_channel::unbounded();
            let (done_tx, done_rx) = bounded(1);
            let pins = Arc::new(Mutex::new(HashMap::new()));

            let source = input.clone();
            std::thread::spawn(move || {
                for (i, piece) in source.chunks(100).enumerate() {
                    piece_tx.send(Piece::Bytes(piece.to_vec())).unwrap();
                    if i % 13 == 0 {
                        piece_tx.send(Piece::Idle).unwrap();
                    }
                }
            });
            // A lookup publishes the pin before it answers yes.
            let (lookup_pins, blocks) = (Arc::clone(&pins), blocks.clone());
            std::thread::spawn(move || {
                for (i, control) in query_rx.iter().enumerate() {
                    let ControlFrame::Query(query) = control else {
                        panic!("the scanner queued another frame than QUERY");
                    };
                    if i % 3 == 0 {
                        std::thread::sleep(Duration::from_micros(50));
                    }
                    let found = match blocks.get(&query.strong) {
                        Some(block) => {
                            let mut pins = lookup_pins.lock().unwrap();
                            pins.insert(query.offset, block.clone());
                            true
                        }
                        None => false,
                    };
                    let _ = answer_tx.send(Answer {
                        offset: query.offset,
                        found,
                    });
                }
            });
            std::thread::spawn(move || {
                let (mut output, mut flushes) = (Vec::new(), 0);
                for i in 0.. {
                    let next = items.next();
                    if i % 7 == 0 {
                        std::thread::sleep(Duration::from_micros(200));
                    }
                    match next {
                        Item::Literal(bytes) => output.extend_from_slice(bytes),
                        Item::Reference => {
                            let c = output.len() as u64;
                            output.extend_from_slice(&pins.lock().unwrap()[&c]);
                        }
                        Item::Run { byte, length } => {
                            output.extend(std::iter::repeat_n(byte, length as usize));
                        }
                        Item::Flush => flushes += 1,
                        Item::End => break,
                        Item::Stopped => panic!("the stream stopped"),
                    }
                }
                done_tx.send((output, flushes)).unwrap();
            });
            let table = HashTable::build(&values);
            let (stop, _spool, _tempdir) = unraised();
            std::thread::spawn(move || {
                let lookahead = Lookahead::reserve(lookahead, BS).unwrap();
                let mut scanner =
                    Scanner::new(table, BS, StrongHash::Blake3, lookahead, 1 << 20, None);
                let ended = transfer(
                    &mut scanner,
                    &piece_rx,
                    &answer_rx,
                    &mut data,
                    &query_tx,
                    &stop,
                );
                assert!(ended);
                data.end();
            });
            let (output, flushes) = done_rx
                .recv()
                .expect("the data writer ended without the output");
            assert!(output == input, "lookahead {lookahead}: the output differs");
            assert!(flushes > 0, "lookahead {lookahead}: no flush");
        }
    }

    /// Wait, with no deadline, until the thread `tid` of this process
    /// sleeps in `futex`: crossbeam's `Select` parks a thread there. The
    /// state comes first, because a thread that is waking up can still show
    /// its last system call.
    fn wait_until_parked(tid: i32) {
        let task = format!("/proc/self/task/{tid}");
        loop {
            let stat = std::fs::read_to_string(format!("{task}/stat")).unwrap();
            // The state follows the name, which ends with the last `)`.
            let state = stat[stat.rfind(')').unwrap() + 2..].chars().next();
            if state == Some('S') {
                let syscall = std::fs::read_to_string(format!("{task}/syscall")).unwrap();
                let number = syscall.split_whitespace().next().unwrap();
                if number.parse() == Ok(libc::SYS_futex) {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// With a lookahead of one window, the scanner waits for the answer to
    /// the query of its front window, and the server sends none after the
    /// stop. So the stop must end that wait: `transfer` returns without the
    /// end of the stream, and the data writer sees the stop once the
    /// scanner drops the data queue (DESIGN.md §6.3 and §6.6).
    #[test]
    fn the_stop_ends_the_wait_for_an_answer() {
        let block = noise(BS as usize, 3);
        let table = HashTable::build(&[WeakHasher::new(BS as usize).hash(&block)]);
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(crate::segments::Directory::open(
            path.as_os_str().as_encoded_bytes(),
        ));
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        let stop = Stop::sending(spool.stopper());
        let (piece_tx, piece_rx) = bounded(2);
        piece_tx
            .send(Piece::Bytes([&block[..], &block[..]].concat()))
            .unwrap();
        let (_answer_tx, answer_rx) = crossbeam_channel::unbounded();
        let (query_tx, query_rx) = crossbeam_channel::unbounded();
        let (mut data, mut items) = data_queue(4);
        let (tid_tx, tid_rx) = bounded(1);
        let scanning = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                tid_tx
                    .send(rustix::thread::gettid().as_raw_nonzero())
                    .unwrap();
                let lookahead = Lookahead::reserve(BS.into(), BS).unwrap();
                let mut scanner =
                    Scanner::new(table, BS, StrongHash::Blake3, lookahead, 1 << 20, None);
                let ended = transfer(
                    &mut scanner,
                    &piece_rx,
                    &answer_rx,
                    &mut data,
                    &query_tx,
                    &stop,
                );
                // The pieces stay open: only the stop can end the scan.
                drop(piece_tx);
                ended
            })
        };
        // The query of the first window, which gets no answer. Once the
        // scanner has sent it, its only wait is the one for the answer, or
        // for the stop. No deadline: a scanner that waits only for answers
        // hangs the test.
        let tid = tid_rx.recv().unwrap();
        assert!(matches!(query_rx.recv(), Ok(ControlFrame::Query(_))));
        wait_until_parked(tid.get());
        stop.raise();
        assert!(!scanning.join().unwrap(), "the scan ended the stream");
        assert!(matches!(items.next(), Item::Stopped));
    }

    /// At the stop, the data writer of the forward stream takes none of the
    /// frames that the data queue still holds, and ends without STREAM_END.
    /// The scanner sees the stop only at the start of its next round, so it
    /// can still queue a frame, and end the stream, after the writer has
    /// ended (DESIGN.md §6.6).
    #[test]
    fn the_data_writer_takes_no_queued_frame_after_the_stop() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("tmp-data");
        let dir = Arc::new(crate::segments::Directory::open(
            path.as_os_str().as_encoded_bytes(),
        ));
        let (_writer, spool) = Spool::pipe(dir, u64::MAX);
        let stop = Stop::sending(spool.stopper());
        let (mut data, items) = data_queue(8);
        data.queue(Frame::Literal(b"queued".to_vec()));
        data.queue(Frame::Reference);
        stop.raise();
        let settings = crate::settings::tests::settings(&["--compression-level", "0"]);
        let session = FrameWriter::new(Vec::new());
        let (items, taken) = Counted::new(items);
        let (session, _) = write_stream(session, items, &settings, "client", &stop);
        assert!(session.into_inner().is_empty(), "the writer sent frames");
        assert_eq!(taken.load(SeqCst), 0, "the writer took a queued frame");
        data.queue(Frame::Literal(b"late".to_vec()));
        data.end();
    }

    /// The data writer is idle only while it waits for items, after it has
    /// taken and written every queued item (DESIGN.md §6.3). Its token
    /// wakes the scanner, which then finds the writer idle.
    #[test]
    fn the_writer_is_idle_only_after_every_queued_item() {
        let (mut data, mut items) = data_queue(4);
        assert!(data.writer_idle(), "nothing to write yet");
        data.queue(Frame::Reference);
        assert!(!data.writer_idle(), "an item waits");
        assert!(matches!(items.next(), Item::Reference));
        assert!(!data.writer_idle(), "the writer holds an item");
        let writer = std::thread::spawn(move || {
            let literal = match items.next() {
                Item::Literal(bytes) => Some(bytes.to_vec()),
                _ => None,
            };
            (items, literal)
        });
        // No deadline: a writer that does not say that it is idle hangs the
        // test.
        data.wakes.recv().unwrap();
        assert!(data.writer_idle());
        data.queue(Frame::Literal(b"next".to_vec()));
        let (_items, literal) = writer.join().unwrap();
        assert_eq!(literal.as_deref(), Some(&b"next"[..]));
        assert!(!data.writer_idle(), "the writer holds an item");
    }

    /// What a transport saw: the bytes of each write call, or a flush.
    #[derive(Debug, PartialEq, Eq)]
    enum Seen {
        Write(Vec<u8>),
        Flush,
    }

    /// A transport that records what it sees.
    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<Seen>>>);

    impl Write for Recorder {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().push(Seen::Write(buf.to_vec()));
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.0.lock().unwrap().push(Seen::Flush);
            Ok(())
        }
    }

    /// The data writer makes a flush that the scanner asks for only once it
    /// has nothing left to write: frames queued behind the flush come
    /// first, because the bytes before it could not leave sooner (DESIGN.md
    /// §6.2). Without compression, the flush sends the CHUNK that holds both
    /// frames, then flushes the session.
    #[test]
    fn the_data_writer_flushes_only_when_it_has_nothing_left_to_write() {
        let recorder = Recorder::default();
        let (mut data, items) = data_queue(8);
        data.queue(Frame::Literal(b"before".to_vec()));
        data.queue(Frame::Flush);
        data.queue(Frame::Literal(b"after".to_vec()));
        let (stop, _spool, _tempdir) = unraised();
        let writer = {
            let session = FrameWriter::new(recorder.clone());
            let settings = crate::settings::tests::settings(&["--compression-level", "0"]);
            std::thread::spawn(move || write_stream(session, items, &settings, "client", &stop))
        };
        // No deadline: a writer that never becomes idle hangs the test.
        data.wakes.recv().unwrap();
        assert!(data.writer_idle());
        let seen = std::mem::take(&mut *recorder.0.lock().unwrap());
        let (flush, writes) = seen.split_last().unwrap();
        assert_eq!(*flush, Seen::Flush, "{seen:?}");
        let written: Vec<u8> = writes
            .iter()
            .flat_map(|seen| match seen {
                Seen::Write(bytes) => bytes.clone(),
                Seen::Flush => panic!("a flush before the last frame: {seen:?}"),
            })
            .collect();
        let mut session = FrameReader::new(&written[..]);
        let chunk = session.expect(frame::CHUNK, "CHUNK").unwrap().to_vec();
        let lit =
            |payload: &[u8]| [&[frame::LIT, payload.len() as u8, 0, 0, 0][..], payload].concat();
        assert_eq!(chunk, [lit(b"before"), lit(b"after")].concat());
        assert!(session.next().unwrap().is_none(), "more than one CHUNK");
        data.end();
        writer.join().unwrap();
    }
}

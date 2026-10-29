// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! The dedup block of a deduplicated stream (DESIGN.md §6.3 and §6.4): what
//! such a stream adds to the blocks that every stream has, and the two
//! roles that a host can play in it, from the start of the run to its end.
//! Client and server build their deduplicated ends from these roles alone.
//! Both roles start with the streams, and work while the streams run.
//!
//! The sender's role (`Sending`) holds, from the start of the run, the
//! queue of its answers and the frames of its side of the list exchange,
//! which the control reader fills, and its own sender of the control
//! writer's queue. At the receiver's HELLO, it starts the scanner of the
//! stream, with an empty table: the lookahead reader takes the stream's
//! bytes and idle points from its spool into the scanner's lookahead
//! buffer, and the scanner, on a thread of its own with its probe threads,
//! turns them into literals, runs, and references, and queues QUERY for the
//! windows that the hash table finds. The scanner is the stream's encoder:
//! its data queue is what the stream's data writer takes (`DataItems`).
//! Then the sender's side of the list exchange runs on a thread of its own,
//! and gives the scanner its table as the target becomes known. With
//! `--wait`, the scanner scans only once its table is whole.
//!
//! The receiver's role (`Receiving`) holds, from the start of the run, its
//! pins, the queue of its lookup workers, and the frames of its side of the
//! list exchange, which the control reader fills. Its thread prepares the
//! index of the host for the run, starts the lookup workers, which answer
//! the queries with the pins, which the decoder reads for REF, and runs the
//! receiver's side of the list exchange. The workers own the index, which
//! closes when they end at Completing. In a run with both streams
//! deduplicated, both hosts update their index at the same time, and each
//! keeps its index closed from the end of its update until the other host's
//! update is done too (DESIGN.md §4.4). At the end of the run, once its
//! index has closed, the receiver updates it again, in a child process
//! (`AfterCommand`).
//!
//! At the stop of a deduplicated stream, its scanner returns at once,
//! without the end of the stream: it would otherwise wait for answers that
//! the receiver no longer sends (DESIGN.md §6.6).

use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Select, Sender, TryRecvError, TrySendError, bounded, unbounded};
use rustix::process::Pid;

use crate::config::{Config, Source, thread_count};
use crate::die::{self, die};
use crate::fsid::FsCache;
use crate::fsops;
use crate::hash::StrongHash;
use crate::hashtable::HashTable;
use crate::index::{self, Access, Index};
use crate::list_exchange::{self, Link, ListBytes, ReceiverExchange, TableUpdate};
use crate::lookup;
use crate::pins::Pins;
use crate::pool::Pool;
use crate::protocol::{Answer, ControlFrame, ExchangeFrame, Query, frame};
use crate::report;
use crate::roots::{self, Location};
use crate::scanner::{Frame, Lookahead, Scanner};
use crate::segments::Directory;
use crate::settings::{Deduplicated, RunSettings};
use crate::spool::{Spool, Taken};
use crate::stream::{Frames, Item, READ_SIZE, Stop};
use crate::sys;
use crate::units::{self, Bytes};
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

/// The scanner of one stream, ready to start: its receiver's block size and
/// strong hash, and the settings of the run (DESIGN.md §6.3), with the
/// resources that those settings size, its reserved lookahead buffer and its
/// probe threads; the updates of its table, which the sender's side of the
/// list exchange gives; and the channels of its sender role: the queue of
/// the answers, and its own sender of the control writer's queue, for its
/// queries. That sender keeps the control writer alive until the scan
/// ends, whatever the host does with its own.
struct Scan {
    block_size: u32,
    strong: StrongHash,
    resources: ScanResources,
    negative_cache: u64,
    table: Feed,
    answers: Receiver<Answer>,
    control: Sender<ControlFrame>,
}

/// What the scanner of a stream holds of this host for the whole stream:
/// its lookahead buffer and its probe threads. The client sets their sizes
/// with `--lookahead` and `--probe-threads`, to any value, so the sender
/// takes them at the receiver's HELLO, before the scanner starts: a host
/// that cannot provide them ends the run there (DESIGN.md §6.2), and not in
/// the middle of the stream.
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

/// The table of a scanner while the list exchange of its stream runs
/// (DESIGN.md §6.3): the updates that the sender's side of the exchange
/// gives, until it ends, and whether the scanner waits for the whole table
/// before it scans, with `--wait`.
struct Feed {
    /// The updates; `None` once the exchange has given every update.
    updates: Option<Receiver<TableUpdate>>,
    /// A table came. An exchange that ended with HASHNONE gives none.
    has_table: bool,
    wait: bool,
    /// The stream, `stdin` or `stdout`, which the notes about its list name.
    stream: &'static str,
    /// A note has said that the stream waits for its whole list.
    waiting: bool,
}

impl Feed {
    /// Give `scanner` every update that has come. True when one came, or
    /// when the exchange has given its last. The last update makes the
    /// table whole, and a note says so. From then on the scanner finds every
    /// block of the list, and with `--wait`, it starts to scan. Without
    /// `--wait`, the note also gives the bytes of the stream that the
    /// scanner had emitted by then: they went out before the whole list
    /// could help them. With `--wait`, the scanner has emitted none.
    fn apply(&mut self, scanner: &mut Scanner) -> bool {
        let Some(updates) = &self.updates else {
            return false;
        };
        let mut progress = false;
        loop {
            match updates.try_recv() {
                Ok(TableUpdate::Table(table)) => {
                    scanner.set_table(table);
                    self.has_table = true;
                }
                Ok(TableUpdate::Values(values)) => scanner.insert(&values),
                Err(TryRecvError::Empty) => return progress,
                Err(TryRecvError::Disconnected) => {
                    self.updates = None;
                    if self.has_table {
                        if self.wait {
                            report::note(format_args!("got {}'s full dedup metadata", self.stream));
                        } else {
                            report::note(format_args!(
                                "got {}'s full dedup metadata after sending {} with partial dedup",
                                self.stream,
                                Bytes(scanner.emitted())
                            ));
                        }
                    }
                    return true;
                }
            }
            progress = true;
        }
    }

    /// Report in a note that the stream waits for its whole list, so that
    /// the wait does not look like a hang: it lasts at least until the
    /// receiver's update is done, which can take long. The scanner calls
    /// this when bytes of the stream arrive. With `--wait`, bytes that
    /// arrive while the exchange still runs wait for the whole list, and
    /// the first of them start the wait. `apply` reports its end.
    fn bytes_arrived(&mut self) {
        if self.wait && self.updates.is_some() && !self.waiting {
            self.waiting = true;
            report::note(format_args!("waiting for {}'s dedup metadata", self.stream));
        }
    }

    /// True when the scanner may scan: at once without `--wait`, and with
    /// it once the table is whole. Without a table, after HASHNONE, the
    /// receiver's stream has ended or stopped before the target, and the
    /// scanner then waits for the stop.
    fn ready(&self) -> bool {
        !self.wait || (self.updates.is_none() && self.has_table)
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
                block_size,
                strong,
                resources,
                negative_cache,
                mut table,
                answers,
                control,
            } = scan;
            let ScanResources {
                lookahead,
                probe_pool,
            } = resources;
            let mut scanner = Scanner::new(
                HashTable::empty(),
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
                &mut table,
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
/// (DESIGN.md §6.3): `requested` threads, or one per core for 0. One
/// thread needs no pool: the scanner probes on its own thread. A thread
/// that cannot start ends the run. `hashsync bench` starts its probe
/// threads here too, so that it measures the scan of a sender with the
/// same `--probe-threads`.
pub fn start_probe_pool(requested: u32) -> Option<Pool> {
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
/// takes the updates of the table first, then the waiting answers, then the
/// stream's bytes and its idle points while the scanner's buffer has room.
/// Once `table` is ready, which with `--wait` takes the whole table, it
/// scans, sends the tail before an idle point when the data writer is
/// idle, and queues ready frames while the data queue has room. A round
/// that moves nothing waits for an update, an answer, bytes, room in the
/// data queue, or, while a tail waits, an idle data writer, and for `stop`.
/// The queries go to `control`, the queue of the control writer. True at
/// the end of the stream, false at the stop.
fn transfer(
    scanner: &mut Scanner,
    pieces: &Receiver<Piece>,
    answers: &Receiver<Answer>,
    table: &mut Feed,
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
        // The values that have arrived go into the table before the next
        // scan, so that it finds every block that they can find.
        let mut progress = table.apply(scanner);
        for answer in answers.try_iter() {
            scanner.answer(answer.offset, answer.found);
            progress = true;
        }
        while reading && scanner.has_room() {
            match pieces.try_recv() {
                Ok(Piece::Bytes(bytes)) => {
                    scanner.push(&bytes);
                    table.bytes_arrived();
                }
                Ok(Piece::Idle) => scanner.idle(),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    scanner.end_input();
                    reading = false;
                }
            }
            progress = true;
        }
        // This thread is the queue's only producer, so a queue with room
        // keeps it. Only a queue that is full before this attempt can hold
        // back a ready frame, so only then does the wait include room.
        let full = data.is_full();
        let scanning = table.ready();
        if scanning {
            progress |= scanner.scan(&mut |offset, strong| {
                // The control writer ends only after this producer.
                let _ = control.send(ControlFrame::Query(Query { offset, strong }));
            });
            // The tail before an idle point leaves only when it can leave
            // at once. While the data writer is busy, as on a slow link, the
            // tail waits, and bytes that arrive meanwhile can fill its
            // windows (DESIGN.md §6.3).
            if scanner.tail_waits() && data.writer_idle() {
                scanner.send_tail();
                progress = true;
            }
            progress |= queue_frames(scanner, data);
        }
        if progress {
            continue;
        }
        // An empty stream has nothing to scan, also before the table.
        if scanner.finished() {
            return true;
        }
        let mut select = Select::new();
        select.recv(answers);
        // The receiver sends no answer after the stop, and the scanner can
        // wait for one alone: when its lookahead is full and the window at
        // its front waits for an answer. A scanner that waits for its table
        // waits for the stop too.
        select.recv(stop.signal());
        if let Some(updates) = &table.updates {
            select.recv(updates);
        }
        if reading && scanner.has_room() {
            select.recv(pieces);
        }
        if scanning && full {
            select.send(&data.items);
        }
        if scanning && scanner.tail_waits() {
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
/// reader fills once the stream runs, the link of its side of the list
/// exchange, and the role's own sender of the control writer's queue, for
/// the queries of its scanner.
pub struct Sending {
    answers: (Sender<Answer>, Receiver<Answer>),
    /// The end of the link where the control reader passes the frames of
    /// the receiver's side of the exchange.
    frames: Sender<ExchangeFrame>,
    link: Link,
    control: Sender<ControlFrame>,
    /// The stream, `stdin` or `stdout`, which the notes about its list name.
    stream: &'static str,
}

impl Sending {
    /// The sender's role in `stream`, `stdin` or `stdout`, with its own
    /// sender of `control`, the queue of this host's control writer, and
    /// `output`, the queue of that writer for the frames of this side of
    /// the list exchange.
    pub fn new(
        stream: &'static str,
        control: &Sender<ControlFrame>,
        output: Sender<ExchangeFrame>,
    ) -> Sending {
        let (frames, input) = unbounded();
        Sending {
            answers: unbounded(),
            frames,
            link: Link::new(input, output),
            control: control.clone(),
            stream,
        }
    }

    /// The end of the answer queue that the control reader fills.
    pub fn answers(&self) -> Sender<Answer> {
        self.answers.0.clone()
    }

    /// Where the control reader passes the frames of the receiver's side of
    /// the list exchange.
    pub fn frames(&self) -> Sender<ExchangeFrame> {
        self.frames.clone()
    }

    /// Start the sender's role at the receiver's HELLO, `hello`, the payload
    /// of the frame (DESIGN.md §5.6 and §6.3), which ends the run if this
    /// build does not support its hashes. `lookahead` then gives the
    /// scanner's lookahead for the receiver's block size, or ends the run.
    /// Then this side takes the scanner's lookahead buffer and probe
    /// threads, or ends the run, and starts the scanner of the stream on
    /// `spool`, with an empty table, at once, or with `--wait` once its
    /// table is whole. `stop` is the stop of the stream on this host. Then
    /// it starts the sender's side of the list exchange, on a thread of its
    /// own, with the sender caching of `settings` and the list cache's
    /// `max_age`, which gives the scanner its table. Returns the stream's
    /// encoder, for its data writer, and the threads.
    pub fn start(
        self,
        hello: &[u8],
        spool: Spool,
        stop: Stop,
        settings: &RunSettings,
        max_age: u64,
        lookahead: impl FnOnce(u32) -> u64,
    ) -> (DataItems, SendingThreads) {
        let hello = list_exchange::read_hello(hello);
        let block_size = hello.block_size;
        let resources = ScanResources::take(lookahead(block_size), block_size, settings);
        let (table_tx, table_rx) = unbounded();
        let scan = Scan {
            block_size,
            strong: hello.strong_hash,
            resources,
            negative_cache: settings.negative_cache,
            table: Feed {
                updates: Some(table_rx),
                has_table: false,
                wait: settings.wait,
                stream: self.stream,
                waiting: false,
            },
            answers: self.answers.1,
            control: self.control,
        };
        let (items, scanning) = start_scan(scan, spool, stop);
        let link = self.link;
        let (strong, sender_cache) = (hello.strong_hash, settings.sender_cache);
        let exchange = std::thread::Builder::new()
            .name("sender".into())
            .spawn(move || list_exchange::send(&link, strong, sender_cache, max_age, table_tx))
            .expect("cannot start the sender's list exchange");
        let threads = SendingThreads { exchange, scanning };
        (items, threads)
    }
}

/// The threads of the sender's role, for their join at the normal end: its
/// side of the list exchange, and its dedup block.
pub struct SendingThreads {
    exchange: JoinHandle<ListBytes>,
    scanning: Scanning,
}

impl SendingThreads {
    /// Join the threads, once the list exchange and the stream have ended.
    /// Returns the bytes of the lists received.
    pub fn join(self) -> ListBytes {
        let lists = self
            .exchange
            .join()
            .expect("the sender's list exchange panicked");
        self.scanning.join();
        lists
    }
}

/// How the receiver of a run gets its index ready, which the lock rule of
/// DESIGN.md §4.4 decides: an update waits for nothing else while it holds
/// its exclusive lock, and no run waits for an exclusive lock while it holds
/// a shared one. The update after the command (`AfterCommand`) follows the
/// rule too, since the run's index has closed by then.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Preparation {
    /// With `--no-update`: open the index read-only, as it is.
    Open,
    /// This host alone updates its index at the start of the run, with the
    /// options: update the index, and keep it read-only on the same lock,
    /// so that no modifying process comes between the update and the use of
    /// the index.
    UpdateAndKeep(update::Options),
    /// Both hosts update their index, at the same time: update it with the
    /// options, and release it, because the other host can still be waiting
    /// for the exclusive lock of its own update then. Open it again,
    /// read-only, once that update is done too (steps L1 and L2).
    UpdateAndReopen(update::Options),
}

impl Preparation {
    /// The preparation for a run with `settings`, whose `deduplicated`
    /// streams tell whether the other host updates an index of its own too.
    /// The update takes `--scan-immutable` from the settings, which
    /// `--no-update` excludes. `--compact` belongs to the update after the
    /// command (`AfterCommand::of`), so that the list does not wait for the
    /// compaction.
    fn of(settings: &RunSettings, deduplicated: Deduplicated) -> Self {
        let options = update::Options {
            scan_immutable: settings.scan_immutable,
            ..update::Options::default()
        };
        match (settings.no_update, deduplicated) {
            (true, _) => Preparation::Open,
            (false, Deduplicated::Both) => Preparation::UpdateAndReopen(options),
            (false, Deduplicated::Stdin | Deduplicated::Stdout) => {
                Preparation::UpdateAndKeep(options)
            }
        }
    }
}

/// The receiver's role in a deduplicated stream (DESIGN.md §4.4, §5.6 and
/// §6.4), from the start of the run: the pins, which hold the blocks that
/// the lookups find, the queue of the lookup workers, which the control
/// reader fills once the stream runs, and the link of its side of the list
/// exchange. The stop of the stream on the receiving host holds the pins
/// from the start too, to put the receiver into Completing.
pub struct Receiving {
    pins: Arc<Pins>,
    queries: (Sender<Query>, Receiver<Query>),
    /// The end of the link where the control reader passes the frames of
    /// the sender's side of the exchange, and the peer's UPDATED.
    frames: Sender<ExchangeFrame>,
    link: Link,
}

impl Receiving {
    /// The receiver's role for the host's index, whose block size and
    /// strong hash `config` gives, with its pins in `tmp_data`, which
    /// answer on `control`, the queue of this host's control writer, and
    /// `output`, the queue of that writer for the frames of this side of
    /// the list exchange.
    pub fn new(
        config: &Config,
        tmp_data: &Arc<Directory>,
        control: &Sender<ControlFrame>,
        output: Sender<ExchangeFrame>,
    ) -> Self {
        let pins = Pins::new(
            Arc::clone(tmp_data),
            config.strong_hash,
            config.block_size,
            control.clone(),
        );
        let (frames, input) = unbounded();
        Receiving {
            pins: Arc::new(pins),
            queries: unbounded(),
            frames,
            link: Link::new(input, output),
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

    /// Where the control reader passes the frames of the sender's side of
    /// the list exchange, and the peer's UPDATED.
    pub fn frames(&self) -> Sender<ExchangeFrame> {
        self.frames.clone()
    }

    /// Start the receiver's role on a thread of its own: prepare the index
    /// of `config` for the run, with the `--no-update`, `--scan-immutable`,
    /// and sender caching of `settings`, and with the other host when both
    /// streams are `deduplicated`, since it then updates an index of its
    /// own at the same time (DESIGN.md §4.4). Then start the
    /// lookups, which take over the index and answer the queries as soon as
    /// they arrive, and run the receiver's side of the list exchange
    /// (DESIGN.md §5.6), unless the stream has ended or stopped by then.
    /// The lookups keep the update after the command for the end of the
    /// role, with the `--no-update` and `--compact` of `settings`.
    pub fn start(
        self,
        config: Arc<Config>,
        settings: &RunSettings,
        deduplicated: Deduplicated,
    ) -> ReceivingThread {
        let preparation = Preparation::of(settings, deduplicated);
        let after_command = AfterCommand::of(&config, settings);
        let sender_cache = settings.sender_cache;
        let thread = std::thread::Builder::new()
            .name("receiver".into())
            .spawn(move || self.run(&config, preparation, sender_cache, after_command))
            .expect("cannot start the receiver's list exchange");
        ReceivingThread(thread)
    }

    /// The body of the role's thread (`start`). The receiver always
    /// prepares its index, so that an update that the run needs always
    /// happens, and an index that does not fit always ends the run. Its
    /// side of the exchange then ends at once with HASHNONE if the stream
    /// has ended or stopped meanwhile: the lists would have no use.
    fn run(
        self,
        config: &Config,
        preparation: Preparation,
        sender_cache: bool,
        after_command: Option<AfterCommand>,
    ) -> (Lookups, ListBytes) {
        let Receiving {
            pins,
            queries,
            frames,
            link,
        } = self;
        // The control reader keeps its own end of the link.
        drop(frames);
        let (index, locations) = prepare(config, preparation, &link);
        if pins.is_completing() {
            list_exchange::send_none(&link, config.hashlist_max_age);
            index.close();
            let lookups = Lookups {
                pins,
                workers: Vec::new(),
                after_command,
            };
            return (lookups, ListBytes::default());
        }
        let meta = index.read_meta().expect("the index lost its metadata");
        let exchange =
            ReceiverExchange::start(link, config.strong_hash, meta.index_id, index.reader());
        let workers = lookup::start(index, locations, &pins, config, queries.1);
        let lists = exchange.finish(sender_cache, config.hashlist_max_age, || {
            pins.is_completing()
        });
        let lookups = Lookups {
            pins,
            workers,
            after_command,
        };
        (lookups, lists)
    }
}

/// The thread of the receiver's role, for its join at the normal end.
pub struct ReceivingThread(JoinHandle<(Lookups, ListBytes)>);

impl ReceivingThread {
    /// Join the thread, once the list exchange has ended. Returns the
    /// lookups, and the bytes of the lists sent.
    pub fn join(self) -> (Lookups, ListBytes) {
        self.0
            .join()
            .expect("the receiver's list exchange panicked")
    }
}

/// Prepare the receiver's index for one run, as `preparation` says
/// (DESIGN.md §4.4), with the other host through `link` when both hosts
/// update their index. Returns the index, read-only with a shared lock,
/// with the current root locations, from which the lookups read candidate
/// blocks.
fn prepare(config: &Config, preparation: Preparation, link: &Link) -> (Index, Vec<Location>) {
    match preparation {
        Preparation::Open => {
            fsops::raise_descriptor_limit();
            let fs_cache = FsCache::new();
            let locations = roots::resolve(config, &fs_cache);
            (index::open_current(config, Access::ReadOnly), locations)
        }
        Preparation::UpdateAndKeep(options) => {
            let (index, locations) = update::perform(config, options);
            (index.into_read_only(&config.index_path), locations)
        }
        // L1. The update, then the lock goes: the other host updates its own
        // index at the same time, and can still be waiting for its exclusive
        // lock. A lock that this host kept could close a cycle of waits: two
        // such runs in opposite directions between the same hosts could
        // each hold one index while the other run's update waits for it.
        // L2. Once the other host's update is done, the index opens again,
        // read-only, which needs a shared lock alone. Another process can
        // have changed the index in between, and a rebuild with other
        // settings ends the run, as with `--no-update`.
        Preparation::UpdateAndReopen(options) => {
            let (index, locations) = update::perform(config, options);
            index.close();
            link.send(frame::UPDATED, Vec::new());
            link.expect_empty(frame::UPDATED, "UPDATED");
            (index::open_current(config, Access::ReadOnly), locations)
        }
    }
}

/// The receiver's role once its list exchange is done (DESIGN.md §6.4):
/// the pins, which the stream's data reader and decoder take, the lookup
/// workers, which own the index until Completing, and the update after the
/// command, for the end of the role.
pub struct Lookups {
    pins: Arc<Pins>,
    workers: Vec<JoinHandle<()>>,
    /// None with `--no-update`.
    after_command: Option<AfterCommand>,
}

impl Lookups {
    pub fn pins(&self) -> &Arc<Pins> {
        &self.pins
    }

    /// The normal end of the receiver's role (DESIGN.md §6.9): join the
    /// workers, which ended at Completing, and close the pins, which no
    /// other thread holds any more. The index closed with the last worker,
    /// so the update after the command can take its exclusive lock now:
    /// before, a shared lock of this process would keep it waiting forever
    /// (DESIGN.md §4.4).
    pub fn end(self) {
        for worker in self.workers {
            worker.join().expect("a lookup worker panicked");
        }
        Arc::into_inner(self.pins)
            .expect("the pins are still in use")
            .close();
        if let Some(after_command) = self.after_command {
            after_command.run();
        }
    }
}

/// The receiver's update after the command (DESIGN.md §3). It indexes the
/// files that the run has just written on this host, such as those that
/// `tar -x` or rsync made from the stream: they are still in the page cache
/// now, so hashing them costs little, and the next run can deduplicate
/// against them. A child process runs it, so that nothing that ends the
/// update ends the run as well, neither an error nor a panic: the run only
/// warns, and keeps the command's status. The child reads the
/// configuration file again, so an edit of the file during the run takes
/// effect.
struct AfterCommand {
    /// The configuration file, which the child selects as this process did.
    source: Source,
    /// End with a compaction, which then covers the changes of this update
    /// too, and which no list waits for.
    compact: bool,
}

impl AfterCommand {
    /// The update after the command of a run with `settings` for the index
    /// of `config`, which `--no-update` skips.
    fn of(config: &Config, settings: &RunSettings) -> Option<Self> {
        (!settings.no_update).then(|| AfterCommand {
            source: config.source.clone(),
            compact: settings.compact,
        })
    }

    /// Run the update in its child, `hashsync update-after-command`, and
    /// wait for it. The child is this executable, whatever its path was, and
    /// it reports as this process does, on stderr, which it shares: without
    /// notes after `-q`, without scale units after `--no-human-readable`,
    /// and as the server's on the server. A fatal end of this process kills
    /// it, as it kills the SSH sessions.
    fn run(&self) {
        // The streams have ended by now, and nothing else would show why the
        // run goes on: the update can take long, since it hashes everything
        // that the run wrote.
        report::note("updating index after the command");
        let mut child = std::process::Command::new("/proc/self/exe");
        child
            .arg0("hashsync")
            .arg("update-after-command")
            .args(self.source.arg())
            .args(self.compact.then_some("--compact"))
            .args(report::is_server().then_some("--server"))
            .args((!report::notes_enabled()).then_some("--quiet"))
            .args((!units::human_readable()).then_some("--no-human-readable"))
            .stdin(Stdio::null())
            .stdout(Stdio::null());
        let status = match die::spawn_guarded(die::KillGuard::Process, &mut child) {
            Ok(started) => sys::wait_exit_status(Pid::from_child(&started)),
            Err(e) => {
                report::warn(format!("cannot start the update after the command: {e}"));
                return;
            }
        };
        if status != 0 {
            report::warn(format!(
                "the update after the command exited with status {status}"
            ));
        }
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

    /// The table of a scanner whose list exchange has given it the whole
    /// table already.
    fn whole() -> Feed {
        Feed {
            updates: None,
            has_table: true,
            wait: false,
            stream: "stdout",
            waiting: false,
        }
    }

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
                    &mut whole(),
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
                    &mut whole(),
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

    /// A stream whose scanner waits for its table, with `--wait`, on a
    /// thread of its own, with an empty table at first.
    struct Waiting {
        /// The scanner, which returns whether it ended the stream.
        scanning: std::thread::JoinHandle<bool>,
        /// The thread ID of the scanner.
        tid: i32,
        /// The bytes of the stream, which the scanner takes as they come:
        /// a send returns once it has taken them.
        pieces: Sender<Piece>,
        table: Sender<TableUpdate>,
        stop: Stop,
        queries: Receiver<ControlFrame>,
        answers: Sender<Answer>,
        items: DataItems,
        _tempdir: tempfile::TempDir,
    }

    fn waiting() -> Waiting {
        let (stop, _spool, tempdir) = unraised();
        let (pieces, piece_rx) = bounded(0);
        let (answers, answer_rx) = crossbeam_channel::unbounded();
        let (query_tx, queries) = crossbeam_channel::unbounded();
        let (table, table_rx) = crossbeam_channel::unbounded();
        let (mut data, items) = data_queue(16);
        let (tid_tx, tid_rx) = bounded(1);
        let scanning = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                tid_tx
                    .send(rustix::thread::gettid().as_raw_nonzero())
                    .unwrap();
                let lookahead = Lookahead::reserve(1 << 20, BS).unwrap();
                let mut scanner = Scanner::new(
                    HashTable::empty(),
                    BS,
                    StrongHash::Blake3,
                    lookahead,
                    0,
                    None,
                );
                let mut table = Feed {
                    updates: Some(table_rx),
                    has_table: false,
                    wait: true,
                    stream: "stdout",
                    waiting: false,
                };
                let ended = transfer(
                    &mut scanner,
                    &piece_rx,
                    &answer_rx,
                    &mut table,
                    &mut data,
                    &query_tx,
                    &stop,
                );
                if ended {
                    data.end();
                }
                ended
            })
        };
        Waiting {
            scanning,
            tid: tid_rx.recv().unwrap().get(),
            pieces,
            table,
            stop,
            queries,
            answers,
            items,
            _tempdir: tempdir,
        }
    }

    /// With `--wait`, the scanner scans only once its table is whole: before
    /// that, it takes the bytes of the stream into its buffer, but sends no
    /// frame and no query. Then the block of the list is a reference
    /// (DESIGN.md §6.3).
    #[test]
    fn with_wait_the_scanner_starts_once_its_table_is_whole() {
        let block = noise(BS as usize, 7);
        let input = [noise(10, 8), block.clone(), noise(10, 9)].concat();
        let mut stream = waiting();
        stream.pieces.send(Piece::Bytes(input)).unwrap();
        drop(stream.pieces);
        // The scanner has the bytes. No deadline: a scanner that never waits
        // hangs the test.
        wait_until_parked(stream.tid);
        assert!(stream.items.items.is_empty(), "a frame before the table");
        assert!(stream.queries.is_empty(), "a query before the table");
        let mut whole = HashTable::for_values(1);
        whole.insert(WeakHasher::new(BS as usize).hash(&block));
        stream.table.send(TableUpdate::Table(whole)).unwrap();
        drop(stream.table);
        let Ok(ControlFrame::Query(query)) = stream.queries.recv() else {
            panic!("no query once the table is whole");
        };
        assert_eq!(query.offset, 10);
        let yes = Answer {
            offset: 10,
            found: true,
        };
        stream.answers.send(yes).unwrap();
        let mut frames = Vec::new();
        loop {
            match stream.items.next() {
                Item::Literal(bytes) => frames.push(("literal", bytes.len())),
                Item::Reference => frames.push(("reference", BS as usize)),
                Item::Flush => {}
                Item::End => break,
                Item::Run { .. } | Item::Stopped => panic!("an unexpected item"),
            }
        }
        assert_eq!(
            frames,
            [("literal", 10), ("reference", 64), ("literal", 10)]
        );
        assert!(stream.scanning.join().unwrap());
    }

    /// An exchange that ends with HASHNONE gives no table: the receiver's
    /// stream has stopped, and the scanner that waits for its table waits
    /// for that stop, without a frame (DESIGN.md §6.3).
    #[test]
    fn with_wait_a_scanner_without_a_table_waits_for_the_stop() {
        let mut stream = waiting();
        drop(stream.table);
        stream.pieces.send(Piece::Bytes(noise(100, 10))).unwrap();
        // The scanner has seen the end of the updates before it took the
        // bytes. No deadline: a scanner that never waits hangs the test.
        wait_until_parked(stream.tid);
        assert!(stream.items.items.is_empty() && stream.queries.is_empty());
        stream.stop.raise();
        assert!(
            !stream.scanning.join().unwrap(),
            "the scan ended the stream"
        );
        assert!(matches!(stream.items.next(), Item::Stopped));
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

//! The sender's scanner (DESIGN.md §6.3): the single owner of the decisions
//! about the frames of a deduplicated stream.
//!
//! The scanner rolls the weak hash over the bytes of the stream. A window
//! of equal bytes becomes part of a run. A window whose weak hash is in the
//! receiver's list becomes a query for the window's full strong hash; a
//! yes answer turns it into a reference, and everything else is literal.
//! The frames leave strictly in order: a query blocks the records behind it
//! until its answer arrives, and a no answer discards every record after
//! the rejected window, which is scanned again from its next byte.
//!
//! The scanner does no I/O and starts no threads. Its driver
//! (`dedup::transfer`, on the host that sends the stream) feeds it the
//! bytes of the stream and the answers, sends the queries that it makes,
//! and takes its data frames while the data queue has room. The driver can
//! lend it a pool of probe threads, which probe the parts of a long stretch
//! of windows at once.
//!
//! In eager delivery (DESIGN.md §6.2), the driver also passes the idle
//! points of the stream's source (`idle`). The bytes before the last idle
//! point leave without waiting for more bytes once they can leave at once:
//! the driver records their tail, which fills no window, only while the
//! data writer has nothing left to write (`tail_waits`, `send_tail`). On a
//! slow link, the tail waits instead, and bytes that arrive meanwhile can
//! fill its windows. A flush of the stream follows the frames of those
//! bytes.
//!
//! Offsets count the bytes of the stream from zero. `e` is the next byte to
//! emit, `p` the next byte to scan, and `r` the end of the bytes read, with
//! `e <= p <= r`:
//!
//! - The buffer holds the bytes `[e, r)`. It accepts more bytes of the
//!   stream while it holds less than `lookahead + B` bytes, and a scan step
//!   starts only while `p - e < lookahead`.
//! - The records cover `[e, p)` in order, without gaps or overlaps.
//! - `f`, the end of the bytes before the last idle point, is at most `r`.
//! - The query states hold every offset at or above `e` that was a weak
//!   hit, also when no record refers to it any more. Each offset is queried
//!   at most once.

use std::collections::{BTreeMap, TryReserveError, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use lru::LruCache;
use rayon::ThreadPool;

use crate::hash::weak::{WeakHasher, mix64};
use crate::hash::{Strong, StrongHash};
use crate::hashtable::{HashTable, Lookup};

/// The bytes that one call of `scan` examines at most, so that answers and
/// emission keep up with a long stretch of misses. With probe threads, this
/// is the budget after a weak hit.
const SCAN_BUDGET: u64 = 64 << 10;

/// With probe threads, each stretch without a hit doubles the budget, up to
/// this many bytes per thread.
const PART_BUDGET: u64 = 1 << 20;

/// The memory that one hash in the negative cache costs: the digest, the
/// links of the recency list, and the slot of the hash table.
const NEGATIVE_ENTRY_BYTES: u64 = 96;

/// A data frame, in output order, or a flush.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// LIT: at most `B` literal bytes.
    Literal(Vec<u8>),
    /// REF: the pin at the current output offset.
    Reference,
    /// RLE: `length` copies of `byte`.
    Run { byte: u8, length: u64 },
    /// Not a frame: the data writer flushes the stream once it has nothing
    /// left to write (DESIGN.md §6.2 and §6.3). The scanner asks for a flush
    /// once the frames of the bytes before an idle point are out, and after
    /// every `lookahead` bytes of output.
    Flush,
}

/// Scanned bytes of the stream that are not emitted yet (DESIGN.md §6.3).
#[derive(Clone, Copy, Debug)]
enum Record {
    Literal {
        offset: u64,
        length: u64,
    },
    /// Constant windows, and at EOF the tail bytes that continue them.
    Run {
        offset: u64,
        length: u64,
        byte: u8,
    },
    /// A window whose query waits for its answer.
    Query {
        offset: u64,
    },
    /// A window answered yes.
    Ref {
        offset: u64,
    },
}

impl Record {
    fn offset(&self) -> u64 {
        match *self {
            Record::Literal { offset, .. }
            | Record::Run { offset, .. }
            | Record::Query { offset }
            | Record::Ref { offset } => offset,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    NotSent,
    Waiting,
    Yes,
    No,
}

/// The query state of a weak hit.
struct Entry {
    strong: Strong,
    state: State,
}

/// What rolls from one window to the next in O(1) (DESIGN.md §6.3): the
/// window's weak-hash polynomial, and the length of the run of equal bytes
/// that ends with its last byte, which can begin before the window. The
/// window is constant when that run is at least as long as the window.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Rolling {
    poly: u64,
    equal: usize,
}

impl Rolling {
    /// The state of `window`, from its bytes; `None` when the window is
    /// constant, which needs no polynomial.
    fn of(weak: &WeakHasher, window: &[u8]) -> Option<Rolling> {
        let equal = trailing_equal(window);
        (equal < window.len()).then(|| Rolling {
            poly: weak.polynomial(window),
            equal,
        })
    }

    /// The state of the next window. `out` is the first byte of this
    /// window, `last` its last byte, and `new` the last byte of the next.
    #[inline(always)]
    fn roll(self, weak: &WeakHasher, out: u8, last: u8, new: u8) -> Rolling {
        Rolling {
            poly: weak.roll(self.poly, out, new),
            equal: if new == last { self.equal + 1 } else { 1 },
        }
    }
}

/// The rolling state of the window at `offset`, which is not constant:
/// a constant window becomes a run and needs none. A state describes its
/// own window, so jumps and rescans leave nothing to reset.
#[derive(Clone, Copy)]
struct Known {
    offset: u64,
    /// The window's first byte, which the next roll removes; the buffer
    /// may have released it since.
    first: u8,
    rolling: Rolling,
}

/// The lookahead of a scanner (DESIGN.md §6.3): the bytes that it scans
/// ahead of its output at most, with its buffer, reserved whole before the
/// stream starts. The buffer is twice as large as the bytes that it
/// accepts, `lookahead + B` (`Scanner::push`). This type exists so that
/// the reservation comes before the stream: the client sets the lookahead
/// to any size, and a host that cannot hold its buffer must end the run at
/// the receiver's HELLO, not abort in the middle of the stream.
pub struct Lookahead {
    bytes: u64,
    buffer: Vec<u8>,
}

impl Lookahead {
    /// Reserve the buffer of a lookahead of `bytes`, for windows of
    /// `block_size` bytes. The reservation fails when the allocator
    /// refuses it, or when its size does not fit the address space.
    pub fn reserve(bytes: u64, block_size: u32) -> Result<Lookahead, TryReserveError> {
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(buffer_size(bytes, block_size.into()))?;
        Ok(Lookahead { bytes, buffer })
    }
}

/// The bytes that the buffer of a scanner reserves: twice the bytes that it
/// accepts.
fn buffer_size(lookahead: u64, block_size: u64) -> usize {
    let accepted = lookahead.saturating_add(block_size);
    usize::try_from(accepted.saturating_mul(2)).unwrap_or(usize::MAX)
}

pub struct Scanner<'a> {
    /// `B`.
    block_size: u64,
    lookahead: u64,
    weak: WeakHasher,
    table: HashTable,
    strong: StrongHash,
    /// The bytes `[base, r)`, with `base <= e`.
    buffer: Vec<u8>,
    base: u64,
    e: u64,
    p: u64,
    /// `f`: the end of the bytes before the last idle point of the stream's
    /// source. The bytes before it leave without waiting for more bytes
    /// (DESIGN.md §6.3).
    flush: u64,
    /// The output offset `e` at the last `Frame::Flush`. The frames of the
    /// bytes before it need no other flush.
    flushed: u64,
    /// The stream's source reached EOF.
    ended: bool,
    /// The rolling state of a recent window: the window at `p` uses it,
    /// or rolls it on from the window before.
    known: Option<Known>,
    records: VecDeque<Record>,
    /// The offsets of the Query records, in record order.
    queries: VecDeque<u64>,
    /// The query states by offset.
    entries: BTreeMap<u64, Entry>,
    /// Full strong hashes answered no; `None` when the configured size
    /// holds no hash.
    negative: Option<LruCache<Strong, ()>>,
    /// The threads that probe the parts of a long stretch; `None` probes
    /// on the scanner's thread.
    pool: Option<&'a ThreadPool>,
    /// The bytes that a call of `scan` examines at most. Each weak hit sets
    /// it to `min_budget` and limits the rest of that call to it; each
    /// stretch that reaches its limit without a hit or a constant window
    /// doubles it, up to `max_budget`.
    budget: u64,
    min_budget: u64,
    max_budget: u64,
    /// The windows of one part of a long stretch. Each part computes the
    /// rolling state of its first window from its bytes.
    part: usize,
}

impl<'a> Scanner<'a> {
    /// A scanner for windows of `block_size` bytes, which probes `table`,
    /// scans at most `lookahead` bytes ahead of its output, in the buffer
    /// that the lookahead reserved, and remembers up to `negative_cache`
    /// bytes of rejected hashes. `pool` probes long stretches in parts.
    pub fn new(
        table: HashTable,
        block_size: u32,
        strong: StrongHash,
        lookahead: Lookahead,
        negative_cache: u64,
        pool: Option<&'a ThreadPool>,
    ) -> Self {
        let Lookahead {
            bytes: lookahead,
            buffer,
        } = lookahead;
        assert!(block_size > 0 && lookahead >= u64::from(block_size));
        let negative_hashes =
            usize::try_from(negative_cache / NEGATIVE_ENTRY_BYTES).unwrap_or(usize::MAX);
        let part = SCAN_BUDGET.max(u64::from(block_size));
        let max_budget = match pool {
            Some(pool) => (pool.current_num_threads() as u64).saturating_mul(PART_BUDGET.max(part)),
            None => SCAN_BUDGET,
        };
        Scanner {
            block_size: u64::from(block_size),
            lookahead,
            weak: WeakHasher::new(block_size as usize),
            table,
            strong,
            buffer,
            base: 0,
            e: 0,
            p: 0,
            flush: 0,
            flushed: 0,
            ended: false,
            known: None,
            records: VecDeque::new(),
            queries: VecDeque::new(),
            entries: BTreeMap::new(),
            negative: NonZeroUsize::new(negative_hashes).map(LruCache::sparse),
            pool,
            budget: SCAN_BUDGET,
            min_budget: SCAN_BUDGET,
            max_budget,
            part: part as usize,
        }
    }

    /// `r`.
    fn end(&self) -> u64 {
        self.base + self.buffer.len() as u64
    }

    fn bytes(&self, offset: u64, length: u64) -> &[u8] {
        let start = (offset - self.base) as usize;
        &self.buffer[start..start + length as usize]
    }

    fn byte(&self, offset: u64) -> u8 {
        self.buffer[(offset - self.base) as usize]
    }

    /// True while the buffer accepts more bytes of the stream.
    pub fn has_room(&self) -> bool {
        self.end() - self.e < self.lookahead.saturating_add(self.block_size)
    }

    /// Append bytes of the stream.
    pub fn push(&mut self, bytes: &[u8]) {
        assert!(!self.ended, "bytes of the stream after its end");
        // A buffer that grows copies itself to a larger allocation, and for
        // a moment both copies use memory. So the lookahead reserved the
        // whole buffer, twice as large as the bytes that it accepts,
        // `lookahead + B`, and the emitted bytes stay until a push finds the
        // buffer full. Then they fill about half of it, so moving the bytes
        // after them costs about one copy per byte. A smaller buffer would
        // move them more often: at 1.5 times the accepted bytes, about two
        // copies per byte. While pushes come only when the buffer has room,
        // only a push of more than the accepted bytes can make the buffer
        // grow.
        if bytes.len() > self.buffer.capacity() - self.buffer.len() {
            self.buffer.drain(..(self.e - self.base) as usize);
            self.base = self.e;
            let size =
                buffer_size(self.lookahead, self.block_size).max(self.buffer.len() + bytes.len());
            self.buffer.reserve_exact(size - self.buffer.len());
        }
        self.buffer.extend_from_slice(bytes);
    }

    /// The stream's source reached EOF.
    pub fn end_input(&mut self) {
        self.ended = true;
    }

    /// Probe `table` from the next scan on: the hash table of the
    /// receiver's list, whole, or for a full list that is still arriving,
    /// sized for its values, which `insert` adds as they arrive (DESIGN.md
    /// §6.3). A scanner starts with the empty table, so that a stream can
    /// start before its list arrives. The windows that it scanned before
    /// keep what it decided for them.
    pub fn set_table(&mut self, table: HashTable) {
        self.table = table;
    }

    /// Add `values` of the receiver's list to the table, which `set_table`
    /// sized for them.
    pub fn insert(&mut self, values: &[u64]) {
        for &value in values {
            self.table.insert(value);
        }
    }

    /// An idle point of the stream's source, in eager delivery: it has no
    /// byte ready, and the scanner has every byte that it gave. These bytes
    /// leave without waiting for more bytes once the data writer is idle
    /// (`tail_waits`), and `next_frame` gives a flush once their frames are
    /// out (DESIGN.md §6.3).
    pub fn idle(&mut self) {
        self.flush = self.end();
    }

    /// True when the tail before the last idle point waits only for the
    /// data writer (DESIGN.md §6.3): fewer than `B` bytes follow `p < f`,
    /// and every record before `p` has left, but a last Literal shorter
    /// than `B`, which the tail extends. The driver calls `send_tail` once
    /// the data writer is idle. This function exists so that the tail
    /// leaves only when it can leave at once: while the writer is busy, on a
    /// slow link, the tail waits, and bytes that arrive meanwhile can fill
    /// its windows, which then need not become literal.
    pub fn tail_waits(&self) -> bool {
        let held = match (self.records.len(), self.records.front()) {
            (0, _) => true,
            (1, Some(&Record::Literal { length, .. })) => length < self.block_size,
            _ => false,
        };
        held && !self.ended && self.p < self.flush && self.end() - self.p < self.block_size
    }

    /// Record the tail that waits (`tail_waits`), so that the bytes before
    /// the last idle point leave: the data writer is idle.
    pub fn send_tail(&mut self) {
        assert!(self.tail_waits(), "no tail waits for the data writer");
        self.scan_tail(self.flush);
    }

    /// Take in an answer (DESIGN.md §6.3). Records change only when
    /// `scan` resolves the queries.
    pub fn answer(&mut self, offset: u64, found: bool) {
        // Output passed the offset. The receiver can answer no for such an
        // offset without a lookup, so the answer changes nothing, not even
        // the negative cache.
        if offset < self.e {
            return;
        }
        let entry = match self.entries.get_mut(&offset) {
            Some(entry) if entry.state == State::Waiting => entry,
            _ => panic!("protocol error: unexpected answer at offset {offset}"),
        };
        entry.state = if found { State::Yes } else { State::No };
        if let Some(negative) = &mut self.negative {
            if found {
                negative.pop(&entry.strong);
            } else {
                negative.put(entry.strong, ());
            }
        }
    }

    /// Resolve the answered queries, then scan up to `budget` bytes.
    /// `query` receives the offset and full strong hash of each new query.
    /// True when anything changed.
    pub fn scan(&mut self, query: &mut impl FnMut(u64, Strong)) -> bool {
        let mut progress = self.resolve();
        let mut budget_end = self.p.saturating_add(self.budget);
        while self.p - self.e < self.lookahead && self.p < budget_end {
            let available = self.end() - self.p;
            if available < self.block_size {
                // At EOF, the bytes that fill no window are a tail. Before
                // EOF, they wait for more bytes, or, before an idle point,
                // for `send_tail` (DESIGN.md §6.3).
                if self.ended && available > 0 {
                    self.scan_tail(self.end());
                    progress = true;
                }
                break;
            }
            progress = true;
            let next_entry = self
                .entries
                .range(self.p..)
                .next()
                .map(|(&offset, _)| offset);
            // Only windows that are not constant get query states, because
            // the search for weak hits stops at constant windows. So a
            // window with a state needs no check for a run (DESIGN.md §6.3,
            // step 1).
            if next_entry != Some(self.p) {
                // Positions before `limit` have a whole window and are
                // neither beyond the lookahead nor a known entry.
                let limit = (self.end() - self.block_size + 1)
                    .min(self.e.saturating_add(self.lookahead))
                    .min(budget_end)
                    .min(next_entry.unwrap_or(u64::MAX));
                match self.skip_misses(limit) {
                    // Lookups compare only the low 32 bits of each value, so
                    // about 3 windows in each GiB of new bytes hit the table
                    // without being in the list. The check of the whole weak
                    // hash makes such a window a miss, which costs no strong
                    // hash, no query, and no rescan after the answer
                    // (DESIGN.md §5.5). The budget stays, as at a constant
                    // window (DESIGN.md §6.3). The probes do not check, so
                    // that they stay fast.
                    Stop::Listed(_, rolling) if !self.table.contains_value(mix64(rolling.poly)) => {
                        self.literal_byte();
                        continue;
                    }
                    Stop::Listed(..) => {}
                    // A stretch that a constant window cut short did not
                    // use up its budget, so it is no sign of a long run of
                    // misses. A run leaves no query to wait for either, so
                    // the budget stays.
                    Stop::Constant(_) => {
                        self.run_window();
                        continue;
                    }
                    Stop::Neither(_) => {
                        self.budget = self.budget.saturating_mul(2).min(self.max_budget);
                        continue;
                    }
                }
                let strong = self.strong.digest(self.bytes(self.p, self.block_size));
                let state = State::NotSent;
                self.entries.insert(self.p, Entry { strong, state });
            }
            // A weak hit, new or known, limits the rest of this call too:
            // closely spaced hits stay on this thread, and answers wait
            // little.
            self.budget = self.min_budget;
            budget_end = budget_end.min(self.p.saturating_add(self.budget));
            self.apply_entry(query);
        }
        progress
    }

    /// The rolling state of the window at `p`, which must be whole: rolled
    /// from the state of the window before, or computed from the window's
    /// bytes. `None` when the window is constant.
    fn rolling(&mut self) -> Option<Rolling> {
        let p = self.p;
        let rolling = match self.known {
            Some(known) if known.offset == p => known.rolling,
            // A window of one byte is constant, so the window before has
            // at least two, and its last byte is at `p` or later.
            Some(known) if known.offset + 1 == p => {
                let last = self.byte(p + self.block_size - 2);
                let new = self.byte(p + self.block_size - 1);
                known.rolling.roll(&self.weak, known.first, last, new)
            }
            _ => Rolling::of(&self.weak, self.bytes(p, self.block_size))?,
        };
        if rolling.equal as u64 >= self.block_size {
            return None;
        }
        self.known = Some(Known {
            offset: p,
            first: self.byte(p),
            rolling,
        });
        Some(rolling)
    }

    /// Record literal bytes from `p` while their windows are neither
    /// constant nor hits of the table. Stops with `p` at the first window
    /// that is constant or a hit, or at `limit`, which must be above `p`,
    /// and returns the stop, whose window counts from the old `p`. A hit is
    /// not checked against the whole weak hash yet: `scan` does that. The
    /// window at `p` must be whole. The probe threads search a long stretch
    /// in parts.
    fn skip_misses(&mut self, limit: u64) -> Stop {
        let Some(first) = self.rolling() else {
            return Stop::Constant(0);
        };
        let start = self.p;
        let window = self.block_size as usize;
        let at = (start - self.base) as usize;
        let n = (limit - start) as usize;
        let bytes = &self.buffer[at..at + n - 1 + window];
        let stop = match self.pool {
            Some(pool) if n >= 2 * self.part => {
                first_stop_in_parts(pool, self.part, &self.table, &self.weak, bytes, first)
            }
            _ => first_stop(&self.table, &self.weak, bytes, first, || false),
        };
        let skipped = match stop {
            Stop::Listed(k, rolling) => {
                self.known = Some(Known {
                    offset: start + k as u64,
                    first: bytes[k],
                    rolling,
                });
                k
            }
            Stop::Constant(k) => k,
            Stop::Neither(rolling) => {
                self.known = Some(Known {
                    offset: limit - 1,
                    first: bytes[n - 1],
                    rolling,
                });
                n
            }
        };
        if skipped > 0 {
            self.push_literal(start, skipped as u64);
            self.p += skipped as u64;
        }
        stop
    }

    /// Apply the query state at `p` (DESIGN.md §6.3, step 3).
    fn apply_entry(&mut self, query: &mut impl FnMut(u64, Strong)) {
        let p = self.p;
        let entry = &self.entries[&p];
        let (state, strong) = (entry.state, entry.strong);
        match state {
            State::Yes => self.skip_window(Record::Ref { offset: p }),
            State::Waiting => self.skip_window(Record::Query { offset: p }),
            State::No => self.literal_byte(),
            State::NotSent => {
                // A hash answered no elsewhere suppresses the query; the
                // entry stays for a later visit.
                if let Some(negative) = &mut self.negative
                    && negative.get(&strong).is_some()
                {
                    return self.literal_byte();
                }
                self.entries.get_mut(&p).unwrap().state = State::Waiting;
                query(p, strong);
                self.skip_window(Record::Query { offset: p });
            }
        }
    }

    /// Append a Query or Ref for the window at `p`, and move past it.
    fn skip_window(&mut self, record: Record) {
        if let Record::Query { offset } = record {
            self.queries.push_back(offset);
        }
        self.records.push_back(record);
        self.p += self.block_size;
    }

    /// Append the constant window at `p` to a run, and move past it
    /// (DESIGN.md §6.3, step 1).
    fn run_window(&mut self) {
        let byte = self.byte(self.p);
        self.push_run(self.p, self.block_size, byte);
        self.p += self.block_size;
    }

    /// Record the byte at `p` as literal, and move on to the next window.
    /// The rolling state moves to this window first, so that the next
    /// window rolls on from it instead of hashing all its bytes
    /// (DESIGN.md §6.3): a stretch that stops at the entry here leaves the
    /// state of the window before.
    fn literal_byte(&mut self) {
        self.rolling();
        self.push_literal(self.p, 1);
        self.p += 1;
    }

    /// Record the tail from `p` to `end`, which fills no window, at EOF or
    /// before an idle point: it continues a last Run while its bytes match,
    /// and the rest is literal. A Run already emitted is not continued
    /// (DESIGN.md §6.3). Before an idle point, every Run has left.
    fn scan_tail(&mut self, end: u64) {
        let tail = self.bytes(self.p, end - self.p);
        let (matching, byte) = match self.records.back() {
            Some(&Record::Run { byte, .. }) => {
                (tail.iter().take_while(|&&b| b == byte).count(), byte)
            }
            _ => (0, 0),
        };
        let rest = tail.len() - matching;
        if matching > 0 {
            self.push_run(self.p, matching as u64, byte);
        }
        if rest > 0 {
            self.push_literal(self.p + matching as u64, rest as u64);
        }
        self.p = end;
    }

    /// Append literal bytes at `offset`, where the records end, extending
    /// a last Literal.
    fn push_literal(&mut self, offset: u64, length: u64) {
        if let Some(Record::Literal {
            length: last_length,
            ..
        }) = self.records.back_mut()
        {
            *last_length += length;
            return;
        }
        self.records.push_back(Record::Literal { offset, length });
    }

    /// Append `length` copies of `byte` at `offset`, where the records end,
    /// extending a last Run of that byte.
    fn push_run(&mut self, offset: u64, length: u64, byte: u8) {
        if let Some(Record::Run {
            length: last_length,
            byte: last_byte,
            ..
        }) = self.records.back_mut()
            && *last_byte == byte
        {
            *last_length += length;
            return;
        }
        self.records.push_back(Record::Run {
            offset,
            length,
            byte,
        });
    }

    /// Resolve the Query records in order (DESIGN.md §6.3): a yes turns
    /// the record into a Ref; a no discards the record and every later
    /// one, records the rejected byte as literal, and scans again from the
    /// byte after it. A waiting query stops resolution.
    fn resolve(&mut self) -> bool {
        let mut progress = false;
        while let Some(&offset) = self.queries.front() {
            match self.entries[&offset].state {
                State::Waiting => break,
                State::Yes => {
                    self.queries.pop_front();
                    let i = self.record_index(offset);
                    self.records[i] = Record::Ref { offset };
                }
                State::No => {
                    self.queries.clear();
                    let i = self.record_index(offset);
                    self.records.truncate(i);
                    self.push_literal(offset, 1);
                    self.p = offset + 1;
                }
                State::NotSent => unreachable!("a Query record without a request"),
            }
            progress = true;
        }
        progress
    }

    fn record_index(&self, offset: u64) -> usize {
        self.records
            .binary_search_by_key(&offset, Record::offset)
            .expect("no record at a query's offset")
    }

    /// The next data frame, if the first record is ready (DESIGN.md §6.3):
    /// a Ref, a Run for its known length, or up to `B` literal bytes. A
    /// last Literal shorter than `B` waits to grow until the stream is
    /// complete, except for its bytes before `f` once `p` has reached `f`.
    /// A Query blocks everything behind it. A flush comes first once the
    /// frames before an `f` past the last flush are out, and once
    /// `lookahead` bytes have left since the last flush.
    pub fn next_frame(&mut self) -> Option<Frame> {
        // The second flush exists for sources that never pause, such as a
        // file, and for batch delivery, where no idle point flushes the
        // stream. References and runs stand for far more bytes than their
        // frames, so without it, the data writer's buffers could hold them
        // until the end of the stream, and the receiver would get nothing
        // before then (DESIGN.md §6.3).
        let idle_point = self.flush > self.flushed && self.e >= self.flush;
        if idle_point || self.e - self.flushed >= self.lookahead {
            self.flushed = self.e;
            return Some(Frame::Flush);
        }
        let (frame, length) = match *self.records.front()? {
            Record::Query { .. } => return None,
            Record::Ref { .. } => {
                self.records.pop_front();
                (Frame::Reference, self.block_size)
            }
            Record::Run { length, byte, .. } => {
                self.records.pop_front();
                (Frame::Run { byte, length }, length)
            }
            Record::Literal { offset, length } => {
                let last = self.records.len() == 1;
                let complete = self.ended && self.p == self.end();
                let n = if length < self.block_size && last && !complete {
                    // Only the bytes before `f` leave, together with the
                    // tail that `send_tail` adds to them; the rest waits to
                    // grow.
                    if self.p < self.flush || offset >= self.flush {
                        return None;
                    }
                    length.min(self.flush - offset)
                } else {
                    length.min(self.block_size)
                };
                let bytes = self.bytes(offset, n).to_vec();
                if n == length {
                    self.records.pop_front();
                } else {
                    self.records[0] = Record::Literal {
                        offset: offset + n,
                        length: length - n,
                    };
                }
                (Frame::Literal(bytes), n)
            }
        };
        self.e += length;
        while let Some(entry) = self.entries.first_entry()
            && *entry.key() < self.e
        {
            entry.remove();
        }
        Some(frame)
    }

    /// All of the stream is emitted: its source reached EOF, and `e = p =
    /// r`. A flush that has not gone out has no use then: the end of the
    /// stream sends every byte.
    pub fn finished(&self) -> bool {
        self.ended && self.e == self.end()
    }
}

/// The length of the run of equal bytes at the end of `window`, which must
/// not be empty. Whole chunks are compared first, so that a long run takes
/// few steps.
fn trailing_equal(window: &[u8]) -> usize {
    const CHUNK: usize = 32;
    let last = window[window.len() - 1];
    let mut chunks = window.rchunks_exact(CHUNK);
    let mut equal = 0;
    for chunk in &mut chunks {
        // Compare every byte of the chunk, without a branch per byte.
        if chunk.iter().fold(false, |differ, &b| differ | (b != last)) {
            return equal + chunk.iter().rev().take_while(|&&b| b == last).count();
        }
        equal += CHUNK;
    }
    equal
        + chunks
            .remainder()
            .iter()
            .rev()
            .take_while(|&&b| b == last)
            .count()
}

/// Where `first_stop` ended in its windows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stop {
    /// Window `k` is constant.
    Constant(usize),
    /// Window `k` may be in the table; its rolling state.
    Listed(usize, Rolling),
    /// No window was either: the rolling state of the last one.
    Neither(Rolling),
}

impl Stop {
    /// The window of a Constant or Listed stop.
    fn window(self) -> Option<usize> {
        match self {
            Stop::Constant(k) | Stop::Listed(k, _) => Some(k),
            Stop::Neither(_) => None,
        }
    }

    /// This stop, with its window counted from `start` windows earlier:
    /// the stop of a part, as a stop of the whole stretch.
    fn after(self, start: usize) -> Stop {
        match self {
            Stop::Constant(k) => Stop::Constant(start + k),
            Stop::Listed(k, rolling) => Stop::Listed(start + k, rolling),
            neither => neither,
        }
    }
}

/// The first window of `bytes` that is constant or whose weak hash may be
/// in `table`; `first` is the rolling state of the first window, which must
/// not be constant. `stop`, checked every 4096 windows, can end the search
/// early, and the result is then meaningless.
///
/// Probes of different windows are independent, so the loop rolls on to the
/// next `AHEAD` windows before it probes the first of them, and their
/// buckets load meanwhile: a table larger than the caches costs one memory
/// latency per `AHEAD` probes instead of one per probe. A constant window
/// is found when its turn comes, before its probe, so rolling can go past
/// it by up to `AHEAD` windows.
fn first_stop(
    table: &HashTable,
    weak: &WeakHasher,
    bytes: &[u8],
    first: Rolling,
    stop: impl Fn() -> bool,
) -> Stop {
    const AHEAD: usize = 16;
    let window = weak.block_size();
    let n = bytes.len() + 1 - window;
    // The state of window `j + 1` from that of window `j`.
    let roll = |rolling: Rolling, j: usize| {
        rolling.roll(weak, bytes[j], bytes[j + window - 1], bytes[j + window])
    };
    // The stop at window `k`, from its rolling state and its lookup.
    let check = |k: usize, (rolling, lookup): (Rolling, Lookup)| {
        if rolling.equal >= window {
            Some(Stop::Constant(k))
        } else if table.finish_lookup(lookup) {
            Some(Stop::Listed(k, rolling))
        } else {
            None
        }
    };
    // The ring entry of a window: its rolling state, and the lookup of its
    // weak hash, whose bucket starts to load now.
    let entry = |rolling: Rolling| (rolling, table.start_lookup(mix64(rolling.poly)));
    // `ring[j % AHEAD]` holds the entry of window `j`, for the next `AHEAD`
    // windows.
    let mut ring = [(first, Lookup::default()); AHEAD];
    // The state of the window after the last one in the ring.
    let mut rolling = first;
    for (j, slot) in ring[..n.min(AHEAD)].iter_mut().enumerate() {
        *slot = entry(rolling);
        if j + 1 < n {
            rolling = roll(rolling, j);
        }
    }
    // Check window `k`, and replace it in the ring with window
    // `k + AHEAD`, whose state `rolling` holds.
    let mut k = 0;
    while k + AHEAD < n {
        let current = ring[k % AHEAD];
        ring[k % AHEAD] = entry(rolling);
        if k + AHEAD + 1 < n {
            rolling = roll(rolling, k + AHEAD);
        }
        if let Some(found) = check(k, current) {
            return found;
        }
        k += 1;
        if k % 4096 == 0 && stop() {
            return Stop::Neither(first);
        }
    }
    // The ring holds the remaining windows.
    (k..n)
        .find_map(|k| check(k, ring[k % AHEAD]))
        .unwrap_or(Stop::Neither(ring[(n - 1) % AHEAD].0))
}

/// `first_stop` over consecutive parts of `part` windows of `bytes`, on
/// the threads of `pool`. The threads take the parts in window order, so
/// all of them search the windows before the first stop, wherever it is.
/// The first stop in window order wins; a part after a found stop is
/// skipped, or ends early. Every part but the first computes the rolling
/// state of its first window from its bytes.
fn first_stop_in_parts(
    pool: &ThreadPool,
    part: usize,
    table: &HashTable,
    weak: &WeakHasher,
    bytes: &[u8],
    first: Rolling,
) -> Stop {
    let window = weak.block_size();
    let n = bytes.len() + 1 - window;
    let parts = n.div_ceil(part);
    // The next part to search, and the earliest window with a stop yet.
    let next = AtomicUsize::new(0);
    let earliest = AtomicUsize::new(usize::MAX);
    let found: Vec<Option<Stop>> = pool.broadcast(|_| {
        loop {
            let i = next.fetch_add(1, Relaxed);
            let start = i * part;
            if i >= parts || earliest.load(Relaxed) < start {
                return None;
            }
            let end = (start + part).min(n);
            let bytes = &bytes[start..end - 1 + window];
            let first = match i {
                0 => Some(first),
                _ => Rolling::of(weak, &bytes[..window]),
            };
            let stop = match first {
                None => Stop::Constant(0),
                Some(first) => {
                    first_stop(table, weak, bytes, first, || earliest.load(Relaxed) < start)
                }
            };
            if let Some(k) = stop.window() {
                earliest.fetch_min(start + k, Relaxed);
                // The parts that this thread could take next begin after
                // this stop, so they need no search.
                return Some(stop.after(start));
            }
            // Without stops, the last part ends the stretch.
            if end == n {
                return Some(stop);
            }
        }
    });
    // A part ends early only after a stop, so without stops the last one
    // ended with the last window.
    found
        .into_iter()
        .flatten()
        .min_by_key(|stop| stop.window().unwrap_or(usize::MAX))
        .expect("the parts ended without the last one")
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{HashMap, HashSet};

    use proptest::prelude::*;

    use super::*;

    const BS: u32 = 32;

    /// Deterministic pseudo-random bytes.
    pub(crate) fn noise(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..len)
            .map(|_| {
                state ^= state >> 12;
                state ^= state << 25;
                state ^= state >> 27;
                (state.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 56) as u8
            })
            .collect()
    }

    /// A receiver that holds `blocks`. `stale` blocks are in the list but
    /// fail verification, like a changed file or a hash collision.
    struct Receiver {
        blocks: HashMap<Strong, Vec<u8>>,
        stale: HashSet<Strong>,
        /// Queries not answered yet, in arrival order.
        pending: Vec<(u64, Strong)>,
        /// Every query offset, to check that none repeats.
        queried: HashSet<u64>,
        queries: Vec<(u64, Strong)>,
        pins: HashMap<u64, Vec<u8>>,
        output: Vec<u8>,
        /// The output offset of each flush.
        flushes: Vec<u64>,
    }

    impl Receiver {
        fn new(blocks: &[Vec<u8>], stale: &[Vec<u8>]) -> Self {
            let digest = |b: &Vec<u8>| StrongHash::Blake3.digest(b);
            Receiver {
                blocks: blocks
                    .iter()
                    .chain(stale)
                    .map(|b| (digest(b), b.clone()))
                    .collect(),
                stale: stale.iter().map(digest).collect(),
                pending: Vec::new(),
                queried: HashSet::new(),
                queries: Vec::new(),
                pins: HashMap::new(),
                output: Vec::new(),
                flushes: Vec::new(),
            }
        }

        /// The list the sender gets: the weak hashes of every block, sorted
        /// and distinct.
        fn values(&self) -> Vec<u64> {
            let weak = WeakHasher::new(BS as usize);
            let mut values: Vec<u64> = self.blocks.values().map(|b| weak.hash(b)).collect();
            values.sort_unstable();
            values.dedup();
            values
        }

        /// The table of the list.
        fn table(&self) -> HashTable {
            HashTable::build(&self.values())
        }

        fn query(&mut self, offset: u64, strong: Strong) {
            assert!(self.queried.insert(offset), "offset {offset} queried twice");
            self.pending.push((offset, strong));
            self.queries.push((offset, strong));
        }

        /// Answer the pending query at `index` like DESIGN.md §6.4.
        fn answer(&mut self, index: usize, scanner: &mut Scanner) {
            let (offset, strong) = self.pending.remove(index);
            let c = self.output.len() as u64;
            let found =
                offset >= c && !self.stale.contains(&strong) && self.blocks.contains_key(&strong);
            if found {
                self.pins.insert(offset, self.blocks[&strong].clone());
            }
            scanner.answer(offset, found);
        }

        fn take(&mut self, frame: Frame) {
            let c = self.output.len() as u64;
            match frame {
                Frame::Literal(bytes) => {
                    assert!(!bytes.is_empty() && bytes.len() <= BS as usize);
                    self.output.extend_from_slice(&bytes);
                }
                Frame::Reference => {
                    let pin = self.pins.get(&c).unwrap_or_else(|| panic!("no pin at {c}"));
                    self.output.extend_from_slice(pin);
                }
                Frame::Run { byte, length } => {
                    assert!(length > 0);
                    self.output
                        .extend(std::iter::repeat_n(byte, length as usize));
                }
                Frame::Flush => self.flushes.push(c),
            }
            let c = self.output.len() as u64;
            self.pins.retain(|&offset, _| offset >= c);
        }
    }

    /// Three probe threads, shared by the tests.
    fn probe_pool() -> &'static ThreadPool {
        static POOL: std::sync::OnceLock<ThreadPool> = std::sync::OnceLock::new();
        POOL.get_or_init(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap()
        })
    }

    /// A transfer driven by `choices`, which decide chunk sizes, answer
    /// order and timing, and data-queue room; they are reused cyclically.
    struct Run<'a> {
        scanner: Scanner<'a>,
        receiver: Receiver,
        input: &'a [u8],
        pushed: usize,
        /// `transfer` makes idle points now and then, as in eager delivery.
        idle_points: bool,
        /// The values of the list that the table has yet to take, last
        /// first, when the table grows while the stream goes on, as when the
        /// list arrives after the stream has started (DESIGN.md §6.3).
        arriving: Vec<u64>,
        /// The table is sized for the list.
        sized: bool,
    }

    impl<'a> Run<'a> {
        /// `part` lends the scanner the probe pool, with parts of that many
        /// windows.
        fn new(
            input: &'a [u8],
            receiver: Receiver,
            lookahead: u64,
            negative_cache: u64,
            part: Option<usize>,
        ) -> Self {
            let mut scanner = Scanner::new(
                receiver.table(),
                BS,
                StrongHash::Blake3,
                Lookahead::reserve(lookahead, BS).unwrap(),
                negative_cache,
                part.map(|_| probe_pool()),
            );
            if let Some(part) = part {
                scanner.part = part;
            }
            Run {
                scanner,
                receiver,
                input,
                pushed: 0,
                idle_points: false,
                arriving: Vec::new(),
                sized: true,
            }
        }

        /// Start the scanner with the empty table, which `grow` then sizes
        /// and fills while the stream goes on.
        fn growing(mut self) -> Self {
            self.scanner.set_table(HashTable::empty());
            self.arriving = self.receiver.values();
            self.arriving.reverse();
            self.sized = false;
            self
        }

        /// The next step of a growing table: its size, then up to `n` more
        /// values, in the order of the list. False when the table is whole.
        fn grow(&mut self, n: usize) -> bool {
            if !self.sized {
                let size = self.arriving.len();
                self.scanner.set_table(HashTable::for_values(size));
                self.sized = true;
                return true;
            }
            let start = self.arriving.len().saturating_sub(n);
            let values: Vec<u64> = self.arriving.drain(start..).rev().collect();
            self.scanner.insert(&values);
            !values.is_empty()
        }

        /// Push up to `n` more input bytes, or end the input. False when
        /// neither is possible.
        fn push(&mut self, n: usize) -> bool {
            if self.pushed < self.input.len() && self.scanner.has_room() {
                let n = n.clamp(1, self.input.len() - self.pushed);
                self.scanner.push(&self.input[self.pushed..self.pushed + n]);
                self.pushed += n;
                true
            } else if self.pushed == self.input.len() && !self.scanner.ended {
                self.scanner.end_input();
                true
            } else {
                false
            }
        }

        fn scan(&mut self) -> bool {
            let receiver = &mut self.receiver;
            self.scanner
                .scan(&mut |offset, strong| receiver.query(offset, strong))
        }

        fn take_frame(&mut self) -> bool {
            match self.scanner.next_frame() {
                Some(frame) => {
                    self.receiver.take(frame);
                    true
                }
                None => false,
            }
        }

        fn transfer(&mut self, choices: &[u8]) {
            let mut step = 0usize;
            let mut choose = |modulo: usize| {
                step += 1;
                usize::from(choices[step % choices.len()]) % modulo
            };
            while !self.scanner.finished() {
                let mut progress = false;
                if choose(3) > 0 {
                    progress |= self.push(1 + choose(97));
                }
                if self.idle_points && !self.scanner.ended {
                    match choose(10) {
                        0 => self.idle_and_drain(),
                        1 => self.scanner.idle(),
                        _ => {}
                    }
                    // Now and then, the data writer is idle.
                    if self.scanner.tail_waits() && choose(3) == 0 {
                        self.scanner.send_tail();
                        progress = true;
                    }
                }
                for _ in 0..choose(4) {
                    if self.receiver.pending.is_empty() {
                        break;
                    }
                    let index = choose(self.receiver.pending.len());
                    self.receiver.answer(index, &mut self.scanner);
                    progress = true;
                }
                if choose(4) == 0 {
                    progress |= self.grow(1 + choose(3));
                }
                progress |= self.scan();
                // A data queue with room for 0 to 3 frames.
                for _ in 0..choose(4) {
                    progress |= self.take_frame();
                }
                if progress {
                    continue;
                }
                // Nothing moved: let the slowest event happen. A scanner
                // that needs none of them has stalled.
                let moved = (!self.receiver.pending.is_empty() && {
                    self.receiver.answer(0, &mut self.scanner);
                    true
                }) || self.push(1)
                    || self.take_frame()
                    || self.grow(1);
                assert!(moved, "the scanner stalled");
            }
        }

        /// An idle point, and then no more input, while the answers come,
        /// the frames leave, and the data writer is idle, until nothing
        /// moves. Every byte pushed so far must then be out, and a flush
        /// must follow it (DESIGN.md §6.3).
        fn idle_and_drain(&mut self) {
            self.scanner.idle();
            loop {
                let mut progress = self.scan();
                while !self.receiver.pending.is_empty() {
                    self.receiver.answer(0, &mut self.scanner);
                    progress = true;
                }
                while self.take_frame() {
                    progress = true;
                }
                if self.scanner.tail_waits() {
                    self.scanner.send_tail();
                    progress = true;
                }
                if !progress {
                    break;
                }
            }
            let pushed = self.pushed as u64;
            assert_eq!(
                self.receiver.output.len() as u64,
                pushed,
                "bytes before the idle point stayed back"
            );
            if pushed > 0 {
                assert_eq!(self.receiver.flushes.last(), Some(&pushed), "no flush");
            }
        }

        /// Transfer with 8-byte pushes, each followed by the answers to
        /// every query so far: the answers arrive as early as possible.
        fn transfer_promptly(&mut self) {
            while !self.scanner.finished() {
                let pushed = self.push(8);
                loop {
                    let mut progress = self.scan();
                    while !self.receiver.pending.is_empty() {
                        self.receiver.answer(0, &mut self.scanner);
                        progress = true;
                    }
                    while self.take_frame() {
                        progress = true;
                    }
                    if !progress {
                        break;
                    }
                }
                assert!(pushed || self.scanner.finished(), "the scanner stalled");
            }
        }
    }

    /// Transfer `input` with prompt answers; return the receiver.
    fn transfer_promptly(input: &[u8], blocks: &[Vec<u8>], stale: &[Vec<u8>]) -> Receiver {
        let mut run = Run::new(input, Receiver::new(blocks, stale), 1 << 20, 1 << 20, None);
        run.transfer_promptly();
        run.receiver
    }

    #[test]
    fn unaligned_blocks_become_references() {
        let block = noise(BS as usize, 1);
        let mut input = b"abc".to_vec();
        input.extend_from_slice(&block);
        input.extend_from_slice(b"tail");
        let receiver = transfer_promptly(&input, std::slice::from_ref(&block), &[]);
        assert_eq!(receiver.output, input);
        assert_eq!(receiver.queries.len(), 1);
        assert_eq!(receiver.queries[0].0, 3);
    }

    #[test]
    fn an_empty_list_sends_everything_as_literals() {
        let input = noise(1000, 2);
        let receiver = Receiver::new(&[], &[]);
        let mut scanner = Scanner::new(
            receiver.table(),
            BS,
            StrongHash::Blake3,
            lookahead(1 << 20),
            1 << 20,
            None,
        );
        scanner.push(&input);
        scanner.end_input();
        while scanner.scan(&mut |_, _| panic!("no query without a list")) {}
        let mut output = Vec::new();
        while let Some(frame) = scanner.next_frame() {
            let Frame::Literal(bytes) = frame else {
                panic!("a reference without a list");
            };
            assert!(bytes.len() <= BS as usize);
            output.extend_from_slice(&bytes);
        }
        assert_eq!(output, input);
        assert!(scanner.finished());
    }

    /// A scanner starts with the empty table, and a block becomes a
    /// reference only once its weak hash is in the table: the block that it
    /// scanned before stays literal (DESIGN.md §6.3).
    #[test]
    fn a_block_becomes_a_reference_once_its_weak_hash_is_in_the_table() {
        let block = noise(BS as usize, 3);
        let mut receiver = Receiver::new(std::slice::from_ref(&block), &[]);
        let mut scanner = Scanner::new(
            HashTable::empty(),
            BS,
            StrongHash::Blake3,
            lookahead(1 << 20),
            1 << 20,
            None,
        );
        let first = [noise(5, 4), block.clone(), noise(5, 5)].concat();
        scanner.push(&first);
        while scanner.scan(&mut |offset, strong| receiver.query(offset, strong)) {}
        assert!(receiver.queries.is_empty(), "a query without a list");
        scanner.set_table(HashTable::for_values(1));
        scanner.insert(&receiver.values());
        scanner.push(&block);
        scanner.end_input();
        while !scanner.finished() {
            while scanner.scan(&mut |offset, strong| receiver.query(offset, strong)) {}
            while !receiver.pending.is_empty() {
                receiver.answer(0, &mut scanner);
            }
            while let Some(frame) = scanner.next_frame() {
                receiver.take(frame);
            }
        }
        assert_eq!(receiver.output, [first.clone(), block].concat());
        let offsets: Vec<u64> = receiver.queries.iter().map(|&(offset, _)| offset).collect();
        assert_eq!(
            offsets,
            [first.len() as u64],
            "the block before the table was queried"
        );
    }

    #[test]
    fn a_window_that_only_shares_a_fingerprint_makes_no_query() {
        // The one list value has the low 32 bits of the window's weak
        // hash, which the table keeps, but other high bits. The table finds
        // the window; the check of the whole weak hash does not.
        let window = noise(BS as usize, 60);
        let weak = WeakHasher::new(BS as usize).hash(&window);
        let table = HashTable::build(&[weak ^ 1 << 63]);
        assert!(table.finish_lookup(table.start_lookup(weak)));
        let input = [noise(10, 61), window, noise(10, 62)].concat();
        let mut scanner = Scanner::new(
            table,
            BS,
            StrongHash::Blake3,
            lookahead(1 << 20),
            1 << 20,
            None,
        );
        scanner.push(&input);
        scanner.end_input();
        while scanner.scan(&mut |offset, _| panic!("a query at {offset}")) {}
        let mut output = Vec::new();
        while let Some(frame) = scanner.next_frame() {
            let Frame::Literal(bytes) = frame else {
                panic!("{frame:?} without a list value");
            };
            output.extend_from_slice(&bytes);
        }
        assert_eq!(output, input);
        assert!(scanner.finished());
    }

    /// Scan all of `scanner`'s input without a list, and take every frame.
    fn frames_without_a_list(scanner: &mut Scanner) -> Vec<Frame> {
        while scanner.scan(&mut |_, _| panic!("no query without a list")) {}
        std::iter::from_fn(|| scanner.next_frame()).collect()
    }

    /// A lookahead of `bytes`, for windows of `BS` bytes.
    fn lookahead(bytes: u64) -> Lookahead {
        Lookahead::reserve(bytes, BS).unwrap()
    }

    fn scanner_without_a_list() -> Scanner<'static> {
        let table = Receiver::new(&[], &[]).table();
        Scanner::new(
            table,
            BS,
            StrongHash::Blake3,
            lookahead(1 << 20),
            1 << 20,
            None,
        )
    }

    #[test]
    fn constant_windows_become_runs() {
        // Each run covers whole constant windows. Bytes after the last
        // whole window of a run are scanned like any others and become
        // literal, except for a tail at EOF, which continues the run
        // (DESIGN.md §6.3).
        let b = BS as usize;
        let input = [
            noise(10, 50),
            vec![7; 2 * b + 5],
            noise(10, 51),
            vec![9; 3 * b + 4],
        ]
        .concat();
        let mut scanner = scanner_without_a_list();
        scanner.push(&input);
        scanner.end_input();
        let frames = frames_without_a_list(&mut scanner);
        let runs: Vec<(u8, u64)> = frames
            .iter()
            .filter_map(|frame| match *frame {
                Frame::Run { byte, length } => Some((byte, length)),
                _ => None,
            })
            .collect();
        assert_eq!(runs, [(7, 2 * u64::from(BS)), (9, 3 * u64::from(BS) + 4)]);
        let mut receiver = Receiver::new(&[], &[]);
        frames.into_iter().for_each(|frame| receiver.take(frame));
        assert_eq!(receiver.output, input);
        assert!(scanner.finished());
    }

    #[test]
    fn a_tail_continues_only_a_run_that_waits() {
        // A run of three windows and a short tail of the same byte.
        let b = BS as usize;
        let run = |length: u64| Frame::Run { byte: 5, length };
        let mut scanner = scanner_without_a_list();
        scanner.push(&vec![5; 3 * b + 7]);
        scanner.end_input();
        assert_eq!(
            frames_without_a_list(&mut scanner),
            [run(3 * u64::from(BS) + 7)]
        );

        // The run left before the tail arrived: the tail is literal.
        let mut scanner = scanner_without_a_list();
        scanner.push(&vec![5; 3 * b]);
        assert_eq!(
            frames_without_a_list(&mut scanner),
            [run(3 * u64::from(BS))]
        );
        scanner.push(&[5; 7]);
        scanner.end_input();
        assert_eq!(
            frames_without_a_list(&mut scanner),
            [Frame::Literal(vec![5; 7])]
        );
        assert!(scanner.finished());
    }

    #[test]
    fn a_short_last_literal_waits_for_more_output_or_eof() {
        // Without an idle point, as in batch delivery, the last `B - 1`
        // bytes wait for a whole window before EOF, and a Literal shorter
        // than `B` at the end waits to grow (DESIGN.md §6.3).
        let b = BS as usize;
        let input = noise(2 * b + 10, 60);
        let mut scanner = scanner_without_a_list();
        scanner.push(&input);
        let frames = frames_without_a_list(&mut scanner);
        assert_eq!(frames, [Frame::Literal(input[..b].to_vec())]);
        scanner.end_input();
        let frames = frames_without_a_list(&mut scanner);
        assert_eq!(
            frames,
            [
                Frame::Literal(input[b..2 * b].to_vec()),
                Frame::Literal(input[2 * b..].to_vec())
            ]
        );
        assert!(scanner.finished());
    }

    /// `frames_without_a_list`, with a data writer that is idle: a tail
    /// that waits for it leaves too.
    fn frames_with_an_idle_writer(scanner: &mut Scanner) -> Vec<Frame> {
        let mut frames = frames_without_a_list(scanner);
        if scanner.tail_waits() {
            scanner.send_tail();
            frames.extend(frames_without_a_list(scanner));
        }
        frames
    }

    #[test]
    fn an_idle_point_sends_the_tail_and_a_short_literal_then_a_flush() {
        // In eager delivery, the bytes before an idle point leave without
        // waiting for more output once the data writer is idle: the last
        // `B - 1` bytes, which fill no window, become literal, with the
        // short last Literal before them. A flush follows their frames,
        // once. The bytes after the idle point wait as usual (DESIGN.md
        // §6.3).
        let b = BS as usize;
        let input = noise(2 * b + 15, 61);
        let mut scanner = scanner_without_a_list();
        scanner.push(&input[..2 * b + 10]);
        scanner.idle();
        scanner.push(&input[2 * b + 10..]);
        // Until the data writer is idle, the tail waits, and so does the
        // short Literal before it.
        assert_eq!(
            frames_without_a_list(&mut scanner),
            [Frame::Literal(input[..b].to_vec())]
        );
        assert!(scanner.tail_waits());
        scanner.send_tail();
        assert_eq!(
            frames_without_a_list(&mut scanner),
            [
                Frame::Literal(input[b..2 * b].to_vec()),
                Frame::Literal(input[2 * b..2 * b + 10].to_vec()),
                Frame::Flush,
            ]
        );
        assert!(!scanner.tail_waits(), "the bytes after the idle point");
        scanner.idle();
        assert_eq!(
            frames_with_an_idle_writer(&mut scanner),
            [Frame::Literal(input[2 * b + 10..].to_vec()), Frame::Flush]
        );
        // An idle point without new bytes needs no flush.
        scanner.idle();
        assert_eq!(frames_with_an_idle_writer(&mut scanner), []);
        scanner.end_input();
        assert_eq!(frames_without_a_list(&mut scanner), []);
        assert!(scanner.finished());
    }

    #[test]
    fn a_tail_before_an_idle_point_waits_for_the_records_before_it() {
        // A run of three windows and a short tail of the same byte before an
        // idle point. The tail leaves only once every record before it has
        // left, so it cannot continue the run, which has left: it is literal
        // (DESIGN.md §6.3).
        let b = BS as usize;
        let mut scanner = scanner_without_a_list();
        scanner.push(&vec![5; 3 * b + 7]);
        scanner.idle();
        assert_eq!(
            frames_with_an_idle_writer(&mut scanner),
            [
                Frame::Run {
                    byte: 5,
                    length: 3 * u64::from(BS)
                },
                Frame::Literal(vec![5; 7]),
                Frame::Flush
            ]
        );
    }

    #[test]
    fn a_query_before_an_idle_point_delays_its_flush() {
        // The tail waits for the answer of a query before it, and the flush
        // for the frames of every byte before the idle point.
        let block = noise(BS as usize, 62);
        let input = [&block[..], b"xy"].concat();
        let mut run = Run::new(
            &input,
            Receiver::new(std::slice::from_ref(&block), &[]),
            1 << 20,
            1 << 20,
            None,
        );
        run.push(input.len());
        run.scanner.idle();
        run.scan();
        assert!(!run.take_frame(), "a frame before the answer");
        assert!(!run.scanner.tail_waits(), "a tail before the answer");
        run.receiver.answer(0, &mut run.scanner);
        while run.scan() || run.take_frame() {}
        run.scanner.send_tail();
        while run.take_frame() {}
        assert_eq!(run.receiver.output, input);
        assert_eq!(run.receiver.flushes, [input.len() as u64]);
    }

    #[test]
    fn without_idle_points_a_flush_follows_every_lookahead_of_output() {
        // No idle point comes, as in batch delivery, or with a source that
        // never pauses. Once `lookahead` bytes of output have left since the
        // last flush, the next item is a flush, and no flush comes sooner. A
        // reference counts its whole block, and a run its whole length, as
        // the bytes that they stand for (DESIGN.md §6.3).
        let b = BS as usize;
        let blocks: Vec<Vec<u8>> = (0..3).map(|i| noise(b, 80 + i)).collect();
        let mut input = Vec::new();
        for i in 0..30 {
            input.extend_from_slice(&blocks[i % blocks.len()]);
            input.extend(std::iter::repeat_n(9, b + i));
            input.extend_from_slice(&noise(i % 7 + 1, 90 + i as u64));
        }
        let lookahead = 4 * u64::from(BS);
        let receiver = Receiver::new(&blocks, &[]);
        let mut run = Run::new(&input, receiver, lookahead, 1 << 20, None);
        // The output since the last flush, and the items so far.
        let mut since = 0;
        let (mut flushes, mut references, mut runs) = (0, 0, 0);
        while !run.scanner.finished() {
            let pushed = run.push(8);
            loop {
                let mut progress = run.scan();
                while !run.receiver.pending.is_empty() {
                    run.receiver.answer(0, &mut run.scanner);
                    progress = true;
                }
                while let Some(frame) = run.scanner.next_frame() {
                    let before = run.receiver.output.len() as u64;
                    let flush = frame == Frame::Flush;
                    references += usize::from(frame == Frame::Reference);
                    runs += usize::from(matches!(frame, Frame::Run { .. }));
                    run.receiver.take(frame);
                    if flush {
                        assert!(since >= lookahead, "a flush after {since} bytes");
                        since = 0;
                        flushes += 1;
                    } else {
                        assert!(since < lookahead, "no flush after {since} bytes");
                        since += run.receiver.output.len() as u64 - before;
                    }
                    progress = true;
                }
                if !progress {
                    break;
                }
            }
            assert!(pushed || run.scanner.finished(), "the scanner stalled");
        }
        assert_eq!(run.receiver.output, input);
        assert_eq!((references, runs), (30, 30));
        assert!(flushes > 0, "no flush");
    }

    #[test]
    fn the_flush_of_an_idle_point_starts_the_count_of_the_lookahead_again() {
        // After the flush of an idle point at 3 blocks, the next flush comes
        // only once `lookahead` bytes, 4 blocks, have left after it: at 7
        // blocks, and not at 4 or 8, as it would if only those flushes counted
        // (DESIGN.md §6.3).
        let b = BS as usize;
        let table = Receiver::new(&[], &[]).table();
        let mut scanner = Scanner::new(
            table,
            BS,
            StrongHash::Blake3,
            lookahead(4 * u64::from(BS)),
            1 << 20,
            None,
        );
        let input = noise(9 * b, 85);
        scanner.push(&input[..3 * b]);
        scanner.idle();
        let frames = frames_with_an_idle_writer(&mut scanner);
        assert_eq!(frames.last(), Some(&Frame::Flush));
        scanner.push(&input[3 * b..]);
        scanner.end_input();
        let mut frames = Vec::new();
        while !scanner.finished() {
            frames.extend(frames_without_a_list(&mut scanner));
        }
        let mut offset = 3 * b;
        let mut flushes = Vec::new();
        for frame in frames {
            match frame {
                Frame::Literal(bytes) => offset += bytes.len(),
                Frame::Flush => flushes.push(offset),
                frame => panic!("{frame:?} without a list"),
            }
        }
        assert_eq!(offset, input.len());
        assert_eq!(flushes, [7 * b]);
    }

    #[test]
    fn a_tail_that_waits_for_the_data_writer_keeps_its_block() {
        // An idle point comes 10 bytes into a listed block. While the data
        // writer is busy, as on a slow link, the tail before the idle point
        // waits, and the rest of the block arrives meanwhile: the block is
        // queried, as without the idle point. With an idle data writer, the
        // tail leaves as literal, and the block cannot be found any more
        // (DESIGN.md §6.3).
        let block = noise(BS as usize, 63);
        let input = [&noise(5, 64)[..], &block].concat();
        for writer_idle in [false, true] {
            let receiver = Receiver::new(std::slice::from_ref(&block), &[]);
            let mut run = Run::new(&input, receiver, 1 << 20, 1 << 20, None);
            run.push(15);
            run.scanner.idle();
            run.scan();
            assert!(run.scanner.tail_waits());
            if writer_idle {
                run.scanner.send_tail();
            }
            run.transfer_promptly();
            assert_eq!(run.receiver.output, input);
            let queries: Vec<u64> = run.receiver.queries.iter().map(|q| q.0).collect();
            let expected: &[u64] = if writer_idle { &[] } else { &[5] };
            assert_eq!(queries, expected, "writer idle: {writer_idle}");
        }
    }

    #[test]
    fn a_constant_window_is_a_run_even_when_listed() {
        // The list holds the weak hash of the constant window; the run
        // takes precedence, so there is no query (DESIGN.md §6.3, step 1).
        let block = vec![7; BS as usize];
        let input = [&block[..], &block, b"xy"].concat();
        let receiver = transfer_promptly(&input, std::slice::from_ref(&block), &[]);
        assert_eq!(receiver.output, input);
        assert!(receiver.queries.is_empty());
    }

    #[test]
    fn a_match_overlapping_a_rejected_window_is_found() {
        // The stale block's window at 0 is rejected; the valid block
        // starts inside it, at 5, and is found by the rescan.
        let valid = noise(BS as usize, 3);
        let mut stale = noise(5, 4);
        stale.extend_from_slice(&valid[..BS as usize - 5]);
        let mut input = stale[..5].to_vec();
        input.extend_from_slice(&valid);
        let receiver = transfer_promptly(&input, std::slice::from_ref(&valid), &[stale]);
        assert_eq!(receiver.output, input);
        let offsets: Vec<u64> = receiver.queries.iter().map(|q| q.0).collect();
        assert_eq!(offsets, vec![0, 5]);
        assert_eq!(receiver.pins.len(), 0, "every pin was used or released");
    }

    #[test]
    fn a_rejected_hash_is_not_queried_again() {
        let stale = noise(BS as usize, 5);
        let mut input = Vec::new();
        for i in 0..4 {
            input.extend_from_slice(&stale);
            input.extend_from_slice(&noise(7, 10 + i));
        }
        let receiver = transfer_promptly(&input, &[], std::slice::from_ref(&stale));
        assert_eq!(receiver.output, input);
        assert_eq!(
            receiver.queries.len(),
            1,
            "the negative cache suppresses repeats"
        );

        // Without a negative cache, every occurrence is queried.
        let receiver = Receiver::new(&[], std::slice::from_ref(&stale));
        let mut run = Run::new(&input, receiver, 1 << 20, 0, None);
        run.transfer_promptly();
        assert_eq!(run.receiver.output, input);
        assert_eq!(run.receiver.queries.len(), 4);
    }

    #[test]
    fn a_lookahead_of_one_window_still_finds_every_block() {
        let blocks: Vec<Vec<u8>> = (0..5).map(|i| noise(BS as usize, 20 + i)).collect();
        let input: Vec<u8> = blocks
            .iter()
            .flat_map(|b| [b.clone(), b"xy".to_vec()])
            .flatten()
            .collect();
        let mut run = Run::new(
            &input,
            Receiver::new(&blocks, &[]),
            BS.into(),
            1 << 20,
            None,
        );
        run.transfer(&[7, 2, 200, 1, 3, 3, 9]);
        assert_eq!(run.receiver.output, input);
        assert_eq!(run.receiver.queries.len(), 5);
    }

    /// The buffer is reserved once, twice as large as the bytes that it
    /// accepts, and never grows: a buffer that grows copies itself, and
    /// both copies use memory for a moment.
    #[test]
    fn the_buffer_is_reserved_once() {
        let lookahead = 64 << 10;
        let accepted = (lookahead + u64::from(BS)) as usize;
        let size = 2 * accepted;
        let input = noise(1 << 20, 8);
        let mut run = Run::new(&input, Receiver::new(&[], &[]), lookahead, 0, None);
        let mut reserved = None;
        while !run.scanner.finished() {
            // The consumer takes one frame per round, so the buffer stays
            // full, and only the drop of the emitted bytes makes room.
            let mut progress = false;
            while run.push(4096) {
                let buffer = &run.scanner.buffer;
                let place = (buffer.as_ptr(), buffer.capacity());
                assert_eq!(*reserved.get_or_insert(place), place, "the buffer moved");
                progress = true;
            }
            while run.scan() {
                progress = true;
            }
            progress |= run.take_frame();
            assert!(progress, "the scanner stalled");
        }
        assert_eq!(reserved.unwrap().1, size);
        assert_eq!(run.receiver.output, input);
    }

    /// A hit limits the rest of its call of `scan` to `min_budget` bytes,
    /// also after a long stretch without hits has grown the budget
    /// (DESIGN.md §6.3): closely spaced matches then stay on the scanner's
    /// thread, and the driver processes answers again soon.
    #[test]
    fn a_hit_limits_the_rest_of_the_scan() {
        let blocks: Vec<Vec<u8>> = (0..20).map(|i| noise(BS as usize, 40 + i)).collect();
        let mut input = noise(200 * BS as usize, 39);
        let first = input.len() as u64;
        input.extend(blocks.iter().flatten());
        let receiver = Receiver::new(&blocks, &[]);
        let mut run = Run::new(&input, receiver, 1 << 20, 1 << 20, Some(2));
        let min_budget = 4 * u64::from(BS);
        run.scanner.min_budget = min_budget;
        run.scanner.budget = min_budget;
        run.scanner.max_budget = 64 * min_budget;
        while run.push(input.len()) {}
        // Scan without answers until the first query.
        while run.receiver.queries.is_empty() {
            assert!(run.scan(), "the scanner stalled");
        }
        assert_eq!(run.receiver.queries[0].0, first);
        assert!(
            run.scanner.p < first + min_budget + u64::from(BS),
            "the call scanned on to {} after the hit at {first}",
            run.scanner.p
        );
    }

    /// A window answered no records one literal byte, and the rolling
    /// state moves to it, so that the next window rolls on from it
    /// (DESIGN.md §6.3). The rescan here reaches the window's entry at the
    /// end of a stretch, which leaves the state of the window before.
    #[test]
    fn the_window_after_one_answered_no_rolls_on() {
        let b = BS as usize;
        let (first, second) = (noise(b, 70), noise(b, 71));
        let input = [&first[..], &noise(5, 72), &second].concat();
        let mut run = Run::new(
            &input,
            Receiver::new(&[], &[first, second]),
            1 << 20,
            1 << 20,
            None,
        );
        run.push(input.len());
        run.scan();
        let at = (b + 5) as u64;
        let pending: Vec<u64> = run.receiver.pending.iter().map(|q| q.0).collect();
        assert_eq!(pending, [0, at]);
        // Reject the second window, then the first: the rescan from 1
        // reaches the second window's entry.
        run.receiver.answer(1, &mut run.scanner);
        run.receiver.answer(0, &mut run.scanner);
        run.scan();
        // The window after the entry is not whole yet.
        assert_eq!(run.scanner.p, at + 1);
        assert_eq!(run.scanner.known.map(|known| known.offset), Some(at));
    }

    /// Valid and stale blocks that overlap each other, and an input piece
    /// where a rescan after a rejection finds a window that the first scan
    /// jumped over, and jumps over a window that the first scan queried:
    /// the stale window at 0 hides the valid window at 8 until it is
    /// rejected, and the valid window at 36, queried by the first scan, is
    /// covered by the reference at 8 after the rescan.
    fn overlapping_blocks(seed: u64) -> (Vec<Vec<u8>>, Vec<Vec<u8>>, Vec<u8>) {
        let b = BS as usize;
        let join = |a: &[u8], b: &[u8]| [a, b].concat();
        let hidden = noise(b, seed);
        let skipped = join(&hidden[b - 4..], &noise(b - 4, seed ^ 1));
        let hider = join(&noise(8, seed ^ 2), &hidden[..b - 8]);
        let other = noise(b, seed ^ 3);
        let other_stale = join(&noise(b / 8, seed ^ 4), &other[..b - b / 8]);
        let piece = [&hider[..8], &hidden, &skipped[4..]].concat();
        debug_assert_eq!(&piece[..b], &hider[..]);
        debug_assert_eq!(&piece[36..36 + b], &skipped[..]);
        (
            vec![hidden, skipped, other],
            vec![hider, other_stale],
            piece,
        )
    }

    #[test]
    fn a_late_answer_for_a_skipped_window_is_obsolete() {
        // The answer for the skipped window arrives after the output
        // passed it (see `overlapping_blocks`).
        let (valid, stale, input) = overlapping_blocks(30);
        let mut run = Run::new(
            &input,
            Receiver::new(&valid, &stale),
            1 << 20,
            1 << 20,
            None,
        );
        run.push(input.len());
        run.push(0);
        run.scan();
        let offsets = |run: &Run| run.receiver.queries.iter().map(|q| q.0).collect::<Vec<_>>();
        assert_eq!(offsets(&run), vec![0, 36]);
        // Reject the stale window: the rescan finds W and jumps over 36.
        let stale = run.receiver.pending.iter().position(|q| q.0 == 0).unwrap();
        run.receiver.answer(stale, &mut run.scanner);
        run.scan();
        assert_eq!(offsets(&run), vec![0, 36, 8]);
        let w_query = run.receiver.pending.iter().position(|q| q.0 == 8).unwrap();
        run.receiver.answer(w_query, &mut run.scanner);
        while run.scan() || run.take_frame() {}
        assert!(run.scanner.finished());
        // V's answer is obsolete now: the receiver answers no, and the
        // scanner ignores it.
        assert!(run.receiver.output.len() > 36);
        run.receiver.answer(0, &mut run.scanner);
        assert_eq!(run.receiver.output, input);
    }

    /// Input pieces: noise, whole blocks, parts of blocks, the piece of
    /// `overlapping_blocks`, and equal bytes.
    fn input_strategy() -> impl Strategy<Value = (Vec<(u8, u8)>, u64)> {
        (
            proptest::collection::vec((any::<u8>(), any::<u8>()), 0..40),
            any::<u64>(),
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]

        /// Rolling and probing, also in parts, stop at the first window in
        /// order that is constant or may be listed, with its rolling state,
        /// or else end with the state of the last window, for any part size
        /// (DESIGN.md §6.3).
        #[test]
        fn parts_find_the_first_stop_in_order(
            windows in 1usize..20_000,
            listed in proptest::collection::vec(any::<prop::sample::Index>(), 0..4),
            equal in proptest::option::of(
                (any::<prop::sample::Index>(), 0..3 * BS as usize, any::<u8>()),
            ),
            part in 1usize..5000,
            seed in any::<u64>(),
        ) {
            let window = BS as usize;
            let mut bytes = noise(windows - 1 + window, seed);
            // Equal bytes, which make constant windows when there are enough.
            if let Some((at, length, byte)) = equal {
                let at = at.index(bytes.len());
                let end = (at + length).min(bytes.len());
                bytes[at..end].fill(byte);
            }
            let weak = WeakHasher::new(window);
            let bytes_of = |k: usize| &bytes[k..k + window];
            let values: Vec<u64> = listed
                .iter()
                .map(|i| weak.hash(bytes_of(i.index(windows))))
                .collect();
            let table = HashTable::build(&values);
            // Each window from its own bytes, in order.
            let state = |k: usize| Rolling {
                poly: weak.polynomial(bytes_of(k)),
                equal: bytes_of(k).iter().rev().take_while(|&&b| b == bytes[k + window - 1]).count(),
            };
            let expected = (0..windows)
                .find_map(|k| {
                    if state(k).equal == window {
                        Some(Stop::Constant(k))
                    } else if table.finish_lookup(table.start_lookup(weak.hash(bytes_of(k)))) {
                        Some(Stop::Listed(k, state(k)))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| Stop::Neither(state(windows - 1)));
            // The scanner searches only from a window that is not constant.
            let Some(first) = Rolling::of(&weak, bytes_of(0)) else {
                prop_assert_eq!(expected, Stop::Constant(0));
                return Ok(());
            };
            prop_assert_eq!(first, state(0));
            prop_assert_eq!(first_stop(&table, &weak, &bytes, first, || false), expected);
            let in_parts = first_stop_in_parts(probe_pool(), part, &table, &weak, &bytes, first);
            prop_assert_eq!(in_parts, expected);
        }

        /// Byte-exact reconstruction under any chunking, answer order and
        /// delay, rejection, rescan, runs across chunks, backpressure, scan
        /// budget, idle points, split of stretches among probe threads, and
        /// a table that grows while the stream goes on (DESIGN.md §11).
        #[test]
        fn output_is_reproduced_exactly(
            (pieces, seed) in input_strategy(),
            choices in proptest::collection::vec(any::<u8>(), 1..64),
            lookahead_windows in 1u64..6,
            negative in any::<bool>(),
            part in proptest::option::of(1usize..100),
            min_budget in 1u64..300,
            idle_points in any::<bool>(),
            growing in any::<bool>(),
        ) {
            let (valid, stale, piece) = overlapping_blocks(seed);
            let mut input = Vec::new();
            for (kind, n) in pieces {
                let block = match kind % 5 {
                    0 => &valid[usize::from(n) % valid.len()],
                    1 => &stale[usize::from(n) % stale.len()],
                    2 => {
                        input.extend_from_slice(&noise(usize::from(n % 50), seed ^ u64::from(n)));
                        continue;
                    }
                    3 => {
                        input.extend_from_slice(&piece);
                        continue;
                    }
                    _ => {
                        // Equal bytes: shorter or longer than a window.
                        let length = usize::from(n) % (3 * BS as usize) + 1;
                        input.extend(std::iter::repeat_n(n, length));
                        continue;
                    }
                };
                // A whole block, or its end, or its start.
                match n % 4 {
                    0 => input.extend_from_slice(&block[usize::from(n) % block.len()..]),
                    1 => input.extend_from_slice(&block[..usize::from(n) % block.len()]),
                    _ => input.extend_from_slice(block),
                }
            }
            let negative_cache = if negative { 1 << 20 } else { 0 };
            let receiver = Receiver::new(&valid, &stale);
            let mut run = Run::new(
                &input,
                receiver,
                lookahead_windows * u64::from(BS),
                negative_cache,
                part,
            );
            run.scanner.min_budget = min_budget;
            run.scanner.budget = min_budget;
            run.scanner.max_budget = if part.is_some() { 16 * min_budget } else { min_budget };
            run.idle_points = idle_points;
            if growing {
                run = run.growing();
            }
            run.transfer(&choices);
            prop_assert_eq!(&run.receiver.output, &input);
            prop_assert!(run.scanner.records.is_empty() && run.scanner.entries.is_empty());
        }
    }
}

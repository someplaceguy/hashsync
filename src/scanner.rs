//! The sender's scanner (DESIGN.md §6.3): the single owner of all output
//! decisions.
//!
//! The scanner rolls the weak hash over the command's output. A window
//! whose weak hash may be in the receiver's list becomes a query for the
//! window's full strong hash; a yes answer turns it into a reference, and
//! everything else is literal. Output leaves strictly in order: a query
//! blocks the records behind it until its answer arrives, and a no answer
//! discards every record after the rejected window, which is scanned again
//! from its next byte. Runs of repeated bytes arrive with PLAN.md step 5.
//!
//! The scanner does no I/O and starts no threads. Its driver (`serve`)
//! feeds it command output and answers, sends the queries that it makes,
//! and takes its data frames while the data queue has room.
//!
//! Offsets count output bytes from zero. `e` is the next byte to emit, `p`
//! the next byte to scan, and `r` the end of the bytes read, with
//! `e <= p <= r`:
//!
//! - The buffer holds the bytes `[e, r)`. It accepts command output while
//!   it holds less than `lookahead + B` bytes, and a scan step starts only
//!   while `p - e < lookahead`.
//! - The records cover `[e, p)` in order, without gaps or overlaps.
//! - The query states hold every offset at or above `e` that was a weak
//!   hit, also when no record refers to it any more. Each offset is queried
//!   at most once.

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;

use lru::LruCache;

use crate::hash::weak::{WeakHasher, mix64};
use crate::hash::{Strong, StrongHash};
use crate::hashtable::HashTable;

/// The bytes that one call of `scan` examines at most, so that answers and
/// emission keep up with a long stretch of misses.
const SCAN_BUDGET: u64 = 64 << 10;

/// The memory that one hash in the negative cache costs: the digest, the
/// links of the recency list, and the slot of the hash table.
const NEGATIVE_ENTRY_BYTES: u64 = 96;

/// A data frame, in output order.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// LIT: at most `B` literal bytes.
    Literal(Vec<u8>),
    /// REF: the pin at the current output offset.
    Reference,
}

/// Scanned output that is not emitted yet (DESIGN.md §6.3).
#[derive(Clone, Copy, Debug)]
enum Record {
    Literal {
        offset: u64,
        length: u64,
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
            Record::Literal { offset, .. } | Record::Query { offset } | Record::Ref { offset } => {
                offset
            }
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

/// What is known of the weak-hash polynomial of the window at `p`.
#[derive(Clone, Copy)]
enum Roll {
    /// Nothing: compute it from the window's bytes.
    Stale,
    /// The polynomial of the window at `p`.
    Current(u64),
    /// The polynomial of the window at `p - 1`, whose first byte was
    /// `out`; that byte may have left the buffer since.
    Behind { poly: u64, out: u8 },
}

pub struct Scanner {
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
    /// The command's stdout reached EOF.
    ended: bool,
    roll: Roll,
    records: VecDeque<Record>,
    /// The offsets of the Query records, in record order.
    queries: VecDeque<u64>,
    /// The query states by offset.
    entries: BTreeMap<u64, Entry>,
    /// Full strong hashes answered no; `None` when the configured size
    /// holds no hash.
    negative: Option<LruCache<Strong, ()>>,
}

impl Scanner {
    /// A scanner for windows of `block_size` bytes, which probes `table`,
    /// scans at most `lookahead` bytes ahead of its output, and remembers
    /// up to `negative_cache` bytes of rejected hashes.
    pub fn new(
        table: HashTable,
        block_size: u32,
        strong: StrongHash,
        lookahead: u64,
        negative_cache: u64,
    ) -> Self {
        assert!(block_size > 0 && lookahead >= u64::from(block_size));
        let negative_hashes =
            usize::try_from(negative_cache / NEGATIVE_ENTRY_BYTES).unwrap_or(usize::MAX);
        Scanner {
            block_size: u64::from(block_size),
            lookahead,
            weak: WeakHasher::new(block_size as usize),
            table,
            strong,
            buffer: Vec::new(),
            base: 0,
            e: 0,
            p: 0,
            ended: false,
            roll: Roll::Stale,
            records: VecDeque::new(),
            queries: VecDeque::new(),
            entries: BTreeMap::new(),
            negative: NonZeroUsize::new(negative_hashes).map(LruCache::sparse),
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

    /// True while the buffer accepts more command output.
    pub fn has_room(&self) -> bool {
        self.end() - self.e < self.lookahead.saturating_add(self.block_size)
    }

    /// Append command output.
    pub fn push(&mut self, bytes: &[u8]) {
        assert!(!self.ended, "command output after its end");
        // Drop the emitted bytes once they fill half the buffer, so that
        // moving the rest costs O(1) per byte.
        let emitted = (self.e - self.base) as usize;
        if emitted > 0 && emitted >= self.buffer.len() / 2 {
            self.buffer.drain(..emitted);
            self.base = self.e;
        }
        self.buffer.extend_from_slice(bytes);
    }

    /// The command's stdout reached EOF.
    pub fn end_input(&mut self) {
        self.ended = true;
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

    /// Resolve the answered queries, then scan up to `SCAN_BUDGET` bytes.
    /// `query` receives the offset and full strong hash of each new query.
    /// True when anything changed.
    pub fn scan(&mut self, query: &mut impl FnMut(u64, Strong)) -> bool {
        let mut progress = self.resolve();
        let budget_end = self.p.saturating_add(SCAN_BUDGET);
        while self.p - self.e < self.lookahead && self.p < budget_end {
            let available = self.end() - self.p;
            if available < self.block_size {
                // At EOF, a tail shorter than a window is literal.
                if self.ended && available > 0 {
                    self.push_literal(self.p, available);
                    self.p = self.end();
                    self.roll = Roll::Stale;
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
            if next_entry != Some(self.p) {
                // Positions before `limit` have a whole window and are
                // neither beyond the lookahead nor a known entry.
                let limit = (self.end() - self.block_size + 1)
                    .min(self.e.saturating_add(self.lookahead))
                    .min(budget_end)
                    .min(next_entry.unwrap_or(u64::MAX));
                if !self.skip_misses(limit) {
                    continue;
                }
                let strong = self.strong.digest(self.bytes(self.p, self.block_size));
                let state = State::NotSent;
                self.entries.insert(self.p, Entry { strong, state });
            }
            self.apply_entry(query);
        }
        progress
    }

    /// Record literal bytes from `p` while their windows miss the weak
    /// table. True at a hit, with `p` at its window; false once `p`
    /// reaches `limit`, which must be above `p`.
    fn skip_misses(&mut self, limit: u64) -> bool {
        let start = self.p;
        let mut poly = self.polynomial();
        let window = self.block_size as usize;
        let buffer = &self.buffer;
        let mut i = (start - self.base) as usize;
        let end = (limit - self.base) as usize;
        let hit = loop {
            if self.table.contains(mix64(poly)) {
                self.roll = Roll::Current(poly);
                break true;
            }
            let out = buffer[i];
            i += 1;
            if i == end {
                self.roll = Roll::Behind { poly, out };
                break false;
            }
            poly = self.weak.roll(poly, out, buffer[i + window - 1]);
        };
        self.p = self.base + i as u64;
        if self.p > start {
            self.push_literal(start, self.p - start);
        }
        hit
    }

    /// The polynomial of the window at `p`, which must be whole.
    fn polynomial(&mut self) -> u64 {
        let poly = match self.roll {
            Roll::Current(poly) => poly,
            Roll::Behind { poly, out } => {
                let new = self.bytes(self.p + self.block_size - 1, 1)[0];
                self.weak.roll(poly, out, new)
            }
            Roll::Stale => self.weak.polynomial(self.bytes(self.p, self.block_size)),
        };
        self.roll = Roll::Current(poly);
        poly
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
        self.roll = Roll::Stale;
    }

    /// Record the byte at `p` as literal, and move on to the next window.
    fn literal_byte(&mut self) {
        let poly = self.polynomial();
        let out = self.bytes(self.p, 1)[0];
        self.roll = Roll::Behind { poly, out };
        self.push_literal(self.p, 1);
        self.p += 1;
    }

    /// Append literal bytes, extending a Literal that ends where they
    /// start.
    fn push_literal(&mut self, offset: u64, length: u64) {
        if let Some(Record::Literal {
            offset: last,
            length: last_length,
        }) = self.records.back_mut()
            && *last + *last_length == offset
        {
            *last_length += length;
            return;
        }
        self.records.push_back(Record::Literal { offset, length });
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
                    self.roll = Roll::Stale;
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
    /// a Ref, or up to `B` literal bytes. A last Literal shorter than `B`
    /// waits to grow, unless `flush` is set, which the driver does before
    /// it waits, or the output is complete. A Query blocks everything
    /// behind it.
    pub fn next_frame(&mut self, flush: bool) -> Option<Frame> {
        let (frame, length) = match *self.records.front()? {
            Record::Query { .. } => return None,
            Record::Ref { .. } => {
                self.records.pop_front();
                (Frame::Reference, self.block_size)
            }
            Record::Literal { offset, length } => {
                let last = self.records.len() == 1;
                let complete = self.ended && self.p == self.end();
                if length < self.block_size && last && !flush && !complete {
                    return None;
                }
                let n = length.min(self.block_size);
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

    /// All output is emitted: stdout reached EOF, and `e = p = r`.
    pub fn finished(&self) -> bool {
        self.ended && self.e == self.end()
    }
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
            }
        }

        /// The list the sender gets: the weak hashes of every block.
        fn table(&self) -> HashTable {
            let weak = WeakHasher::new(BS as usize);
            let mut values: Vec<u64> = self.blocks.values().map(|b| weak.hash(b)).collect();
            values.sort_unstable();
            values.dedup();
            HashTable::build(&values)
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
            }
            let c = self.output.len() as u64;
            self.pins.retain(|&offset, _| offset >= c);
        }
    }

    /// A transfer driven by `choices`, which decide chunk sizes, answer
    /// order and timing, and data-queue room; they are reused cyclically.
    struct Run<'a> {
        scanner: Scanner,
        receiver: Receiver,
        input: &'a [u8],
        pushed: usize,
    }

    impl<'a> Run<'a> {
        fn new(input: &'a [u8], receiver: Receiver, lookahead: u64, negative_cache: u64) -> Self {
            let scanner = Scanner::new(
                receiver.table(),
                BS,
                StrongHash::Blake3,
                lookahead,
                negative_cache,
            );
            Run {
                scanner,
                receiver,
                input,
                pushed: 0,
            }
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

        fn take_frame(&mut self, flush: bool) -> bool {
            match self.scanner.next_frame(flush) {
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
                for _ in 0..choose(4) {
                    if self.receiver.pending.is_empty() {
                        break;
                    }
                    let index = choose(self.receiver.pending.len());
                    self.receiver.answer(index, &mut self.scanner);
                    progress = true;
                }
                progress |= self.scan();
                // A data queue with room for 0 to 3 frames.
                for _ in 0..choose(4) {
                    progress |= self.take_frame(false);
                }
                if progress {
                    continue;
                }
                // Nothing moved: emit a short literal, as before a wait,
                // or let the slowest event happen. A scanner that needs
                // none of them has stalled.
                let moved = self.take_frame(true)
                    || (!self.receiver.pending.is_empty() && {
                        self.receiver.answer(0, &mut self.scanner);
                        true
                    })
                    || self.push(1)
                    || self.take_frame(false);
                assert!(moved, "the scanner stalled");
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
                    while self.take_frame(false) {
                        progress = true;
                    }
                    if !progress && !self.take_frame(true) {
                        break;
                    }
                }
                assert!(pushed || self.scanner.finished(), "the scanner stalled");
            }
        }
    }

    /// Transfer `input` with prompt answers; return the receiver.
    fn transfer_promptly(input: &[u8], blocks: &[Vec<u8>], stale: &[Vec<u8>]) -> Receiver {
        let mut run = Run::new(input, Receiver::new(blocks, stale), 1 << 20, 1 << 20);
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
        let mut scanner = Scanner::new(receiver.table(), BS, StrongHash::Blake3, 1 << 20, 1 << 20);
        scanner.push(&input);
        scanner.end_input();
        while scanner.scan(&mut |_, _| panic!("no query without a list")) {}
        let mut output = Vec::new();
        while let Some(frame) = scanner.next_frame(false) {
            let Frame::Literal(bytes) = frame else {
                panic!("a reference without a list");
            };
            assert!(bytes.len() <= BS as usize);
            output.extend_from_slice(&bytes);
        }
        assert_eq!(output, input);
        assert!(scanner.finished());
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
        let mut run = Run::new(&input, receiver, 1 << 20, 0);
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
        let mut run = Run::new(&input, Receiver::new(&blocks, &[]), BS.into(), 1 << 20);
        run.transfer(&[7, 2, 200, 1, 3, 3, 9]);
        assert_eq!(run.receiver.output, input);
        assert_eq!(run.receiver.queries.len(), 5);
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
        let mut run = Run::new(&input, Receiver::new(&valid, &stale), 1 << 20, 1 << 20);
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
        while run.scan() || run.take_frame(true) {}
        assert!(run.scanner.finished());
        // V's answer is obsolete now: the receiver answers no, and the
        // scanner ignores it.
        assert!(run.receiver.output.len() > 36);
        run.receiver.answer(0, &mut run.scanner);
        assert_eq!(run.receiver.output, input);
    }

    /// Input pieces: noise, whole blocks, parts of blocks, and the piece
    /// of `overlapping_blocks`.
    fn input_strategy() -> impl Strategy<Value = (Vec<(u8, u8)>, u64)> {
        (
            proptest::collection::vec((any::<u8>(), any::<u8>()), 0..40),
            any::<u64>(),
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]

        /// Byte-exact reconstruction under any chunking, answer order and
        /// delay, rejection, rescan, and backpressure (DESIGN.md §11).
        #[test]
        fn output_is_reproduced_exactly(
            (pieces, seed) in input_strategy(),
            choices in proptest::collection::vec(any::<u8>(), 1..64),
            lookahead_windows in 1u64..6,
            negative in any::<bool>(),
        ) {
            let (valid, stale, piece) = overlapping_blocks(seed);
            let mut input = Vec::new();
            for (kind, n) in pieces {
                let block = match kind % 4 {
                    0 => &valid[usize::from(n) % valid.len()],
                    1 => &stale[usize::from(n) % stale.len()],
                    2 => {
                        input.extend_from_slice(&noise(usize::from(n % 50), seed ^ u64::from(n)));
                        continue;
                    }
                    _ => {
                        input.extend_from_slice(&piece);
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
            let mut run = Run::new(&input, receiver, lookahead_windows * u64::from(BS), negative_cache);
            run.transfer(&choices);
            prop_assert_eq!(&run.receiver.output, &input);
            prop_assert!(run.scanner.records.is_empty() && run.scanner.entries.is_empty());
        }
    }
}

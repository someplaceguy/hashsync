// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! The receiver's pins (DESIGN.md §6.4): the verified blocks that yes
//! answers promise, kept in extents of segments (DESIGN.md §6.5) until
//! the output passes their query offsets.
//!
//! The pins of one block, by full strong hash, share one extent, so a
//! query for a pinned block needs no lookup. Its pin shares the extent,
//! with no write, unless the share would keep more segments open, or
//! more bytes, than an extent of the pin's own would. Then the pin gets a
//! copy of the block in a new extent, which replaces the old one for all
//! pins of the block.
//!
//! One lock protects the output offset `c`, the published pins, the pinned
//! blocks, the open segments of the pins, the cache of verified blocks, and
//! the receiver's sender of the control queue, whose drop marks Completing.
//! So a lookup either publishes its pin before the output passes the pin's
//! offset, and the output detaches the pin later, or it sees the obsolete
//! offset and releases its extent at once. An extent is released after the
//! write of its lookup, when the output detaches the last pin of its block,
//! or when a copy replaces it. A read of a released extent, for REF or for
//! a copy, still gets its bytes: the read holds the file of the segment
//! open, and the bytes of an extent never change.
//!
//! The release of the last extent in use of a sealed segment gives its
//! file back, and the thread that released the extent closes it, unless a
//! read still holds the file: then the file closes at the end of the read.
//! Source reads, segment reads, writes, and closes, and the writes of the
//! destination happen outside the lock. The one exception is the creation
//! of a segment, which happens under the lock, once per segment. Answers
//! are queued under the lock, until Completing.
//!
//! At the normal end, after the lookups have ended, `close` closes all
//! open segments of the pins, which frees their storage (DESIGN.md §6.9).
//!
//! Pin storage failures end the run with `die` (DESIGN.md §6.7): a yes
//! answer cannot be taken back.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam_channel::{Receiver, Sender, bounded};
use lru::LruCache;

use crate::hash::{Strong, StrongHash};
use crate::protocol::{Answer, ControlFrame, Misses};
use crate::segments::{Directory, Extent, SEGMENT_MINIMUM, Segments};

/// A verified block: exactly the buffer whose digest was checked. The
/// cache and a lookup share it until the lookup has written it.
pub type Block = Arc<Vec<u8>>;

/// Why the lookup of a query's block in the index found no block that
/// verified (DESIGN.md §6.4, steps 2 to 5). The summary counts the two
/// apart (`Misses`), so that a stale index does not look like a weak hash
/// with false positives.
#[derive(Debug)]
pub enum Miss {
    /// No candidate: no block of the index has the strong hash, so the weak
    /// hash of the query's window is in the list only by chance.
    NotIndexed,
    /// Candidates, but none of those that the lookup tried, at most
    /// `candidate_attempts`, verified: their files changed or could not be
    /// read, for example.
    Unverified,
}

/// The bytes of verified blocks that the cache holds, or one block when a
/// block is larger (DESIGN.md §6.4).
const CACHE_BYTES: u64 = 256 << 20;

/// The pins detached per acquisition of the lock when the output advances,
/// so that other threads never wait long for the lock.
const DETACH_BATCH: usize = 256;

/// Read the block at `offset` of the segment file `file` into `buf`, and
/// require the full digest `strong` of that buffer. EOF is `UnexpectedEof`.
fn read_block(
    file: &File,
    offset: u64,
    buf: &mut [u8],
    hash: StrongHash,
    strong: &Strong,
) -> std::io::Result<()> {
    file.read_exact_at(buf, offset)?;
    if hash.digest(buf) != *strong {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the block does not match its digest",
        ));
    }
    Ok(())
}

/// A pinned block: the block of one or more published pins.
struct Pinned {
    /// The extent of the block, which all its pins share.
    extent: Extent,
    /// `last`: the highest query offset of the pins of the block. The
    /// output detaches the pins in offset order, so the pin at `last` is
    /// the last pin of the block, and the extent is released with it.
    last: u64,
}

/// Where a lookup takes the block that it writes to a new extent.
enum Source {
    /// The cache of verified blocks.
    Cache(Block),
    /// The extent of the pinned block, which a copy reads back: the file of
    /// its segment, and the offset of the extent in that file.
    Pinned(Arc<File>, u64),
    /// The candidates of the index, which `find` reads.
    Index,
}

struct State {
    /// `c`: the decoder has written the stream up to here.
    output: u64,
    /// The published pins, by query offset: the full strong hash of the
    /// block of each.
    pins: BTreeMap<u64, Strong>,
    /// The blocks of the published pins, by full strong hash.
    pinned: HashMap<Strong, Pinned>,
    /// The open segments of the pins, which hold their extents.
    segments: Segments,
    /// Verified blocks by full strong hash. Pins never hold them.
    cache: LruCache<Strong, Block>,
    /// The queue of the control writer, for answers; `None` in Completing.
    answers: Option<Sender<ControlFrame>>,
    /// The sending end of `Pins::completing`, which nothing sends to; `None`
    /// in Completing.
    completing: Option<Sender<()>>,
    /// The queries answered no after a lookup in the index, by the reason,
    /// for the summary (DESIGN.md §8).
    misses: Misses,
}

impl State {
    /// Completing (DESIGN.md §6.4): the lookups neither append, publish,
    /// nor answer.
    fn completing(&self) -> bool {
        self.answers.is_none()
    }

    /// Queue an answer; in Completing, nothing is answered.
    fn answer(&self, offset: u64, found: bool) {
        if let Some(answers) = &self.answers {
            // The control writer ends only once every producer has dropped
            // its sender, and this one is still here.
            let _ = answers.send(ControlFrame::Answer(Answer { offset, found }));
        }
    }

    /// Answer no to a query whose block the lookup in the index did not
    /// find, and count the miss. Only a query that gets its answer counts,
    /// so the counts stop changing at Completing, and the host can read them
    /// once the stream has ended or stopped, while a lookup still runs.
    fn answer_miss(&mut self, offset: u64, miss: Miss) {
        if !self.completing() {
            match miss {
                Miss::NotIndexed => self.misses.false_positives += 1,
                Miss::Unverified => self.misses.not_found += 1,
            }
            self.answer(offset, false);
        }
    }

    /// Publish the pin at `offset` of the block with the full strong hash
    /// `strong`, which is pinned, and answer yes. Two queries at one offset
    /// are a protocol error.
    fn pin(&mut self, offset: u64, strong: &Strong) {
        let previous = self.pins.insert(offset, *strong);
        assert!(
            previous.is_none(),
            "protocol error: two queries at offset {offset}"
        );
        self.answer(offset, true);
    }

    /// Publish the pin at `offset` on the extent of its block `strong`, if
    /// the block is pinned and the share keeps no more segments open, and
    /// no more bytes, than an extent of the pin's own would (DESIGN.md
    /// §6.4). Returns whether the pin is published.
    ///
    /// Below `last`, the extent stays in use until the output passes
    /// `last` anyway. Above `last`, the share keeps the extent in use until
    /// the output passes the pin, and `last` moves up to the pin. If that
    /// costs more than a new extent would, the pin gets a copy instead: a
    /// share would keep a sealed segment open for as long as the block
    /// recurs.
    fn share(&mut self, offset: u64, strong: &Strong) -> bool {
        let Some(pinned) = self.pinned.get_mut(strong) else {
            return false;
        };
        if offset > pinned.last {
            if self.segments.costs_more_to_keep(&pinned.extent) {
                return false;
            }
            pinned.last = offset;
        }
        self.pin(offset, strong);
        true
    }
}

/// The pins and the state that they share with the decoder.
pub struct Pins {
    /// The tmp_data directory, which the pins share with the spools of this
    /// host.
    dir: Arc<Directory>,
    strong: StrongHash,
    /// The bytes of a block, for the buffer of a copy.
    block_size: usize,
    state: Mutex<State>,
    /// Disconnected at Completing, so that a lookup worker that waits for a
    /// query wakes up then.
    completing: Receiver<()>,
}

impl Pins {
    /// Pins of `block_size` bytes in segments of the tmp_data directory
    /// `dir` (DESIGN.md §6.4). Answers go to `answers`, the queue of the
    /// control writer.
    pub fn new(
        dir: Arc<Directory>,
        strong: StrongHash,
        block_size: u32,
        answers: Sender<ControlFrame>,
    ) -> Self {
        Self::with_minimum(dir, strong, block_size, answers, SEGMENT_MINIMUM)
    }

    /// `new`, with a current segment that is full at `minimum` bytes while
    /// no sealed segment is open.
    fn with_minimum(
        dir: Arc<Directory>,
        strong: StrongHash,
        block_size: u32,
        answers: Sender<ControlFrame>,
        minimum: u64,
    ) -> Self {
        let blocks = usize::try_from(CACHE_BYTES / u64::from(block_size)).unwrap_or(usize::MAX);
        let (completing_tx, completing) = bounded(0);
        Pins {
            dir,
            strong,
            block_size: block_size as usize,
            state: Mutex::new(State {
                output: 0,
                pins: BTreeMap::new(),
                pinned: HashMap::new(),
                segments: Segments::new(u64::from(block_size), minimum),
                cache: LruCache::sparse(NonZeroUsize::new(blocks).unwrap_or(NonZeroUsize::MIN)),
                answers: Some(answers),
                completing: Some(completing_tx),
                misses: Misses::default(),
            }),
            completing,
        }
    }

    /// The bytes of a block.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Answer `QUERY { offset, strong }` (DESIGN.md §6.4, steps 1 to 8).
    /// The pin of a pinned block shares its extent, with no lookup and no
    /// write, or gets a copy: from the cache of verified blocks, or from
    /// the extent of the block, read back and verified outside the lock.
    /// Any other block comes from the cache, or from `find`, which reads
    /// and verifies candidates outside the lock. A copy or a found block
    /// goes to a new extent; then the pin is published and answered yes,
    /// unless the output passed `offset` in the meantime. An obsolete
    /// query is answered no, and so is a query whose block `find` did not
    /// find; in Completing, nothing is answered.
    pub fn answer(&self, offset: u64, strong: &Strong, find: impl FnOnce() -> Result<Block, Miss>) {
        let source = {
            let mut state = self.lock();
            if state.completing() {
                return;
            }
            if offset < state.output {
                return state.answer(offset, false);
            }
            if state.share(offset, strong) {
                return;
            }
            if let Some(block) = state.cache.get(strong) {
                Source::Cache(Arc::clone(block))
            } else if let Some(pinned) = state.pinned.get(strong) {
                let extent = &pinned.extent;
                Source::Pinned(Arc::clone(extent.file()), extent.offset())
            } else {
                Source::Index
            }
        };
        let (block, read) = match source {
            Source::Cache(block) => (block, false),
            // A pinned block needs no lookup in the index.
            Source::Pinned(file, at) => {
                let mut buf = vec![0; self.block_size];
                self.read_pin(&file, at, &mut buf, strong, offset);
                (Arc::new(buf), true)
            }
            Source::Index => match find() {
                Ok(block) => (block, true),
                Err(miss) => return self.lock().answer_miss(offset, miss),
            },
        };
        let extent = {
            let mut state = self.lock();
            // Verified reads populate the cache.
            if read {
                state.cache.put(*strong, Arc::clone(&block));
            }
            if state.completing() {
                return;
            }
            if offset < state.output {
                return state.answer(offset, false);
            }
            // Another lookup can have published a pin of the block, or a
            // copy of it, in the meantime.
            if state.share(offset, strong) {
                return;
            }
            // Without a current segment, this creates a file: the only
            // segment I/O under the lock.
            self.dir.allocate(&mut state.segments)
        };
        // Concurrent workers write only their own extents.
        if let Err(e) = extent.file().write_all_at(&block, extent.offset()) {
            self.dir.fail("write", e);
        }
        drop(block);
        self.publish(offset, strong, extent);
    }

    /// The end of a lookup (DESIGN.md §6.4, step 8): `extent` holds the
    /// block of the query at `offset`. Publish the pin and answer yes.
    /// All pins of the block keep the extent of the pin with the highest
    /// offset: above `last`, `extent` replaces the extent of the block, as
    /// a copy. Below `last`, after another lookup published a pin of the
    /// block above `offset`, the pin shares the extent of the block. The
    /// extent that the pins do not keep is released. If the output passed
    /// `offset` during the write, answer no and release `extent`. In
    /// Completing, release it without an answer.
    fn publish(&self, offset: u64, strong: &Strong, extent: Extent) {
        let mut state = self.lock();
        let unused = if state.completing() || offset < state.output {
            // In Completing, this queues nothing.
            state.answer(offset, false);
            Some(extent)
        } else {
            let unused = match state.pinned.entry(*strong) {
                Entry::Vacant(entry) => {
                    entry.insert(Pinned {
                        extent,
                        last: offset,
                    });
                    None
                }
                Entry::Occupied(entry) if offset < entry.get().last => Some(extent),
                Entry::Occupied(mut entry) => {
                    let pinned = entry.get_mut();
                    pinned.last = offset;
                    Some(std::mem::replace(&mut pinned.extent, extent))
                }
            };
            state.pin(offset, strong);
            unused
        };
        let closed = unused.map(|extent| state.segments.release(extent));
        drop(state);
        drop(closed);
    }

    /// Read the pin at the output offset into `buf` for REF, and verify
    /// the buffer against the pin's full digest (DESIGN.md §6.4). The pin
    /// stays published until `reached` moves the output past it. A missing
    /// pin is a protocol error; a pin that cannot be read back intact ends
    /// the run.
    pub fn read_reference(&self, buf: &mut [u8]) {
        let (output, strong, file, at) = {
            let state = self.lock();
            let strong = *state
                .pins
                .get(&state.output)
                .unwrap_or_else(|| panic!("protocol error: REF without a pin at {}", state.output));
            let extent = &state.pinned[&strong].extent;
            (
                state.output,
                strong,
                Arc::clone(extent.file()),
                extent.offset(),
            )
        };
        self.read_pin(&file, at, buf, &strong, output);
    }

    /// Read the block with the full digest `strong` from the extent at `at`
    /// of the segment file `file` into `buf`, for the pin at `offset`, and
    /// verify the buffer. A pin that cannot be read back intact ends the
    /// run.
    fn read_pin(&self, file: &File, at: u64, buf: &mut [u8], strong: &Strong, offset: u64) {
        if let Err(e) = read_block(file, at, buf, self.strong, strong) {
            self.dir
                .fail("read", format_args!("the pin at offset {offset}: {e}"));
        }
    }

    /// The decoder wrote the stream up to `offset`: publish it, and
    /// detach the pins below it, in batches. A detached pin at `last` of
    /// its block releases the extent of the block. After each batch, close
    /// the segments that it removed from the open segments, outside the
    /// lock (DESIGN.md §6.4 and §6.5).
    pub fn reached(&self, offset: u64) {
        let mut state = self.lock();
        assert!(offset >= state.output, "the output offset went back");
        state.output = offset;
        loop {
            let mut closed = Vec::new();
            let mut detached = 0;
            while detached < DETACH_BATCH
                && let Some(pin) = state.pins.first_entry()
                && *pin.key() < offset
            {
                let (at, strong) = pin.remove_entry();
                let Entry::Occupied(pinned) = state.pinned.entry(strong) else {
                    unreachable!("the pin at {at} has no pinned block");
                };
                if pinned.get().last == at {
                    let extent = pinned.remove().extent;
                    closed.extend(state.segments.release(extent).into_iter().flatten());
                }
                detached += 1;
            }
            drop(state);
            drop(closed);
            if detached < DETACH_BATCH {
                return;
            }
            state = self.lock();
        }
    }

    /// Enter Completing (DESIGN.md §6.4), once the stream has ended or
    /// stopped: no lookup reads, publishes, or answers any more. The pins
    /// drop their sender of the control queue, so the control writer can
    /// end once the other producers are gone, and the lookup workers that
    /// wait for a query wake up and end. A second call does nothing.
    pub fn complete(&self) {
        let mut state = self.lock();
        state.answers = None;
        state.completing = None;
    }

    /// A receiver that disconnects at Completing, for a lookup worker that
    /// waits for its next query.
    pub fn completing(&self) -> Receiver<()> {
        self.completing.clone()
    }

    /// True in Completing: the stream has ended or stopped, and the lookups
    /// have no use any more, nor has the list exchange of the stream
    /// (DESIGN.md §5.6).
    pub fn is_completing(&self) -> bool {
        self.lock().completing()
    }

    /// The queries that the lookups answered no after they looked their
    /// blocks up in the index, by the reason (DESIGN.md §8). The counts are
    /// final once the stream has ended or stopped: from Completing on, no
    /// query gets an answer.
    pub fn misses(&self) -> Misses {
        self.lock().misses
    }

    /// The normal end of the pins, after the lookups have ended
    /// (DESIGN.md §6.9): close all open segments of the pins, which frees
    /// their storage. The tmp_data directory closes with its last user.
    pub fn close(self) {
        // This function exists so that no thread still holds the pins when
        // the caller closes them. It takes the pins by value, so the caller
        // must get them out of the `Arc` that it shares with its threads.
        // `Arc::into_inner` does that, and fails while a thread still holds
        // them. A `drop` of the `Arc` succeeds then, and closes nothing.
        //
        // The lookups, which hold the extents that they write, have ended:
        // only the pinned blocks hold the files.
        drop(self);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::os::unix::ffi::OsStrExt;
    use std::path::PathBuf;

    use crossbeam_channel::{Receiver, TryRecvError, unbounded};

    use super::*;
    use crate::segments::{self, descriptors};

    const BS: u32 = 4096;

    struct Setup {
        pins: Pins,
        answers: Receiver<ControlFrame>,
        /// The tmp_data directory of the pins.
        tmp_data: PathBuf,
        _dir: tempfile::TempDir,
    }

    /// The pins of a run.
    fn setup(block_size: u32) -> Setup {
        setup_with(|dir, answers| Pins::new(dir, StrongHash::Blake3, block_size, answers))
    }

    /// Pins whose current segment is full at `minimum` bytes while no
    /// sealed segment is open.
    fn setup_with_minimum(block_size: u32, minimum: u64) -> Setup {
        setup_with(|dir, answers| {
            Pins::with_minimum(dir, StrongHash::Blake3, block_size, answers, minimum)
        })
    }

    /// The pins that `new` makes in a new tmp_data directory, which only
    /// the pins use.
    fn setup_with(new: impl FnOnce(Arc<Directory>, Sender<ControlFrame>) -> Pins) -> Setup {
        let dir = tempfile::tempdir().unwrap();
        let tmp_data = dir.path().join("tmp-data");
        let (tx, answers) = unbounded();
        let pins = new(
            Arc::new(Directory::open(tmp_data.as_os_str().as_bytes())),
            tx,
        );
        assert_eq!(
            std::fs::read_dir(&tmp_data).unwrap().count(),
            0,
            "the segment that the opening creates has no name"
        );
        Setup {
            pins,
            answers,
            tmp_data: std::fs::canonicalize(tmp_data).unwrap(),
            _dir: dir,
        }
    }

    /// A block that is different for each `seed`: the seed is in its first
    /// bytes.
    fn block(seed: u64, block_size: u32) -> (Strong, Block) {
        let mut bytes: Vec<u8> = (0..block_size)
            .map(|i| (i as u8).wrapping_mul(31))
            .collect();
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        (StrongHash::Blake3.digest(&bytes), Arc::new(bytes))
    }

    /// `count` different blocks.
    fn blocks(count: u64, block_size: u32) -> Vec<(Strong, Block)> {
        (0..count).map(|seed| block(seed, block_size)).collect()
    }

    /// The `find` of a query that must not look its block up in the index.
    fn no_lookup() -> Result<Block, Miss> {
        panic!("a lookup of a pinned block")
    }

    fn answers(setup: &Setup) -> Vec<(u64, bool)> {
        setup.answers.try_iter().map(answer).collect()
    }

    /// The offset and the result of the answer `frame`.
    fn answer(frame: ControlFrame) -> (u64, bool) {
        let ControlFrame::Answer(answer) = frame else {
            panic!("the pins queued another frame than ANSWER");
        };
        (answer.offset, answer.found)
    }

    fn read_back(setup: &Setup, block_size: u32) -> Vec<u8> {
        let mut buf = vec![0; block_size as usize];
        setup.pins.read_reference(&mut buf);
        buf
    }

    /// Pin `blocks` in order, each at its query offset in `offsets`: the
    /// extents follow the order of `blocks`.
    fn pin_all(setup: &Setup, offsets: impl IntoIterator<Item = u64>, blocks: &[(Strong, Block)]) {
        for (offset, (strong, data)) in offsets.into_iter().zip(blocks) {
            setup.pins.answer(offset, strong, || Ok(Arc::clone(data)));
        }
    }

    /// Allocate an extent as a lookup does for a verified block, without
    /// the write and the publication.
    fn allocate(setup: &Setup) -> Extent {
        let mut state = setup.pins.lock();
        setup.pins.dir.allocate(&mut state.segments)
    }

    /// The extent of the pin at the query offset `offset`: the position of
    /// its segment in the open segments, oldest first, and its offset in
    /// that segment.
    fn extent(setup: &Setup, offset: u64) -> (usize, u64) {
        let state = setup.pins.lock();
        let extent = &state.pinned[&state.pins[&offset]].extent;
        let open = state.segments.open();
        let position = open
            .iter()
            .position(|&(file, _)| file == Arc::as_ptr(extent.file()));
        (
            position.expect("a pin in a closed segment"),
            extent.offset(),
        )
    }

    /// The open segments of the pins, oldest first: the count of the
    /// extents in use of each one.
    fn open_segments(setup: &Setup) -> Vec<usize> {
        segments::open_segments(&setup.pins.lock().segments, &setup.tmp_data)
    }

    #[test]
    fn a_found_block_is_pinned_answered_and_released() {
        let setup = setup(BS);
        let (strong, data) = block(1, BS);
        setup.pins.answer(0, &strong, || Ok(Arc::clone(&data)));
        assert_eq!(answers(&setup), vec![(0, true)]);
        assert_eq!(read_back(&setup, BS), *data);
        assert_eq!(open_segments(&setup), [1]);
        setup.pins.reached(u64::from(BS));
        assert!(setup.pins.lock().pins.is_empty());
        assert!(setup.pins.lock().pinned.is_empty());
        // The current segment is not full: it stays open without extents
        // in use.
        assert_eq!(open_segments(&setup), [0]);
    }

    #[test]
    fn the_pins_of_a_block_share_its_extent() {
        // Queries for a pinned block need no lookup, no read, and no write.
        // Below the highest pin of the block, and above it while the extent
        // is in the current segment, the pins share the extent. The extent
        // is released with the highest pin.
        let setup = setup(BS);
        let (strong, data) = block(2, BS);
        setup.pins.answer(10, &strong, || Ok(Arc::clone(&data)));
        // A read of the block would put it in the cache again.
        setup.pins.lock().cache.clear();
        setup.pins.answer(30, &strong, no_lookup);
        setup.pins.answer(20, &strong, no_lookup);
        assert!(setup.pins.lock().cache.is_empty(), "a share read the block");
        assert_eq!(answers(&setup), [10, 30, 20].map(|offset| (offset, true)));
        for offset in [10, 20, 30] {
            assert_eq!(extent(&setup, offset), (0, 0));
        }
        for offset in [10, 20, 30] {
            setup.pins.reached(offset);
            assert_eq!(read_back(&setup, BS), *data);
            assert_eq!(open_segments(&setup), [1]);
        }
        setup.pins.reached(31);
        assert!(setup.pins.lock().pinned.is_empty());
        assert_eq!(open_segments(&setup), [0]);
    }

    #[test]
    fn a_verified_block_is_cached_for_later_pins() {
        // The output passes the only pin of a block. A later query for the
        // block takes it from the cache of verified blocks, with no lookup,
        // and writes it to a new extent: the extents follow each other
        // without gaps.
        let setup = setup(BS);
        let (strong, data) = block(2, BS);
        setup.pins.answer(10, &strong, || Ok(Arc::clone(&data)));
        setup.pins.reached(20);
        setup
            .pins
            .answer(20, &strong, || panic!("the cache must answer"));
        assert_eq!(answers(&setup), vec![(10, true), (20, true)]);
        assert_eq!(extent(&setup, 20), (0, u64::from(BS)));
        assert_eq!(read_back(&setup, BS), *data);
    }

    #[test]
    fn a_pinned_block_in_a_sealed_segment_gets_a_copy_above_its_pins() {
        // One extent fills a segment while no sealed segment is open, and
        // two while one is. The first pin of block `h` fills the first
        // segment, and the pin of block `b` goes to the second one. A query
        // for `h` above its pins gets a copy of `h` from the cache, which
        // fills the second segment. The copy replaces the extent of `h` for
        // all its pins, so the first segment closes. Without `h` in the
        // cache, the next copy reads `h` back from its extent, and goes to
        // a third segment. A query below the highest pin of `h` shares the
        // extent. No query for `h` after the first one looks it up.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, size);
        let (h, data) = block(1, BS);
        let (b, other) = block(2, BS);
        setup.pins.answer(0, &h, || Ok(Arc::clone(&data)));
        setup.pins.answer(size, &b, || Ok(Arc::clone(&other)));
        assert_eq!(open_segments(&setup), [1, 1]);
        setup.pins.answer(2 * size, &h, no_lookup);
        assert_eq!(open_segments(&setup), [2]);
        assert_eq!(extent(&setup, 0), (0, size));
        // The cache still holds the buffer of the first lookup.
        let cached = setup.pins.lock().cache.peek(&h).cloned().unwrap();
        assert!(Arc::ptr_eq(&cached, &data), "the copy read the extent");
        setup.pins.lock().cache.clear();
        setup.pins.answer(4 * size, &h, no_lookup);
        setup.pins.answer(3 * size, &h, no_lookup);
        assert_eq!(open_segments(&setup), [1, 1]);
        for i in [0, 2, 3, 4] {
            assert_eq!(extent(&setup, i * size), (1, 0));
        }
        assert!(setup.pins.lock().cache.contains(&h), "a verified read");
        assert_eq!(answers(&setup), [0, 1, 2, 4, 3].map(|i| (i * size, true)));
        for (i, expected) in (1..).zip([&data, &other, &data, &data, &data]) {
            assert_eq!(read_back(&setup, BS), **expected);
            setup.pins.reached(i * size);
        }
        assert_eq!(open_segments(&setup), []);
    }

    #[test]
    fn a_block_alone_in_a_sealed_segment_is_shared_while_a_copy_would_be_alone() {
        // One extent fills a segment while no sealed segment is open, two
        // while one is, and four while two are. The pin of block `h` fills
        // the first segment alone, and a copy of `h` would fill a new
        // segment alone once the first one closes. So the query for `h`
        // above its pin shares the extent, with no read and no write.
        // Blocks `b` and `c` then fill a second segment. A copy of `h` would
        // now go to a segment that gets other extents too, so the next
        // query for `h` gets a copy, read back from the extent of `h`, and
        // the first segment closes.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, size);
        let (h, data) = block(1, BS);
        setup.pins.answer(0, &h, || Ok(Arc::clone(&data)));
        // A read of the block would put it in the cache again.
        setup.pins.lock().cache.clear();
        setup.pins.answer(size, &h, no_lookup);
        assert_eq!(open_segments(&setup), [1]);
        assert_eq!(extent(&setup, size), (0, 0));
        assert!(setup.pins.lock().cache.is_empty(), "a share read the block");
        let (b, other) = block(2, BS);
        let (c, more) = block(3, BS);
        setup.pins.answer(2 * size, &b, || Ok(Arc::clone(&other)));
        setup.pins.answer(3 * size, &c, || Ok(Arc::clone(&more)));
        assert_eq!(open_segments(&setup), [1, 2]);
        setup.pins.answer(4 * size, &h, no_lookup);
        assert_eq!(open_segments(&setup), [2, 1]);
        for i in [0, 1, 4] {
            assert_eq!(extent(&setup, i * size), (1, 0));
        }
        assert!(setup.pins.lock().cache.contains(&h), "a verified read");
        assert_eq!(answers(&setup), [0, 1, 2, 3, 4].map(|i| (i * size, true)));
        for (i, expected) in (1..).zip([&data, &data, &other, &more, &data]) {
            assert_eq!(read_back(&setup, BS), **expected);
            setup.pins.reached(i * size);
        }
        assert_eq!(open_segments(&setup), []);
    }

    #[test]
    fn a_pin_published_during_a_lookup_is_shared() {
        // While the lookup of the query at 10 reads its candidates, the
        // lookup of the query at 20, for the same block, publishes its pin.
        // The query at 10 then shares the extent of that pin, with no
        // write: the extent of the next block follows at once.
        let setup = setup(BS);
        let (strong, data) = block(4, BS);
        setup.pins.answer(10, &strong, || {
            setup.pins.answer(20, &strong, || Ok(Arc::clone(&data)));
            Ok(Arc::clone(&data))
        });
        let (other, more) = block(5, BS);
        setup.pins.answer(30, &other, || Ok(Arc::clone(&more)));
        assert_eq!(answers(&setup), vec![(20, true), (10, true), (30, true)]);
        assert_eq!(extent(&setup, 10), (0, 0));
        assert_eq!(extent(&setup, 20), (0, 0));
        assert_eq!(extent(&setup, 30), (0, u64::from(BS)));
    }

    #[test]
    fn racing_lookups_keep_the_extent_of_the_highest_pin() {
        // Three lookups write one block to new extents, and none of them
        // sees the pins of the others before it publishes its own. At each
        // publication, all pins of the block keep the extent of the pin
        // with the highest offset, and the other extent is released. One
        // extent fills a segment while no sealed segment is open, and two
        // while one is.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, size);
        let (strong, data) = block(6, BS);
        let written = |setup: &Setup| {
            let extent = allocate(setup);
            extent.file().write_all_at(&data, extent.offset()).unwrap();
            extent
        };
        let (high, low) = (written(&setup), written(&setup));
        setup.pins.publish(2 * size, &strong, high);
        setup.pins.publish(size, &strong, low);
        // The extent of the low pin was in the current segment, which stays
        // open.
        assert_eq!(open_segments(&setup), [1, 0]);
        assert_eq!(extent(&setup, size), (0, 0));
        let top = written(&setup);
        setup.pins.publish(3 * size, &strong, top);
        assert_eq!(open_segments(&setup), [1]);
        for i in 1..4 {
            assert_eq!(extent(&setup, i * size), (0, size));
        }
        assert_eq!(answers(&setup), [2, 1, 3].map(|i| (i * size, true)));
        setup.pins.reached(size);
        for i in 2..5 {
            assert_eq!(read_back(&setup, BS), *data);
            setup.pins.reached(i * size);
        }
        assert_eq!(open_segments(&setup), []);
    }

    #[test]
    fn nothing_found_and_obsolete_queries_are_answered_no() {
        // Candidates that do not verify, and no candidate at all, both get
        // a no. The summary counts the first as not found, and the second as
        // a false positive of the weak hash, also when the output passes its
        // query during the lookup. A query that is obsolete before its
        // lookup gets none, and counts in neither (DESIGN.md §8).
        let setup = setup(BS);
        let (strong, data) = block(3, BS);
        let (unknown, _) = block(4, BS);
        setup.pins.answer(0, &strong, || Err(Miss::Unverified));
        setup.pins.answer(10, &unknown, || Err(Miss::NotIndexed));
        setup.pins.reached(100);
        setup
            .pins
            .answer(50, &strong, || panic!("no lookup below the output"));
        // The output passes the queries while their candidates are read.
        // The block that the first one finds goes to the cache.
        setup.pins.answer(200, &strong, || {
            setup.pins.reached(300);
            Ok(Arc::clone(&data))
        });
        setup.pins.answer(400, &unknown, || {
            setup.pins.reached(500);
            Err(Miss::NotIndexed)
        });
        let no = [0, 10, 50, 200, 400].map(|offset| (offset, false));
        assert_eq!(answers(&setup), no);
        let misses = Misses {
            false_positives: 2,
            not_found: 1,
        };
        assert_eq!(setup.pins.misses(), misses);
        assert!(setup.pins.lock().pins.is_empty());
        // No query got an extent, so no segment was created.
        assert_eq!(open_segments(&setup), []);
    }

    #[test]
    fn completing_suppresses_answers_and_pins() {
        // Three lookups read their candidates when the stream ends: one
        // finds its block, one finds no candidate, and one finds candidates
        // that do not verify. None of them answers, the first publishes no
        // pin, and the others count as neither a false positive nor not
        // found: the counts are final at Completing (DESIGN.md §8).
        let setup = setup(BS);
        let (strong, data) = block(4, BS);
        let (other, _) = block(5, BS);
        let (third, _) = block(6, BS);
        let size = u64::from(BS);
        setup.pins.answer(0, &strong, || {
            setup.pins.answer(size, &other, || {
                setup.pins.answer(2 * size, &third, || {
                    setup.pins.complete();
                    Err(Miss::Unverified)
                });
                Err(Miss::NotIndexed)
            });
            Ok(Arc::clone(&data))
        });
        setup
            .pins
            .answer(3 * size, &strong, || panic!("no lookup in Completing"));
        assert!(answers(&setup).is_empty());
        assert!(setup.pins.lock().pins.is_empty());
        assert_eq!(setup.pins.misses(), Misses::default());
    }

    #[test]
    fn completing_drops_the_sender_of_the_pins_and_wakes_the_workers() {
        // The control writer takes the answers that the queue holds, and
        // then finds that the pins have dropped their sender. A lookup
        // worker that waits for a query wakes up (DESIGN.md §6.4).
        let setup = setup(BS);
        let (strong, data) = block(6, BS);
        setup.pins.answer(0, &strong, || Ok(Arc::clone(&data)));
        let completing = setup.pins.completing();
        assert!(matches!(completing.try_recv(), Err(TryRecvError::Empty)));
        assert!(!setup.pins.lock().completing());
        setup.pins.complete();
        setup.pins.complete();
        assert!(setup.pins.lock().completing());
        assert_eq!(answers(&setup), vec![(0, true)]);
        assert!(
            matches!(setup.answers.try_recv(), Err(TryRecvError::Disconnected)),
            "the pins kept their sender"
        );
        assert!(completing.recv().is_err(), "the workers were not woken");
    }

    #[test]
    fn each_sealed_open_segment_doubles_the_bytes_of_a_full_segment() {
        // The minimum is two extents. The first segment is full with two
        // extents. While it is open, the second one is full with four.
        // While both are open, the third one is full with eight. 5000-byte
        // extents follow each other without alignment or padding.
        for block_size in [BS, 5000] {
            let size = u64::from(block_size);
            let setup = setup_with_minimum(block_size, 2 * size);
            let blocks = blocks(8, block_size);
            pin_all(&setup, (0..).map(|i| i * size), &blocks);
            assert_eq!(open_segments(&setup), [2, 4, 2]);
            let extents: Vec<(usize, u64)> = (0..8).map(|i| extent(&setup, i * size)).collect();
            let expected = [
                (0, 0),
                (0, 1),
                (1, 0),
                (1, 1),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 1),
            ];
            assert_eq!(extents, expected.map(|(segment, i)| (segment, i * size)));
            // Each pin reads back from its segment. A sealed segment closes
            // when the output passes its last pin. The closure of the
            // second segment makes the third one full with its two
            // extents, so it is sealed, and it closes after its last pin.
            let passed: [&[usize]; 8] = [
                &[1, 4, 2],
                &[4, 2],
                &[3, 2],
                &[2, 2],
                &[1, 2],
                &[2],
                &[1],
                &[],
            ];
            for ((i, (_, data)), open) in (1..).zip(&blocks).zip(passed) {
                assert_eq!(read_back(&setup, block_size), **data);
                setup.pins.reached(i * size);
                assert_eq!(open_segments(&setup), open);
            }
        }
    }

    #[test]
    fn an_extent_in_use_keeps_its_sealed_segment_open() {
        // The query at the highest offset gets the first extent. The
        // output passes the pins of the other queries, which releases
        // their extents: the second segment closes. The first segment
        // stays open until the output passes the last pin.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, 2 * size);
        let blocks = blocks(6, BS);
        pin_all(&setup, [5, 0, 1, 2, 3, 4].map(|i| i * size), &blocks);
        assert_eq!(open_segments(&setup), [2, 4]);
        setup.pins.reached(5 * size);
        assert_eq!(open_segments(&setup), [1]);
        assert_eq!(read_back(&setup, BS), *blocks[0].1);
        setup.pins.reached(6 * size);
        assert_eq!(open_segments(&setup), []);
    }

    #[test]
    fn the_full_rule_counts_the_sealed_segments_that_are_open_now() {
        // The minimum is two extents. The second segment gets two extents
        // while the first one is open, so it is not full. Then the first
        // one closes, and the second one is full: it is sealed, and the
        // next extent goes to a third segment.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, 2 * size);
        let blocks = blocks(5, BS);
        pin_all(&setup, (0..).map(|i| i * size), &blocks[..4]);
        assert_eq!(open_segments(&setup), [2, 2]);
        setup.pins.reached(2 * size);
        assert_eq!(open_segments(&setup), [2]);
        pin_all(&setup, [4 * size], &blocks[4..]);
        assert_eq!(open_segments(&setup), [2, 1]);
        assert_eq!(extent(&setup, 4 * size), (1, 0));
    }

    #[test]
    fn a_full_segment_closes_when_the_output_passes_its_last_pin() {
        // The output keeps up with the pins. The current segment stays
        // open without extents in use until it is full, with two extents.
        // Then it is sealed, and it closes when the output passes its last
        // pin. The next extent goes to a new segment.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, 2 * size);
        let blocks = blocks(3, BS);
        let steps: [(u64, &[usize]); 3] = [(0, &[0]), (size, &[]), (0, &[0])];
        for ((i, (strong, data)), (offset, passed)) in (0..).zip(&blocks).zip(steps) {
            setup.pins.answer(i * size, strong, || Ok(Arc::clone(data)));
            assert_eq!(open_segments(&setup), [1]);
            assert_eq!(extent(&setup, i * size), (0, offset));
            assert_eq!(read_back(&setup, BS), **data);
            setup.pins.reached((i + 1) * size);
            assert_eq!(open_segments(&setup), passed);
        }
    }

    #[test]
    fn a_current_segment_without_extents_in_use_closes_when_it_becomes_full() {
        // The minimum is two extents. The queries at the lowest offsets
        // get the extents of the second segment, which is not full with
        // two extents while the first one is open. The output passes
        // them: the second segment stays open without extents in use.
        // Then the output passes the pins of the first segment, which
        // closes. Now the second segment is full, and nothing uses it: it
        // closes too. The next extent goes to a new segment.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, 2 * size);
        let blocks = blocks(5, BS);
        pin_all(&setup, [2, 3, 0, 1].map(|i| i * size), &blocks[..4]);
        assert_eq!(open_segments(&setup), [2, 2]);
        setup.pins.reached(2 * size);
        assert_eq!(open_segments(&setup), [2, 0]);
        setup.pins.reached(4 * size);
        assert_eq!(open_segments(&setup), []);
        pin_all(&setup, [4 * size], &blocks[4..]);
        assert_eq!(open_segments(&setup), [1]);
        assert_eq!(extent(&setup, 4 * size), (0, 0));
        assert_eq!(read_back(&setup, BS), *blocks[4].1);
    }

    #[test]
    fn one_advance_of_the_output_detaches_more_pins_than_one_batch() {
        // The minimum is 64 extents. The pins of different blocks fill
        // three segments, with 64, 128, and 256 extents, and a part of the
        // fourth one. One advance of the output passes all pins: it
        // detaches them in three batches, releases their extents, and
        // closes the four segments.
        let block_size = 512;
        let size = u64::from(block_size);
        let setup = setup_with_minimum(block_size, 64 * size);
        let count = 2 * DETACH_BATCH as u64 + 44;
        pin_all(&setup, (0..).map(|i| i * size), &blocks(count, block_size));
        assert_eq!(open_segments(&setup), [64, 128, 256, 108]);
        setup.pins.reached(count * size);
        assert!(setup.pins.lock().pins.is_empty());
        assert!(setup.pins.lock().pinned.is_empty());
        assert_eq!(open_segments(&setup), []);
    }

    #[test]
    fn an_obsolete_write_releases_its_extent() {
        // One extent fills the first segment, and two fill the second one.
        // While the lookup of the second query writes its block, the
        // output passes that query and the pins of the first and the third
        // query. The lookup then answers no and releases its extent, the
        // last one in use of the second segment: that segment closes.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, size);
        let blocks = blocks(4, BS);
        pin_all(&setup, [0], &blocks[..1]);
        let written = allocate(&setup);
        pin_all(&setup, [2 * size, 3 * size], &blocks[2..]);
        assert_eq!(open_segments(&setup), [1, 2, 1]);
        setup.pins.reached(3 * size);
        assert_eq!(open_segments(&setup), [1, 1]);
        setup.pins.publish(size, &blocks[1].0, written);
        assert_eq!(open_segments(&setup), [1]);
        assert_eq!(
            answers(&setup),
            vec![(0, true), (2 * size, true), (3 * size, true), (size, false)]
        );
        assert!(!setup.pins.lock().pins.contains_key(&size));
        assert_eq!(read_back(&setup, BS), *blocks[3].1);
        // The closure makes the third segment full with its one extent: it
        // is sealed, and the next extent goes to a new segment.
        pin_all(&setup, [4 * size], &blocks[..1]);
        assert_eq!(open_segments(&setup), [1, 1]);
        assert_eq!(extent(&setup, 4 * size), (1, 0));
    }

    #[test]
    fn a_write_that_ends_in_completing_releases_its_extent() {
        // One extent fills the first segment. The lookup neither publishes
        // nor answers. It releases its extent, the only one in use of the
        // segment: the segment closes.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, size);
        let (strong, _) = block(8, BS);
        let written = allocate(&setup);
        assert_eq!(open_segments(&setup), [1]);
        setup.pins.complete();
        setup.pins.publish(0, &strong, written);
        assert!(answers(&setup).is_empty());
        assert!(setup.pins.lock().pins.is_empty());
        assert_eq!(open_segments(&setup), []);
    }

    #[test]
    fn closing_the_pins_closes_all_segments() {
        // One extent fills the first segment, and two fill the second one:
        // four pins keep two sealed segments and the current segment open.
        let size = u64::from(BS);
        let setup = setup_with_minimum(BS, size);
        pin_all(&setup, (0..).map(|i| i * size), &blocks(4, BS));
        assert_eq!(open_segments(&setup), [1, 2, 1]);
        setup.pins.close();
        assert_eq!(
            descriptors(&setup.tmp_data),
            (0, 0),
            "the tmp_data directory or a segment is still open"
        );
    }

    #[test]
    fn the_first_segment_of_a_run_is_full_at_16_mib() {
        // Different blocks of 1 MiB, in the pins of a run: sixteen extents
        // fill the first segment.
        let block_size = 1 << 20;
        let setup = setup(block_size);
        let size = u64::from(block_size);
        pin_all(&setup, (0..).map(|i| i * size), &blocks(17, block_size));
        assert_eq!(open_segments(&setup), [16, 1]);
    }

    #[test]
    fn concurrent_lookups_release_every_extent() {
        // Four lookup workers pin blocks while the output advances. The
        // lookups of three queries start when the output reaches the first
        // of them. The output waits for the answers of the even queries
        // and reads their pins, as for REF. It passes the odd queries
        // without their answers, as literals do. Such a query becomes
        // obsolete before its lookup or during its write, or its pin is
        // published first. With 16 blocks, the queries in flight are for
        // different blocks. With two blocks, they share extents, and they
        // copy them out of sealed segments, also while the output reads
        // them. The output empties the cache now and then, so that copies
        // also read their blocks back from extents. In each case, the
        // extent is released, so every sealed segment closes.
        const QUERIES: u64 = 2000;
        let block_size = 512;
        let size = u64::from(block_size);
        for (count, minimum) in [(16, 4), (2, 4), (2, 1)] {
            let setup = setup_with_minimum(block_size, minimum * size);
            let blocks = blocks(count, block_size);
            let block_of = |i: u64| &blocks[(i % count) as usize];
            let (queries, queue) = unbounded();
            let mut found = HashMap::new();
            std::thread::scope(|scope| {
                for _ in 0..4 {
                    scope.spawn(|| {
                        for i in &queue {
                            let (strong, data) = block_of(i);
                            let find = || Ok(Arc::clone(data));
                            setup.pins.answer(i * size, strong, find);
                        }
                    });
                }
                for i in 0..QUERIES {
                    if i % 3 == 0 {
                        for query in i..(i + 3).min(QUERIES) {
                            queries.send(query).unwrap();
                        }
                    }
                    if i % 5 == 0 {
                        setup.pins.lock().cache.clear();
                    }
                    if i % 2 == 0 {
                        while !found.contains_key(&(i * size)) {
                            let (offset, yes) = answer(setup.answers.recv().unwrap());
                            found.insert(offset, yes);
                        }
                        assert!(found[&(i * size)], "a query at the output is obsolete");
                        assert_eq!(read_back(&setup, block_size), *block_of(i).1);
                    }
                    setup.pins.reached((i + 1) * size);
                }
                // The workers end.
                drop(queries);
            });
            found.extend(setup.answers.try_iter().map(answer));
            assert_eq!(found.len() as u64, QUERIES, "one answer for each query");
            assert!(setup.pins.lock().pins.is_empty());
            assert!(setup.pins.lock().pinned.is_empty());
            // Only a current segment can be open, without extents in use.
            let open = open_segments(&setup);
            assert!(matches!(open[..], [] | [0]), "extents in use: {open:?}");
        }
    }

    #[test]
    fn a_corrupt_pin_is_detected() {
        let setup = setup(BS);
        let (strong, data) = block(5, BS);
        setup.pins.answer(0, &strong, || Ok(Arc::clone(&data)));
        let (file, offset) = {
            let state = setup.pins.lock();
            let extent = &state.pinned[&strong].extent;
            (Arc::clone(extent.file()), extent.offset())
        };
        file.write_all_at(b"X", offset + 7).unwrap();
        let mut buf = vec![0; BS as usize];
        let error = read_block(&file, offset, &mut buf, StrongHash::Blake3, &strong).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        // A pin past the end of its segment is EOF, not a short block.
        let error = read_block(&file, 1 << 40, &mut buf, StrongHash::Blake3, &strong).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    #[should_panic(expected = "protocol error: REF without a pin at 0")]
    fn a_reference_needs_a_pin() {
        let setup = setup(BS);
        read_back(&setup, BS);
    }
}

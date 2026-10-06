//! The receiver's pins (DESIGN.md §6.4): the verified blocks that yes
//! answers promise, kept in extents of segments (DESIGN.md §6.5) until
//! the output passes their query offsets.
//!
//! One lock protects the output offset `c`, the published pins, the open
//! segments of the pins, the cache of verified blocks, and the queue of
//! answers, whose closure marks Completing. So a lookup either publishes
//! its pin before the output passes the pin's offset, and the pin is
//! released later, or it sees the obsolete offset and releases its extent
//! at once. An extent is released only after its last use, the REF read or
//! the lookup's write.
//!
//! The release of the last extent in use of a sealed segment gives its
//! file back, and the thread that released the extent closes it. Source
//! reads, segment reads, writes, and closes, and stdout writes happen
//! outside the lock. The one exception is the creation of a segment, which
//! happens under the lock, once per segment. Answers are queued under the
//! lock, until Completing closes the queue.
//!
//! At the normal end, after the lookups have ended, `close` closes all
//! open segments of the pins, which frees their storage (DESIGN.md §6.8).
//!
//! Pin storage failures end the run with `die` (DESIGN.md §6.6): a yes
//! answer cannot be taken back.

use std::collections::BTreeMap;
use std::fs::File;
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam_channel::Sender;
use lru::LruCache;

use crate::hash::{Strong, StrongHash};
use crate::protocol::Answer;
use crate::segments::{Directory, Extent, SEGMENT_MINIMUM, Segments};

/// A verified block: exactly the buffer whose digest was checked. The
/// cache and a lookup share it until the lookup has written it.
pub type Block = Arc<Vec<u8>>;

/// The bytes of verified blocks that the cache holds, or one block when a
/// block is larger (DESIGN.md §6.4).
const CACHE_BYTES: u64 = 256 << 20;

/// The pins released per acquisition of the lock when the output advances,
/// so that other threads never wait long for the lock.
const RELEASE_BATCH: usize = 256;

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

/// A published pin.
struct Pin {
    extent: Extent,
    strong: Strong,
}

struct State {
    /// `c`: reconstruction has written the output up to here.
    output: u64,
    /// The published pins, by query offset.
    pins: BTreeMap<u64, Pin>,
    /// The open segments of the pins, which hold their extents.
    segments: Segments,
    /// Verified blocks by full strong hash. Pins never hold them.
    cache: LruCache<Strong, Block>,
    /// The queue of answers for the control writer; `None` in Completing.
    answers: Option<Sender<Answer>>,
}

impl State {
    /// Completing (DESIGN.md §6.8): the answer queue is closed, and lookups
    /// neither append, publish, nor answer.
    fn completing(&self) -> bool {
        self.answers.is_none()
    }

    /// Queue an answer; in Completing, nothing is answered.
    fn answer(&self, offset: u64, found: bool) {
        if let Some(answers) = &self.answers {
            // Before the queue closes, the control writer ends only with
            // the process.
            let _ = answers.send(Answer { offset, found });
        }
    }
}

/// The pins and the state that they share with reconstruction.
pub struct Pins {
    /// The tmp_data directory, which the pins share with the spool of the
    /// data session.
    dir: Arc<Directory>,
    strong: StrongHash,
    state: Mutex<State>,
}

impl Pins {
    /// Pins of `block_size` bytes in segments of the tmp_data directory
    /// `dir` (DESIGN.md §6.4). Answers go to `answers`.
    pub fn new(
        dir: Arc<Directory>,
        strong: StrongHash,
        block_size: u32,
        answers: Sender<Answer>,
    ) -> Self {
        Self::with_minimum(dir, strong, block_size, answers, SEGMENT_MINIMUM)
    }

    /// `new`, with a current segment that is full at `minimum` bytes while
    /// no sealed segment is open.
    fn with_minimum(
        dir: Arc<Directory>,
        strong: StrongHash,
        block_size: u32,
        answers: Sender<Answer>,
        minimum: u64,
    ) -> Self {
        let blocks = usize::try_from(CACHE_BYTES / u64::from(block_size)).unwrap_or(usize::MAX);
        Pins {
            dir,
            strong,
            state: Mutex::new(State {
                output: 0,
                pins: BTreeMap::new(),
                segments: Segments::new(u64::from(block_size), minimum),
                cache: LruCache::sparse(NonZeroUsize::new(blocks).unwrap_or(NonZeroUsize::MIN)),
                answers: Some(answers),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Answer `QUERY { offset, strong }` (DESIGN.md §6.4, steps 1 to 8).
    /// The block comes from the cache of verified blocks, or from `find`,
    /// which reads and verifies candidates outside the lock. A found block
    /// goes to a new extent; then the pin is published and answered yes,
    /// unless the output passed `offset` in the meantime. An obsolete
    /// query is answered no; in Completing, nothing is answered.
    pub fn answer(&self, offset: u64, strong: &Strong, find: impl FnOnce() -> Option<Block>) {
        let cached = {
            let mut state = self.lock();
            if state.completing() {
                return;
            }
            if offset < state.output {
                return state.answer(offset, false);
            }
            state.cache.get(strong).cloned()
        };
        let searched = cached.is_none();
        let Some(block) = cached.or_else(find) else {
            return self.lock().answer(offset, false);
        };
        let extent = {
            let mut state = self.lock();
            // Verified reads populate the cache.
            if searched {
                state.cache.put(*strong, Arc::clone(&block));
            }
            if state.completing() {
                return;
            }
            if offset < state.output {
                return state.answer(offset, false);
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
    /// block of the query at `offset`. Publish the pin and answer yes. If
    /// the output passed `offset` during the write, answer no and release
    /// the extent. In Completing, release the extent without an answer.
    fn publish(&self, offset: u64, strong: &Strong, extent: Extent) {
        let mut state = self.lock();
        if state.completing() || offset < state.output {
            // In Completing, this queues nothing.
            state.answer(offset, false);
            let closed = state.segments.release(extent);
            drop(state);
            return drop(closed);
        }
        let previous = state.pins.insert(
            offset,
            Pin {
                extent,
                strong: *strong,
            },
        );
        assert!(
            previous.is_none(),
            "protocol error: two queries at offset {offset}"
        );
        state.answer(offset, true);
    }

    /// Read the pin at the output offset into `buf` for REF, and verify
    /// the buffer against the pin's full digest (DESIGN.md §6.4). The pin
    /// stays in use until `reached` moves the output past it. A missing pin
    /// is a protocol error; a pin that cannot be read back intact ends
    /// the run.
    pub fn read_reference(&self, buf: &mut [u8]) {
        let (output, file, offset, strong) = {
            let state = self.lock();
            let pin = state
                .pins
                .get(&state.output)
                .unwrap_or_else(|| panic!("protocol error: REF without a pin at {}", state.output));
            let extent = &pin.extent;
            let file = Arc::clone(extent.file());
            (state.output, file, extent.offset(), pin.strong)
        };
        if let Err(e) = read_block(&file, offset, buf, self.strong, &strong) {
            self.dir
                .fail("read", format_args!("the pin at offset {output}: {e}"));
        }
    }

    /// Reconstruction wrote the output up to `offset`: publish it, and
    /// release the pins below it, in batches. After each batch, close the
    /// segments that it removed from the open segments, outside the lock
    /// (DESIGN.md §6.4 and §6.5).
    pub fn reached(&self, offset: u64) {
        let mut state = self.lock();
        assert!(offset >= state.output, "the output offset went back");
        state.output = offset;
        loop {
            let mut closed = Vec::new();
            let mut released = 0;
            while released < RELEASE_BATCH
                && let Some(pin) = state.pins.first_entry()
                && *pin.key() < offset
            {
                let extent = pin.remove().extent;
                closed.extend(state.segments.release(extent).into_iter().flatten());
                released += 1;
            }
            drop(state);
            drop(closed);
            if released < RELEASE_BATCH {
                return;
            }
            state = self.lock();
        }
    }

    /// Enter Completing (DESIGN.md §6.8), and close the answer queue: the
    /// control writer ends after it has sent the answers that the queue
    /// holds.
    pub fn complete(&self) {
        self.lock().answers = None;
    }

    pub fn is_completing(&self) -> bool {
        self.lock().completing()
    }

    /// The normal end of the pins, after the lookups have ended
    /// (DESIGN.md §6.8): close all open segments of the pins, which frees
    /// their storage. The tmp_data directory closes with its last user.
    pub fn close(self) {
        // This function exists so that no thread still holds the pins when
        // the caller closes them. It takes the pins by value, so the caller
        // must get them out of the `Arc` that it shares with its threads.
        // `Arc::into_inner` does that, and fails while a thread still holds
        // them. A `drop` of the `Arc` succeeds then, and closes nothing.
        //
        // The lookups, which hold the extents that they write, have ended:
        // only the pins hold the files.
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
        answers: Receiver<Answer>,
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
    fn setup_with(new: impl FnOnce(Arc<Directory>, Sender<Answer>) -> Pins) -> Setup {
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

    fn block(seed: u8, block_size: u32) -> (Strong, Block) {
        let bytes: Vec<u8> = (0..block_size)
            .map(|i| (i as u8).wrapping_mul(31) ^ seed)
            .collect();
        (StrongHash::Blake3.digest(&bytes), Arc::new(bytes))
    }

    /// `count` different blocks.
    fn blocks(count: u8, block_size: u32) -> Vec<(Strong, Block)> {
        (0..count).map(|seed| block(seed, block_size)).collect()
    }

    fn answers(setup: &Setup) -> Vec<(u64, bool)> {
        setup
            .answers
            .try_iter()
            .map(|a| (a.offset, a.found))
            .collect()
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
            setup.pins.answer(offset, strong, || Some(Arc::clone(data)));
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
        let extent = &state.pins[&offset].extent;
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
        setup.pins.answer(0, &strong, || Some(Arc::clone(&data)));
        assert_eq!(answers(&setup), vec![(0, true)]);
        assert_eq!(read_back(&setup, BS), *data);
        assert_eq!(open_segments(&setup), [1]);
        setup.pins.reached(u64::from(BS));
        assert!(setup.pins.lock().pins.is_empty());
        // The current segment is not full: it stays open without extents
        // in use.
        assert_eq!(open_segments(&setup), [0]);
    }

    #[test]
    fn a_verified_block_is_cached_for_other_offsets() {
        let setup = setup(BS);
        let (strong, data) = block(2, BS);
        setup.pins.answer(10, &strong, || Some(Arc::clone(&data)));
        setup
            .pins
            .answer(20, &strong, || panic!("the cache must answer"));
        assert_eq!(answers(&setup), vec![(10, true), (20, true)]);
        // Each pin has its own extent, even with identical content, and
        // the extents follow each other without gaps.
        assert_eq!(extent(&setup, 10), (0, 0));
        assert_eq!(extent(&setup, 20), (0, u64::from(BS)));
    }

    #[test]
    fn nothing_found_and_obsolete_queries_are_answered_no() {
        let setup = setup(BS);
        let (strong, data) = block(3, BS);
        setup.pins.answer(0, &strong, || None);
        setup.pins.reached(100);
        setup
            .pins
            .answer(50, &strong, || panic!("no lookup below the output"));
        // The output passes the query while its candidates are read.
        setup.pins.answer(200, &strong, || {
            setup.pins.reached(300);
            Some(Arc::clone(&data))
        });
        assert_eq!(answers(&setup), vec![(0, false), (50, false), (200, false)]);
        assert!(setup.pins.lock().pins.is_empty());
        // No query got an extent, so no segment was created.
        assert_eq!(open_segments(&setup), []);
    }

    #[test]
    fn completing_suppresses_answers_and_pins() {
        let setup = setup(BS);
        let (strong, data) = block(4, BS);
        setup.pins.answer(0, &strong, || {
            setup.pins.complete();
            Some(Arc::clone(&data))
        });
        setup
            .pins
            .answer(BS.into(), &strong, || panic!("no lookup in Completing"));
        assert!(answers(&setup).is_empty());
        assert!(setup.pins.lock().pins.is_empty());
    }

    #[test]
    fn completing_closes_the_answer_queue() {
        // The control writer takes the answers that the queue holds, and
        // then finds the queue closed: it ends (DESIGN.md §6.8).
        let setup = setup(BS);
        let (strong, data) = block(6, BS);
        setup.pins.answer(0, &strong, || Some(Arc::clone(&data)));
        assert!(!setup.pins.is_completing());
        setup.pins.complete();
        assert!(setup.pins.is_completing());
        assert_eq!(answers(&setup), vec![(0, true)]);
        assert!(
            matches!(setup.answers.try_recv(), Err(TryRecvError::Disconnected)),
            "the queue is still open"
        );
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
            setup
                .pins
                .answer(i * size, strong, || Some(Arc::clone(data)));
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
    fn one_advance_of_the_output_releases_more_pins_than_one_batch() {
        // The minimum is 64 extents. The pins fill three segments, with
        // 64, 128, and 256 extents, and a part of the fourth one. One
        // advance of the output passes all pins: it releases them in three
        // batches, and it closes the four segments.
        let block_size = 512;
        let size = u64::from(block_size);
        let setup = setup_with_minimum(block_size, 64 * size);
        let count = 2 * RELEASE_BATCH as u64 + 44;
        let (strong, data) = block(9, block_size);
        for i in 0..count {
            setup
                .pins
                .answer(i * size, &strong, || Some(Arc::clone(&data)));
        }
        assert_eq!(open_segments(&setup), [64, 128, 256, 108]);
        setup.pins.reached(count * size);
        assert!(setup.pins.lock().pins.is_empty());
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
        // Blocks of 1 MiB, in the pins of a run: sixteen extents fill the
        // first segment.
        let block_size = 1 << 20;
        let setup = setup(block_size);
        let (strong, data) = block(7, block_size);
        for i in 0..17 {
            let offset = i * u64::from(block_size);
            setup
                .pins
                .answer(offset, &strong, || Some(Arc::clone(&data)));
        }
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
        // published first. In each case, the extent is released, so every
        // sealed segment closes.
        const QUERIES: u64 = 2000;
        let block_size = 512;
        let size = u64::from(block_size);
        let setup = setup_with_minimum(block_size, 4 * size);
        let blocks = blocks(16, block_size);
        let block_of = |i: u64| &blocks[(i % 16) as usize];
        let (queries, queue) = unbounded();
        let mut found = HashMap::new();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for i in &queue {
                        let (strong, data) = block_of(i);
                        let find = || Some(Arc::clone(data));
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
                if i % 2 == 0 {
                    while !found.contains_key(&(i * size)) {
                        let answer = setup.answers.recv().unwrap();
                        found.insert(answer.offset, answer.found);
                    }
                    assert!(found[&(i * size)], "a query at the output is obsolete");
                    assert_eq!(read_back(&setup, block_size), *block_of(i).1);
                }
                setup.pins.reached((i + 1) * size);
            }
            // The workers end.
            drop(queries);
        });
        found.extend(setup.answers.try_iter().map(|a| (a.offset, a.found)));
        assert_eq!(found.len() as u64, QUERIES, "one answer for each query");
        assert!(setup.pins.lock().pins.is_empty());
        // Only a current segment can be open, without extents in use.
        let open = open_segments(&setup);
        assert!(matches!(open[..], [] | [0]), "extents in use: {open:?}");
    }

    #[test]
    fn a_corrupt_pin_is_detected() {
        let setup = setup(BS);
        let (strong, data) = block(5, BS);
        setup.pins.answer(0, &strong, || Some(Arc::clone(&data)));
        let (file, offset) = {
            let state = setup.pins.lock();
            let extent = &state.pins[&0].extent;
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

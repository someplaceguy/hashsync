//! The receiver's pins (DESIGN.md §6.4): the verified blocks that yes
//! answers promise, kept in one open, unlinked file until the output
//! passes their query offsets.
//!
//! One lock protects the output offset `c`, the published pins, the next
//! extent, the cache of verified blocks, and the queue of answers, whose
//! closure marks Completing. So a lookup either publishes its pin before
//! the output passes the pin's offset, and the pin is released later, or
//! it sees the obsolete offset and releases its extent at once. An extent
//! is released only after its last use, the REF read or the lookup's
//! write. A reclaimer thread punches released extents in the background,
//! in merged calls. Source reads, pin-file I/O, and stdout writes happen
//! outside the lock; answers are queued under it, until Completing closes
//! the queue.
//!
//! At the normal end, after the lookups have ended, `close` ends the
//! reclaimer and then closes the pin file, which frees all its remaining
//! storage (DESIGN.md §6.7).
//!
//! Pin-file failures end the run with `die` (DESIGN.md §6.5): a yes
//! answer cannot be taken back.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::File;
use std::num::NonZeroUsize;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;

use bstr::ByteSlice;
use crossbeam_channel::{Receiver, Sender, unbounded};
use lru::LruCache;
use rustix::fs::{AtFlags, FallocateFlags, Mode, OFlags};
use rustix::io::{Errno, retry_on_intr};

use crate::cache;
use crate::die::die;
use crate::fsops;
use crate::hash::{Strong, StrongHash};
use crate::protocol::Answer;

/// A verified block: exactly the buffer whose digest was checked. The
/// cache and a lookup share it until the lookup has written it.
pub type Block = Arc<Vec<u8>>;

/// The bytes of verified blocks that the cache holds (DESIGN.md §6.4).
const CACHE_BYTES: u64 = 256 << 20;

/// The pins released per acquisition of the lock when the output advances,
/// so that other threads never wait long for the lock.
const RELEASE_BATCH: usize = 256;

/// The bytes of released extents that the reclaimer can hold back to punch
/// them together (DESIGN.md §6.4).
const HOLDBACK: u64 = 128 << 20;

/// The open, unlinked pin file. Each pin owns one extent of `stride`
/// bytes, and the block fills its first `B` bytes. `stride` is `B` rounded
/// up to the filesystem's allocation granularity, so that punching an
/// extent frees whole filesystem blocks and never touches a neighbor:
/// partial blocks would only be zeroed.
pub struct PinFile {
    file: File,
    /// The directory of the file, for messages.
    dir: Vec<u8>,
    stride: u64,
}

impl PinFile {
    /// Create the pin file in `dir`, which is created as a private
    /// directory when it does not exist, and check that the filesystem
    /// punches holes (DESIGN.md §6.4). Failures end the run.
    pub fn create(dir: &[u8], block_size: u32) -> PinFile {
        let shown = dir.as_bstr();
        let path = Path::new(OsStr::from_bytes(dir));
        if let Err(e) = fsops::create_dir_all(path, 0o700) {
            die(format!("cannot create pin directory {shown}: {e}"));
        }
        let dir_flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let dir_fd = retry_on_intr(|| rustix::fs::open(path, dir_flags, Mode::empty()))
            .unwrap_or_else(|e| die(format!("cannot open pin directory {shown}: {e}")));
        let fd = open_unlinked(dir_fd.as_fd())
            .unwrap_or_else(|e| die(format!("cannot create a pin file in {shown}: {e}")));
        let statfs = retry_on_intr(|| rustix::fs::fstatfs(&fd))
            .unwrap_or_else(|e| die(format!("cannot examine pin directory {shown}: {e}")));
        let granularity = statfs.f_bsize.max(statfs.f_frsize).max(1) as u64;
        let stride = u64::from(block_size)
            .div_ceil(granularity)
            .checked_mul(granularity)
            .unwrap_or_else(|| {
                die(format!(
                    "pin directory {shown}: allocation granularity {granularity} is too large"
                ))
            });
        let pin_file = PinFile {
            file: File::from(fd),
            dir: dir.to_vec(),
            stride,
        };
        // The probe: an extent that was written must be punchable.
        let probe = vec![0; usize::try_from(stride).expect("stride above usize")];
        if let Err(e) = pin_file.file.write_all_at(&probe, 0) {
            pin_file.fail("write", e);
        }
        if let Err(e) = pin_file.punch(0, stride) {
            die(format!(
                "pin directory {shown} does not support hole punching: {e}"
            ));
        }
        pin_file
    }

    /// End the run after a pin-file operation failed.
    fn fail(&self, action: &str, error: impl std::fmt::Display) -> ! {
        die(format!(
            "cannot {action} the pin file in {}: {error}",
            self.dir.as_bstr()
        ))
    }

    /// Read the block of the extent at `extent` into `buf`, and require
    /// the full digest `strong` of that buffer. EOF is `UnexpectedEof`.
    fn read_block(
        &self,
        extent: u64,
        buf: &mut [u8],
        hash: StrongHash,
        strong: &Strong,
    ) -> std::io::Result<()> {
        self.file.read_exact_at(buf, extent)?;
        if hash.digest(buf) != *strong {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the block does not match its digest",
            ));
        }
        Ok(())
    }

    /// Free `length` bytes from `start`, without changing the file size.
    fn punch(&self, start: u64, length: u64) -> Result<(), Errno> {
        let mode = FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE;
        retry_on_intr(|| rustix::fs::fallocate(&self.file, mode, start, length))
    }
}

/// A new file in `dir` without a name: `O_TMPFILE`, or, where the
/// filesystem or kernel lacks it, a unique name that is removed before any
/// payload is written (DESIGN.md §6.4). Mode 0600, close-on-exec.
fn open_unlinked(dir: BorrowedFd<'_>) -> Result<OwnedFd, Errno> {
    let mode = Mode::from_raw_mode(0o600);
    let tmpfile = OFlags::TMPFILE | OFlags::RDWR | OFlags::CLOEXEC;
    match retry_on_intr(|| rustix::fs::openat(dir, ".", tmpfile, mode)) {
        // The filesystem lacks `O_TMPFILE`, or the kernel does.
        Err(Errno::NOTSUP | Errno::ISDIR) => {}
        result => return result,
    }
    let named = OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::RDWR | OFlags::CLOEXEC;
    loop {
        let name = format!(".pins-{}", cache::hex(&cache::random_bytes::<8>()));
        match retry_on_intr(|| rustix::fs::openat(dir, name.as_str(), named, mode)) {
            Ok(fd) => {
                retry_on_intr(|| rustix::fs::unlinkat(dir, name.as_str(), AtFlags::empty()))?;
                return Ok(fd);
            }
            Err(Errno::EXIST) => {}
            Err(e) => return Err(e),
        }
    }
}

/// A published pin.
#[derive(Clone, Copy)]
struct Pin {
    extent: u64,
    strong: Strong,
}

struct State {
    /// `c`: reconstruction has written the output up to here.
    output: u64,
    /// The published pins, by query offset.
    pins: BTreeMap<u64, Pin>,
    /// The start of the next extent. Extents are never reused.
    next_extent: u64,
    /// Verified blocks by full strong hash. Pins never hold them.
    cache: LruCache<Strong, Block>,
    /// The queue of answers for the control writer; `None` in Completing.
    answers: Option<Sender<Answer>>,
}

impl State {
    /// Completing (DESIGN.md §6.7): the answer queue is closed, and lookups
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

/// Punch the extents that arrive from `released` (DESIGN.md §6.4). They
/// wait until they total `holdback` bytes; then each run of neighbors goes
/// in one call. Merged calls cost much less on some filesystems: XFS, for
/// one, writes a range back before it punches it, and holds the inode lock
/// that pin writes and reads also need. Once the queue is closed and
/// empty, return without punching the extents that wait: closing the pin
/// file frees them.
fn reclaim(file: &PinFile, released: &Receiver<u64>, holdback: u64) {
    let mut pending = Vec::new();
    while let Ok(extent) = released.recv() {
        pending.push(extent);
        pending.extend(released.try_iter());
        if (pending.len() as u64).saturating_mul(file.stride) >= holdback {
            punch_all(file, &mut pending);
        }
    }
}

/// Punch `extents`, each run of neighbors with one call, and empty the list.
fn punch_all(file: &PinFile, extents: &mut Vec<u64>) {
    extents.sort_unstable();
    let mut extents = extents.drain(..).peekable();
    while let Some(start) = extents.next() {
        let mut end = start + file.stride;
        while extents.next_if_eq(&end).is_some() {
            end += file.stride;
        }
        if let Err(e) = file.punch(start, end - start) {
            file.fail("punch a hole in", e);
        }
    }
}

/// The pins and the state that they share with reconstruction.
pub struct Pins {
    file: Arc<PinFile>,
    strong: StrongHash,
    state: Mutex<State>,
    /// Extents that nothing uses any more, for the reclaimer.
    released: Sender<u64>,
    reclaimer: JoinHandle<()>,
}

impl Pins {
    /// Pins of `block_size` bytes in `file`, with their reclaimer; answers
    /// go to `answers`.
    pub fn new(
        file: PinFile,
        strong: StrongHash,
        block_size: u32,
        answers: Sender<Answer>,
    ) -> Self {
        Self::with_holdback(file, strong, block_size, answers, HOLDBACK)
    }

    /// `new`, with a reclaimer that holds back up to `holdback` bytes of
    /// released extents.
    fn with_holdback(
        file: PinFile,
        strong: StrongHash,
        block_size: u32,
        answers: Sender<Answer>,
        holdback: u64,
    ) -> Self {
        let file = Arc::new(file);
        let (released, queue) = unbounded();
        let reclaimer = {
            let file = Arc::clone(&file);
            std::thread::Builder::new()
                .name("reclaimer".into())
                .spawn(move || reclaim(&file, &queue, holdback))
                .expect("cannot start the reclaimer")
        };
        let blocks = usize::try_from(CACHE_BYTES / u64::from(block_size)).unwrap_or(usize::MAX);
        Pins {
            file,
            strong,
            released,
            reclaimer,
            state: Mutex::new(State {
                output: 0,
                pins: BTreeMap::new(),
                next_extent: 0,
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
            // `pwrite` takes an `off_t`: the whole extent must fit in it.
            let extent = state.next_extent;
            state.next_extent = extent
                .checked_add(self.file.stride)
                .filter(|&end| end <= i64::MAX as u64)
                .unwrap_or_else(|| self.file.fail("extend", "file offsets exhausted"));
            extent
        };
        // Concurrent workers write only their own extents.
        if let Err(e) = self.file.file.write_all_at(&block, extent) {
            self.file.fail("write", e);
        }
        drop(block);
        let mut state = self.lock();
        if state.completing() {
            // Closing the pin file frees the unpublished extent.
            return;
        }
        if offset < state.output {
            state.answer(offset, false);
            return self.release(extent);
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
    /// stays live until `reached` moves the output past it. A missing pin
    /// is a protocol error; a pin that cannot be read back intact ends
    /// the run.
    pub fn read_reference(&self, buf: &mut [u8]) {
        let (offset, pin) = {
            let state = self.lock();
            let pin = *state
                .pins
                .get(&state.output)
                .unwrap_or_else(|| panic!("protocol error: REF without a pin at {}", state.output));
            (state.output, pin)
        };
        if let Err(e) = self
            .file
            .read_block(pin.extent, buf, self.strong, &pin.strong)
        {
            self.file
                .fail("read", format_args!("the pin at offset {offset}: {e}"));
        }
    }

    /// Reconstruction wrote the output up to `offset`: publish it, and
    /// release the pins below it to the reclaimer, in batches
    /// (DESIGN.md §6.4).
    pub fn reached(&self, offset: u64) {
        let mut state = self.lock();
        assert!(offset >= state.output, "the output offset went back");
        state.output = offset;
        loop {
            for _ in 0..RELEASE_BATCH {
                match state.pins.first_entry() {
                    Some(pin) if *pin.key() < offset => self.release(pin.remove().extent),
                    _ => return,
                }
            }
            drop(state);
            state = self.lock();
        }
    }

    /// Hand an extent that nothing uses any more to the reclaimer.
    fn release(&self, extent: u64) {
        // The reclaimer ends only after `close` closes its queue.
        let _ = self.released.send(extent);
    }

    /// Enter Completing (DESIGN.md §6.7), and close the answer queue: the
    /// control writer ends after it has sent the answers that the queue
    /// holds.
    pub fn complete(&self) {
        self.lock().answers = None;
    }

    pub fn is_completing(&self) -> bool {
        self.lock().completing()
    }

    /// The normal end of the pins, after the lookups have ended
    /// (DESIGN.md §6.7): close the reclaimer's queue, wait for the
    /// reclaimer, and then close the pin file, which frees all its
    /// remaining storage.
    pub fn close(self) {
        let Pins {
            file,
            released,
            reclaimer,
            ..
        } = self;
        drop(released);
        reclaimer.join().expect("the reclaimer panicked");
        let file = Arc::into_inner(file).expect("the pin file is still in use");
        drop(file);
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use crossbeam_channel::TryRecvError;

    use super::*;

    const BS: u32 = 4096;

    struct Setup {
        pins: Pins,
        answers: Receiver<Answer>,
        _dir: tempfile::TempDir,
    }

    /// Pins whose reclaimer punches every released extent at once.
    fn setup(block_size: u32) -> Setup {
        setup_with_holdback(block_size, 0)
    }

    fn setup_with_holdback(block_size: u32, holdback: u64) -> Setup {
        let dir = tempfile::tempdir().unwrap();
        let pin_dir = dir.path().join("pins");
        let file = PinFile::create(pin_dir.as_os_str().as_bytes(), block_size);
        let mode = std::fs::metadata(&pin_dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700, "the pin directory is private");
        assert_eq!(
            std::fs::read_dir(&pin_dir).unwrap().count(),
            0,
            "the pin file has no name"
        );
        let (tx, answers) = unbounded();
        Setup {
            pins: Pins::with_holdback(file, StrongHash::Blake3, block_size, tx, holdback),
            answers,
            _dir: dir,
        }
    }

    fn block(seed: u8, block_size: u32) -> (Strong, Block) {
        let bytes: Vec<u8> = (0..block_size)
            .map(|i| (i as u8).wrapping_mul(31) ^ seed)
            .collect();
        (StrongHash::Blake3.digest(&bytes), Arc::new(bytes))
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

    /// The `length` bytes at `offset` of `file`.
    fn bytes_at(file: &File, offset: u64, length: u64) -> Vec<u8> {
        let mut buf = vec![0; length as usize];
        file.read_exact_at(&mut buf, offset).unwrap();
        buf
    }

    /// Close the pins, which waits for the reclaimer, and return another
    /// descriptor of the pin file. It keeps the file open: the extents that
    /// the reclaimer punched read as zeros, the others keep their bytes.
    fn close(setup: Setup) -> File {
        let file = setup.pins.file.file.try_clone().unwrap();
        setup.pins.close();
        file
    }

    fn zeros(bytes: &[u8]) -> bool {
        bytes.iter().all(|&b| b == 0)
    }

    #[test]
    fn a_found_block_is_pinned_answered_and_released() {
        let setup = setup(BS);
        let (strong, data) = block(1, BS);
        setup.pins.answer(0, &strong, || Some(Arc::clone(&data)));
        assert_eq!(answers(&setup), vec![(0, true)]);
        assert_eq!(read_back(&setup, BS), *data);
        let extent = setup.pins.lock().pins[&0].extent;
        let stride = setup.pins.file.stride;
        setup.pins.reached(u64::from(BS));
        assert!(setup.pins.lock().pins.is_empty());
        let file = close(setup);
        assert!(zeros(&bytes_at(&file, extent, stride)));
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
        // Each pin has its own extent, even with identical content.
        let state = setup.pins.lock();
        assert_ne!(state.pins[&10].extent, state.pins[&20].extent);
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
        let state = setup.pins.lock();
        assert!(state.pins.is_empty());
        assert_eq!(state.next_extent, 0, "no extent for an obsolete query");
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
        // then finds the queue closed: it ends (DESIGN.md §6.7).
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

    /// The descriptors of this process that refer to a file in `dir`.
    fn descriptors_in(dir: &Path) -> usize {
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
            .filter(|target| target.starts_with(dir) && target != dir)
            .count()
    }

    #[test]
    fn closing_the_pins_closes_the_pin_file() {
        // The other threads of the test process open and close their own
        // descriptors, but none in this directory.
        let setup = setup(BS);
        let pin_dir = std::fs::canonicalize(setup._dir.path().join("pins")).unwrap();
        assert_eq!(descriptors_in(&pin_dir), 1, "the pin file is open");
        setup.pins.close();
        assert_eq!(descriptors_in(&pin_dir), 0, "the pin file is still open");
    }

    #[test]
    fn releasing_a_pin_keeps_its_neighbors() {
        // 5000-byte blocks leave padding in 8192-byte strides on common
        // filesystems; every pin still owns its whole stride.
        for block_size in [BS, 5000] {
            let setup = setup(block_size);
            let stride = setup.pins.file.stride;
            assert!(stride >= u64::from(block_size));
            let blocks: Vec<(Strong, Block)> = (0..4).map(|i| block(i, block_size)).collect();
            for (i, (strong, data)) in blocks.iter().enumerate() {
                let offset = i as u64 * u64::from(block_size);
                setup.pins.answer(offset, strong, || Some(Arc::clone(data)));
            }
            // Release the first two pins; the next two stay intact.
            setup.pins.reached(2 * u64::from(block_size));
            assert_eq!(read_back(&setup, block_size), *blocks[2].1);
            let kept: Vec<u64> = (2..4)
                .map(|i| setup.pins.lock().pins[&(i * u64::from(block_size))].extent)
                .collect();
            let file = close(setup);
            assert!(zeros(&bytes_at(&file, 0, 2 * stride)));
            for (extent, (_, data)) in kept.into_iter().zip(&blocks[2..]) {
                assert_eq!(bytes_at(&file, extent, block_size.into()), **data);
            }
        }
    }

    #[test]
    fn released_extents_wait_for_the_holdback() {
        // The reclaimer holds one released extent back. Two reach the
        // holdback, and it punches them in one call. At the end, it does
        // not punch the extents that it holds back: closing the file frees
        // them.
        let stride = setup(BS).pins.file.stride;
        for released in [1, 2] {
            let setup = setup_with_holdback(BS, 2 * stride);
            let blocks: Vec<(Strong, Block)> = (0..3).map(|i| block(i, BS)).collect();
            for (i, (strong, data)) in blocks.iter().enumerate() {
                setup
                    .pins
                    .answer(i as u64 * u64::from(BS), strong, || Some(Arc::clone(data)));
            }
            setup.pins.reached(released * u64::from(BS));
            let file = close(setup);
            for (i, (_, data)) in blocks.iter().enumerate() {
                let punched = released == 2 && i < 2;
                let expected = if punched {
                    vec![0; BS as usize]
                } else {
                    data.to_vec()
                };
                assert_eq!(
                    bytes_at(&file, i as u64 * stride, BS.into()),
                    expected,
                    "{released} released, extent {i}"
                );
            }
        }
    }

    #[test]
    fn a_corrupt_pin_is_detected() {
        let setup = setup(BS);
        let (strong, data) = block(5, BS);
        setup.pins.answer(0, &strong, || Some(Arc::clone(&data)));
        let extent = setup.pins.lock().pins[&0].extent;
        setup.pins.file.file.write_all_at(b"X", extent + 7).unwrap();
        let mut buf = vec![0; BS as usize];
        let error = setup
            .pins
            .file
            .read_block(extent, &mut buf, StrongHash::Blake3, &strong)
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        // A pin past the end of the file is EOF, not a short block.
        let error = setup
            .pins
            .file
            .read_block(1 << 40, &mut buf, StrongHash::Blake3, &strong)
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    #[should_panic(expected = "protocol error: REF without a pin at 0")]
    fn a_reference_needs_a_pin() {
        let setup = setup(BS);
        read_back(&setup, BS);
    }
}

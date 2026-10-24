//! Segments (DESIGN.md §6.5): open, unlinked files in the tmp_data
//! directory, which hold the receiver's pins and the bytes of spools.
//!
//! Each user of segments, the pins or a spool, has open segments of its
//! own, in a `Segments`: at most one current segment, which gets the new
//! extents, and sealed segments, which get none. The full rule seals the
//! current segment, so that the open files stay few. An extent in use
//! holds its segment, and only extents in use hold a sealed segment. So
//! the release of the last extent in use of a sealed segment gives the
//! file of the segment back, and the caller drops it after it releases its
//! lock. A caller can also keep a reference to the file of an extent, for
//! I/O outside its lock: the file then stays open until that reference is
//! dropped, but the reference does not keep the segment open, so it
//! cannot disturb the rules. Closing an unlinked file frees its storage.
//!
//! Failures to create the directory or a segment, and failures of segment
//! I/O, end the run with `die` (DESIGN.md §6.7).

use std::ffi::OsStr;
use std::fs::File;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Arc, Weak};

use rustix::fs::{AtFlags, Mode, OFlags};
use rustix::io::{Errno, retry_on_intr};

use crate::cache;
use crate::die::die;
use crate::escapes::quoted;
use crate::fsops;
use crate::names;

/// The bytes at which the current segment is full while no sealed segment
/// is open (DESIGN.md §6.5).
pub const SEGMENT_MINIMUM: u64 = 16 << 20;

/// The tmp_data directory, open for the run: each segment is a new,
/// unlinked file in it (DESIGN.md §6.5).
pub struct Directory {
    fd: OwnedFd,
    /// Its path, for messages and for `expire_temporaries`.
    path: Vec<u8>,
}

impl Directory {
    /// Open the tmp_data directory at `path`, which is created as a
    /// private directory when it does not exist. Failures end the run.
    ///
    /// The users of the directory create their segments later, at their
    /// first allocations, so this creates a segment and closes it at once:
    /// a directory that cannot hold segments ends the run here, before the
    /// command starts (DESIGN.md §6.5).
    pub fn open(path: &[u8]) -> Self {
        let shown = quoted(path);
        let dir = Path::new(OsStr::from_bytes(path));
        if let Err(e) = fsops::create_dir_all(dir, 0o700) {
            die(format!("cannot create tmp_data directory {shown}: {e}"));
        }
        let flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let fd = retry_on_intr(|| rustix::fs::open(dir, flags, Mode::empty()))
            .unwrap_or_else(|e| die(format!("cannot open tmp_data directory {shown}: {e}")));
        let directory = Directory {
            fd,
            path: path.to_vec(),
        };
        drop(directory.create_segment());
        directory
    }

    /// Remove the segment files that failed runs left in the directory,
    /// once they are older than `max_age` seconds, the `hashlist_max_age`
    /// of this host (`cache::expire_temporaries`). A segment that a run
    /// creates without `O_TMPFILE` has a `.hashsync-` name until the run
    /// unlinks it (`open_unlinked`), so a run that fails in between leaves
    /// the file. Failures only print warnings, unlike those of the other
    /// operations on the directory: a file that stays does no harm
    /// (DESIGN.md §6.5).
    pub fn expire_temporaries(&self, max_age: u64) {
        cache::expire_temporaries(Path::new(OsStr::from_bytes(&self.path)), max_age);
    }

    /// The file of a new segment. A failure ends the run.
    fn create_segment(&self) -> File {
        match open_unlinked(self.fd.as_fd()) {
            Ok(fd) => File::from(fd),
            Err(e) => self.fail("create", e),
        }
    }

    /// Allocate an extent in `segments` (DESIGN.md §6.5, steps A1 to A3),
    /// in a new segment of this directory when one is needed. Exhausted
    /// file offsets end the run.
    pub fn allocate(&self, segments: &mut Segments) -> Extent {
        segments
            .allocate(|| self.create_segment())
            .unwrap_or_else(|| self.fail("extend", "file offsets exhausted"))
    }

    /// End the run after a segment operation failed.
    pub fn fail(&self, action: &str, error: impl std::fmt::Display) -> ! {
        die(format!(
            "cannot {action} a segment in tmp_data directory {}: {error}",
            quoted(&self.path)
        ))
    }
}

/// A new file in `dir` without a name: `O_TMPFILE`, or, where the
/// filesystem or kernel lacks it, a unique temporary name
/// (`names::temporary_name`) that is removed before any data is written
/// (DESIGN.md §6.5). Mode 0600, close-on-exec.
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
        let name = names::temporary_name();
        match retry_on_intr(|| rustix::fs::openat(dir, name.as_str(), named, mode)) {
            // Expiry in another run can remove the name first, with a very
            // small age bound (`Directory::expire_temporaries`), which
            // leaves the file unlinked, as this unlink would.
            Ok(fd) => {
                match retry_on_intr(|| rustix::fs::unlinkat(dir, name.as_str(), AtFlags::empty())) {
                    Ok(()) | Err(Errno::NOENT) => return Ok(fd),
                    Err(e) => return Err(e),
                }
            }
            Err(Errno::EXIST) => {}
            Err(e) => return Err(e),
        }
    }
}

/// One segment. The references to it count its extents in use, and one
/// more while it is the current segment. Its file has references of its
/// own, for I/O: they keep the file open, but not the segment.
struct Segment<F> {
    file: Arc<F>,
}

/// The bytes of one extent: a segment and an offset in it (DESIGN.md
/// §6.5). An extent in use holds its segment, so the segment stays open.
/// `Segments::release` ends the use.
pub struct Extent<F = File> {
    segment: Arc<Segment<F>>,
    offset: u64,
}

impl<F> Extent<F> {
    /// The file of the segment, for I/O outside the lock of the user. A
    /// clone keeps the file open, but not the segment: when a release
    /// closes the segment, its file closes with the last clone.
    pub fn file(&self) -> &Arc<F> {
        &self.segment.file
    }

    /// The offset of the extent in the file.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

/// A sealed segment, which is open while an extent in use holds it.
struct Sealed<F> {
    segment: Weak<Segment<F>>,
    /// Whether the segment got only one extent before its seal.
    single: bool,
}

impl<F> Sealed<F> {
    fn is_open(&self) -> bool {
        self.segment.strong_count() > 0
    }
}

/// The open segments of one user, with the rules that allocate and release
/// extents (DESIGN.md §6.5). No count of extents in use is stored: each
/// extent in use holds its segment, and `current` holds the current
/// segment too. So a sealed segment is open while one of its extents is in
/// use, and the release of the last one gives its file back. `F` is the
/// file of a segment. The rules do not use the files, so the tests of the
/// rules need none.
pub struct Segments<F = File> {
    /// The current segment, and the bytes of the extents appended to it.
    /// `None` before the first allocation, and from a seal until the next
    /// allocation.
    current: Option<(Arc<Segment<F>>, u64)>,
    /// The sealed segments, in creation order. The list can hold closed
    /// segments until the next count.
    sealed: Vec<Sealed<F>>,
    /// The bytes of every extent.
    extent_size: u64,
    /// The bytes at which the current segment is full while no sealed
    /// segment is open.
    minimum: u64,
}

/// The full rule (DESIGN.md §6.5): a current segment of `size` bytes is
/// full when it holds at least `minimum × 2^sealed` bytes, where `sealed`
/// is the count of the sealed segments that are open.
fn is_full(size: u64, minimum: u64, sealed: usize) -> bool {
    // `minimum << sealed` can lose its high bits; the shifted size cannot.
    // With 64 or more sealed segments, no size reaches the product.
    sealed < u64::BITS as usize && size >> sealed >= minimum
}

impl<F> Segments<F> {
    /// No open segments: the first allocation creates the first segment
    /// (DESIGN.md §6.5, step A1).
    pub fn new(extent_size: u64, minimum: u64) -> Self {
        Segments {
            current: None,
            sealed: Vec::new(),
            extent_size,
            minimum,
        }
    }

    /// The bytes of every extent.
    pub fn extent_size(&self) -> u64 {
        self.extent_size
    }

    /// Whether to keep `extent` in use for longer can cost more storage,
    /// more open segments or more bytes in them, than to release it and
    /// use a new extent in its place. This function exists so that a user
    /// that can copy its extents, as the pins can, needs no knowledge of
    /// the rules of the segments for that choice.
    ///
    /// A new extent would go to the current segment, which is never full
    /// (DESIGN.md §6.5), so to keep an extent of the current segment costs
    /// less. Without a current segment, a new extent would go to a new
    /// segment. If `extent` is the single extent of its sealed segment, and
    /// one extent would fill a new segment even with that segment closed,
    /// a new extent would only put one segment, with the same bytes, in the
    /// place of another. Any other extent is in a sealed segment, which it
    /// keeps open: with the released extents of that segment, or as one
    /// more sealed segment where a new extent would join other extents.
    pub fn costs_more_to_keep(&self, extent: &Extent<F>) -> bool {
        if let Some((current, _)) = &self.current {
            return !Arc::ptr_eq(current, &extent.segment);
        }
        let segment = Arc::as_ptr(&extent.segment);
        let alone = |sealed: &Sealed<F>| sealed.single && sealed.segment.as_ptr() == segment;
        if !self.sealed.iter().any(alone) {
            return true;
        }
        // `n` counts only the sealed segments that are open, and the
        // release of `extent` would close its segment.
        let open = self.sealed.iter().filter(|sealed| sealed.is_open());
        !is_full(self.extent_size, self.minimum, open.count() - 1)
    }

    /// Allocate an extent (DESIGN.md §6.5, steps A1 to A3); `create` makes
    /// the file of a new segment. `None` when the extent does not fit in
    /// the offsets of a file.
    pub fn allocate(&mut self, create: impl FnOnce() -> F) -> Option<Extent<F>> {
        // A1.
        let (segment, size) = self.current.get_or_insert_with(|| {
            let segment = Segment {
                file: Arc::new(create()),
            };
            (Arc::new(segment), 0)
        });
        // A2. `pwrite` takes an `off_t`: the whole extent must fit in it.
        let offset = *size;
        *size = offset
            .checked_add(self.extent_size)
            .filter(|&end| end <= i64::MAX as u64)?;
        let extent = Extent {
            segment: Arc::clone(segment),
            offset,
        };
        // A3. The new extent holds the segment, so the segment stays open.
        self.seal_if_full();
        Some(extent)
    }

    /// Release an extent (DESIGN.md §6.5, steps R1 to R3). Returns the
    /// files of the segments that this removes from the open segments, two
    /// at most. The caller drops them after it releases its lock, because
    /// the drop of the last reference to a file closes it.
    pub fn release(&mut self, extent: Extent<F>) -> [Option<Arc<F>>; 2] {
        // R1 and R2. Only the last extent in use of a sealed segment gives
        // the segment back: `current` holds the current segment too.
        let Some(sealed) = Arc::into_inner(extent.segment) else {
            return [None, None];
        };
        // R3. One sealed segment fewer is open, so the current segment can
        // be full now. Without an extent in use, it is given back too.
        let current = self.seal_if_full().and_then(Arc::into_inner);
        [Some(sealed.file), current.map(|segment| segment.file)]
    }

    /// Give up all the open segments at once, for a user that stops for
    /// good and allocates no more extents. This function exists so that
    /// such a stop needs no release of each extent: the full rule has no
    /// use any more. The returned value holds the current segment, and the
    /// extents in use hold the sealed ones. The caller drops both after it
    /// releases its lock, which closes the files. `self` keeps no open
    /// segment.
    pub fn take(&mut self) -> Segments<F> {
        Segments {
            current: self.current.take(),
            sealed: std::mem::take(&mut self.sealed),
            extent_size: self.extent_size,
            minimum: self.minimum,
        }
    }

    /// Seal the current segment if it is full (DESIGN.md §6.5). Returns
    /// the reference to the segment that `current` held.
    fn seal_if_full(&mut self) -> Option<Arc<Segment<F>>> {
        // `n` counts only the sealed segments that are open.
        self.sealed.retain(Sealed::is_open);
        let (segment, size) = self
            .current
            .take_if(|(_, size)| is_full(*size, self.minimum, self.sealed.len()))?;
        self.sealed.push(Sealed {
            segment: Arc::downgrade(&segment),
            single: size == self.extent_size,
        });
        Some(segment)
    }
}

#[cfg(test)]
impl<F> Segments<F> {
    /// The open segments, oldest first: the file of each one, and the count
    /// of its extents in use. The tests of the users of segments compare
    /// these files with the files of their extents.
    pub fn open(&self) -> Vec<(*const F, usize)> {
        let sealed = self.sealed.iter().filter_map(|sealed| {
            // Counted before the upgrade, which adds a reference.
            let in_use = sealed.segment.strong_count();
            Some((Arc::as_ptr(&sealed.segment.upgrade()?.file), in_use))
        });
        // `current` holds the current segment too.
        let current = self.current.iter().map(|(segment, _)| {
            let in_use = Arc::strong_count(segment) - 1;
            (Arc::as_ptr(&segment.file), in_use)
        });
        sealed.chain(current).collect()
    }
}

/// The open segments of a user of the directory `dir`, which only that
/// user uses, oldest first: the count of the extents in use of each one.
/// The process has `dir` open, and one open file in it for each segment.
#[cfg(test)]
pub fn open_segments(segments: &Segments, dir: &Path) -> Vec<usize> {
    let open = segments.open().into_iter().map(|(_, in_use)| in_use);
    let open: Vec<usize> = open.collect();
    assert_eq!(descriptors(dir), (1, open.len()));
    open
}

/// The descriptors of this process that refer to `dir` itself, and those
/// that refer to a file in it. The other threads of the test process open
/// and close their own descriptors, but none in `dir`.
#[cfg(test)]
pub fn descriptors(dir: &Path) -> (usize, usize) {
    let targets: Vec<std::path::PathBuf> = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter(|target| target.starts_with(dir))
        .collect();
    let of_dir = targets.iter().filter(|target| *target == dir).count();
    (of_dir, targets.len() - of_dir)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io::Write;
    use std::os::unix::fs::{FileExt, PermissionsExt};

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn the_directory_is_private_and_its_segments_have_no_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a/tmp");
        let directory = Directory::open(path.as_os_str().as_bytes());
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700, "the directory is private");
        // The segment that the opening creates is closed.
        let path = std::fs::canonicalize(&path).unwrap();
        assert_eq!(descriptors(&path), (1, 0));
        let mut segment = directory.create_segment();
        segment.write_all(b"data").unwrap();
        let mut read = [0; 4];
        segment.read_exact_at(&mut read, 0).unwrap();
        assert_eq!(&read, b"data");
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
        assert_eq!(descriptors(&path), (1, 1));
        drop((directory, segment));
        assert_eq!(descriptors(&path), (0, 0));
    }

    #[test]
    fn the_full_rule_does_not_overflow() {
        let minimum = SEGMENT_MINIMUM;
        assert!(!is_full(minimum - 1, minimum, 0));
        assert!(is_full(minimum, minimum, 0));
        assert!(!is_full(2 * minimum - 1, minimum, 1));
        assert!(is_full(2 * minimum, minimum, 1));
        // `minimum << 39` is 2^63, and `minimum << 40` would be 0.
        assert!(!is_full((1 << 63) - 1, minimum, 39));
        assert!(is_full(1 << 63, minimum, 39));
        assert!(!is_full(u64::MAX, minimum, 40));
        assert!(!is_full(u64::MAX, 1, 64));
    }

    /// The rules alone, with a model of them. The file of a segment is its
    /// number, in creation order from 0.
    struct Numbered {
        segments: Segments<u64>,
        /// The bytes of the extents of each segment, by number.
        sizes: Vec<u64>,
        /// The count of the extents in use of each segment, by number.
        in_use: Vec<u64>,
    }

    impl Numbered {
        fn new(extent_size: u64, minimum: u64) -> Self {
            Numbered {
                segments: Segments::new(extent_size, minimum),
                sizes: Vec::new(),
                in_use: Vec::new(),
            }
        }

        /// Allocate an extent. Only the newest segment gets extents, which
        /// follow each other without gaps.
        fn allocate(&mut self) -> Extent<u64> {
            // The number of the next segment, if this creates one.
            let next = self.sizes.len() as u64;
            let extent = self.segments.allocate(|| next).unwrap();
            let number = **extent.file() as usize;
            if number == self.sizes.len() {
                self.sizes.push(0);
                self.in_use.push(0);
            }
            assert_eq!(number, self.sizes.len() - 1, "an extent in an old segment");
            assert_eq!(extent.offset(), self.sizes[number]);
            self.sizes[number] += self.segments.extent_size;
            self.in_use[number] += 1;
            extent
        }

        /// Release an extent. The segments that this closes have no extent
        /// in use.
        fn release(&mut self, extent: Extent<u64>) {
            self.in_use[**extent.file() as usize] -= 1;
            for closed in self.segments.release(extent).into_iter().flatten() {
                assert_eq!(self.in_use[*closed as usize], 0, "segment {closed}");
            }
        }

        /// The numbers of the sealed segments that are open, oldest first.
        fn sealed(&self) -> Vec<u64> {
            let segments = self.segments.sealed.iter().map(|sealed| &sealed.segment);
            let open = segments.filter_map(Weak::upgrade);
            open.map(|segment| *segment.file).collect()
        }

        /// The current segment: its number and its size.
        fn current(&self) -> Option<(u64, u64)> {
            let current = self.segments.current.as_ref();
            current.map(|(segment, size)| (*segment.file, *size))
        }
    }

    #[test]
    fn file_offsets_bound_the_open_segments() {
        // Extents of 2^62 bytes, with the segment sizes of a run. One
        // extent fills a segment while fewer than 39 sealed segments are
        // open. The 40th segment is not full with one extent, and a second
        // one does not fit in the offsets of a file.
        let mut numbered = Numbered::new(1 << 62, SEGMENT_MINIMUM);
        let extents: Vec<Extent<u64>> = (0..40).map(|_| numbered.allocate()).collect();
        assert!(extents.iter().all(|extent| extent.offset() == 0));
        assert_eq!(numbered.sealed(), (0..39).collect::<Vec<u64>>());
        assert_eq!(numbered.current(), Some((39, 1 << 62)));
        let segments = &mut numbered.segments;
        assert!(segments.allocate(|| panic!("a new segment")).is_none());
    }

    #[test]
    fn a_reference_to_a_file_does_not_keep_its_segment_open() {
        // Extents of 1 byte, and a minimum of 2. Segment 0 gets two extents
        // and is sealed. Segment 1 gets three: it is not full while
        // segment 0 is open. A reference to the file of segment 0, as for
        // I/O outside a lock, is alive when the extents of segment 0 are
        // released. Segment 0 still closes (step R2), so segment 1 is full
        // now and is sealed (step R3). The file closes with the reference.
        let mut numbered = Numbered::new(1, 2);
        let mut extents: VecDeque<Extent<u64>> = (0..5).map(|_| numbered.allocate()).collect();
        assert_eq!(numbered.sealed(), [0]);
        assert_eq!(numbered.current(), Some((1, 3)));
        let reference = Arc::clone(extents[0].file());
        numbered.release(extents.pop_front().unwrap());
        numbered.release(extents.pop_front().unwrap());
        assert_eq!(numbered.sealed(), [1]);
        assert_eq!(numbered.current(), None);
        assert_eq!(Arc::strong_count(&reference), 1);
    }

    #[test]
    fn keeping_an_extent_costs_more_unless_it_is_current_or_alone() {
        // Extents of 2 bytes, and a minimum of 1: one extent fills a
        // segment while at most one sealed segment is open. So a new extent
        // in place of an extent that is alone is alone too while at most two
        // sealed segments are open, that of the old extent included. To
        // keep such an extent costs no more than a new extent, and to keep
        // an extent of the current segment costs less.
        let mut numbered = Numbered::new(2, 1);
        // Extents 0 and 1 fill segments 0 and 1 alone (step A3).
        let [first, second] = [numbered.allocate(), numbered.allocate()];
        assert!(!numbered.segments.costs_more_to_keep(&first));
        assert!(!numbered.segments.costs_more_to_keep(&second));
        // Extent 2 goes to segment 2, which is not full: while segment 2 is
        // current, a new extent would go there too. Extent 3 fills segment
        // 2, and three sealed segments are open.
        let third = numbered.allocate();
        assert!(numbered.segments.costs_more_to_keep(&first));
        assert!(!numbered.segments.costs_more_to_keep(&third));
        let fourth = numbered.allocate();
        assert_eq!(numbered.sealed(), [0, 1, 2]);
        assert!(numbered.segments.costs_more_to_keep(&first));
        // Segment 0 closes. Segment 2 holds two extents.
        numbered.release(first);
        assert!(!numbered.segments.costs_more_to_keep(&second));
        assert!(numbered.segments.costs_more_to_keep(&third));
        assert!(numbered.segments.costs_more_to_keep(&fourth));
        // Extent 4 goes to segment 3, which is not full until segment 1
        // closes. Then it is sealed with extent 4 alone (step R3).
        let fifth = numbered.allocate();
        numbered.release(second);
        assert_eq!(numbered.sealed(), [2, 3]);
        assert!(!numbered.segments.costs_more_to_keep(&fifth));
    }

    #[test]
    fn a_segment_that_r3_closes_does_not_count_as_open() {
        // Extents of 2 bytes, and a minimum of 1. Segments 0 and 1 get one
        // extent each, segment 2 two, and segment 3, the current segment,
        // two. The release of the extents of segment 3 keeps it open, as
        // the current segment. The release of those of segment 2 closes
        // it: then segment 3 is full, and it closes at its seal (step R3).
        // The list of sealed segments still has segment 3, but only two
        // sealed segments are open. So a new extent in place of extent 1
        // would be alone in a new segment, as extent 1 is in segment 1.
        let mut numbered = Numbered::new(2, 1);
        let mut extents: Vec<Extent<u64>> = (0..6).map(|_| numbered.allocate()).collect();
        assert_eq!(numbered.sealed(), [0, 1, 2]);
        assert_eq!(numbered.current(), Some((3, 4)));
        for extent in extents.drain(2..).rev() {
            numbered.release(extent);
        }
        assert_eq!(numbered.sealed(), [0, 1]);
        assert_eq!(numbered.current(), None);
        assert!(!numbered.segments.costs_more_to_keep(&extents[1]));
    }

    proptest! {
        /// The rules alone, for any order of allocations and releases
        /// (DESIGN.md §6.5). The extents of a segment follow each other
        /// without gaps, and only the newest segment gets extents. The
        /// current segment is never full. A sealed segment is open exactly
        /// while one of its extents is in use. The i-th oldest sealed open
        /// segment holds at least `minimum × 2^(i−1)` bytes, which bounds
        /// the count of open segments by the logarithm of the bytes that
        /// they hold. At most one file is created per `minimum` bytes of
        /// extents.
        #[test]
        fn open_segments_stay_few_in_any_release_order(
            extent_size in 1u64..100,
            minimum in 1u64..1000,
            steps in proptest::collection::vec(
                proptest::option::weighted(0.45, any::<prop::sample::Index>()),
                0..600,
            ),
        ) {
            let mut numbered = Numbered::new(extent_size, minimum);
            let mut extents = Vec::new();
            for step in steps {
                match step {
                    // Release any extent in use.
                    Some(index) if !extents.is_empty() => {
                        numbered.release(extents.swap_remove(index.index(extents.len())));
                    }
                    _ => extents.push(numbered.allocate()),
                }
                let sealed = numbered.sealed();
                let current = numbered.current();
                if let Some((number, size)) = current {
                    prop_assert_eq!(size, numbered.sizes[number as usize]);
                    prop_assert!(!is_full(size, minimum, sealed.len()), "the current segment is full");
                }
                // The open segments are the current segment and the
                // segments with an extent in use.
                let current = current.map(|(number, _)| number);
                let open: Vec<u64> = sealed.iter().copied().chain(current).collect();
                let in_use = (0..)
                    .zip(&numbered.in_use)
                    .filter(|&(number, &count)| count > 0 || current == Some(number))
                    .map(|(number, _)| number);
                prop_assert_eq!(open, in_use.collect::<Vec<u64>>());
                let mut sealed_bytes = 0;
                for (i, &number) in sealed.iter().enumerate() {
                    let size = numbered.sizes[number as usize];
                    prop_assert!(size >= minimum << i, "segment {} is small", number);
                    sealed_bytes += size;
                }
                prop_assert!(sealed_bytes >= minimum * ((1 << sealed.len()) - 1));
                // Each file after the first one follows a sealed segment.
                let after_first = (numbered.sizes.len() as u64).saturating_sub(1);
                let allocated: u64 = numbered.sizes.iter().sum();
                prop_assert!(after_first * minimum <= allocated);
            }
        }

        /// The rules alone, when releases follow the allocation order, as
        /// in a spool (DESIGN.md §6.5). No segment gets an extent after its
        /// size passes the bytes in use plus two minimums. Only the oldest
        /// open segment holds released extents. If it is the current
        /// segment, it holds less than one minimum.
        #[test]
        fn segments_follow_the_bytes_in_use_when_releases_are_in_order(
            extent_size in 1u64..100,
            minimum in 1u64..1000,
            runs in proptest::collection::vec((any::<bool>(), 1usize..80), 0..40),
        ) {
            let mut numbered = Numbered::new(extent_size, minimum);
            let mut extents = VecDeque::new();
            for (allocate, count) in runs {
                for _ in 0..count {
                    if allocate {
                        let bytes_in_use = extents.len() as u64 * extent_size;
                        let extent = numbered.allocate();
                        prop_assert!(extent.offset() < bytes_in_use + 2 * minimum);
                        extents.push_back(extent);
                    } else if let Some(extent) = extents.pop_front() {
                        numbered.release(extent);
                    }
                    let sealed = numbered.sealed();
                    let current = numbered.current();
                    let open = sealed.iter().chain(current.iter().map(|(number, _)| number));
                    for &number in open.skip(1) {
                        let number = number as usize;
                        prop_assert_eq!(numbered.in_use[number] * extent_size, numbered.sizes[number]);
                    }
                    if let Some((_, size)) = current && sealed.is_empty() {
                        prop_assert!(size < minimum);
                    }
                }
            }
        }
    }
}

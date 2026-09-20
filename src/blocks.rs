//! Block selection and file hashing (DESIGN.md §4.1 and §4.5).

use std::fmt;
use std::mem;
use std::ops::Range;
use std::os::fd::BorrowedFd;

use rustix::fs::{Advice, SeekFrom};
use rustix::io::Errno;

use crate::hash::weak::WeakHasher;
use crate::hash::{PREFIX_LEN, StrongHash};

/// Blocks with any period from 1 to `MAX_PERIOD` are not indexed.
pub const MAX_PERIOD: usize = 16;

/// Rows per `Blocks` message. Bounds the size of one message.
pub const BATCH_ROWS: usize = 1024;

/// One batched read of contiguous data requests up to this many bytes,
/// rounded down to whole blocks. A block larger than this is read in one
/// call.
const MAX_BATCH: u64 = 1024 * 1024;

/// A planned read that stops at a hole and is shorter than this window,
/// rounded down to whole blocks, extends to the whole window when more
/// data starts inside it. The extended read includes small holes instead
/// of seeking around them.
const HOLE_WINDOW: u64 = 128 * 1024;

/// One indexed block of a file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockRow {
    pub block_no: u64,
    pub weak: u64,
    pub strong_prefix: [u8; PREFIX_LEN],
}

/// True when `block[i] == block[i - p]` for all `p <= i < block.len()` for
/// some period `p` in `1..=MAX_PERIOD`. A block no longer than `MAX_PERIOD`
/// bytes satisfies this for `p = block.len()` and is periodic.
pub fn is_periodic(block: &[u8]) -> bool {
    (1..=MAX_PERIOD).any(|p| p >= block.len() || block[p..] == block[..block.len() - p])
}

/// What hashing one file did.
#[derive(Default, Debug)]
pub struct Outcome {
    /// Complete blocks read.
    pub blocks_read: u64,
    /// Blocks read that were not periodic, so they were emitted.
    pub blocks_indexed: u64,
    /// Blocks skipped without reading because they were wholly inside a
    /// hole reported by the filesystem.
    pub blocks_in_holes: u64,
}

/// A system call failed while hashing; `what` names the operation.
#[derive(Debug)]
pub struct HashError {
    pub what: &'static str,
    pub errno: Errno,
}

impl fmt::Display for HashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.what, self.errno)
    }
}

/// Hashing policy for one index: block size, weak hash, and strong hash.
pub struct BlockHasher {
    weak: WeakHasher,
    strong: StrongHash,
}

impl BlockHasher {
    pub fn new(block_size: u32, strong: StrongHash) -> Self {
        BlockHasher {
            weak: WeakHasher::new(block_size as usize),
            strong,
        }
    }

    pub fn block_size(&self) -> u32 {
        self.weak.block_size() as u32
    }

    /// Hash the complete blocks of an open regular file, reading until it
    /// ends. A file that changes meanwhile is hashed as read: the caller
    /// stores the metadata observed before hashing, so the next update
    /// rehashes the file. `buf` is a reusable buffer. `emit` receives
    /// bounded batches of rows.
    pub fn hash_file(
        &self,
        fd: BorrowedFd<'_>,
        buf: &mut Vec<u8>,
        emit: &mut dyn FnMut(Vec<BlockRow>),
    ) -> Result<Outcome, HashError> {
        let block_size = self.block_size() as usize;
        let bs = block_size as u64;
        let mut reads = ReadPlanner::new(bs);
        buf.resize(reads.max_batch as usize, 0);
        // Optional hints; failures do not matter.
        let _ = rustix::fs::fadvise(fd, 0, None, Advice::Sequential);
        let _ = rustix::fs::fadvise(fd, 0, None, Advice::NoReuse);

        let mut rows = Vec::with_capacity(BATCH_ROWS);
        let mut outcome = Outcome::default();
        let mut block_no = 0u64;

        loop {
            let range = reads.plan(block_no * bs, |from| rustix::fs::seek(fd, from))?;
            let next = range.start / bs;
            outcome.blocks_in_holes += next - block_no;
            block_no = next;
            if range.is_empty() {
                break;
            }

            let len = (range.end - range.start) as usize;
            let got = read_at(fd, &mut buf[..len], range.start)?;
            for block in buf[..got].chunks_exact(block_size) {
                outcome.blocks_read += 1;
                if !is_periodic(block) {
                    let digest = self.strong.digest(block);
                    let mut strong_prefix = [0u8; PREFIX_LEN];
                    strong_prefix.copy_from_slice(&digest[..PREFIX_LEN]);
                    rows.push(BlockRow {
                        block_no,
                        weak: self.weak.hash(block),
                        strong_prefix,
                    });
                    outcome.blocks_indexed += 1;
                    if rows.len() == BATCH_ROWS {
                        emit(mem::replace(&mut rows, Vec::with_capacity(BATCH_ROWS)));
                    }
                }
                block_no += 1;
            }
            if got < len {
                // The end of the file: no further complete block. The
                // incomplete final block, if any, is discarded.
                break;
            }
        }
        if !rows.is_empty() {
            emit(rows);
        }
        Ok(outcome)
    }
}

/// Sparse-file discovery and bounded read planning (DESIGN.md §4.5).
/// The hasher only needs the resulting ranges, including gaps to count as
/// holes; seek fallbacks and cached extent information stay here.
struct ReadPlanner {
    block_size: u64,
    max_batch: u64,
    hole_window: u64,
    mode: ReadMode,
}

enum ReadMode {
    Sparse {
        /// Discover another extent at or beyond this boundary. Initially
        /// zero; reset to zero when a race invalidates the extent.
        hole_start: u64,
        /// A SEEK_DATA result already obtained at `hole_start`.
        lookahead: Option<SeekData>,
    },
    Sequential,
}

impl ReadPlanner {
    fn new(block_size: u64) -> Self {
        Self {
            block_size,
            // Whole blocks, at least one, even for blocks over 1 MiB.
            max_batch: block_size * (MAX_BATCH / block_size).max(1),
            hole_window: block_size * (HOLE_WINDOW / block_size).max(1),
            mode: ReadMode::Sparse {
                hole_start: 0,
                lookahead: None,
            },
        }
    }

    /// The next aligned range to read, skipping whole blocks in holes.
    /// An empty range means no more data; its start accounts for trailing
    /// hole blocks. The caller supplies the next unread, aligned offset
    /// and stops on a short read.
    fn plan(
        &mut self,
        offset: u64,
        mut raw_seek: impl FnMut(SeekFrom) -> rustix::io::Result<u64>,
    ) -> Result<Range<u64>, HashError> {
        let mut seek = |from| rustix::io::retry_on_intr(|| raw_seek(from));
        let ordinary = offset..offset + self.max_batch;
        let ReadMode::Sparse {
            hole_start,
            lookahead,
        } = &mut self.mode
        else {
            return Ok(ordinary);
        };

        let mut start = offset;
        if offset >= *hole_start {
            let found = match lookahead.take() {
                Some(found) => found,
                None => seek_data(&mut seek, offset)?,
            };
            match found {
                SeekData::Found(data) => match seek(SeekFrom::Hole(data)) {
                    Ok(hole) if hole > data && hole > offset => {
                        start = (data / self.block_size * self.block_size).max(offset);
                        *hole_start = hole;
                    }
                    Ok(_) | Err(Errno::NXIO) => {
                        // Truncation, hole punching, or inconsistent seek
                        // results can invalidate the extent. Read one batch
                        // at the unread offset to make progress, then seek
                        // again. The stale lookahead was consumed above.
                        *hole_start = 0;
                        return Ok(ordinary);
                    }
                    Err(Errno::INVAL | Errno::NOTSUP) => {
                        self.mode = ReadMode::Sequential;
                        return Ok(ordinary);
                    }
                    Err(errno) => {
                        return Err(HashError {
                            what: "lseek",
                            errno,
                        });
                    }
                },
                SeekData::NoData => {
                    let end = seek(SeekFrom::End(0)).map_err(|errno| HashError {
                        what: "lseek",
                        errno,
                    })?;
                    // Only complete blocks before EOF count as holes;
                    // truncation must not move the unread offset backward.
                    let end = (end / self.block_size * self.block_size).max(offset);
                    return Ok(end..end);
                }
                SeekData::Unsupported => {
                    self.mode = ReadMode::Sequential;
                    return Ok(ordinary);
                }
            }
        }

        // Round the hole boundary up so partial overlaps are read in full.
        // For short reads, look ahead and include small holes within one
        // window. Otherwise retain the answer for the following extent.
        let mut len = self
            .max_batch
            .min(hole_start.next_multiple_of(self.block_size) - start);
        if len < self.hole_window {
            match seek_data(&mut seek, *hole_start)? {
                SeekData::Found(data) if data < start + self.hole_window => len = self.hole_window,
                found => *lookahead = Some(found),
            }
        }
        Ok(start..start + len)
    }
}

/// The result of one `SEEK_DATA`, with the conditions the read policy
/// handles mapped to values (DESIGN.md §4.5).
enum SeekData {
    /// The offset of the first data at or after the sought offset.
    Found(u64),
    /// `ENXIO`: no data at or after the sought offset, before the end of
    /// the file.
    NoData,
    /// `EINVAL` or `EOPNOTSUPP`: the filesystem does not support hole
    /// seeking; fall back to reading everything.
    Unsupported,
}

/// `SEEK_DATA` at `offset`. Errors other than the mapped ones abandon the
/// file.
fn seek_data(
    seek: &mut impl FnMut(SeekFrom) -> rustix::io::Result<u64>,
    offset: u64,
) -> Result<SeekData, HashError> {
    match seek(SeekFrom::Data(offset)) {
        Ok(data) => Ok(SeekData::Found(data)),
        Err(Errno::NXIO) => Ok(SeekData::NoData),
        Err(Errno::INVAL | Errno::NOTSUP) => Ok(SeekData::Unsupported),
        Err(errno) => Err(HashError {
            what: "lseek",
            errno,
        }),
    }
}

/// Fill `buf` from `offset`, accumulating short reads. Returns the number
/// of bytes read, which is less than `buf.len()` only at the end of the file.
fn read_at(fd: BorrowedFd<'_>, buf: &mut [u8], offset: u64) -> Result<usize, HashError> {
    let mut done = 0usize;
    while done < buf.len() {
        match rustix::io::retry_on_intr(|| {
            rustix::io::pread(fd, &mut buf[done..], offset + done as u64)
        }) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(errno) => {
                return Err(HashError {
                    what: "pread",
                    errno,
                });
            }
        }
    }
    Ok(done)
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Seek, SeekFrom, Write};
    use std::os::fd::AsFd;

    use rustix::fs::SeekFrom::{Data, End, Hole};

    use super::*;

    /// Supply a precise syscall interleaving without relying on thread
    /// scheduling or the test filesystem's sparse-file support.
    fn plan_with_seeks(
        reads: &mut ReadPlanner,
        offset: u64,
        seeks: &[(rustix::fs::SeekFrom, rustix::io::Result<u64>)],
    ) -> Result<Range<u64>, HashError> {
        let mut seeks = seeks.iter();
        let result = reads.plan(offset, |from| {
            let (expected, result) = seeks.next().expect("unexpected seek");
            assert_eq!(from, *expected);
            *result
        });
        assert!(
            seeks.next().is_none(),
            "planner made fewer seeks than expected"
        );
        result
    }

    #[test]
    fn seek_races_read_one_batch_then_resume_seeking() {
        let data = MAX_BATCH * 2;
        for hole in [Err(Errno::NXIO), Ok(data)] {
            let mut reads = ReadPlanner::new(BS as u64);
            // The reported data disappears before SEEK_HOLE. Recovery
            // must read at the unread offset, not skip to stale data.
            assert_eq!(
                plan_with_seeks(&mut reads, 0, &[(Data(0), Ok(data)), (Hole(data), hole)]).unwrap(),
                0..MAX_BATCH
            );
            // A later hole is still skipped instead of read through.
            assert_eq!(
                plan_with_seeks(
                    &mut reads,
                    MAX_BATCH,
                    &[
                        (Data(MAX_BATCH), Ok(data)),
                        (Hole(data), Ok(data + MAX_BATCH))
                    ],
                )
                .unwrap(),
                data..data + MAX_BATCH
            );
        }
    }

    #[test]
    fn backward_seek_results_read_one_batch_then_resume_seeking() {
        let offset = MAX_BATCH * 2;
        for hole in [BS as u64, offset - 1, offset] {
            let mut reads = ReadPlanner::new(BS as u64);
            // A broken filesystem reports data behind the requested offset
            // and a hole ending before or at the next unread byte.
            assert_eq!(
                plan_with_seeks(
                    &mut reads,
                    offset,
                    &[(Data(offset), Ok(0)), (Hole(0), Ok(hole))]
                )
                .unwrap(),
                offset..offset + MAX_BATCH
            );
            let next = offset + MAX_BATCH;
            let data = next + MAX_BATCH;
            assert_eq!(
                plan_with_seeks(
                    &mut reads,
                    next,
                    &[(Data(next), Ok(data)), (Hole(data), Ok(data + MAX_BATCH))],
                )
                .unwrap(),
                data..data + MAX_BATCH
            );
        }
    }

    #[test]
    fn stale_lookahead_is_discarded_after_a_seek_race() {
        let bs = BS as u64;
        let data = MAX_BATCH * 3;
        for hole in [Err(Errno::NXIO), Ok(data)] {
            let mut reads = ReadPlanner::new(bs);
            assert_eq!(
                plan_with_seeks(
                    &mut reads,
                    0,
                    &[(Data(0), Ok(0)), (Hole(0), Ok(bs)), (Data(bs), Ok(data))],
                )
                .unwrap(),
                0..bs
            );
            assert_eq!(
                plan_with_seeks(&mut reads, bs, &[(Hole(data), hole)]).unwrap(),
                bs..bs + MAX_BATCH
            );
            let offset = bs + MAX_BATCH;
            assert_eq!(
                plan_with_seeks(
                    &mut reads,
                    offset,
                    &[(Data(offset), Ok(data)), (Hole(data), Ok(data + MAX_BATCH))],
                )
                .unwrap(),
                data..data + MAX_BATCH
            );
        }
    }

    #[test]
    fn repeated_seek_races_make_progress_until_eof() {
        let mut reads = ReadPlanner::new(BS as u64);
        for offset in [0, MAX_BATCH, MAX_BATCH * 2] {
            assert_eq!(
                plan_with_seeks(
                    &mut reads,
                    offset,
                    &[(Data(offset), Ok(offset)), (Hole(offset), Err(Errno::NXIO))],
                )
                .unwrap(),
                offset..offset + MAX_BATCH
            );
        }
        let offset = MAX_BATCH * 3;
        // Shrinking past the unread offset must not count removed blocks
        // as holes or move the offset backward.
        assert_eq!(
            plan_with_seeks(
                &mut reads,
                offset,
                &[(Data(offset), Err(Errno::NXIO)), (End(0), Ok(0))],
            )
            .unwrap(),
            offset..offset
        );
    }

    #[test]
    fn interrupted_seeks_are_retried() {
        let bs = BS as u64;
        let mut reads = ReadPlanner::new(bs);
        assert_eq!(
            plan_with_seeks(
                &mut reads,
                0,
                &[
                    (Data(0), Err(Errno::INTR)),
                    (Data(0), Ok(0)),
                    (Hole(0), Err(Errno::INTR)),
                    (Hole(0), Ok(bs)),
                    (Data(bs), Err(Errno::INTR)),
                    (Data(bs), Err(Errno::NXIO)),
                ],
            )
            .unwrap(),
            0..bs
        );
        assert_eq!(
            plan_with_seeks(
                &mut reads,
                bs,
                &[(End(0), Err(Errno::INTR)), (End(0), Ok(bs * 4 + 100))],
            )
            .unwrap(),
            bs * 4..bs * 4
        );
    }

    #[test]
    fn unsupported_seeks_disable_sparse_reading() {
        let bs = BS as u64;
        for errno in [Errno::INVAL, Errno::NOTSUP] {
            for seeks in [
                vec![(Data(0), Err(errno))],
                vec![(Data(0), Ok(0)), (Hole(0), Err(errno))],
                vec![(Data(0), Ok(0)), (Hole(0), Ok(bs)), (Data(bs), Err(errno))],
            ] {
                let mut reads = ReadPlanner::new(bs);
                let first = plan_with_seeks(&mut reads, 0, &seeks).unwrap();
                let offset = first.end;
                assert_eq!(first.start, 0);
                assert_eq!(
                    plan_with_seeks(&mut reads, offset, &[]).unwrap(),
                    offset..offset + MAX_BATCH
                );
            }
        }
    }

    #[test]
    fn other_seek_errors_are_reported() {
        let bs = BS as u64;
        for seeks in [
            vec![(Data(0), Err(Errno::IO))],
            vec![(Data(0), Ok(0)), (Hole(0), Err(Errno::IO))],
            vec![(Data(0), Err(Errno::NXIO)), (End(0), Err(Errno::IO))],
            vec![
                (Data(0), Ok(0)),
                (Hole(0), Ok(bs)),
                (Data(bs), Err(Errno::IO)),
            ],
        ] {
            let error = plan_with_seeks(&mut ReadPlanner::new(bs), 0, &seeks).unwrap_err();
            assert_eq!(error.what, "lseek");
            assert_eq!(error.errno, Errno::IO);
        }
    }

    #[test]
    fn periodic_blocks() {
        assert!(is_periodic(&[0u8; 4096]));
        assert!(is_periodic(&[7u8; 4096]));
        let mut alternating = vec![0u8; 4096];
        for (i, b) in alternating.iter_mut().enumerate() {
            *b = (i % 2) as u8;
        }
        assert!(is_periodic(&alternating));
        let mut period16 = vec![0u8; 4096];
        for (i, b) in period16.iter_mut().enumerate() {
            *b = (i % 16) as u8;
        }
        assert!(is_periodic(&period16));
        // Partial final repetitions count: 4096 is not a multiple of 13.
        let mut period13 = vec![0u8; 4096];
        for (i, b) in period13.iter_mut().enumerate() {
            *b = (i % 13) as u8;
        }
        assert!(is_periodic(&period13));
    }

    #[test]
    fn aperiodic_blocks() {
        let mut period17 = vec![0u8; 4096];
        for (i, b) in period17.iter_mut().enumerate() {
            *b = (i % 17) as u8;
        }
        assert!(!is_periodic(&period17));
        let mut counter = vec![0u8; 4096];
        for (i, b) in counter.iter_mut().enumerate() {
            *b = (i / 16) as u8;
        }
        assert!(!is_periodic(&counter));
        let mut almost = vec![0u8; 4096];
        almost[4095] = 1;
        assert!(!is_periodic(&almost));
    }

    const BS: usize = 4096;

    /// Bytes with no period of 16 or less.
    fn useful(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 7919 % 251) as u8).collect()
    }

    /// A read-write temporary file with `content`.
    fn temp_file(dir: &tempfile::TempDir, content: &[u8]) -> File {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("file"))
            .unwrap();
        file.write_all(content).unwrap();
        file
    }

    /// Write `data` at `offset`, leaving a hole over any gap behind it.
    fn write_at(file: &mut File, offset: usize, data: &[u8]) {
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(data).unwrap();
    }

    /// The indexed block numbers and the outcome of hashing `file` with
    /// `block_size` blocks.
    fn hash_with(block_size: usize, file: &File) -> (Vec<u64>, Outcome) {
        let hasher = BlockHasher::new(block_size as u32, StrongHash::Blake3);
        let mut rows = Vec::new();
        let outcome = hasher
            .hash_file(file.as_fd(), &mut Vec::new(), &mut |batch| {
                rows.extend(batch);
            })
            .unwrap();
        (rows.iter().map(|row| row.block_no).collect(), outcome)
    }

    /// The indexed block numbers and the outcome of hashing `file`.
    fn hash(file: &File) -> (Vec<u64>, Outcome) {
        hash_with(BS, file)
    }

    /// True when the filesystem reports the hole that follows the first
    /// `data_len` bytes of `file`. A filesystem that reports no holes
    /// reads them as data.
    fn reports_holes(file: &File, data_len: usize) -> bool {
        rustix::fs::seek(file.as_fd(), rustix::fs::SeekFrom::Hole(0)).unwrap() == data_len as u64
    }

    #[test]
    fn data_past_a_hole_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = temp_file(&dir, &useful(BS));
        // Block 0 is data, blocks 1 and 2 are a hole, block 3 is data.
        write_at(&mut file, BS * 3, &useful(BS));
        let (blocks, outcome) = hash(&file);
        assert_eq!(blocks, vec![0, 3]);
        // The hole is smaller than the hole window: one read covers the
        // whole file, and the hole blocks are read, not skipped.
        assert_eq!(outcome.blocks_read, 4);
        assert_eq!(outcome.blocks_in_holes, 0);
    }

    #[test]
    fn trailing_hole_blocks_are_counted() {
        let dir = tempfile::tempdir().unwrap();
        let file = temp_file(&dir, &useful(BS));
        // Block 0 is data; blocks 1 to 3 are a hole that ends the file.
        file.set_len((BS * 4) as u64).unwrap();
        let (blocks, outcome) = hash(&file);
        assert_eq!(blocks, vec![0]);
        assert_eq!(outcome.blocks_read + outcome.blocks_in_holes, 4);
        if reports_holes(&file, BS) {
            assert_eq!(outcome.blocks_in_holes, 3);
        }
    }

    #[test]
    fn small_holes_are_read_in_windows() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = temp_file(&dir, &[]);
        // Blocks 0, 2, 4, ..., 78 are data; the odd blocks are holes.
        // The file covers two hole windows and a partial third.
        for block in (0..80usize).step_by(2) {
            write_at(&mut file, BS * block, &useful(BS));
        }
        file.set_len((BS * 80) as u64).unwrap();
        let (blocks, outcome) = hash(&file);
        assert_eq!(blocks, (0..80u64).step_by(2).collect::<Vec<u64>>());
        // Window extension reads every small hole.
        assert_eq!(outcome.blocks_read, 80);
        assert_eq!(outcome.blocks_in_holes, 0);
    }

    #[test]
    fn large_holes_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = temp_file(&dir, &useful(BS));
        // Blocks 0, 256, and 512 are data; the two holes between them
        // are larger than the hole window.
        write_at(&mut file, BS * 256, &useful(BS));
        write_at(&mut file, BS * 512, &useful(BS));
        let (blocks, outcome) = hash(&file);
        assert_eq!(blocks, vec![0, 256, 512]);
        assert_eq!(outcome.blocks_read + outcome.blocks_in_holes, 513);
        if reports_holes(&file, BS) {
            assert_eq!(outcome.blocks_read, 3);
            assert_eq!(outcome.blocks_in_holes, 510);
        }
    }

    #[test]
    fn mixed_hole_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = temp_file(&dir, &useful(BS));
        // Block 1 is a small hole; blocks 3 to 66 are a large hole.
        write_at(&mut file, BS * 2, &useful(BS));
        write_at(&mut file, BS * 67, &useful(BS));
        let (blocks, outcome) = hash(&file);
        assert_eq!(blocks, vec![0, 2, 67]);
        assert_eq!(outcome.blocks_read + outcome.blocks_in_holes, 68);
        if reports_holes(&file, BS) {
            // The window extended over the small hole reads blocks 0 to
            // 31, including the start of the large hole; the remainder of
            // the large hole is skipped.
            assert_eq!(outcome.blocks_read, 33);
            assert_eq!(outcome.blocks_in_holes, 35);
        }
    }

    #[test]
    fn contiguous_batches_and_partial_tail() {
        let dir = tempfile::tempdir().unwrap();
        // One full read batch, two more blocks, and a partial tail.
        let file = temp_file(&dir, &useful(1024 * 1024 + BS * 2 + 100));
        let (blocks, outcome) = hash(&file);
        assert_eq!(blocks, (0..258u64).collect::<Vec<u64>>());
        assert_eq!(outcome.blocks_read, 258);
        assert_eq!(outcome.blocks_in_holes, 0);
    }

    #[test]
    fn unusual_block_size_reads_through_holes() {
        // 3000-byte blocks: the batch and the window round down to whole
        // blocks. The whole file fits in one extended window.
        let dir = tempfile::tempdir().unwrap();
        let mut file = temp_file(&dir, &useful(3000));
        write_at(&mut file, 3000 * 10, &useful(3000));
        file.set_len(3000 * 20).unwrap();
        let (blocks, outcome) = hash_with(3000, &file);
        assert_eq!(blocks, vec![0, 10]);
        assert_eq!(outcome.blocks_read, 20);
        assert_eq!(outcome.blocks_in_holes, 0);
    }

    #[test]
    fn unusual_block_size_skips_large_holes() {
        // 12288-byte blocks make a 122880-byte hole window; the hole of
        // blocks 1 to 14 is larger than that window.
        let dir = tempfile::tempdir().unwrap();
        let mut file = temp_file(&dir, &useful(12288));
        write_at(&mut file, 12288 * 15, &useful(12288));
        let (blocks, outcome) = hash_with(12288, &file);
        assert_eq!(blocks, vec![0, 15]);
        assert_eq!(outcome.blocks_read + outcome.blocks_in_holes, 16);
        if reports_holes(&file, 12288) {
            assert_eq!(outcome.blocks_read, 2);
            assert_eq!(outcome.blocks_in_holes, 14);
        }
    }

    #[test]
    fn blocks_larger_than_the_batch_are_read_whole() {
        // A 2 MiB block exceeds both read sizes: every read is one full
        // block, and a whole-block hole is never read.
        const HUGE: usize = 2 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let mut file = temp_file(&dir, &useful(HUGE));
        // Block 1 is a hole; block 2 is data.
        write_at(&mut file, HUGE * 2, &useful(HUGE));
        let (blocks, outcome) = hash_with(HUGE, &file);
        assert_eq!(blocks, vec![0, 2]);
        assert_eq!(outcome.blocks_read + outcome.blocks_in_holes, 3);
        if reports_holes(&file, HUGE) {
            assert_eq!(outcome.blocks_read, 2);
            assert_eq!(outcome.blocks_in_holes, 1);
        }
    }

    #[test]
    fn shrinking_ends_the_file_early() {
        let dir = tempfile::tempdir().unwrap();
        let file = temp_file(&dir, &useful(BS * 3));
        // Truncation, not a hole, removes the last two blocks before
        // hashing starts, and leaves a partial block behind block 0.
        file.set_len((BS + 100) as u64).unwrap();
        let (blocks, outcome) = hash(&file);
        assert_eq!(blocks, vec![0]);
        assert_eq!(outcome.blocks_read, 1);
        assert_eq!(outcome.blocks_in_holes, 0);
    }
}

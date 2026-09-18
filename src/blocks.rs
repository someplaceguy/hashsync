//! Block selection and file hashing (DESIGN.md §4.1 and §4.5).

use std::fmt;
use std::mem;
use std::os::fd::BorrowedFd;

use rustix::fs::{Advice, SeekFrom};
use rustix::io::Errno;

use crate::hash::weak::WeakHasher;
use crate::hash::{PREFIX_LEN, StrongHash};

/// Blocks with any period from 1 to `MAX_PERIOD` are not indexed.
pub const MAX_PERIOD: usize = 16;

/// Rows per `Blocks` message. Bounds the size of one message.
pub const BATCH_ROWS: usize = 1024;

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
        let block_size = self.block_size();
        let bs = u64::from(block_size);
        buf.resize(block_size as usize, 0);
        // Optional hints; failures do not matter.
        let _ = rustix::fs::fadvise(fd, 0, None, Advice::Sequential);
        let _ = rustix::fs::fadvise(fd, 0, None, Advice::NoReuse);

        let mut rows = Vec::with_capacity(BATCH_ROWS);
        let mut outcome = Outcome::default();
        let mut block_no = 0u64;
        // While `seeking`, `[data_start, hole_start)` is the data region
        // found by the last SEEK_DATA/SEEK_HOLE pair. Seek again for blocks
        // starting at or after `hole_start`.
        let mut seeking = true;
        let mut data_start = 0u64;
        let mut hole_start = 0u64;

        loop {
            let offset = block_no * bs;
            if seeking && offset >= hole_start {
                match rustix::fs::seek(fd, SeekFrom::Data(offset)) {
                    Ok(data) => match rustix::fs::seek(fd, SeekFrom::Hole(data)) {
                        Ok(hole) if hole > data => {
                            data_start = data;
                            hole_start = hole;
                        }
                        // No progress: read everything from here on.
                        Ok(_) | Err(Errno::INVAL | Errno::NOTSUP) => seeking = false,
                        Err(errno) => {
                            return Err(HashError {
                                what: "lseek",
                                errno,
                            });
                        }
                    },
                    // No data before EOF: the blocks that end at or before
                    // the current end of the file are wholly inside a hole.
                    // Blocks beyond it, as after truncation, are not holes.
                    Err(Errno::NXIO) => {
                        let end =
                            rustix::fs::seek(fd, SeekFrom::End(0)).map_err(|errno| HashError {
                                what: "lseek",
                                errno,
                            })?;
                        outcome.blocks_in_holes += (end / bs).saturating_sub(block_no);
                        break;
                    }
                    Err(Errno::INVAL | Errno::NOTSUP) => seeking = false,
                    Err(errno) => {
                        return Err(HashError {
                            what: "lseek",
                            errno,
                        });
                    }
                }
                continue;
            }
            if seeking && offset + bs <= data_start {
                // Wholly inside a hole: skip to the block containing data.
                let next = data_start / bs;
                outcome.blocks_in_holes += next - block_no;
                block_no = next;
                continue;
            }

            if read_at(fd, buf, offset)? < buf.len() {
                // The end of the file: no further complete block.
                break;
            }
            outcome.blocks_read += 1;
            if !is_periodic(buf) {
                let digest = self.strong.digest(buf);
                let mut strong_prefix = [0u8; PREFIX_LEN];
                strong_prefix.copy_from_slice(&digest[..PREFIX_LEN]);
                rows.push(BlockRow {
                    block_no,
                    weak: self.weak.hash(buf),
                    strong_prefix,
                });
                outcome.blocks_indexed += 1;
                if rows.len() == BATCH_ROWS {
                    emit(mem::replace(&mut rows, Vec::with_capacity(BATCH_ROWS)));
                }
            }
            block_no += 1;
        }
        if !rows.is_empty() {
            emit(rows);
        }
        Ok(outcome)
    }
}

/// Fill `buf` from `offset`, accumulating short reads. Returns the number
/// of bytes read, which is less than `buf.len()` only at the end of the file.
fn read_at(fd: BorrowedFd<'_>, buf: &mut [u8], offset: u64) -> Result<usize, HashError> {
    let mut done = 0usize;
    while done < buf.len() {
        match rustix::io::pread(fd, &mut buf[done..], offset + done as u64) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(Errno::INTR) => {}
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

    use super::*;

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

    /// The indexed block numbers and the outcome of hashing `file`.
    fn hash(file: &File) -> (Vec<u64>, Outcome) {
        let hasher = BlockHasher::new(BS as u32, StrongHash::Blake3);
        let mut rows = Vec::new();
        let outcome = hasher
            .hash_file(file.as_fd(), &mut Vec::new(), &mut |batch| {
                rows.extend(batch);
            })
            .unwrap();
        (rows.iter().map(|row| row.block_no).collect(), outcome)
    }

    /// True when the filesystem reports the hole that follows block 0 of
    /// `file`. A filesystem that reports no holes reads them as data.
    fn reports_holes(file: &File) -> bool {
        rustix::fs::seek(file.as_fd(), rustix::fs::SeekFrom::Hole(0)).unwrap() == BS as u64
    }

    #[test]
    fn data_past_a_hole_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = temp_file(&dir, &useful(BS));
        // Block 0 is data, blocks 1 and 2 are a hole, block 3 is data.
        file.seek(SeekFrom::Start((BS * 3) as u64)).unwrap();
        file.write_all(&useful(BS)).unwrap();
        let (blocks, outcome) = hash(&file);
        assert_eq!(blocks, vec![0, 3]);
        assert_eq!(outcome.blocks_read + outcome.blocks_in_holes, 4);
        if reports_holes(&file) {
            assert_eq!(outcome.blocks_in_holes, 2);
        }
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
        if reports_holes(&file) {
            assert_eq!(outcome.blocks_in_holes, 3);
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

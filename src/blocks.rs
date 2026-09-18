//! Block selection and file hashing (DESIGN.md §4.1 and §4.5).

use std::fmt;
use std::mem;
use std::os::fd::BorrowedFd;

use rustix::fs::{Advice, SeekFrom};
use rustix::io::Errno;

use crate::fsops::{self, Meta};
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

#[derive(Default, Debug)]
pub struct Outcome {
    pub blocks_read: u64,
    pub blocks_indexed: u64,
    pub blocks_in_holes: u64,
}

#[derive(Debug)]
pub enum HashError {
    /// A system call failed; `what` names the operation.
    Io { what: &'static str, errno: Errno },
    /// EOF inside the initial full-block range.
    Truncated,
    /// Size or ctime changed while hashing.
    Changed,
}

impl fmt::Display for HashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HashError::Io { what, errno } => write!(f, "{what}: {errno}"),
            HashError::Truncated => write!(f, "file shrank while hashing"),
            HashError::Changed => write!(f, "file changed while hashing"),
        }
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

    /// Hash the complete blocks of an open regular file.
    ///
    /// `meta` is the metadata observed before hashing; the size and ctime
    /// are checked again afterwards. `buf` is a reusable buffer. `emit`
    /// receives bounded batches of rows.
    pub fn hash_file(
        &self,
        fd: BorrowedFd<'_>,
        meta: &Meta,
        buf: &mut Vec<u8>,
        emit: &mut dyn FnMut(Vec<BlockRow>),
    ) -> Result<Outcome, HashError> {
        let block_size = self.block_size();
        let bs = u64::from(block_size);
        let full_blocks = meta.size / bs;
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

        while block_no < full_blocks {
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
                            return Err(HashError::Io {
                                what: "lseek",
                                errno,
                            });
                        }
                    },
                    // No data before EOF: the remaining blocks are holes.
                    Err(Errno::NXIO) => break,
                    Err(Errno::INVAL | Errno::NOTSUP) => seeking = false,
                    Err(errno) => {
                        return Err(HashError::Io {
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

            read_exact_at(fd, buf, offset)?;
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
        outcome.blocks_in_holes += full_blocks - block_no;
        if !rows.is_empty() {
            emit(rows);
        }

        let after = fsops::stat(fd).map_err(|errno| HashError::Io {
            what: "statx",
            errno,
        })?;
        if after.size != meta.size || after.ctime_ns != meta.ctime_ns {
            return Err(HashError::Changed);
        }
        Ok(outcome)
    }
}

/// Fill `buf` from `offset`, accumulating short reads.
fn read_exact_at(fd: BorrowedFd<'_>, buf: &mut [u8], offset: u64) -> Result<(), HashError> {
    let mut done = 0usize;
    while done < buf.len() {
        match rustix::io::pread(fd, &mut buf[done..], offset + done as u64) {
            Ok(0) => return Err(HashError::Truncated),
            Ok(n) => done += n,
            Err(Errno::INTR) => {}
            Err(errno) => {
                return Err(HashError::Io {
                    what: "pread",
                    errno,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
}

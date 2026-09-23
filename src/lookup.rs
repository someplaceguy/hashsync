//! The receiver's lookup workers (DESIGN.md §6.4): a pool that answers
//! queries out of order, each worker with its own index reader.
//!
//! For a query, a worker takes the candidate blocks with the query's
//! strong-hash prefix from the index, reads each candidate from the root's
//! current location, and verifies the full digest of the bytes it read.
//! The first verified block goes to the pins. Every candidate counts
//! against `candidate_attempts`, including one without a usable location,
//! so stale rows cannot make a lookup unbounded. Filesystem failures only
//! reject a candidate.

use std::collections::HashMap;
use std::fs::File;
use std::os::fd::AsFd;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, unbounded};
use rustix::fs::FileType;

use crate::config::Config;
use crate::fsops;
use crate::hash::{PREFIX_LEN, Strong, StrongHash};
use crate::index::{Index, Reader};
use crate::pins::{Block, Pins};
use crate::protocol::Query;
use crate::roots::Location;

/// Start the lookup workers. `locations` are the current root locations
/// from this run's root resolution; an indexed root without one has no
/// usable location. Returns the queue of the queries to answer.
pub fn start(
    index: &Index,
    locations: Vec<Location>,
    pins: &Arc<Pins>,
    config: &Config,
) -> Sender<Query> {
    let mut by_path: HashMap<Vec<u8>, Location> = locations
        .into_iter()
        .map(|location| (location.path.clone(), location))
        .collect();
    let by_root: HashMap<i64, Location> = index
        .root_paths()
        .into_iter()
        .filter_map(|(root_id, path)| Some((root_id, by_path.remove(&path)?)))
        .collect();
    let sources = Arc::new(by_root);
    let (queries, queue) = unbounded();
    // A pool of 4 to 8 workers (DESIGN.md §6.4).
    let workers = std::thread::available_parallelism().map_or(4, |n| n.get().clamp(4, 8));
    for i in 0..workers {
        let mut worker = Worker {
            reader: index.reader(),
            sources: Arc::clone(&sources),
            strong: config.strong_hash,
            block_size: u64::from(config.block_size),
            attempts: config.candidate_attempts,
            buffer: vec![0; config.block_size as usize],
        };
        let pins = Arc::clone(pins);
        let queue: Receiver<Query> = queue.clone();
        std::thread::Builder::new()
            .name(format!("lookup{i}"))
            .spawn(move || {
                while let Ok(query) = queue.recv() {
                    pins.answer(query.offset, &query.strong, || worker.find(&query.strong));
                }
            })
            .expect("cannot start a lookup worker");
    }
    queries
}

struct Worker {
    reader: Reader,
    /// The current location of every indexed root that has one, by
    /// `roots.id`.
    sources: Arc<HashMap<i64, Location>>,
    strong: StrongHash,
    block_size: u64,
    attempts: u32,
    /// The buffer of the next candidate read.
    buffer: Vec<u8>,
}

impl Worker {
    /// A verified block with the full digest `strong`, from the first of
    /// at most `attempts` candidates that verifies (DESIGN.md §6.4, steps
    /// 2 to 5). The block is the very buffer that was verified.
    fn find(&mut self, strong: &Strong) -> Option<Block> {
        for candidate in self.reader.candidates(&strong[..PREFIX_LEN], self.attempts) {
            let Some((root_id, rel_path)) = candidate.path else {
                continue;
            };
            let Some(location) = self.sources.get(&root_id) else {
                continue;
            };
            let Some(offset) = candidate.block_no.checked_mul(self.block_size) else {
                continue;
            };
            if read_candidate(location, &rel_path, offset, &mut self.buffer).is_ok()
                && self.strong.digest(&self.buffer) == *strong
            {
                let fresh = vec![0; self.buffer.len()];
                return Some(Arc::new(std::mem::replace(&mut self.buffer, fresh)));
            }
        }
        None
    }
}

/// Fill `buf` from `offset` of the regular file at `rel_path` in the root
/// at `location` (DESIGN.md §6.4, steps 4 and 5). A single-file root has an
/// empty `rel_path`. No symlink is followed; the file is classified from
/// its own `O_PATH` descriptor, then reopened through procfs for reading.
/// Every descriptor closes on return.
fn read_candidate(
    location: &Location,
    rel_path: &[u8],
    offset: u64,
    buf: &mut [u8],
) -> std::io::Result<()> {
    let root = fsops::open_path(location.dir.as_fd(), &location.name)?;
    let file = if rel_path.is_empty() {
        root
    } else {
        fsops::open_beneath(root.as_fd(), rel_path)?
    };
    if fsops::stat(file.as_fd())?.kind != FileType::RegularFile {
        return Err(std::io::ErrorKind::InvalidInput.into());
    }
    let readable = File::from(fsops::reopen_file(file.as_fd())?);
    drop(file);
    readable.read_exact_at(buf, offset)
}

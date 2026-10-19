//! The receiver's index (DESIGN.md §4.4): a RocksDB database in one
//! directory.
//!
//! A lock on the file `hashsync.lock` in the directory gives process
//! exclusion: a modifying process holds an exclusive lock and opens the
//! database read/write, read-only processes hold shared locks and open it
//! read-only. Lock contention waits forever; there is no timeout. All
//! threads of a process share one database handle, and the lock stays
//! until the handle has closed.

pub mod changes;
mod layout;

use std::ffi::OsStr;
use std::mem;
use std::ops::RangeInclusive;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;

use bstr::ByteSlice;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType,
    DBPinnableSlice, DBRawIterator, LogLevel, Options, ReadOptions, SliceTransform,
    WaitForCompactOptions, WriteBatch,
};
use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;

use crate::config::Config;
use crate::die::die;
use crate::fsops;
use crate::hash::{PREFIX_LEN, weak};
use crate::pool::Pool;
use crate::report;
use crate::sys;
use layout::{Cf, ID_LEN, InodeRow, RootRow, id_key, u64_at};

pub const SCHEMA_VERSION: u64 = 1;

/// The file in the index directory whose lock excludes other processes.
const LOCK_FILE: &str = "hashsync.lock";

/// The block cache that all column families of a process share.
const BLOCK_CACHE: usize = 256 << 20;

/// The memory of the memtables of all column families together.
const WRITE_BUFFERS: usize = 256 << 20;

/// Changes per bounded transaction: entries written or removed, as the
/// write batch counts them.
const COMMIT_WORK: usize = 50_000;

/// Unwrap a database result. Database errors are fatal bugs or failures
/// and abort (DESIGN.md §6.7).
pub fn db<T>(result: Result<T, rocksdb::Error>) -> T {
    result.unwrap_or_else(|e| panic!("database error: {e}"))
}

/// The settings that must match between the index and the configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub block_size: u32,
    pub weak_hash_id: String,
    pub strong_hash_id: String,
}

impl Settings {
    pub fn from_config(config: &Config) -> Self {
        Settings {
            block_size: config.block_size,
            weak_hash_id: weak::ID.to_string(),
            strong_hash_id: config.strong_hash.id().to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Meta {
    pub schema_version: u64,
    pub settings: Settings,
    pub index_id: [u8; 16],
}

impl Meta {
    pub fn compatible(&self, settings: &Settings) -> bool {
        self.schema_version == SCHEMA_VERSION && self.settings == *settings
    }
}

pub struct Stats {
    /// Distinct filesystems of the inode rows.
    pub filesystems: u64,
    pub roots: u64,
    pub roots_complete: u64,
    pub inodes: u64,
    pub inodes_incomplete: u64,
    pub paths: u64,
    pub blocks: u64,
    pub distinct_weak: u64,
    pub indexed_bytes: u64,
}

fn os_path(path: &[u8]) -> &Path {
    Path::new(OsStr::from_bytes(path))
}

/// The database handle of a process, with the lock that gives the process
/// its access.
struct Shared {
    db: DB,
    /// The lock file, with this process's lock on it. Rust drops fields in
    /// order, so the lock goes after the database has closed.
    _lock: OwnedFd,
}

/// The index of a process: read/write in a modifying process (`update`,
/// `prune`, and the receiver of a run for its update), read-only in the
/// others, and in that receiver after its update (`into_read_only`).
pub struct Index {
    shared: Arc<Shared>,
}

/// A read-only view of the index for another thread during a transfer: a
/// lookup worker, or the receiver's export of its hash list. Reads see the
/// latest committed transaction of this process.
pub struct Reader {
    shared: Arc<Shared>,
}

/// Open the lock file of the index at `path` and lock it, waiting forever
/// on contention (DESIGN.md §4.4): exclusive and read/write for a
/// modifying process, which creates the file, shared and read-only for the
/// others. A lock that another process holds makes this process say, in a
/// note, that it waits, and for which index: a wait can be long, and
/// without the note it looks like a hang. A second note says when the wait
/// is over. Without it, the wait note would stay the last line on stderr
/// while the process goes on with its work, such as an update, which can
/// take long too, and the process would seem to be still waiting.
///
/// The lock belongs to the open file description (`F_OFD_SETLKW`), not to
/// the process as a classic POSIX lock does. Closing another descriptor of
/// the file does not release it. And when the process exits with the
/// database open, the kernel releases the classic locks, such as RocksDB's
/// lock of its `LOCK` file, while it closes the descriptors, but this lock
/// only after that, when it releases the open file description: a process
/// that gets this lock never finds `LOCK` held. Close-on-exec keeps
/// children from inheriting the descriptor, and so the lock.
fn lock(path: &[u8], exclusive: bool) -> Result<OwnedFd, Errno> {
    // An exclusive lock needs the file open for writing. Read/write and not
    // write-only: `into_read_only` turns that lock into a shared one on the
    // same descriptor, and a shared lock needs it open for reading, else
    // the change fails with `EBADF`.
    let flags = if exclusive {
        OFlags::RDWR | OFlags::CREATE
    } else {
        OFlags::RDONLY
    };
    let file = rustix::io::retry_on_intr(|| {
        rustix::fs::open(
            os_path(path).join(LOCK_FILE),
            flags | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o666),
        )
    })?;
    if !sys::try_lock_file(file.as_fd(), exclusive)? {
        report::note(format_args!(
            "waiting for the lock of index {}",
            path.as_bstr()
        ));
        sys::lock_file(file.as_fd(), exclusive)?;
        report::note(format_args!(
            "acquired the lock of index {}",
            path.as_bstr()
        ));
    }
    Ok(file)
}

/// The options of the database. The column families have their own
/// (`column_families`).
fn db_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    // One flush and one compaction at a time: RocksDB gives a quarter of
    // the background jobs, at least one, to flushes. A compaction from
    // level 0 splits into subcompactions on up to half the cores, each in
    // a thread of its own.
    options.set_max_background_jobs(2);
    let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
    let subcompactions = (cores / 2).max(1);
    options.set_max_subcompactions(u32::try_from(subcompactions).unwrap_or(u32::MAX));
    options.set_db_write_buffer_size(WRITE_BUFFERS);
    // The scan threads of an update commit at the same time (DESIGN.md
    // §4.6). With unordered writes, each thread inserts its batch into the
    // memtables on its own; only the write-ahead log keeps their order.
    options.set_unordered_write(true);
    // RocksDB's own log file stays in the index directory: warnings only,
    // and at most two old logs. Read-only processes write no log.
    options.set_log_level(LogLevel::Warn);
    options.set_keep_log_file_num(2);
    options
}

/// The column families of DESIGN.md §4.4, and their options.
fn column_families() -> Vec<ColumnFamilyDescriptor> {
    // A hyper clock cache, RocksDB's default kind, sized automatically: its
    // lookups take no lock, and it allocates through `new`, which the
    // `mimalloc` feature replaces (`src/cxx_allocation.cc`). An LRU cache
    // allocates its entries through libc `malloc`.
    let cache = Cache::new_hyper_clock_cache(BLOCK_CACHE, 0);
    Cf::ALL
        .iter()
        .map(|&cf| {
            let mut table = BlockBasedOptions::default();
            table.set_block_cache(&cache);
            let mut options = Options::default();
            options.set_compression_type(DBCompressionType::Lz4);
            match cf {
                // Point lookups; new files miss.
                Cf::Identities | Cf::Inodes => table.set_bloom_filter(10.0, false),
                // Candidate lookups by strong-hash prefix; false hits of
                // the weak table miss.
                Cf::Strong => {
                    options.set_prefix_extractor(SliceTransform::create_fixed_prefix(PREFIX_LEN));
                    table.set_bloom_filter(10.0, false);
                    table.set_whole_key_filtering(false);
                }
                _ => {}
            }
            options.set_block_based_table_factory(&table);
            ColumnFamilyDescriptor::new(cf.name(), options)
        })
        .collect()
}

/// Open the index of a modifying process, creating it when necessary.
/// Waits for exclusive access.
pub fn open_modifying(path: &[u8]) -> Index {
    if let Err(e) = fsops::create_dir_all(os_path(path), 0o777) {
        die(format!(
            "cannot create index directory {}: {e}",
            path.as_bstr()
        ));
    }
    let lock = lock(path, true)
        .unwrap_or_else(|e| die(format!("cannot lock index {}: {e}", path.as_bstr())));
    let db = DB::open_cf_descriptors(&db_options(), os_path(path), column_families())
        .unwrap_or_else(|e| die(format!("cannot open index {}: {e}", path.as_bstr())));
    Index {
        shared: Arc::new(Shared { db, _lock: lock }),
    }
}

/// Open the index of a read-only process, waiting while a modifying
/// process has it. `None` when there is no database: no directory or no
/// lock file, or a directory that the first update left before it created
/// the database with all its column families.
pub fn open_read_only(path: &[u8]) -> Option<Index> {
    let lock = match lock(path, false) {
        Ok(lock) => lock,
        Err(Errno::NOENT) => return None,
        Err(e) => die(format!("cannot open index {}: {e}", path.as_bstr())),
    };
    match rustix::io::retry_on_intr(|| rustix::fs::stat(os_path(path).join("CURRENT"))) {
        Ok(_) => {}
        Err(Errno::NOENT) => return None,
        Err(e) => die(format!("cannot open index {}: {e}", path.as_bstr())),
    }
    // A database without every column family: the first update ended
    // before it created them all.
    let names = DB::list_cf(&Options::default(), os_path(path))
        .unwrap_or_else(|e| die(format!("cannot open index {}: {e}", path.as_bstr())));
    if Cf::ALL
        .iter()
        .any(|cf| !names.iter().any(|name| name == cf.name()))
    {
        return None;
    }
    let db =
        DB::open_cf_descriptors_read_only(&db_options(), os_path(path), column_families(), false)
            .unwrap_or_else(|e| die(format!("cannot open index {}: {e}", path.as_bstr())));
    Some(Index {
        shared: Arc::new(Shared { db, _lock: lock }),
    })
}

/// The entries of one column family in a range of keys, in key order,
/// from one snapshot of the database.
struct Scan<'a> {
    raw: DBRawIterator<'a>,
}

impl Scan<'_> {
    /// The current entry; `None` after the last one.
    fn entry(&self) -> Option<(&[u8], &[u8])> {
        let item = self.raw.item();
        if item.is_none() {
            db(self.raw.status());
        }
        item
    }

    fn next(&mut self) {
        self.raw.next();
    }
}

/// The first key after all keys that start with `prefix`; `None` when no
/// key is after them.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

impl Shared {
    fn cf(&self, cf: Cf) -> &ColumnFamily {
        self.db
            .cf_handle(cf.name())
            .unwrap_or_else(|| panic!("database error: no column family {}", cf.name()))
    }

    fn get(&self, cf: Cf, key: &[u8]) -> Option<DBPinnableSlice<'_>> {
        db(self.db.get_pinned_cf(self.cf(cf), key))
    }

    /// The entries of `cf` from the key `start` up to the key `end`,
    /// excluded, or up to the last entry for `None`. `fill_cache` keeps
    /// the blocks that the scan reads in the block cache; bulk scans do
    /// not change the cache.
    fn scan_range(&self, cf: Cf, start: &[u8], end: Option<Vec<u8>>, fill_cache: bool) -> Scan<'_> {
        let mut options = ReadOptions::default();
        // Seek in the order of all keys, whatever prefix extractor the
        // column family has.
        options.set_total_order_seek(true);
        options.fill_cache(fill_cache);
        // Without an upper bound, the step past the last entry would skip
        // each removed entry that follows, up to the next live one.
        // Removals can leave long runs of removed entries, for example
        // when the reconciliation removes the `inode_paths` entries of
        // roots one by one: the removal of many orphans with consecutive
        // IDs would then take quadratic time.
        if let Some(end) = end {
            options.set_iterate_upper_bound(end);
        }
        let mut raw = self.db.raw_iterator_cf_opt(self.cf(cf), options);
        raw.seek(start);
        Scan { raw }
    }

    /// The entries of `cf` whose keys start with `prefix`. An empty prefix
    /// scans the whole column family, without filling the block cache.
    fn scan(&self, cf: Cf, prefix: &[u8]) -> Scan<'_> {
        self.scan_range(cf, prefix, prefix_end(prefix), !prefix.is_empty())
    }

    /// Call `visit` for each entry of `cf` whose key starts with `prefix`.
    fn each(&self, cf: Cf, prefix: &[u8], mut visit: impl FnMut(&[u8], &[u8])) {
        let mut scan = self.scan(cf, prefix);
        while let Some((key, value)) = scan.entry() {
            visit(key, value);
            scan.next();
        }
    }

    /// The last key of `cf`; `None` when it is empty.
    fn last_key(&self, cf: Cf) -> Option<Vec<u8>> {
        let mut options = ReadOptions::default();
        options.set_total_order_seek(true);
        let mut raw = self.db.raw_iterator_cf_opt(self.cf(cf), options);
        raw.seek_to_last();
        let key = raw.key().map(<[u8]>::to_vec);
        if key.is_none() {
            db(raw.status());
        }
        key
    }

    /// The largest ID of the rows of `cf`, keyed by ID; 0 when it is
    /// empty.
    fn last_id(&self, cf: Cf) -> u64 {
        self.last_key(cf).map_or(0, |key| u64_at(&key, 0))
    }

    /// The weak hash of every block row, from one snapshot, sorted as
    /// unsigned 64-bit values (DESIGN.md §5.4). The reads and the sort use
    /// one thread per core: the first keys of the files of `blocks` split
    /// it into parts, which are read in parallel.
    fn sorted_weak_hashes(&self) -> Vec<u64> {
        let snapshot = self.db.snapshot();
        let mut bounds: Vec<Vec<u8>> = db(self.db.live_files())
            .into_iter()
            .filter(|file| file.column_family_name == Cf::Blocks.name())
            .filter_map(|file| file.start_key)
            .collect();
        bounds.sort_unstable();
        bounds.dedup();
        // Part `i` ends before `bounds[i]`, and starts at `bounds[i - 1]`.
        let part = |i: usize| {
            let mut options = ReadOptions::default();
            options.set_snapshot(&snapshot);
            options.set_total_order_seek(true);
            options.fill_cache(false);
            if let Some(end) = bounds.get(i) {
                options.set_iterate_upper_bound(end.clone());
            }
            let mut raw = self.db.raw_iterator_cf_opt(self.cf(Cf::Blocks), options);
            match i.checked_sub(1) {
                Some(start) => raw.seek(&bounds[start]),
                None => raw.seek_to_first(),
            }
            let mut values = Vec::new();
            while let Some(value) = raw.value() {
                values.push(u64_at(value, 0));
                raw.next();
            }
            db(raw.status());
            values
        };
        let pool = Pool::start("export", 0);
        pool.install(|| {
            let parts: Vec<Vec<u64>> = (0..=bounds.len()).into_par_iter().map(part).collect();
            // Each part is freed when it has been copied, so the sort
            // has only one copy of the values.
            let mut values = Vec::with_capacity(parts.iter().map(Vec::len).sum());
            for part in parts {
                values.extend_from_slice(&part);
            }
            values.par_sort_unstable();
            values
        })
    }
}

/// A candidate (DESIGN.md §6.4): an indexed block with one path of its
/// inode.
pub struct Candidate {
    pub block_no: u64,
    /// The root ID and relative path of one path of the block's inode;
    /// `None` for an inode without paths.
    pub path: Option<(u64, Vec<u8>)>,
}

impl Reader {
    /// The distinct weak hashes, sorted as unsigned 64-bit values, from
    /// one database snapshot (DESIGN.md §5.4). No column family orders
    /// the blocks by weak hash, so the sort is in memory.
    pub fn export_weak_hashes(&self) -> Vec<u64> {
        let mut values = self.shared.sorted_weak_hashes();
        values.dedup();
        values
    }

    /// At most `limit` candidates of the blocks whose strong-hash prefix is
    /// `prefix`: one for every path of each block's inode, and one for each
    /// block whose inode has no path. Any order would do; this one is
    /// fixed, by inode ID, then by block number, so that the same index
    /// always gives the same candidates. The keys of `strong` have this
    /// order, and reading stops after `limit` candidates.
    pub fn candidates(&self, prefix: &[u8], limit: u32) -> Vec<Candidate> {
        assert_eq!(
            prefix.len(),
            PREFIX_LEN,
            "a candidate prefix has the wrong length"
        );
        let limit = limit as usize;
        let shared = &self.shared;
        let mut options = ReadOptions::default();
        // The prefix extractor of `strong` is `PREFIX_LEN` bytes: its bloom
        // filters skip the files without the prefix.
        options.set_prefix_same_as_start(true);
        let mut blocks = shared
            .db
            .raw_iterator_cf_opt(shared.cf(Cf::Strong), options);
        blocks.seek(prefix);
        let mut found = Vec::new();
        while found.len() < limit {
            let Some(key) = blocks.key() else {
                db(blocks.status());
                break;
            };
            if !key.starts_with(prefix) {
                break;
            }
            let inode_id = u64_at(key, PREFIX_LEN);
            let block_no = u64_at(key, PREFIX_LEN + ID_LEN);
            let before = found.len();
            let mut paths = shared.scan(Cf::InodePaths, &id_key(inode_id));
            while found.len() < limit
                && let Some((path, _)) = paths.entry()
            {
                found.push(Candidate {
                    block_no,
                    path: Some((u64_at(path, ID_LEN), path[2 * ID_LEN..].to_vec())),
                });
                paths.next();
            }
            if found.len() == before && found.len() < limit {
                found.push(Candidate {
                    block_no,
                    path: None,
                });
            }
            blocks.next();
        }
        found
    }
}

/// Changes to write together: one atomic RocksDB write batch. A modifying
/// process commits them with `commit`, or with `checkpoint` once they reach
/// the bound of a transaction (DESIGN.md §4.4). Reads do not see the
/// changes before the commit. The database has unordered writes
/// (`db_options`): while a thread commits, a read on another thread can see
/// part of the transaction. So no thread may read rows that another thread
/// can commit at the same time.
pub struct Transaction<'a> {
    index: &'a Index,
    batch: WriteBatch,
}

impl Transaction<'_> {
    pub fn put(&mut self, cf: Cf, key: &[u8], value: &[u8]) {
        self.batch.put_cf(self.index.shared.cf(cf), key, value);
    }

    pub fn delete(&mut self, cf: Cf, key: &[u8]) {
        self.batch.delete_cf(self.index.shared.cf(cf), key);
    }

    /// Remove the entries of `cf` from the key `first` to the key `last`,
    /// both included, with one range deletion.
    fn delete_range(&mut self, cf: Cf, first: &[u8], last: &[u8]) {
        // A range deletion stops before its end key. The key just after
        // `last` is `last` with a zero byte.
        let end = [last, &[0]].concat();
        self.batch
            .delete_range_cf(self.index.shared.cf(cf), first, &end[..]);
    }

    /// Remove every entry of `cf` with one range deletion. Reads committed
    /// entries.
    fn clear(&mut self, cf: Cf) {
        if let Some(last) = self.index.shared.last_key(cf) {
            self.delete_range(cf, b"", &last);
        }
    }

    /// Write the pending changes atomically.
    pub fn commit(&mut self) {
        if !self.batch.is_empty() {
            #[cfg(test)]
            tests::before_commit();
            db(self.index.shared.db.write(mem::take(&mut self.batch)));
        }
    }

    /// Commit once the pending changes reach the bound of a transaction.
    pub fn checkpoint(&mut self) {
        if self.batch.len() >= COMMIT_WORK {
            self.commit();
        }
    }

    /// Remove every path of the roots with IDs in `roots` from `paths` and
    /// `inode_paths`, committing between bounded transactions. No root in
    /// the range may keep its paths: the `paths` rows of the range then
    /// form one key range, which each transaction removes up to its last
    /// row with one range deletion. The same transaction removes the
    /// `inode_paths` entries of those rows one by one: their keys start
    /// with the inode ID, so they are spread among the entries of other
    /// roots. Reads committed entries.
    fn remove_root_paths(&mut self, roots: RangeInclusive<u64>) {
        let index = self.index;
        let mut scan = index.shared.scan_range(
            Cf::Paths,
            &id_key(*roots.start()),
            prefix_end(&id_key(*roots.end())),
            false,
        );
        // The rows that the next range deletion removes: from the key
        // `first` to the key `last`.
        let mut first = None;
        let mut last = Vec::new();
        while let Some((key, value)) = scan.entry() {
            self.delete(
                Cf::InodePaths,
                &layout::inode_path_key(u64_at(value, 0), u64_at(key, 0), &key[ID_LEN..]),
            );
            let range_first = first.get_or_insert_with(|| key.to_vec());
            last.clear();
            last.extend_from_slice(key);
            if self.batch.len() >= COMMIT_WORK {
                self.delete_range(Cf::Paths, range_first, &last);
                self.commit();
                first = None;
            }
            scan.next();
        }
        if let Some(first) = first {
            self.delete_range(Cf::Paths, &first, &last);
        }
    }

    /// Remove every path of an inode from `inode_paths` and `paths`,
    /// committing between bounded transactions. Reads committed entries.
    fn remove_inode_paths(&mut self, inode_id: u64) {
        let index = self.index;
        index
            .shared
            .each(Cf::InodePaths, &id_key(inode_id), |key, _| {
                let root_id = u64_at(key, ID_LEN);
                self.delete(Cf::Paths, &layout::path_key(root_id, &key[2 * ID_LEN..]));
                self.delete(Cf::InodePaths, key);
                self.checkpoint();
            });
    }

    /// Remove every block of an inode from `blocks` and `strong`,
    /// committing between bounded transactions. Reads committed entries.
    fn remove_blocks(&mut self, inode_id: u64) {
        let index = self.index;
        index
            .shared
            .each(Cf::Blocks, &id_key(inode_id), |key, value| {
                let block_no = u64_at(key, ID_LEN);
                let prefix = layout::block_strong_prefix(value);
                self.delete(Cf::Strong, &layout::strong_key(prefix, inode_id, block_no));
                self.delete(Cf::Blocks, key);
                self.checkpoint();
            });
    }
}

impl Index {
    /// A reader for another thread of this process.
    pub fn reader(&self) -> Reader {
        Reader {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Close the database, then release the lock. The readers must be gone
    /// first: the database closes with its last handle.
    pub fn close(self) {
        let shared = Arc::into_inner(self.shared).expect("a reader outlives the index");
        drop(shared);
    }

    /// The index of a finished update as a read-only index, for the
    /// receiver of a run (DESIGN.md §4.4). This function exists so that
    /// other read-only processes, such as `stats`, can use the index while
    /// the run goes on, and no modifying process can come between the
    /// update and the run. It closes the read/write database, turns the
    /// exclusive lock into a shared one on the same open file description,
    /// which never releases it, and opens the database of `path` read-only.
    /// `finish_update` has already waited for RocksDB's background work, so
    /// the close does not wait.
    pub fn into_read_only(self, path: &[u8]) -> Index {
        let Shared { db, _lock: lock } =
            Arc::into_inner(self.shared).expect("a reader outlives the index");
        drop(db);
        sys::lock_file(lock.as_fd(), false)
            .unwrap_or_else(|e| die(format!("cannot lock index {}: {e}", path.as_bstr())));
        let db = DB::open_cf_descriptors_read_only(
            &db_options(),
            os_path(path),
            column_families(),
            false,
        )
        .unwrap_or_else(|e| die(format!("cannot open index {}: {e}", path.as_bstr())));
        Index {
            shared: Arc::new(Shared { db, _lock: lock }),
        }
    }

    /// An empty transaction.
    pub fn transaction(&self) -> Transaction<'_> {
        Transaction {
            index: self,
            batch: WriteBatch::default(),
        }
    }

    /// Apply `remove` to every item on the threads of the current pool,
    /// each thread in bounded transactions of its own.
    fn remove_all<T: Sync>(&self, items: &[T], remove: impl Fn(&mut Transaction<'_>, &T) + Sync) {
        items
            .par_iter()
            .fold(
                || self.transaction(),
                |mut txn, item| {
                    remove(&mut txn, item);
                    txn
                },
            )
            .for_each(|mut txn| txn.commit());
    }

    /// Every root row, by ID.
    fn roots(&self) -> Vec<(u64, RootRow)> {
        let mut roots = Vec::new();
        self.shared.each(Cf::Roots, b"", |key, value| {
            roots.push((u64_at(key, 0), RootRow::decode(value)));
        });
        roots
    }

    /// Every root row: its ID and its representative path.
    pub fn root_paths(&self) -> Vec<(u64, Vec<u8>)> {
        self.roots()
            .into_iter()
            .map(|(root_id, row)| (root_id, row.path))
            .collect()
    }

    /// The inode ID of an identity (an `identities` key).
    fn inode_id(&self, identity: &[u8]) -> Option<u64> {
        let value = self.shared.get(Cf::Identities, identity)?;
        Some(u64_at(&value, 0))
    }

    /// The row of an inode ID in use.
    fn inode_row(&self, inode_id: u64) -> InodeRow {
        let value = self
            .shared
            .get(Cf::Inodes, &id_key(inode_id))
            .expect("database error: a missing inode row");
        InodeRow::decode(&value)
    }

    /// The ID for the next new inode row: one more than the largest ID in
    /// use. Removal takes every reference to an inode away before its
    /// row, so a reused ID meets no old entries.
    fn next_inode_id(&self) -> u64 {
        self.shared.last_id(Cf::Inodes) + 1
    }

    /// The stored metadata, or `None` before the first reset. A malformed
    /// value makes the metadata incompatible, which rebuilds the index.
    pub fn read_meta(&self) -> Option<Meta> {
        let get = |name: &str| self.shared.get(Cf::Meta, name.as_bytes());
        let number = |name: &str| {
            get(name)
                .and_then(|value| <[u8; 8]>::try_from(&value[..]).ok())
                .map_or(0, u64::from_be_bytes)
        };
        let text = |name: &str| {
            get(name)
                .and_then(|value| String::from_utf8(value.to_vec()).ok())
                .unwrap_or_default()
        };
        get("schema_version")?;
        Some(Meta {
            schema_version: number("schema_version"),
            settings: Settings {
                block_size: u32::try_from(number("block_size")).unwrap_or(0),
                weak_hash_id: text("weak_hash_id"),
                strong_hash_id: text("strong_hash_id"),
            },
            index_id: get("index_id")
                .and_then(|value| <[u8; 16]>::try_from(&value[..]).ok())
                .unwrap_or([0; 16]),
        })
    }

    /// Remove every entry and store new metadata with a new index ID, in
    /// one transaction (DESIGN.md §4.4).
    pub fn reset(&self, settings: &Settings) {
        let mut index_id = [0u8; 16];
        rustix::io::retry_on_intr(|| {
            rustix::rand::getrandom(&mut index_id, rustix::rand::GetRandomFlags::empty())
        })
        .unwrap_or_else(|e| panic!("getrandom: {e}"));
        let mut txn = self.transaction();
        for cf in Cf::ALL {
            txn.clear(cf);
        }
        txn.put(Cf::Meta, b"schema_version", &SCHEMA_VERSION.to_be_bytes());
        txn.put(
            Cf::Meta,
            b"block_size",
            &u64::from(settings.block_size).to_be_bytes(),
        );
        txn.put(Cf::Meta, b"weak_hash_id", settings.weak_hash_id.as_bytes());
        txn.put(
            Cf::Meta,
            b"strong_hash_id",
            settings.strong_hash_id.as_bytes(),
        );
        txn.put(Cf::Meta, b"index_id", &index_id);
        txn.commit();
    }

    /// End an update (DESIGN.md §4.4): write the memtables to the database
    /// files, so that a later read-only process has no write-ahead log to
    /// replay, then wait for the compactions that RocksDB has scheduled.
    /// Closing the database would cancel them, and short updates would
    /// leave ever more of them to later ones.
    pub fn finish_update(&self) {
        // `set_flush` flushes the column families one after the other, and
        // the last flush removes the old write-ahead log. Parallel flushes
        // can keep it: each flush records the oldest log that the other
        // column families still need, and two flushes that end together
        // both see the old log of the other one. The default options have
        // no timeout.
        let mut options = WaitForCompactOptions::default();
        options.set_flush(true);
        db(self.shared.db.wait_for_compact(&options));
    }

    /// Reconcile roots before scanning (DESIGN.md §4.2): remove the paths
    /// and rows of unfinished roots and of incomplete inodes, then create
    /// rows for the roots to scan. Returns their IDs in order.
    ///
    /// The blocks go first, in bounded transactions, and so do the paths
    /// when some roots keep theirs. The rows then change in one
    /// transaction, which also removes all paths when no root keeps its
    /// paths.
    pub fn reconcile_roots(&self, paths: &[&[u8]]) -> Vec<u64> {
        // Every root without an identifier is unfinished; the others keep
        // their paths.
        let roots = self.roots();
        let last_retained = roots
            .iter()
            .filter(|(_, row)| row.identifier.is_some())
            .map(|&(root_id, _)| root_id)
            .max();
        let mut incomplete = Vec::new();
        self.shared.each(Cf::Inodes, b"", |key, value| {
            let row = InodeRow::decode(value);
            if row.size.is_none() {
                incomplete.push((u64_at(key, 0), row));
            }
        });

        // A root can have many paths, and there can be many roots: the
        // removals run on one thread per core. A path of an incomplete
        // inode under an unfinished root is removed twice, which changes
        // nothing.
        let pool = Pool::start("reconcile", 0);
        pool.install(|| {
            if last_retained.is_some() {
                let parts = root_parts(&roots, rayon::current_num_threads());
                self.remove_all(&parts, |txn, part| txn.remove_root_paths(part.clone()));
            }
            self.remove_all(&incomplete, |txn, &(inode_id, _)| {
                txn.remove_inode_paths(inode_id);
                txn.remove_blocks(inode_id);
            });
        });
        drop(pool);

        let mut txn = self.transaction();
        if last_retained.is_none() {
            // No root keeps its paths: they all go, with one range
            // deletion in each column family.
            txn.clear(Cf::Paths);
            txn.clear(Cf::InodePaths);
        }
        for (root_id, row) in &roots {
            if row.identifier.is_none() {
                txn.delete(Cf::Roots, &id_key(*root_id));
            }
        }
        for (inode_id, row) in &incomplete {
            txn.delete(Cf::Inodes, &id_key(*inode_id));
            txn.delete(Cf::Identities, &row.identity);
        }
        // IDs of removed roots can return: their paths are gone.
        let mut next = last_retained.unwrap_or(0) + 1;
        let ids = paths
            .iter()
            .map(|path| {
                let root_id = next;
                next += 1;
                let row = RootRow {
                    identifier: None,
                    path: path.to_vec(),
                };
                txn.put(Cf::Roots, &id_key(root_id), &row.encode());
                root_id
            })
            .collect();
        txn.commit();
        ids
    }

    pub fn stats(&self) -> Stats {
        let shared = &self.shared;
        let roots = self.roots();
        let mut stats = Stats {
            filesystems: 0,
            roots: roots.len() as u64,
            roots_complete: roots
                .iter()
                .filter(|(_, row)| row.identifier.is_some())
                .count() as u64,
            inodes: 0,
            inodes_incomplete: 0,
            paths: 0,
            blocks: 0,
            distinct_weak: 0,
            indexed_bytes: 0,
        };
        shared.each(Cf::Inodes, b"", |_, value| {
            stats.inodes += 1;
            match InodeRow::decode(value).size {
                Some(size) => stats.indexed_bytes = stats.indexed_bytes.saturating_add(size),
                None => stats.inodes_incomplete += 1,
            }
        });
        // The identities of one filesystem are adjacent.
        let mut last_fsid = Vec::new();
        shared.each(Cf::Identities, b"", |key, _| {
            let fsid = layout::identity_fsid(key);
            if stats.filesystems == 0 || fsid != last_fsid {
                stats.filesystems += 1;
                last_fsid = fsid.to_vec();
            }
        });
        shared.each(Cf::Paths, b"", |_, _| stats.paths += 1);
        let mut weak = shared.sorted_weak_hashes();
        stats.blocks = weak.len() as u64;
        weak.dedup();
        stats.distinct_weak = weak.len() as u64;
        stats
    }
}

/// The unfinished roots of `roots`, which are in ID order, as parts for a
/// pool of `threads` threads: ranges of root IDs without a retained root
/// inside, so that the paths of a part form one key range. About four
/// parts for each thread let the threads share the work. A part removes
/// its paths with one range deletion for each transaction, so the range
/// deletions do not increase with the number of roots.
fn root_parts(roots: &[(u64, RootRow)], threads: usize) -> Vec<RangeInclusive<u64>> {
    let unfinished = |row: &RootRow| row.identifier.is_none();
    let count = roots.iter().filter(|(_, row)| unfinished(row)).count();
    let roots_per_part = count.div_ceil(4 * threads).max(1);
    roots
        .chunk_by(|(_, a), (_, b)| unfinished(a) == unfinished(b))
        .filter(|run| unfinished(&run[0].1))
        .flat_map(|run| run.chunks(roots_per_part))
        .map(|part| part[0].0..=part[part.len() - 1].0)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::os::unix::ffi::OsStrExt;
    use std::process::Command;

    use rustix::fs::FlockOperation;

    use super::*;

    thread_local! {
        /// Commits that this thread can make before the next one fails;
        /// `None` for no limit.
        static COMMITS_LEFT: Cell<Option<u32>> = const { Cell::new(None) };
    }

    /// Fail every commit of this thread after the next `commits`: an
    /// interruption between transactions, as when the process dies.
    pub fn interrupt_after_commits(commits: Option<u32>) {
        COMMITS_LEFT.set(commits);
    }

    pub fn before_commit() {
        match COMMITS_LEFT.get() {
            Some(0) => panic!("injected interruption"),
            Some(n) => COMMITS_LEFT.set(Some(n - 1)),
            None => {}
        }
    }

    pub fn settings() -> Settings {
        Settings {
            block_size: 4096,
            weak_hash_id: "test".into(),
            strong_hash_id: "test".into(),
        }
    }

    /// A new modifying index in `dir`.
    pub fn new_index(dir: &Path) -> Index {
        let index = open_modifying(dir.join("index").as_os_str().as_bytes());
        index.reset(&settings());
        index
    }

    /// Write rows through one transaction: `(inode_id, identity, size)`
    /// inodes, `(root_id, path)` roots, `(inode_id, root_id, rel_path)`
    /// paths, and `(inode_id, block_no, weak, strong prefix)` blocks.
    pub fn insert(
        index: &Index,
        inodes: &[(u64, &[u8], Option<u64>)],
        roots: &[(u64, &[u8])],
        paths: &[(u64, u64, &[u8])],
        blocks: &[(u64, u64, u64, [u8; PREFIX_LEN])],
    ) {
        let mut txn = index.transaction();
        for &(inode_id, identity, size) in inodes {
            let row = InodeRow {
                size,
                ctime_ns: 123,
                identity: identity.to_vec(),
            };
            txn.put(Cf::Inodes, &id_key(inode_id), &row.encode());
            txn.put(Cf::Identities, identity, &id_key(inode_id));
        }
        for &(root_id, path) in roots {
            let row = RootRow {
                identifier: None,
                path: path.to_vec(),
            };
            txn.put(Cf::Roots, &id_key(root_id), &row.encode());
        }
        for &(inode_id, root_id, rel_path) in paths {
            txn.put(
                Cf::Paths,
                &layout::path_key(root_id, rel_path),
                &id_key(inode_id),
            );
            txn.put(
                Cf::InodePaths,
                &layout::inode_path_key(inode_id, root_id, rel_path),
                &[],
            );
        }
        for &(inode_id, block_no, weak, prefix) in blocks {
            txn.put(
                Cf::Blocks,
                &layout::block_key(inode_id, block_no),
                &layout::block_value(weak, &prefix),
            );
            txn.put(
                Cf::Strong,
                &layout::strong_key(&prefix, inode_id, block_no),
                &[],
            );
        }
        txn.commit();
    }

    /// The number of entries in a column family.
    pub fn count(index: &Index, cf: Cf) -> usize {
        let mut n = 0;
        index.shared.each(cf, b"", |_, _| n += 1);
        n
    }

    /// Give root rows identifiers: they become completed immutable groups,
    /// which keep their paths.
    fn complete_roots(index: &Index, root_ids: &[u64]) {
        let mut txn = index.transaction();
        for &root_id in root_ids {
            let row = RootRow {
                identifier: Some(format!("group{root_id}").into_bytes()),
                path: format!("/group{root_id}").into_bytes(),
            };
            txn.put(Cf::Roots, &id_key(root_id), &row.encode());
        }
        txn.commit();
    }

    /// A path as `(root_id, rel_path, inode_id)`.
    type PathRow = (u64, Vec<u8>, u64);

    /// The paths of the `paths` rows and of the `inode_paths` entries,
    /// each sorted.
    fn path_rows(index: &Index) -> (Vec<PathRow>, Vec<PathRow>) {
        let mut rows = Vec::new();
        index.shared.each(Cf::Paths, b"", |key, value| {
            rows.push((u64_at(key, 0), key[ID_LEN..].to_vec(), u64_at(value, 0)));
        });
        let mut entries = Vec::new();
        index.shared.each(Cf::InodePaths, b"", |key, _| {
            entries.push((
                u64_at(key, ID_LEN),
                key[2 * ID_LEN..].to_vec(),
                u64_at(key, 0),
            ));
        });
        rows.sort();
        entries.sort();
        (rows, entries)
    }

    /// The exit code of a completed probe. The probe process exits with
    /// it because a plain successful exit proves nothing: a filter that
    /// selects no test also exits zero.
    const PROBE_RAN: i32 = 42;

    /// Assert, from a separate process, that the index at `path` is locked
    /// against a read-only process (`busy`), or that a read-only process
    /// can have it (`open`). The probe tries a shared lock of the lock file
    /// once, without waiting: this test binary runs `lock_probe_child`
    /// selected by the environment.
    fn probe(path: &Path, expect: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "index::tests::lock_probe_child", "--ignored"])
            .env("HASHSYNC_TEST_LOCK_DB", path)
            .env("HASHSYNC_TEST_LOCK_EXPECT", expect)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(PROBE_RAN),
            "the probe did not run, or did not find the index {expect}:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Not a test of its own: the separate process that the lock tests
    /// start through `probe`. It tries a classic POSIX lock, which
    /// conflicts with the open file description locks of other owners. On
    /// the expected outcome it exits the process with `PROBE_RAN` instead
    /// of returning, which tells the parent that the probe really ran.
    #[test]
    #[ignore = "helper process of the lock tests"]
    fn lock_probe_child() {
        let Some(path) = std::env::var_os("HASHSYNC_TEST_LOCK_DB") else {
            return;
        };
        let expect = std::env::var("HASHSYNC_TEST_LOCK_EXPECT").unwrap();
        let file = rustix::fs::open(
            Path::new(&path).join(LOCK_FILE),
            OFlags::RDONLY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .unwrap();
        let result = rustix::fs::fcntl_lock(&file, FlockOperation::NonBlockingLockShared);
        match (expect.as_str(), result) {
            ("busy", Err(Errno::AGAIN | Errno::ACCESS)) | ("open", Ok(())) => {}
            (expect, result) => panic!("expected {expect}, got {result:?}"),
        }
        drop(file);
        std::process::exit(PROBE_RAN);
    }

    /// A modifying process excludes other processes for as long as its
    /// index is open, including the readers of its other threads, and
    /// closing it leaves no lock (DESIGN.md §4.4). Every probe is a
    /// separate process (DESIGN.md §11).
    #[test]
    fn a_modifying_process_excludes_others() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = new_index(dir.path());
        let reader = index.reader();
        probe(&path, "busy");
        drop(index);
        // The reader keeps the database, and so the lock.
        probe(&path, "busy");
        assert!(reader.candidates(&[0; PREFIX_LEN], 1).is_empty());
        drop(reader);
        probe(&path, "open");

        let read_only = open_read_only(path.as_os_str().as_bytes()).unwrap();
        probe(&path, "open");
        assert!(read_only.read_meta().unwrap().compatible(&settings()));
    }

    /// The lock belongs to its open file description: closing another
    /// descriptor of the lock file in the same process, which would
    /// release a classic POSIX lock, keeps it (DESIGN.md §4.4).
    #[test]
    fn closing_another_descriptor_keeps_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = new_index(dir.path());
        drop(std::fs::File::open(path.join(LOCK_FILE)).unwrap());
        probe(&path, "busy");
        drop(index);
        probe(&path, "open");
    }

    /// A read-only process finds no index where there is no directory, a
    /// directory without the lock file, or without a database, or a
    /// database without all its column families: the first update ended
    /// early.
    #[test]
    fn a_missing_database_is_no_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let no_index = || open_read_only(path.as_os_str().as_bytes()).is_none();
        assert!(no_index());
        std::fs::create_dir(&path).unwrap();
        assert!(no_index());
        std::fs::write(path.join(LOCK_FILE), b"").unwrap();
        assert!(no_index());
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        drop(DB::open_cf(&options, &path, ["default", "roots"]).unwrap());
        assert!(no_index());
    }

    /// The export sorts the weak hashes as unsigned values and
    /// deduplicates across inodes (DESIGN.md §5.4).
    #[test]
    fn weak_hashes_export_distinct_and_unsigned_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        assert_eq!(index.reader().export_weak_hashes(), Vec::<u64>::new());
        // 7 repeats on another inode.
        let values = [7, 0, u64::MAX, 1 << 63, i64::MAX as u64];
        let mut blocks: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(block_no, &weak)| (1, block_no as u64, weak, [0; PREFIX_LEN]))
            .collect();
        blocks.push((2, 0, 7, [0; PREFIX_LEN]));
        insert(
            &index,
            &[(1, b"\x01a", Some(4096)), (2, b"\x01b", Some(4096))],
            &[],
            &[],
            &blocks,
        );
        assert_eq!(
            index.reader().export_weak_hashes(),
            vec![0, 7, i64::MAX as u64, 1 << 63, u64::MAX]
        );
        let stats = index.stats();
        assert_eq!((stats.blocks, stats.distinct_weak), (6, 5));
    }

    /// Candidates follow the inode IDs and block numbers, include blocks
    /// of inodes without paths, and stop at the limit (DESIGN.md §4.4 and
    /// §6.4).
    #[test]
    fn candidates_are_ordered_and_limited() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        let (one, two) = ([0x11; PREFIX_LEN], [0x22; PREFIX_LEN]);
        insert(
            &index,
            &[(1, b"\x01\x01", Some(8192)), (2, b"\x01\x02", Some(8192))],
            &[(1, b"/r")],
            &[(2, 1, b"a"), (2, 1, b"b")],
            &[
                (2, 1, 0, one),
                (2, 0, 0, one),
                (1, 5, 0, one),
                (1, 6, 0, two),
            ],
        );
        let reader = index.reader();
        let found: Vec<String> = reader
            .candidates(&one, 10)
            .into_iter()
            .map(|c| match c.path {
                Some((root, rel)) => format!("{} {root}/{}", c.block_no, rel.as_bstr()),
                None => format!("{} -", c.block_no),
            })
            .collect();
        assert_eq!(found, ["5 -", "0 1/a", "0 1/b", "1 1/a", "1 1/b"]);
        assert_eq!(reader.candidates(&one, 2).len(), 2);
        assert_eq!(reader.candidates(&one, 1).len(), 1);
        assert_eq!(reader.candidates(&two, 10).len(), 1);
        assert!(reader.candidates(&[0x33; PREFIX_LEN], 10).is_empty());
        // Neighbors of the prefix are not candidates.
        assert!(reader.candidates(&[0x10; PREFIX_LEN], 10).is_empty());
        let mut after = one;
        after[PREFIX_LEN - 1] += 1;
        assert!(reader.candidates(&after, 10).is_empty());
    }

    /// A scan of the entries of one inode stops at its last entry, also
    /// when removed entries follow. When some roots keep their paths, the
    /// reconciliation removes the `inode_paths` entries of the other roots
    /// one by one, so the paths of vanished files remain as removed
    /// entries, in the order of their inode IDs. Without the stop, a
    /// lookup of the inode before them, and the removal of each orphan,
    /// would skip all of them.
    #[test]
    fn scans_stop_before_removed_entries() {
        use rocksdb::perf::{PerfContext, PerfMetric, PerfStatsLevel, set_perf_stats};

        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        // Inode 1 has a block and a path. The paths of inodes 2 to 1001
        // follow, and are removed.
        let identities: Vec<Vec<u8>> = (1..=1001u64)
            .map(|id| layout::identity(b"\x01", &id.to_be_bytes()))
            .collect();
        let names: Vec<String> = (1..=1001).map(|id| format!("f{id}")).collect();
        let inodes: Vec<(u64, &[u8], Option<u64>)> = (1..)
            .zip(&identities)
            .map(|(id, identity)| (id, &identity[..], Some(4096)))
            .collect();
        let paths: Vec<(u64, u64, &[u8])> = (1..)
            .zip(&names)
            .map(|(id, name)| (id, 1, name.as_bytes()))
            .collect();
        let prefix = [0x11; PREFIX_LEN];
        insert(&index, &inodes, &[(1, b"/r")], &paths, &[(1, 0, 0, prefix)]);
        let mut txn = index.transaction();
        for &(inode_id, root_id, rel_path) in &paths[1..] {
            txn.delete(Cf::Paths, &layout::path_key(root_id, rel_path));
            txn.delete(
                Cf::InodePaths,
                &layout::inode_path_key(inode_id, root_id, rel_path),
            );
        }
        txn.commit();

        // RocksDB counts the removed entries that the reads of this thread
        // skip.
        set_perf_stats(PerfStatsLevel::EnableCount);
        let mut perf = PerfContext::default();
        perf.reset();
        assert_eq!(index.reader().candidates(&prefix, 10).len(), 1);
        let mut txn = index.transaction();
        txn.remove_inode_paths(2);
        txn.commit();
        let skipped = perf.metric(PerfMetric::InternalDeleteSkippedCount);
        set_perf_stats(PerfStatsLevel::Disable);
        // At most the removed path of inode 2 itself.
        assert!(skipped <= 1, "{skipped} removed entries skipped");
    }

    /// A reader of a modifying process sees each commit of the process at
    /// once (DESIGN.md §4.4).
    #[test]
    fn readers_see_later_commits() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        let reader = index.reader();
        let prefix = [0x11; PREFIX_LEN];
        assert!(reader.candidates(&prefix, 10).is_empty());
        insert(
            &index,
            &[(7, &layout::identity(b"\x01", b"\x02"), Some(4096))],
            &[(1, b"/r")],
            &[(7, 1, b"a")],
            &[(7, 0, 0, prefix)],
        );
        let found = reader.candidates(&prefix, 10);
        assert_eq!(found.len(), 1, "the reader must see the commit");
        assert_eq!(found[0].path, Some((1, b"a".to_vec())));
    }

    /// A reset removes every entry, keeps the index usable, and renews
    /// the index ID; reconciliation reuses the IDs of removed roots only
    /// after their paths are gone. When no root keeps its paths, their
    /// removal takes one range deletion in each column family, which
    /// leaves the paths that the scan adds later (DESIGN.md §4.2).
    #[test]
    fn reset_and_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        let first_id = index.read_meta().unwrap().index_id;
        insert(
            &index,
            &[(1, b"\x01\x01", Some(4096)), (2, b"\x01\x02", None)],
            &[(1, b"/r"), (2, b"/q")],
            &[(1, 1, b"a"), (2, 1, b"b"), (1, 2, b"")],
            &[(1, 0, 5, [1; PREFIX_LEN]), (2, 0, 6, [2; PREFIX_LEN])],
        );
        // The incomplete inode goes with its paths and blocks, the
        // unfinished roots with their paths; the complete inode stays.
        assert_eq!(index.reconcile_roots(&[b"/s", b"/t"]), [1, 2]);
        assert_eq!(
            index.root_paths(),
            [(1, b"/s".to_vec()), (2, b"/t".to_vec())]
        );
        for (cf, n) in [
            (Cf::Inodes, 1),
            (Cf::Identities, 1),
            (Cf::Paths, 0),
            (Cf::InodePaths, 0),
            (Cf::Blocks, 1),
            (Cf::Strong, 1),
        ] {
            assert_eq!(count(&index, cf), n, "{cf:?}");
        }
        assert_eq!(index.next_inode_id(), 2);
        insert(&index, &[], &[], &[(1, 2, b"a")], &[]);
        let (rows, entries) = path_rows(&index);
        assert_eq!(rows, [(2, b"a".to_vec(), 1)], "a later path must stay");
        assert_eq!(entries, rows);

        index.reset(&settings());
        for cf in Cf::ALL {
            let expected = if cf == Cf::Meta { 5 } else { 0 };
            assert_eq!(count(&index, cf), expected, "{cf:?}");
        }
        let meta = index.read_meta().unwrap();
        assert!(meta.compatible(&settings()));
        assert_ne!(meta.index_id, first_id);
        assert_eq!(index.next_inode_id(), 1);
        assert_eq!(index.reconcile_roots(&[b"/u"]), [1]);
    }

    /// Roots that keep their paths lose only the paths of incomplete
    /// inodes. The other roots lose both entries of every path, and their
    /// rows; new roots get IDs after the kept ones (DESIGN.md §4.2). The
    /// paths at the ends of each root's key range test the ends of the
    /// range deletions.
    #[test]
    fn reconcile_keeps_the_paths_of_retained_roots() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        // Roots 2 and 5 keep their paths; there is no root 8.
        let root_ids = [1, 2, 3, 4, 5, 6, 7, 9];
        let root_names: Vec<String> = root_ids.iter().map(|id| format!("/r{id}")).collect();
        let roots: Vec<(u64, &[u8])> = root_ids
            .iter()
            .zip(&root_names)
            .map(|(&id, name)| (id, name.as_bytes()))
            .collect();
        let mut paths: Vec<(u64, u64, &[u8])> = Vec::new();
        for &root_id in &root_ids {
            paths.extend([(1, root_id, &b""[..]), (2, root_id, b"\xff\xff")]);
        }
        // Inode 3 is incomplete, with paths under a kept root and an
        // unfinished one.
        paths.extend([(3, 2, &b"i"[..]), (3, 3, b"i")]);
        insert(
            &index,
            &[
                (1, b"\x01\x01", Some(4096)),
                (2, b"\x01\x02", Some(4096)),
                (3, b"\x01\x03", None),
            ],
            &roots,
            &paths,
            &[
                (1, 0, 5, [1; PREFIX_LEN]),
                (3, 0, 6, [3; PREFIX_LEN]),
                (3, 1, 7, [4; PREFIX_LEN]),
            ],
        );
        complete_roots(&index, &[2, 5]);

        assert_eq!(index.reconcile_roots(&[b"/new"]), [6]);
        let ids: Vec<u64> = index.root_paths().iter().map(|&(id, _)| id).collect();
        assert_eq!(ids, [2, 5, 6]);
        let (rows, entries) = path_rows(&index);
        let kept = |root_id| {
            [
                (root_id, b"".to_vec(), 1),
                (root_id, b"\xff\xff".to_vec(), 2),
            ]
        };
        assert_eq!(rows, [kept(2), kept(5)].concat());
        assert_eq!(entries, rows);
        for (cf, n) in [
            (Cf::Inodes, 2),
            (Cf::Identities, 2),
            (Cf::Blocks, 1),
            (Cf::Strong, 1),
        ] {
            assert_eq!(count(&index, cf), n, "{cf:?}");
        }
    }

    /// Each transaction that removes root paths removes both entries of
    /// the paths that it covers: an interruption between transactions
    /// leaves both entries of every other path (DESIGN.md §4.4). The next
    /// reconciliation removes the rest, and a root that keeps its paths
    /// loses none, also between the removed roots.
    #[test]
    fn interrupted_root_path_removal_leaves_whole_paths() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        // Roots 1 and 2 together have enough paths that their removal
        // commits in the middle, after `COMMIT_WORK` paths.
        const MANY_PATHS: usize = COMMIT_WORK + 10_000;
        let names: Vec<String> = (0..MANY_PATHS).map(|i| format!("f{i:05}")).collect();
        let mut paths: Vec<(u64, u64, &[u8])> = names
            .iter()
            .enumerate()
            .map(|(i, name)| (1, 1 + (2 * i / MANY_PATHS) as u64, name.as_bytes()))
            .collect();
        paths.extend([(1, 3, &b""[..]), (1, 3, b"\xff\xff"), (1, 4, b"a")]);
        insert(
            &index,
            &[(1, b"\x01\x01", Some(4096))],
            &[(1, b"/a"), (2, b"/b"), (3, b"/c"), (4, b"/d")],
            &paths,
            &[],
        );
        complete_roots(&index, &[3]);

        interrupt_after_commits(Some(1));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut txn = index.transaction();
            txn.remove_root_paths(1..=2);
            txn.commit();
        }));
        interrupt_after_commits(None);
        assert!(
            result.is_err(),
            "the injected failure must stop the removal"
        );
        let (rows, entries) = path_rows(&index);
        assert_eq!(entries, rows, "every path must keep both entries");
        let left: Vec<&PathRow> = rows.iter().filter(|(root_id, ..)| *root_id <= 2).collect();
        assert_eq!(left.len(), MANY_PATHS - COMMIT_WORK);
        assert!(left.iter().all(|(root_id, ..)| *root_id == 2));
        assert_eq!(rows.len(), left.len() + 3);

        index.reconcile_roots(&[]);
        let (rows, entries) = path_rows(&index);
        assert_eq!(rows, [(3, b"".to_vec(), 1), (3, b"\xff\xff".to_vec(), 1)]);
        assert_eq!(entries, rows);
    }

    /// The parts of the unfinished roots cover each of them once, never
    /// contain a retained root, and contain more roots when there are
    /// fewer threads.
    #[test]
    fn root_parts_leave_out_retained_roots() {
        // Roots 2 and 5 are retained; there is no root 8.
        let roots: Vec<(u64, RootRow)> = [1, 2, 3, 4, 5, 6, 7, 9]
            .into_iter()
            .map(|id| {
                let row = RootRow {
                    identifier: [2, 5].contains(&id).then(|| b"group".to_vec()),
                    path: format!("/r{id}").into_bytes(),
                };
                (id, row)
            })
            .collect();
        // Six unfinished roots, four parts per thread: two roots per part.
        assert_eq!(root_parts(&roots, 1), [1..=1, 3..=4, 6..=7, 9..=9]);
        assert_eq!(
            root_parts(&roots, 64),
            [1..=1, 3..=3, 4..=4, 6..=6, 7..=7, 9..=9]
        );
        assert!(root_parts(&roots[1..2], 1).is_empty());
    }
}

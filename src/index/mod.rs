// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

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

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::mem;
use std::ops::{Range, RangeInclusive};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use rayon::ThreadPool;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use rocksdb::{
    BlockBasedOptions, BottommostLevelCompaction, Cache, ColumnFamily, ColumnFamilyDescriptor,
    CompactOptions, DB, DBCompressionType, DBPinnableSlice, DBRawIterator, LogLevel, Options,
    ReadOptions, SliceTransform, WaitForCompactOptions, WriteBatch,
};
use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;

use crate::config::Config;
use crate::die::die;
use crate::escapes::quoted_from_home;
use crate::fsops;
use crate::hash::{PREFIX_LEN, weak};
use crate::pool::Pool;
use crate::report;
use crate::sys;
pub use layout::RootRow;
use layout::{Cf, ID_LEN, InodeRow, id_key, u64_at};

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

/// End the process after `action`, such as "open" or "lock", failed on the
/// index at `path`.
fn fatal(action: &str, path: &[u8], error: impl std::fmt::Display) -> ! {
    die(format!(
        "cannot {action} index {}: {error}",
        quoted_from_home(path)
    ))
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
/// `prune`, `compact`, the receiver of a run for its update at the start,
/// and its child for the update after the command), read-only in the
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

/// Open the lock file of the index at `path` and lock it for `access`,
/// waiting forever on contention (DESIGN.md §4.4): exclusive and
/// read/write for a modifying process, shared and read-only for the
/// others. With `create`, which only an update passes, it creates the file
/// when there is none. A lock that another process holds makes this
/// process say, in a note, that it waits, and for which index: a wait can
/// be long, and without the note it looks like a hang. A second note says
/// when the wait is over. Without it, the wait note would stay the last
/// line on stderr while the process goes on with its work, such as an
/// update, which can take long too, and the process would seem to be
/// still waiting.
///
/// The lock belongs to the open file description (`F_OFD_SETLKW`), not to
/// the process as a classic POSIX lock does. Closing another descriptor of
/// the file does not release it. And when the process exits with the
/// database open, the kernel releases the classic locks, such as RocksDB's
/// lock of its `LOCK` file, while it closes the descriptors, but this lock
/// only after that, when it releases the open file description: a process
/// that gets this lock never finds `LOCK` held. Close-on-exec keeps
/// children from inheriting the descriptor, and so the lock.
fn lock(path: &[u8], access: Access, create: bool) -> Result<OwnedFd, Errno> {
    // An exclusive lock needs the file open for writing. Read/write and not
    // write-only: `into_read_only` turns that lock into a shared one on the
    // same descriptor, and a shared lock needs it open for reading, else
    // the change fails with `EBADF`.
    let (mut flags, exclusive) = match access {
        Access::Modifying => (OFlags::RDWR, true),
        Access::ReadOnly => (OFlags::RDONLY, false),
    };
    if create {
        flags |= OFlags::CREATE;
    }
    let file = rustix::io::retry_on_intr(|| {
        rustix::fs::open(
            os_path(path).join(LOCK_FILE),
            flags | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o666),
        )
    })?;
    if !sys::try_lock_file(file.as_fd(), exclusive)? {
        let index = quoted_from_home(path);
        report::note(format_args!("waiting for the lock of index {index}"));
        sys::lock_file(file.as_fd(), exclusive)?;
        report::note(format_args!("acquired the lock of index {index}"));
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

/// Open the index of an update, creating its directory, its lock file,
/// and its database when necessary (DESIGN.md §4.4). Waits for exclusive
/// access.
pub fn open_modifying(path: &[u8]) -> Index {
    if let Err(e) = fsops::create_dir_all(os_path(path), 0o777) {
        die(format!(
            "cannot create index directory {}: {e}",
            quoted_from_home(path)
        ));
    }
    let lock = lock(path, Access::Modifying, true).unwrap_or_else(|e| fatal("lock", path, e));
    open_read_write(path, lock)
}

/// How a process uses the index (DESIGN.md §4.4): a modifying process
/// takes the exclusive lock and opens the database read/write, and a
/// read-only process takes a shared lock and opens it read-only.
#[derive(Clone, Copy)]
pub enum Access {
    Modifying,
    ReadOnly,
}

/// Open the index of `config` for `access`, for a process that needs an
/// index that an update has made and that matches the configuration, such
/// as prune, compact, and a run that does not update its index or opens it
/// again after its update (DESIGN.md §4.4). Otherwise the process ends
/// with a message that says which update to run.
pub fn open_current(config: &Config, access: Access) -> Index {
    let (index, meta) = open_existing(config, access);
    if !meta.compatible(&Settings::from_config(config)) {
        die(format!(
            "index {} does not match the configuration; run `{}`",
            quoted_from_home(&config.index_path),
            config.update_command()
        ));
    }
    index
}

/// Open the index of `config` for `access`, for a process that needs an
/// index that an update has made, whatever its settings, such as `stats`,
/// which reports them. Returns the index with its metadata. Without an
/// index, the process ends with a message that says which update to run;
/// a database without metadata, which the first update leaves when it
/// ends before it writes them, is no index either. Unlike
/// `open_modifying`, it creates no directory, lock file, or database, also
/// for a modifying process (DESIGN.md §4.4), to avoid leaving leftover
/// files, especially when the user configured the wrong index path.
pub fn open_existing(config: &Config, access: Access) -> (Index, Meta) {
    let path = &config.index_path;
    let no_index = || -> ! {
        die(format!(
            "no index at {}; run `{}`",
            quoted_from_home(path),
            config.update_command()
        ))
    };
    let Some(index) = open_database(path, access) else {
        no_index();
    };
    let Some(meta) = index.read_meta() else {
        no_index();
    };
    (index, meta)
}

/// Open the index at `path` for `access`, waiting for its lock, but create
/// nothing: `None` when there is no database. That is no directory or no
/// lock file, which an update creates before the database, or a directory
/// that the first update left before it created the database with all its
/// column families.
fn open_database(path: &[u8], access: Access) -> Option<Index> {
    let lock = match lock(path, access, false) {
        Ok(lock) => lock,
        Err(Errno::NOENT) => return None,
        Err(e) => fatal("open", path, e),
    };
    if !has_database(path) {
        return None;
    }
    Some(match access {
        Access::Modifying => open_read_write(path, lock),
        Access::ReadOnly => open_read_only(path, lock),
    })
}

/// Open the database of the index at `path` read/write, creating it when
/// necessary, for the process that holds `lock`, its exclusive lock.
fn open_read_write(path: &[u8], lock: OwnedFd) -> Index {
    let db = DB::open_cf_descriptors(&db_options(), os_path(path), column_families())
        .unwrap_or_else(|e| fatal("open", path, e));
    Index {
        shared: Arc::new(Shared { db, _lock: lock }),
    }
}

/// True when the index directory `path` holds a database with all its
/// column families. The caller holds a lock of the index, so no update
/// creates the database meanwhile.
fn has_database(path: &[u8]) -> bool {
    match rustix::io::retry_on_intr(|| rustix::fs::stat(os_path(path).join("CURRENT"))) {
        Ok(_) => {}
        Err(Errno::NOENT) => return false,
        Err(e) => fatal("open", path, e),
    }
    // A database without every column family: the first update ended
    // before it created them all.
    let names =
        DB::list_cf(&Options::default(), os_path(path)).unwrap_or_else(|e| fatal("open", path, e));
    Cf::ALL
        .iter()
        .all(|cf| names.iter().any(|name| name == cf.name()))
}

/// Open the database of the index at `path` read-only, for the process
/// that holds `lock`, a shared lock of the index.
fn open_read_only(path: &[u8], lock: OwnedFd) -> Index {
    let db =
        DB::open_cf_descriptors_read_only(&db_options(), os_path(path), column_families(), false)
            .unwrap_or_else(|e| fatal("open", path, e));
    Index {
        shared: Arc::new(Shared { db, _lock: lock }),
    }
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

/// One current location of a configured root, as the reconciliation of an
/// update sees it (DESIGN.md §4.2): its path, and for an immutable root,
/// the identifier of its group, which all copies of the group share.
pub struct CurrentRoot<'a> {
    pub path: &'a [u8],
    pub identifier: Option<&'a [u8]>,
}

/// What the reconciliation of an update decided (DESIGN.md §4.2): which
/// current roots the scan traverses, and the immutable groups, whose
/// completion markers the changes of the scan maintain
/// (`changes::Changes`).
#[cfg_attr(test, derive(Default))]
pub struct Reconciliation {
    /// The roots to scan, in the order of the current roots: every mutable
    /// root and every unfinished group.
    scans: Vec<ScanRoot>,
    /// The immutable groups of the configuration by root ID: the completed
    /// groups, which the update keeps without a scan, and the unfinished
    /// ones, which it scans.
    groups: HashMap<u64, Group>,
}

/// A root that an update scans: a mutable root, or an unfinished group.
pub struct ScanRoot {
    /// The ID of its new row.
    pub root_id: u64,
    /// The current roots that the scan can traverse for it, as indexes
    /// among the current roots that the reconciliation got, in the order in
    /// which the scan tries them: the mutable root, or every copy of the
    /// group. The row names the first one.
    pub locations: Vec<usize>,
}

/// An immutable group of an update, with the row that its marker changes.
struct Group {
    identifier: Vec<u8>,
    /// The path of the root row: the representative copy of a completed
    /// group, and the first copy of an unfinished one, unless its scan
    /// traverses another copy (`changes::Changes::scan_copy`).
    path: Vec<u8>,
    /// The group was completely indexed before the update.
    complete: bool,
}

impl Reconciliation {
    /// The roots to scan, in the order of the current roots.
    pub fn scans(&self) -> &[ScanRoot] {
        &self.scans
    }
}

/// What a prune removes from the index (DESIGN.md §4.2), as `prune` finds
/// it in the current copies of the roots.
#[derive(Default)]
pub struct Pruning {
    /// The roots that go, with all their paths.
    pub roots: Vec<u64>,
    /// The paths that go from roots that stay: the root ID, the relative
    /// path, and the inode ID of each.
    pub paths: Vec<(u64, Vec<u8>, u64)>,
    /// The completed groups that stay, but unfinished: the lookup of a path
    /// that goes failed, so the next update scans the group again. Each one
    /// has the path of a copy that prune opened for its row, by which a run
    /// with `--no-update` finds the copies of an unfinished group.
    pub unfinished: Vec<(u64, Vec<u8>)>,
}

/// What a prune removed.
pub struct Pruned {
    /// The paths of the roots that went, and the other paths that went.
    pub paths: u64,
    /// The inodes that no path referred to any more, which went with their
    /// blocks.
    pub orphans: u64,
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
    /// roots. Reads committed entries. Returns the number of paths.
    fn remove_root_paths(&mut self, roots: RangeInclusive<u64>) -> u64 {
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
        let mut removed = 0;
        while let Some((key, value)) = scan.entry() {
            self.delete(
                Cf::InodePaths,
                &layout::inode_path_key(u64_at(value, 0), u64_at(key, 0), &key[ID_LEN..]),
            );
            let range_first = first.get_or_insert_with(|| key.to_vec());
            last.clear();
            last.extend_from_slice(key);
            removed += 1;
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
        removed
    }

    /// Remove one path from `paths` and `inode_paths`.
    fn remove_path(&mut self, root_id: u64, rel_path: &[u8], inode_id: u64) {
        self.delete(Cf::Paths, &layout::path_key(root_id, rel_path));
        self.delete(
            Cf::InodePaths,
            &layout::inode_path_key(inode_id, root_id, rel_path),
        );
        self.checkpoint();
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

    /// Remove an inode with its paths and blocks. The inode is marked
    /// incomplete first, in the same transaction as the removal of its
    /// paths and first blocks: block removal can commit between batches,
    /// and an interrupted removal must not leave a complete inode with
    /// missing blocks — a later scan would reuse it as unchanged. Recovery
    /// removes incomplete inodes instead (DESIGN.md §4.2). The removal reads
    /// committed entries.
    fn delete_inode(&mut self, inode_id: u64, mut row: InodeRow) {
        row.size = None;
        self.put(Cf::Inodes, &id_key(inode_id), &row.encode());
        self.remove_inode_paths(inode_id);
        self.remove_blocks(inode_id);
        self.delete(Cf::Inodes, &id_key(inode_id));
        self.delete(Cf::Identities, &row.identity);
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
    /// `finish_writes` has already waited for RocksDB's background work, so
    /// the close does not wait.
    pub fn into_read_only(self, path: &[u8]) -> Index {
        let Shared { db, _lock: lock } =
            Arc::into_inner(self.shared).expect("a reader outlives the index");
        drop(db);
        sys::lock_file(lock.as_fd(), false).unwrap_or_else(|e| fatal("lock", path, e));
        open_read_only(path, lock)
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
    pub fn roots(&self) -> Vec<(u64, RootRow)> {
        let mut roots = Vec::new();
        self.shared.each(Cf::Roots, b"", |key, value| {
            roots.push((u64_at(key, 0), RootRow::decode(value)));
        });
        roots
    }

    /// The paths of the root `root_id`, in key order: the relative path and
    /// the inode ID of each.
    pub fn root_paths(&self, root_id: u64) -> Vec<(Vec<u8>, u64)> {
        let start = id_key(root_id);
        let mut scan = self
            .shared
            .scan_range(Cf::Paths, &start, prefix_end(&start), false);
        let mut paths = Vec::new();
        while let Some((key, value)) = scan.entry() {
            paths.push((key[ID_LEN..].to_vec(), u64_at(value, 0)));
            scan.next();
        }
        paths
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

    /// End the writes of a modifying process, at the end of an update and
    /// of a compaction (DESIGN.md §4.4): write the memtables to the
    /// database files, so that a later read-only process has no write-ahead
    /// log to replay, then wait for the compactions that RocksDB has
    /// scheduled. Closing the database would cancel them, and short updates
    /// would leave ever more of them to later ones.
    pub fn finish_writes(&self) {
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

    /// Compact every column family completely, then end the writes as
    /// `finish_writes` does. A prune ends with it, and so do an update with
    /// `--compact` and `hashsync compact`, so that the entries that updates
    /// and prunes removed leave the database files (DESIGN.md §4.4). The
    /// compaction of a column family writes its memtable to the files
    /// first, then moves every entry to the last level, where the removed
    /// entries go with the deletions that removed them.
    pub fn compact(&self) {
        let shared = &self.shared;
        for cf in Cf::ALL {
            let cf = shared.cf(cf);
            shared.db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
        }
        // A deletion can still be in the files after that. A compaction
        // moves a file to a lower level without rewriting it when no other
        // file overlaps it, and the newer entries of a key sort before the
        // older ones. So a file that only deletes the first entry of an
        // older file does not overlap that file, and both can reach the
        // last level unchanged, where the compaction above rewrites
        // nothing. Each file that still holds a deletion is therefore
        // compacted again with the files beside it, which drops the
        // deletion and the entry that it removes. Doing that for the whole
        // range would rewrite the whole database on every prune: 5 s for
        // an index of all of /nix/store.
        let mut options = CompactOptions::default();
        options.set_bottommost_level_compaction(BottommostLevelCompaction::ForceOptimized);
        for file in db(shared.db.live_files()) {
            if file.num_deletions > 0 {
                let cf = shared
                    .db
                    .cf_handle(&file.column_family_name)
                    .expect("database error: a file of an unknown column family");
                shared
                    .db
                    .compact_range_cf_opt(cf, file.start_key, file.end_key, &options);
            }
        }
        // A manual compaction does not report its failure, but RocksDB
        // keeps it, and the wait returns it.
        self.finish_writes();
    }

    /// Reconcile the roots before the scan (DESIGN.md §4.2), with
    /// `current`, the current locations of the configured roots. A
    /// completed group stays, without a scan, while the configuration names
    /// its identifier and none of its paths leads to an incomplete inode,
    /// unless `scan_immutable` asks for a scan of every group. Every other
    /// root loses its paths and its row, and so do the incomplete inodes.
    /// Then every mutable root, and the first copy of each unfinished
    /// group, gets a new row without an identifier, for the scan.
    ///
    /// The markers of the completed roots that do not stay go first, in one
    /// transaction, so that no marker outlives a path of its root. The
    /// blocks go next, in bounded transactions, and so do the paths when
    /// some roots keep theirs. The rows then change in one transaction,
    /// which also removes all paths when no root keeps its paths.
    pub fn reconcile_roots(
        &self,
        current: &[CurrentRoot<'_>],
        scan_immutable: bool,
    ) -> Reconciliation {
        // The copies of each configured group, in order.
        let mut copies: HashMap<&[u8], Vec<usize>> = HashMap::new();
        for (i, root) in current.iter().enumerate() {
            if let Some(identifier) = root.identifier {
                copies.entry(identifier).or_default().push(i);
            }
        }
        let mut incomplete = Vec::new();
        self.shared.each(Cf::Inodes, b"", |key, value| {
            let row = InodeRow::decode(value);
            if row.size.is_none() {
                incomplete.push((u64_at(key, 0), row));
            }
        });
        let mut referring = HashSet::new();
        for (inode_id, _) in &incomplete {
            self.shared
                .each(Cf::InodePaths, &id_key(*inode_id), |key, _| {
                    referring.insert(u64_at(key, ID_LEN));
                });
        }

        // Step 1. A row of a configured group that some update scanned
        // completely stays, unless this update scans every group again. The
        // updates give a group one row, so a second row of a group could
        // only come from a damaged index; it goes too.
        let mut roots = self.roots();
        let mut staying = HashSet::new();
        let mut txn = self.transaction();
        for (root_id, row) in &mut roots {
            let Some(identifier) = &row.identifier else {
                continue;
            };
            if !scan_immutable
                && copies.contains_key(identifier.as_slice())
                && !referring.contains(root_id)
                && staying.insert(identifier.clone())
            {
                continue;
            }
            row.identifier = None;
            txn.put(Cf::Roots, &id_key(*root_id), &row.encode());
        }
        txn.commit();
        // Every root without an identifier is unfinished now; the others
        // keep their paths.
        let last_retained = roots
            .iter()
            .filter(|(_, row)| row.identifier.is_some())
            .map(|&(root_id, _)| root_id)
            .max();

        // Step 2. A root can have many paths, and there can be many roots:
        // the removals run on one thread per core. A path of an incomplete
        // inode under an unfinished root is removed twice, which changes
        // nothing.
        let pool = Pool::start("reconcile", 0);
        pool.install(|| {
            if last_retained.is_some() {
                let losing: Vec<(u64, bool)> = roots
                    .iter()
                    .map(|(root_id, row)| (*root_id, row.identifier.is_none()))
                    .collect();
                let parts = root_parts(&losing, rayon::current_num_threads());
                self.remove_all(&parts, |txn, part| {
                    txn.remove_root_paths(part.clone());
                });
            }
            self.remove_all(&incomplete, |txn, &(inode_id, _)| {
                txn.remove_inode_paths(inode_id);
                txn.remove_blocks(inode_id);
            });
        });
        drop(pool);

        // Step 3.
        let mut txn = self.transaction();
        if last_retained.is_none() {
            // No root keeps its paths: they all go, with one range
            // deletion in each column family.
            txn.clear(Cf::Paths);
            txn.clear(Cf::InodePaths);
        }
        let mut kept: HashMap<&[u8], (u64, &[u8])> = HashMap::new();
        for (root_id, row) in &roots {
            match &row.identifier {
                Some(identifier) => {
                    kept.insert(identifier, (*root_id, &row.path));
                }
                None => txn.delete(Cf::Roots, &id_key(*root_id)),
            }
        }
        for (inode_id, row) in &incomplete {
            txn.delete(Cf::Inodes, &id_key(*inode_id));
            txn.delete(Cf::Identities, &row.identity);
        }
        let mut reconciliation = Reconciliation {
            scans: Vec::new(),
            groups: HashMap::new(),
        };
        // IDs of removed roots can return: their paths are gone. Each new
        // row takes the path of a current root, which no other row has: a
        // kept group keeps a path of one of its own copies.
        let mut next = last_retained.unwrap_or(0) + 1;
        for (i, root) in current.iter().enumerate() {
            let group = match root.identifier {
                Some(identifier) if copies[identifier][0] != i => continue,
                Some(identifier) => kept.get(identifier).map(|&kept| (identifier, kept)),
                None => None,
            };
            if let Some((identifier, (root_id, stored))) = group {
                // The representative path stays while it is a copy of the
                // group: a moved copy, or one that went away, gives its
                // place to the first copy.
                let still_a_copy = copies[identifier]
                    .iter()
                    .any(|&copy| current[copy].path == stored);
                let path = if still_a_copy { stored } else { root.path };
                if path != stored {
                    let row = RootRow {
                        identifier: Some(identifier.to_vec()),
                        path: path.to_vec(),
                    };
                    txn.put(Cf::Roots, &id_key(root_id), &row.encode());
                }
                reconciliation.groups.insert(
                    root_id,
                    Group {
                        identifier: identifier.to_vec(),
                        path: path.to_vec(),
                        complete: true,
                    },
                );
                continue;
            }
            let root_id = next;
            next += 1;
            let row = RootRow {
                identifier: None,
                path: root.path.to_vec(),
            };
            txn.put(Cf::Roots, &id_key(root_id), &row.encode());
            let locations = match root.identifier {
                None => vec![i],
                Some(identifier) => {
                    let group = Group {
                        identifier: identifier.to_vec(),
                        path: root.path.to_vec(),
                        complete: false,
                    };
                    reconciliation.groups.insert(root_id, group);
                    copies[identifier].clone()
                }
            };
            reconciliation.scans.push(ScanRoot { root_id, locations });
        }
        txn.commit();
        reconciliation
    }

    /// Remove what `pruning` names (DESIGN.md §4.2), on the threads of
    /// `pool`. The markers of the groups that go or become unfinished go
    /// first, in one transaction, so that none of them outlives a path of
    /// its group. Then the paths go, in bounded transactions, then the rows
    /// of the roots that go, in one transaction, and last the inodes that
    /// no path refers to any more, with their blocks. An interruption
    /// leaves work that the next prune or update finishes.
    pub fn prune(&self, pool: &ThreadPool, pruning: &Pruning) -> Pruned {
        let going: HashSet<u64> = pruning.roots.iter().copied().collect();
        let roots = self.roots();
        let mut txn = self.transaction();
        let unmarked = roots
            .iter()
            .filter(|(root_id, row)| row.identifier.is_some() && going.contains(root_id))
            .map(|(root_id, row)| (*root_id, &row.path))
            .chain(
                pruning
                    .unfinished
                    .iter()
                    .map(|(root_id, path)| (*root_id, path)),
            );
        for (root_id, path) in unmarked {
            let row = RootRow {
                identifier: None,
                path: path.clone(),
            };
            txn.put(Cf::Roots, &id_key(root_id), &row.encode());
        }
        txn.commit();

        let removed = AtomicU64::new(pruning.paths.len() as u64);
        let losing: Vec<(u64, bool)> = roots
            .iter()
            .map(|(root_id, _)| (*root_id, going.contains(root_id)))
            .collect();
        pool.install(|| {
            let parts = root_parts(&losing, rayon::current_num_threads());
            self.remove_all(&parts, |txn, part| {
                removed.fetch_add(txn.remove_root_paths(part.clone()), Relaxed);
            });
            self.remove_all(&pruning.paths, |txn, (root_id, rel_path, inode_id)| {
                txn.remove_path(*root_id, rel_path, *inode_id);
            });
        });

        let mut txn = self.transaction();
        for root_id in &pruning.roots {
            txn.delete(Cf::Roots, &id_key(*root_id));
        }
        txn.commit();
        Pruned {
            paths: removed.into_inner(),
            orphans: self.remove_orphans(pool),
        }
    }

    /// Remove the inodes that no path refers to any more, and their blocks,
    /// in bounded transactions, once every result of an update has
    /// committed (DESIGN.md §4.6), and at the end of a prune. This also
    /// removes orphans left by earlier interrupted cleanup. Returns their
    /// number.
    ///
    /// The threads of `pool` share the inode IDs by ranges, about four for
    /// each thread. A thread reads only entries whose keys start with an
    /// inode ID in its ranges, and changes only the entries of those
    /// inodes, so no thread reads the entries that another thread changes.
    fn remove_orphans(&self, pool: &ThreadPool) -> u64 {
        // The inode IDs start at 1 and are less than `end`.
        let end = self.next_inode_id();
        let parts = 4 * pool.current_num_threads() as u64;
        let size = (end - 1).div_ceil(parts).max(1);
        let mut ranges = Vec::new();
        let mut start = 1;
        while start < end {
            let next = start.saturating_add(size).min(end);
            ranges.push(start..next);
            start = next;
        }
        pool.install(|| {
            ranges
                .into_par_iter()
                .map(|ids| self.remove_orphans_in(ids))
                .sum()
        })
    }

    /// Remove the inodes with IDs in `ids` that have no paths, and their
    /// blocks, in bounded transactions. Returns their number.
    fn remove_orphans_in(&self, ids: Range<u64>) -> u64 {
        let mut txn = self.transaction();
        let mut orphans = 0;
        let (start, end) = (id_key(ids.start), id_key(ids.end));
        // Both scans read the snapshot of their start, which the removals do
        // not change.
        let mut inodes = self
            .shared
            .scan_range(Cf::Inodes, &start, Some(end.to_vec()), false);
        let mut paths = self
            .shared
            .scan_range(Cf::InodePaths, &start, Some(end.to_vec()), false);
        while let Some((key, value)) = inodes.entry() {
            let inode_id = u64_at(key, 0);
            while let Some((path, _)) = paths.entry()
                && u64_at(path, 0) < inode_id
            {
                paths.next();
            }
            if paths
                .entry()
                .is_none_or(|(path, _)| u64_at(path, 0) != inode_id)
            {
                txn.delete_inode(inode_id, InodeRow::decode(value));
                orphans += 1;
                txn.checkpoint();
            }
            inodes.next();
        }
        txn.commit();
        orphans
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

/// The roots that lose their paths, as parts for a pool of `threads`
/// threads. `roots` holds every root, in ID order, each with true when it
/// loses its paths. A part is a range of root IDs without a root that keeps
/// its paths inside, so that the paths of a part form one key range. About
/// four parts for each thread let the threads share the work. A part removes
/// its paths with one range deletion for each transaction, so the range
/// deletions do not increase with the number of roots.
fn root_parts(roots: &[(u64, bool)], threads: usize) -> Vec<RangeInclusive<u64>> {
    let count = roots.iter().filter(|&&(_, losing)| losing).count();
    let roots_per_part = count.div_ceil(4 * threads).max(1);
    roots
        .chunk_by(|(_, a), (_, b)| a == b)
        .filter(|run| run[0].1)
        .flat_map(|run| run.chunks(roots_per_part))
        .map(|part| part[0].0..=part[part.len() - 1].0)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::os::unix::ffi::OsStrExt;
    use std::process::Command;

    use bstr::ByteSlice;
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
    /// which keep their paths while the configuration names them
    /// (`configured`).
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

    /// The identifiers and paths of the groups that `complete_roots` makes
    /// of `root_ids`, for `configured`.
    fn group_names(root_ids: &[u64]) -> Vec<(Vec<u8>, Vec<u8>)> {
        root_ids
            .iter()
            .map(|id| {
                let name = format!("group{id}");
                (name.clone().into_bytes(), format!("/{name}").into_bytes())
            })
            .collect()
    }

    /// The current roots of a configuration: the copies of the groups of
    /// `groups`, as identifiers and paths, and the mutable roots of
    /// `mutable`.
    fn configured<'a>(
        groups: &'a [(Vec<u8>, Vec<u8>)],
        mutable: &[&'a [u8]],
    ) -> Vec<CurrentRoot<'a>> {
        let copies = groups.iter().map(|(identifier, path)| CurrentRoot {
            path,
            identifier: Some(identifier),
        });
        let mutable = mutable.iter().map(|&path| CurrentRoot {
            path,
            identifier: None,
        });
        copies.chain(mutable).collect()
    }

    /// The roots that a reconciliation gave the scan: their root IDs and
    /// the indexes of their current roots.
    pub fn scans(reconciliation: &Reconciliation) -> Vec<(u64, Vec<usize>)> {
        let scans = reconciliation.scans().iter();
        scans
            .map(|scan| (scan.root_id, scan.locations.clone()))
            .collect()
    }

    /// The root rows: IDs, identifiers, and paths.
    fn root_rows(index: &Index) -> Vec<(u64, Option<Vec<u8>>, Vec<u8>)> {
        index
            .roots()
            .into_iter()
            .map(|(root_id, row)| (root_id, row.identifier, row.path))
            .collect()
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

        let read_only = open_database(path.as_os_str().as_bytes(), Access::ReadOnly).unwrap();
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

    /// A process that needs an index that an update has made, modifying or
    /// read-only, finds no index where there is no directory, a directory
    /// without the lock file, or without a database, or a database without
    /// all its column families: the first update ended early. It adds no
    /// file to what it finds, so that a wrong index path leaves no files
    /// behind (DESIGN.md §4.4).
    #[test]
    fn a_missing_database_is_no_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let no_index = || {
            [Access::Modifying, Access::ReadOnly]
                .into_iter()
                .all(|access| open_database(path.as_os_str().as_bytes(), access).is_none())
        };
        let files = || {
            let mut names: Vec<String> = std::fs::read_dir(&path)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect();
            names.sort();
            names
        };
        assert!(no_index());
        assert!(!path.exists());
        std::fs::create_dir(&path).unwrap();
        assert!(no_index());
        assert_eq!(files(), Vec::<String>::new());
        std::fs::write(path.join(LOCK_FILE), b"").unwrap();
        assert!(no_index());
        assert_eq!(files(), [LOCK_FILE]);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        drop(DB::open_cf(&options, &path, ["default", "roots"]).unwrap());
        let database = files();
        assert!(no_index());
        assert_eq!(files(), database);
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
        let reconciliation = index.reconcile_roots(&configured(&[], &[b"/s", b"/t"]), false);
        assert_eq!(scans(&reconciliation), [(1, vec![0]), (2, vec![1])]);
        assert_eq!(
            root_rows(&index),
            [(1, None, b"/s".to_vec()), (2, None, b"/t".to_vec())]
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
        let reconciliation = index.reconcile_roots(&configured(&[], &[b"/u"]), false);
        assert_eq!(scans(&reconciliation), [(1, vec![0])]);
    }

    /// Roots that keep their paths keep all of them. The other roots lose
    /// both entries of every path, and their rows, and incomplete inodes
    /// lose their paths and blocks; new roots get IDs after the kept ones
    /// (DESIGN.md §4.2). The paths at the ends of each root's key range
    /// test the ends of the range deletions.
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
        // Inode 3 is incomplete, with paths under two unfinished roots.
        paths.extend([(3, 3, &b"i"[..]), (3, 4, b"i")]);
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

        let groups = group_names(&[2, 5]);
        let reconciliation = index.reconcile_roots(&configured(&groups, &[b"/new"]), false);
        assert_eq!(scans(&reconciliation), [(6, vec![2])]);
        let ids: Vec<u64> = index.roots().iter().map(|&(id, _)| id).collect();
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

    /// Give the root rows of `roots` their identifiers: they become
    /// completed immutable groups, at the paths that they have.
    pub fn give_identifiers(index: &Index, roots: &[(u64, &[u8])]) {
        let rows: HashMap<u64, RootRow> = index.roots().into_iter().collect();
        let mut txn = index.transaction();
        for &(root_id, identifier) in roots {
            let row = RootRow {
                identifier: Some(identifier.to_vec()),
                path: rows[&root_id].path.clone(),
            };
            txn.put(Cf::Roots, &id_key(root_id), &row.encode());
        }
        txn.commit();
    }

    /// The index of `kept_groups_follow_the_configuration`, before its
    /// reconciliation: roots 1, 2, 4, and 5 are completed groups, and root
    /// 3 is not. Root 4 has a path of the incomplete inode 3.
    fn groups_to_reconcile(index: &Index) {
        insert(
            index,
            &[
                (1, b"\x01\x01", Some(4096)),
                (2, b"\x01\x02", Some(4096)),
                (3, b"\x01\x03", None),
            ],
            &[(1, b"/a"), (2, b"/b"), (3, b"/c"), (4, b"/d"), (5, b"/e")],
            &[
                (1, 1, b"f"),
                (2, 2, b"f"),
                (1, 3, b"f"),
                (3, 4, b"f"),
                (1, 4, b"g"),
                (2, 5, b"f"),
            ],
            &[(3, 0, 5, [3; PREFIX_LEN])],
        );
        give_identifiers(
            index,
            &[(1, b"one"), (2, b"two"), (4, b"four"), (5, b"five")],
        );
    }

    /// The configuration of `kept_groups_follow_the_configuration`: group
    /// `one` with its stored copy second, group `two` without its stored
    /// copy, the mutable root `/c`, group `four` as it was, a new group
    /// `six`, and no group `five`.
    fn reconciled_configuration() -> Vec<CurrentRoot<'static>> {
        let copy = |identifier: &'static [u8], path: &'static [u8]| CurrentRoot {
            path,
            identifier: Some(identifier),
        };
        vec![
            copy(b"one", b"/a1"),
            copy(b"one", b"/a"),
            copy(b"two", b"/b2"),
            CurrentRoot {
                path: b"/c",
                identifier: None,
            },
            copy(b"four", b"/d"),
            copy(b"six", b"/f"),
        ]
    }

    /// A completed group stays, with its paths and without a scan, while
    /// the configuration names its identifier and none of its paths leads
    /// to an incomplete inode. It keeps its representative path while that
    /// is one of its copies, and takes its first copy otherwise. Every
    /// other root goes, with its paths, and the mutable roots and the first
    /// copy of each other group get new rows, for the scan (DESIGN.md
    /// §4.2).
    #[test]
    fn kept_groups_follow_the_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        groups_to_reconcile(&index);
        let current = reconciled_configuration();
        let reconciliation = index.reconcile_roots(&current, false);
        assert_eq!(
            scans(&reconciliation),
            [(3, vec![3]), (4, vec![4]), (5, vec![5])]
        );
        let mut groups: Vec<(u64, &[u8], &[u8], bool)> = reconciliation
            .groups
            .iter()
            .map(|(&root_id, group)| {
                let (identifier, path) = (&group.identifier[..], &group.path[..]);
                (root_id, identifier, path, group.complete)
            })
            .collect();
        groups.sort();
        let expected: [(u64, &[u8], &[u8], bool); 4] = [
            (1, b"one", b"/a", true),
            (2, b"two", b"/b2", true),
            (4, b"four", b"/d", false),
            (5, b"six", b"/f", false),
        ];
        assert_eq!(groups, expected);
        let some = |identifier: &[u8]| Some(identifier.to_vec());
        assert_eq!(
            root_rows(&index),
            [
                (1, some(b"one"), b"/a".to_vec()),
                (2, some(b"two"), b"/b2".to_vec()),
                (3, None, b"/c".to_vec()),
                (4, None, b"/d".to_vec()),
                (5, None, b"/f".to_vec()),
            ]
        );
        let (rows, entries) = path_rows(&index);
        assert_eq!(rows, [(1, b"f".to_vec(), 1), (2, b"f".to_vec(), 2)]);
        assert_eq!(entries, rows);
        // The complete inodes stay for reuse, also without paths.
        for (cf, n) in [(Cf::Inodes, 2), (Cf::Blocks, 0), (Cf::Strong, 0)] {
            assert_eq!(count(&index, cf), n, "{cf:?}");
        }
    }

    /// The markers of the groups that do not stay go in a transaction of
    /// their own, before the paths of their groups go: an interruption
    /// after it leaves those groups unfinished, without paths or with
    /// them, but never a marker without all the paths of its group
    /// (DESIGN.md §4.2). The next reconciliation finishes the work.
    #[test]
    fn markers_go_before_the_paths_of_their_groups() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        groups_to_reconcile(&index);
        let current = reconciled_configuration();

        // An interruption before the first commit changes nothing.
        let before = (root_rows(&index), path_rows(&index));
        interrupt_after_commits(Some(0));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            index.reconcile_roots(&current, false)
        }));
        interrupt_after_commits(None);
        assert!(result.is_err(), "the injected failure must stop it");
        assert_eq!((root_rows(&index), path_rows(&index)), before);

        // The interruption injects a failure into the commits of this
        // thread, so the paths that the pool removes go.
        interrupt_after_commits(Some(1));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            index.reconcile_roots(&current, false)
        }));
        interrupt_after_commits(None);
        assert!(result.is_err(), "the injected failure must stop it");
        let some = |identifier: &[u8]| Some(identifier.to_vec());
        assert_eq!(
            root_rows(&index),
            [
                (1, some(b"one"), b"/a".to_vec()),
                (2, some(b"two"), b"/b".to_vec()),
                (3, None, b"/c".to_vec()),
                (4, None, b"/d".to_vec()),
                (5, None, b"/e".to_vec()),
            ]
        );
        let (rows, entries) = path_rows(&index);
        assert_eq!(rows, [(1, b"f".to_vec(), 1), (2, b"f".to_vec(), 2)]);
        assert_eq!(entries, rows);

        let reconciliation = index.reconcile_roots(&current, false);
        assert_eq!(
            scans(&reconciliation),
            [(3, vec![3]), (4, vec![4]), (5, vec![5])]
        );
        assert_eq!(root_rows(&index).len(), 5);
        assert_eq!(path_rows(&index).0.len(), 2);
    }

    /// An unfinished group gets one row, at its first copy, and its scan
    /// can take any of its copies, in their order (DESIGN.md §4.2).
    #[test]
    fn an_unfinished_group_is_scanned_at_one_of_its_copies() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        let copy = |path: &'static [u8]| CurrentRoot {
            path,
            identifier: Some(b"g"),
        };
        let mutable = CurrentRoot {
            path: b"/m",
            identifier: None,
        };
        let reconciliation = index.reconcile_roots(&[copy(b"/a"), mutable, copy(b"/b")], false);
        assert_eq!(scans(&reconciliation), [(1, vec![0, 2]), (2, vec![1])]);
        assert_eq!(
            root_rows(&index),
            [(1, None, b"/a".to_vec()), (2, None, b"/m".to_vec())]
        );
    }

    /// Kept groups that exchange their paths exchange them in their rows
    /// too, so that no two rows share a path (DESIGN.md §4.2).
    #[test]
    fn kept_groups_can_exchange_paths() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        insert(&index, &[], &[(1, b"/p"), (2, b"/q")], &[], &[]);
        give_identifiers(&index, &[(1, b"x"), (2, b"y")]);
        let groups = [
            (b"x".to_vec(), b"/q".to_vec()),
            (b"y".to_vec(), b"/p".to_vec()),
        ];
        let reconciliation = index.reconcile_roots(&configured(&groups, &[b"/r"]), false);
        assert_eq!(scans(&reconciliation), [(3, vec![2])]);
        let some = |identifier: &[u8]| Some(identifier.to_vec());
        assert_eq!(
            root_rows(&index),
            [
                (1, some(b"x"), b"/q".to_vec()),
                (2, some(b"y"), b"/p".to_vec()),
                (3, None, b"/r".to_vec()),
            ]
        );
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

        index.reconcile_roots(&configured(&group_names(&[3]), &[]), false);
        let (rows, entries) = path_rows(&index);
        assert_eq!(rows, [(3, b"".to_vec(), 1), (3, b"\xff\xff".to_vec(), 1)]);
        assert_eq!(entries, rows);
    }

    /// With `scan_immutable`, no completed group stays: each one loses its
    /// marker and its paths, and its first copy gets a new row for the
    /// scan, as a group that the configuration names for the first time.
    /// The complete inodes stay for reuse (DESIGN.md §4.2).
    #[test]
    fn scan_immutable_scans_every_group_again() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        insert(
            &index,
            &[(1, b"\x01\x01", Some(4096)), (2, b"\x01\x02", Some(4096))],
            &[(1, b"/group1"), (2, b"/group2")],
            &[(1, 1, b"f"), (2, 2, b"f")],
            &[(1, 0, 5, [1; PREFIX_LEN])],
        );
        complete_roots(&index, &[1, 2]);
        let groups = group_names(&[1, 2]);
        let current = configured(&groups, &[b"/m"]);
        let reconciliation = index.reconcile_roots(&current, true);
        assert_eq!(
            scans(&reconciliation),
            [(1, vec![0]), (2, vec![1]), (3, vec![2])]
        );
        assert!(reconciliation.groups.values().all(|group| !group.complete));
        assert_eq!(
            root_rows(&index),
            [
                (1, None, b"/group1".to_vec()),
                (2, None, b"/group2".to_vec()),
                (3, None, b"/m".to_vec()),
            ]
        );
        assert_eq!(path_rows(&index), (vec![], vec![]));
        for (cf, n) in [(Cf::Inodes, 2), (Cf::Blocks, 1)] {
            assert_eq!(count(&index, cf), n, "{cf:?}");
        }
    }

    /// The index of the prune tests: the mutable root 1 and the completed
    /// groups 2 to 4. Inode 1 has a path under roots 1 and 2, inode 2 under
    /// root 2 alone, inode 3 under root 3, and inode 4 two under root 4.
    fn index_to_prune(index: &Index) {
        let inodes: Vec<(u64, Vec<u8>)> = (1..=4)
            .map(|id| (id, layout::identity(b"\x01", &[id as u8])))
            .collect();
        let inodes: Vec<(u64, &[u8], Option<u64>)> = inodes
            .iter()
            .map(|(id, identity)| (*id, &identity[..], Some(4096)))
            .collect();
        let blocks: Vec<_> = (1..=4)
            .map(|id| (id, 0, id, [id as u8; PREFIX_LEN]))
            .collect();
        insert(
            index,
            &inodes,
            &[(1, b"/m"), (2, b"/g2"), (3, b"/g3"), (4, b"/g4")],
            &[
                (1, 1, b"a"),
                (1, 2, b"a"),
                (2, 2, b"b"),
                (3, 3, b"c"),
                (4, 4, b"d"),
                (4, 4, b"e"),
            ],
            &blocks,
        );
        give_identifiers(index, &[(2, b"two"), (3, b"three"), (4, b"four")]);
    }

    /// A prune removes the roots that go, with all their paths, and the
    /// other paths that go, and then the inodes that no path refers to any
    /// more, with their blocks. A group that goes, or that becomes
    /// unfinished, loses its marker, and an unfinished one takes the path
    /// that the prune gives it. A group that loses paths otherwise keeps
    /// its marker (DESIGN.md §4.2).
    #[test]
    fn prune_removes_roots_paths_and_orphans() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        index_to_prune(&index);
        let pruning = Pruning {
            roots: vec![2],
            paths: vec![(3, b"c".to_vec(), 3), (4, b"d".to_vec(), 4)],
            unfinished: vec![(4, b"/g4b".to_vec())],
        };
        let pool = Pool::start("test", 2);
        let pruned = index.prune(&pool, &pruning);
        // The paths a and b of root 2, then c and d; inodes 2 and 3.
        assert_eq!((pruned.paths, pruned.orphans), (4, 2));
        let some = |identifier: &[u8]| Some(identifier.to_vec());
        assert_eq!(
            root_rows(&index),
            [
                (1, None, b"/m".to_vec()),
                (3, some(b"three"), b"/g3".to_vec()),
                (4, None, b"/g4b".to_vec()),
            ]
        );
        let (rows, entries) = path_rows(&index);
        assert_eq!(rows, [(1, b"a".to_vec(), 1), (4, b"e".to_vec(), 4)]);
        assert_eq!(entries, rows);
        for (cf, n) in [
            (Cf::Inodes, 2),
            (Cf::Identities, 2),
            (Cf::Blocks, 2),
            (Cf::Strong, 2),
        ] {
            assert_eq!(count(&index, cf), n, "{cf:?}");
        }
    }

    /// The markers go in a transaction of their own, before any path: an
    /// interruption of a prune never leaves the marker of a group that goes,
    /// or that becomes unfinished, without all the paths of the group. The
    /// next prune finishes the work (DESIGN.md §4.2).
    #[test]
    fn prune_removes_markers_before_paths() {
        let dir = tempfile::tempdir().unwrap();
        let index = new_index(dir.path());
        index_to_prune(&index);
        let pruning = Pruning {
            roots: vec![2],
            paths: vec![(4, b"d".to_vec(), 4)],
            unfinished: vec![(4, b"/g4".to_vec())],
        };
        let pool = Pool::start("test", 1);
        let prune = || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                index.prune(&pool, &pruning);
            }))
        };

        // An interruption before the first commit changes nothing.
        let before = (root_rows(&index), path_rows(&index));
        interrupt_after_commits(Some(0));
        let result = prune();
        interrupt_after_commits(None);
        assert!(result.is_err(), "the injected failure must stop it");
        assert_eq!((root_rows(&index), path_rows(&index)), before);

        // The injected failure is for the commits of this thread, so the
        // paths that the pool removes go, and the rows stay.
        interrupt_after_commits(Some(1));
        let result = prune();
        interrupt_after_commits(None);
        assert!(result.is_err(), "the injected failure must stop it");
        let some = |identifier: &[u8]| Some(identifier.to_vec());
        assert_eq!(
            root_rows(&index),
            [
                (1, None, b"/m".to_vec()),
                (2, None, b"/g2".to_vec()),
                (3, some(b"three"), b"/g3".to_vec()),
                (4, None, b"/g4".to_vec()),
            ]
        );
        let paths: Vec<PathRow> = path_rows(&index).0;
        let expected = [
            (1, b"a".to_vec(), 1),
            (3, b"c".to_vec(), 3),
            (4, b"e".to_vec(), 4),
        ];
        assert_eq!(paths, expected);

        prune().unwrap();
        assert_eq!(root_rows(&index).len(), 3);
        assert_eq!(path_rows(&index).0, expected);
        assert_eq!(count(&index, Cf::Inodes), 3);
    }

    /// The parts of the roots that lose their paths cover each of them
    /// once, never contain a root that keeps its paths, and contain more
    /// roots when there are fewer threads.
    #[test]
    fn root_parts_leave_out_retained_roots() {
        // Roots 2 and 5 keep their paths; there is no root 8.
        let roots: Vec<(u64, bool)> = [1, 2, 3, 4, 5, 6, 7, 9]
            .into_iter()
            .map(|id| (id, ![2, 5].contains(&id)))
            .collect();
        // Six roots lose their paths, four parts per thread: two roots per
        // part.
        assert_eq!(root_parts(&roots, 1), [1..=1, 3..=4, 6..=7, 9..=9]);
        assert_eq!(
            root_parts(&roots, 64),
            [1..=1, 3..=3, 4..=4, 6..=6, 7..=7, 9..=9]
        );
        assert!(root_parts(&roots[1..2], 1).is_empty());
    }
}

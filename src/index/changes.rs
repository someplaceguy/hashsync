// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! The index changes of an update's scan (DESIGN.md §4.6): the claims of
//! the regular files that the scan finds, and the rows that the scan tasks
//! write for them.
//!
//! The first claim of a file makes its task the owner. The owner reuses the
//! file's unchanged row, or hashes the file into its row, and then
//! publishes the result. The tasks of the other links wait for that result,
//! or use it. Each thread of the scan pool collects the changes of its
//! tasks in a transaction of its own. The transaction commits when it
//! reaches the bound of a transaction, before its task publishes an inode
//! row that the tasks of other links depend on, and after the scan. Only
//! the owner of a file changes that file's inode, so the owner reads the
//! committed rows of its inode without waiting for other threads. After the
//! scan, the inodes that no path references any more are removed.
//!
//! The changes also keep the completion markers of the immutable groups
//! (DESIGN.md §4.2). A root whose scan fails, because an entry under it
//! fails, is remembered until the scan ends, and so is every root with a
//! path of a file whose processing fails, and a single-file root that its
//! permissions keep from being opened for reading. Once every transaction
//! has committed, the unfinished groups that the scan traversed without a
//! failure get their markers. A completed group, which the update does not
//! scan, loses its marker when an owner abandons a file that the group
//! shares, before the file's paths go.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard};

use rayon::ThreadPool;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use super::layout::{self, Cf, ID_LEN, InodeRow, RootRow, id_key, u64_at};
use super::{Group, Index, Reconciliation, Transaction};
use crate::blocks::BlockRow;
use crate::fsid::{FsCache, Identity};

/// A path of a file under a root.
pub struct RelPath {
    pub root_id: u64,
    pub rel_path: Vec<u8>,
}

#[derive(Default, Debug)]
pub struct Summary {
    pub paths: u64,
    pub blocks: u64,
    /// Inodes that ended the update without a path and were removed.
    pub orphans: u64,
    /// The immutable groups of the configuration: those that were
    /// complete before the update, which it reused without a scan, those
    /// that its scan completed, and those that are unfinished after it:
    /// scanned with a failure, or reused until they lost their markers.
    pub groups_reused: u64,
    pub groups_completed: u64,
    pub groups_incomplete: u64,
}

/// The index changes of one update's scan.
pub struct Changes<'a> {
    index: &'a Index,
    fs_cache: &'a FsCache,
    /// The pool whose tasks write the changes.
    pool: &'a ThreadPool,
    /// One transaction for each thread of the pool, by the index of the
    /// thread in the pool.
    transactions: Vec<Mutex<Transaction<'a>>>,
    files: Files,
    /// The index had no inode rows when the scan began, so it had no
    /// identities either: an identity is added and removed in the
    /// transaction of its inode row. The owners look up none.
    empty: bool,
    /// The ID for the next new inode row.
    next_inode_id: AtomicU64,
    paths: AtomicU64,
    blocks: AtomicU64,
    /// The immutable groups of the update, by root ID.
    groups: &'a HashMap<u64, Group>,
    /// The IDs of the roots whose scan failed.
    failed: Mutex<HashSet<u64>>,
    /// The paths of the copies that the scan traverses for unfinished
    /// groups, by root ID, where these are not the first copies, which the
    /// rows name until the scan ends.
    copies: Mutex<HashMap<u64, Vec<u8>>>,
}

impl<'a> Changes<'a> {
    /// The changes of a scan whose tasks run on `pool`, of the roots that
    /// `reconciliation` gave the update.
    pub fn new(
        index: &'a Index,
        fs_cache: &'a FsCache,
        pool: &'a ThreadPool,
        reconciliation: &'a Reconciliation,
    ) -> Self {
        let next_inode_id = index.next_inode_id();
        Changes {
            index,
            fs_cache,
            pool,
            transactions: (0..pool.current_num_threads())
                .map(|_| Mutex::new(index.transaction()))
                .collect(),
            files: Files::default(),
            empty: next_inode_id == 1,
            next_inode_id: AtomicU64::new(next_inode_id),
            paths: AtomicU64::new(0),
            blocks: AtomicU64::new(0),
            groups: &reconciliation.groups,
            failed: Mutex::default(),
            copies: Mutex::default(),
        }
    }

    /// Remember that the scan of the root `root_id` did not index all of
    /// it: an entry under it failed, or no copy of its group was there. An
    /// unfinished group of such a root stays unfinished, and the next update
    /// scans it again (DESIGN.md §4.5).
    pub fn fail_root(&self, root_id: u64) {
        self.failed.lock().unwrap().insert(root_id);
    }

    /// Remember that the scan of the root `root_id` traverses the copy at
    /// `path`, and not the first copy of its group, which was not there and
    /// may vanish (DESIGN.md §4.2). After the scan, the row of the group
    /// names the copy whose paths it has, whether the group is complete or
    /// not.
    pub fn scan_copy(&self, root_id: u64, path: &[u8]) {
        self.copies.lock().unwrap().insert(root_id, path.to_vec());
    }

    /// Claim a file too small to index (DESIGN.md §4.1). The first claim
    /// removes the rows that the file had. No link adds a path. A completed
    /// group with a path of the file keeps its marker, as it does when
    /// another root's scan rehashes the file: the group's scan read this
    /// file, which the user promised would not change, and a completed
    /// group is never scanned again for such a change (DESIGN.md §4.2).
    pub fn claim_small(&self, identity: Identity) {
        if self.files.claim_small(&identity)
            && let Some(existing) = self.existing(&self.identity_key(&identity))
        {
            let mut writer = self.writer();
            writer.txn.delete_inode(existing.inode_id, existing.row);
            writer.txn.checkpoint();
        }
    }

    /// Claim a file that can be indexed, found at `path`. `size` and
    /// `ctime_ns` are the metadata observed before hashing: the first claim
    /// reuses a complete row that stores them (DESIGN.md §4.2), and
    /// otherwise its owner stores them with the new content. A file that
    /// changes while it is hashed then has a later ctime than its row, so
    /// the next update hashes it again (DESIGN.md §4.5).
    pub fn claim(
        &self,
        identity: Identity,
        path: RelPath,
        size: u64,
        ctime_ns: i64,
    ) -> Claim<'_, 'a> {
        let path = match self.files.claim(&identity, path) {
            Found::Owner(path) => path,
            Found::Link(inode_id, path) => {
                self.writer().insert_path(inode_id, &path);
                return Claim::Linked;
            }
            Found::Dropped(why, path) => {
                self.drop_link(why, &path);
                return Claim::Linked;
            }
            Found::Deferred => return Claim::Linked,
        };
        // The owner looks up its identity once, for the reuse and for the
        // replacement of the row.
        let key = self.identity_key(&identity);
        let existing = self.existing(&key);
        if let Some(existing) = &existing
            && existing.reusable(size, ctime_ns)
        {
            // The row committed before the scan: the owner's path and the
            // waiting paths can go into the same transaction.
            let mut writer = self.writer();
            writer.insert_path(existing.inode_id, &path);
            for path in self.files.publish(&identity, existing.inode_id) {
                writer.insert_path(existing.inode_id, &path);
            }
            return Claim::Unchanged;
        }
        Claim::Owner(Owner {
            changes: self,
            identity,
            key,
            existing: existing.map(|existing| existing.inode_id),
            path,
            size,
            ctime_ns,
        })
    }

    /// End the scan (DESIGN.md §4.6): check that every claimed file has a
    /// result, and commit the transaction of every thread, in parallel on
    /// the threads of the pool. Only then do the unfinished groups that the
    /// scan traversed without a failure get their completion markers, so
    /// that a directory conflict that a late task finds ends the update
    /// before any group is complete (DESIGN.md §4.2). The row of each group
    /// that the scan traversed names the copy that it traversed, whether
    /// the group is complete now or stays unfinished. Then remove the
    /// inodes without paths, and their blocks, in bounded transactions,
    /// also on the threads of the pool.
    pub fn finish(self) -> Summary {
        // `#[must_use]` does not catch an owner that an early return drops.
        let pending = self.files.pending();
        assert!(pending == 0, "{pending} claimed files have no result");
        self.pool.install(|| {
            self.transactions
                .par_iter()
                .for_each(|txn| txn.lock().unwrap().commit());
        });
        let mut summary = Summary {
            paths: self.paths.into_inner(),
            blocks: self.blocks.into_inner(),
            ..Summary::default()
        };
        // A completed group fails only when it loses its marker.
        let failed = self.failed.into_inner().unwrap();
        let copies = self.copies.into_inner().unwrap();
        let mut txn = self.index.transaction();
        for (&root_id, group) in self.groups {
            if failed.contains(&root_id) {
                summary.groups_incomplete += 1;
                // A run with `--no-update` finds the copies of an
                // unfinished group by the path of its row. The row names the
                // first copy, which was not there when the scan took
                // another, so it takes the path of that other copy.
                if let Some(path) = copies.get(&root_id) {
                    let row = RootRow {
                        identifier: None,
                        path: path.clone(),
                    };
                    txn.put(Cf::Roots, &id_key(root_id), &row.encode());
                    txn.checkpoint();
                }
            } else if group.complete {
                summary.groups_reused += 1;
            } else {
                let path = copies.get(&root_id).unwrap_or(&group.path);
                let row = RootRow {
                    identifier: Some(group.identifier.clone()),
                    path: path.clone(),
                };
                txn.put(Cf::Roots, &id_key(root_id), &row.encode());
                txn.checkpoint();
                summary.groups_completed += 1;
            }
        }
        txn.commit();
        summary.orphans = self.index.remove_orphans(self.pool);
        summary
    }

    /// The file `identity`, which its owner found at `path`, has no rows,
    /// for the reason `why`: no link adds a path, and the roots of the
    /// links, the owner's, those that waited, and those that come later,
    /// are unsuccessful as `drop_link` says (DESIGN.md §4.6).
    fn drop_file(&self, why: Dropped, identity: &Identity, path: &RelPath) {
        self.drop_link(why, path);
        for path in self.files.discard(identity, why) {
            self.drop_link(why, &path);
        }
    }

    /// A link at `path` of a file that has no rows, for the reason `why`,
    /// adds no path. Its root is unsuccessful when the file's processing
    /// failed. A file that its permissions keep from being opened for
    /// reading is skipped below the root, as any entry is that the
    /// permissions of the root's copy deny, which a scan would find again
    /// on every update; but a single-file root that is the link cannot be
    /// read at all, so that root is unsuccessful, and its group does not
    /// complete without its file (DESIGN.md §4.5).
    fn drop_link(&self, why: Dropped, path: &RelPath) {
        let fails = match why {
            Dropped::Small => false,
            Dropped::Denied => path.rel_path.is_empty(),
            Dropped::Failed => true,
        };
        if fails {
            self.fail_root(path.root_id);
        }
    }

    /// Remove, in `txn`, the marker of every completed group with a path of
    /// the inode `inode_id`, which an owner abandons (DESIGN.md §4.2 and
    /// §4.6). The inode loses its rows although its file may still be
    /// indexable, so the next update scans the group again, and its root
    /// counts as failed. The marker goes before the paths, so that no
    /// commit leaves the group complete without them. Reads the committed
    /// entries of the inode, which only its owner changes. Several threads
    /// can remove the marker of one group; they write the same row.
    fn invalidate_groups(&self, txn: &mut Transaction<'_>, inode_id: u64) {
        let mut done = HashSet::new();
        self.index
            .shared
            .each(Cf::InodePaths, &id_key(inode_id), |key, _| {
                let root_id = u64_at(key, ID_LEN);
                if let Some(group) = self.groups.get(&root_id)
                    && group.complete
                    && done.insert(root_id)
                {
                    let row = RootRow {
                        identifier: None,
                        path: group.path.clone(),
                    };
                    txn.put(Cf::Roots, &id_key(root_id), &row.encode());
                    self.fail_root(root_id);
                }
            });
    }

    /// The transaction of the calling thread, which must be a thread of the
    /// pool. Only the tasks of that thread lock it, one at a time: a task
    /// that finds it locked runs inside another task of the thread, which
    /// is a bug (`Hashing`).
    fn writer(&self) -> Writer<'_, 'a> {
        let thread = self
            .pool
            .current_thread_index()
            .expect("index changes come from the scan pool");
        Writer {
            changes: self,
            txn: self.transactions[thread]
                .try_lock()
                .expect("one task of a thread holds its transaction at a time"),
        }
    }

    /// The `identities` key of a scan identity (DESIGN.md §4.4).
    fn identity_key(&self, identity: &Identity) -> Vec<u8> {
        layout::identity(&self.fs_cache.info(identity.fs).fsid, &identity.handle)
    }

    /// The row of an identity (an `identities` key), for the owner of the
    /// file; `None` for a new identity. Only the owner changes the row, so
    /// it stays valid for the owner's changes.
    fn existing(&self, identity: &[u8]) -> Option<Existing> {
        if self.empty {
            return None;
        }
        let inode_id = self.index.inode_id(identity)?;
        Some(Existing {
            inode_id,
            row: self.index.inode_row(inode_id),
        })
    }
}

/// What the claim of a file that can be indexed gives its task.
#[must_use]
pub enum Claim<'c, 'a> {
    /// Another link of a claimed file. Its path goes to the owner's row
    /// once that row has committed, and goes nowhere if the file has no
    /// rows.
    Linked,
    /// The first claim, of a file whose row is current. Its path, and the
    /// paths that waited, go to that row.
    Unchanged,
    /// The first claim, of a file that its task must hash.
    Owner(Owner<'c, 'a>),
}

/// The owner of a file that its task must hash. The owner ends with
/// `fail` or `deny`, or with `begin` and then `end` or `abandon`, which
/// publish the file's result to the other links. `Changes::finish` checks
/// that every claimed file has a result.
#[must_use]
pub struct Owner<'c, 'a> {
    changes: &'c Changes<'a>,
    identity: Identity,
    /// The `identities` key of `identity`.
    key: Vec<u8>,
    /// The ID of the identity's existing inode row.
    existing: Option<u64>,
    /// The owner's own path.
    path: RelPath,
    size: u64,
    ctime_ns: i64,
}

impl<'c, 'a> Owner<'c, 'a> {
    /// Start to replace the blocks of the file: mark its inode incomplete,
    /// with the observed ctime, then remove the old blocks of an existing
    /// row in bounded transactions (DESIGN.md §4.6). A new identity gets a
    /// new row.
    pub fn begin(self) -> Hashing<'c, 'a> {
        let mut writer = self.changes.writer();
        let row = InodeRow {
            size: None,
            ctime_ns: self.ctime_ns,
            identity: self.key,
        };
        let inode_id = match self.existing {
            Some(inode_id) => {
                // The mark may commit before any block is removed; recovery
                // then removes the blocks of the incomplete inode.
                writer.txn.put(Cf::Inodes, &id_key(inode_id), &row.encode());
                writer.txn.remove_blocks(inode_id);
                inode_id
            }
            None => {
                let inode_id = self.changes.next_inode_id.fetch_add(1, Relaxed);
                writer
                    .txn
                    .put(Cf::Identities, &row.identity, &id_key(inode_id));
                writer.txn.put(Cf::Inodes, &id_key(inode_id), &row.encode());
                inode_id
            }
        };
        Hashing {
            writer,
            identity: self.identity,
            inode_id,
            row,
            path: self.path,
            size: self.size,
        }
    }

    /// The file cannot be opened for reading, and not because of its
    /// permissions (`deny`): no link adds a path, and the roots of the links
    /// are unsuccessful. The existing row of the file, if any, gets no path
    /// either, so it goes with the orphans after the scan, unless a
    /// completed group, which the update does not scan, keeps a path of it.
    pub fn fail(self) {
        self.changes
            .drop_file(Dropped::Failed, &self.identity, &self.path);
    }

    /// The file cannot be opened for reading because its permissions deny
    /// it: no link adds a path, nor does the existing row get one, as with
    /// `fail`, but only a single-file root that is a link of the file is
    /// unsuccessful (`Changes::drop_link`).
    pub fn deny(self) {
        self.changes
            .drop_file(Dropped::Denied, &self.identity, &self.path);
    }
}

/// A file that its owner hashes into an incomplete inode row. It holds the
/// lock of the transaction of its thread until `end` or `abandon`. So while
/// the file is hashed, its task must not call a rayon function that can
/// run other tasks on the thread, such as `join`, `scope`, or a parallel
/// iterator: a task that writes changes there finds that lock taken, and
/// panics.
#[must_use]
pub struct Hashing<'c, 'a> {
    writer: Writer<'c, 'a>,
    identity: Identity,
    inode_id: u64,
    row: InodeRow,
    /// The owner's own path.
    path: RelPath,
    size: u64,
}

impl Hashing<'_, '_> {
    /// Add a bounded batch of the file's block rows.
    pub fn blocks(&mut self, rows: &[BlockRow]) {
        let txn = &mut self.writer.txn;
        for row in rows {
            txn.put(
                Cf::Blocks,
                &layout::block_key(self.inode_id, row.block_no),
                &layout::block_value(row.weak, &row.strong_prefix),
            );
            txn.put(
                Cf::Strong,
                &layout::strong_key(&row.strong_prefix, self.inode_id, row.block_no),
                &[],
            );
        }
        txn.checkpoint();
        self.writer
            .changes
            .blocks
            .fetch_add(rows.len() as u64, Relaxed);
    }

    /// Hashing succeeded: store the size and the owner's path, which make
    /// the inode complete even when no block rows were added
    /// (DESIGN.md §4.6). Commit, so that the row has committed before the
    /// other links add their paths to it. Then publish the row, and add the
    /// paths that waited for it.
    pub fn end(mut self) {
        // The ctime is already there from `begin`.
        self.row.size = Some(self.size);
        self.writer
            .txn
            .put(Cf::Inodes, &id_key(self.inode_id), &self.row.encode());
        self.writer.insert_path(self.inode_id, &self.path);
        self.writer.txn.commit();
        let changes = self.writer.changes;
        for path in changes.files.publish(&self.identity, self.inode_id) {
            self.writer.insert_path(self.inode_id, &path);
        }
    }

    /// Hashing failed: remove the rows that `begin` and `blocks` wrote, and
    /// publish that the file has no rows, which drops the paths of the
    /// other links and makes their roots unsuccessful. A completed group
    /// with a path of the inode loses its marker before that path goes.
    /// The removal reads committed entries, so the transaction commits
    /// first.
    pub fn abandon(mut self) {
        self.writer.txn.commit();
        let changes = self.writer.changes;
        changes.invalidate_groups(&mut self.writer.txn, self.inode_id);
        self.writer.txn.delete_inode(self.inode_id, self.row);
        self.writer.txn.checkpoint();
        changes.drop_file(Dropped::Failed, &self.identity, &self.path);
    }
}

/// The transaction of the calling thread, locked for the changes of one
/// task.
struct Writer<'c, 'a> {
    changes: &'c Changes<'a>,
    txn: MutexGuard<'c, Transaction<'a>>,
}

impl Writer<'_, '_> {
    /// Add a path of an inode whose row has committed, or commits with
    /// this transaction.
    fn insert_path(&mut self, inode_id: u64, path: &RelPath) {
        self.txn.put(
            Cf::Paths,
            &layout::path_key(path.root_id, &path.rel_path),
            &id_key(inode_id),
        );
        self.txn.put(
            Cf::InodePaths,
            &layout::inode_path_key(inode_id, path.root_id, &path.rel_path),
            &[],
        );
        self.changes.paths.fetch_add(1, Relaxed);
        self.txn.checkpoint();
    }
}

/// The inode row that an identity had when its owner looked it up.
struct Existing {
    inode_id: u64,
    row: InodeRow,
}

impl Existing {
    /// The row is complete and stores the observed size and ctime, so the
    /// file's content can be reused without hashing (DESIGN.md §4.2). An
    /// incomplete row has no size and is never reusable.
    fn reusable(&self, size: u64, ctime_ns: i64) -> bool {
        self.row.size == Some(size) && self.row.ctime_ns == ctime_ns
    }
}

/// The file visited set (DESIGN.md §4.5 and §4.6): the state of each
/// claimed regular file, by identity.
#[derive(Default)]
struct Files(Mutex<HashMap<Identity, FileState>>);

/// The state of a claimed file (DESIGN.md §4.6).
enum FileState {
    /// The owner has no result yet: the paths of other links wait here.
    Pending(Vec<RelPath>),
    /// The owner's inode row has committed: other links add their paths
    /// to it.
    Indexed(u64),
    /// The file has no rows, for the reason given: other links add
    /// nothing.
    Dropped(Dropped),
}

/// Why a claimed file has no rows, which decides whether the roots of its
/// links are unsuccessful (`Changes::drop_link`).
#[derive(Clone, Copy, PartialEq)]
enum Dropped {
    /// The file is too small to index.
    Small,
    /// Its permissions deny opening it for reading.
    Denied,
    /// Its processing failed.
    Failed,
}

/// What the claim of a file that can be indexed finds.
enum Found {
    /// The first claim: the task owns the file, and adds this path.
    Owner(RelPath),
    /// Another link of a file whose inode row has committed: the task adds
    /// this path to the row.
    Link(u64, RelPath),
    /// Another link of a file that has no rows, for the reason given: this
    /// path goes nowhere, and its root is unsuccessful as
    /// `Changes::drop_link` says.
    Dropped(Dropped, RelPath),
    /// Another link of a file whose owner has no result yet: the path waits
    /// for that result.
    Deferred,
}

impl Files {
    /// Claim a file too small to index; true for the first claim. Nothing
    /// waits for such a file, so its entry is dropped at once.
    fn claim_small(&self, identity: &Identity) -> bool {
        let mut files = self.0.lock().unwrap();
        if files.contains_key(identity) {
            return false;
        }
        files.insert(identity.clone(), FileState::Dropped(Dropped::Small));
        true
    }

    /// Claim a file that can be indexed, with the path of this link.
    fn claim(&self, identity: &Identity, path: RelPath) -> Found {
        let mut files = self.0.lock().unwrap();
        match files.get_mut(identity) {
            None => {
                files.insert(identity.clone(), FileState::Pending(Vec::new()));
                Found::Owner(path)
            }
            Some(FileState::Pending(paths)) => {
                paths.push(path);
                Found::Deferred
            }
            Some(FileState::Indexed(inode_id)) => Found::Link(*inode_id, path),
            Some(FileState::Dropped(why)) => Found::Dropped(*why, path),
        }
    }

    /// The owner's inode row has committed: later links add their paths to
    /// it. Returns the paths that waited, for the owner to add.
    fn publish(&self, identity: &Identity, inode_id: u64) -> Vec<RelPath> {
        match self.settle(identity, FileState::Indexed(inode_id)) {
            FileState::Pending(paths) => paths,
            _ => panic!("the result of a file was published twice"),
        }
    }

    /// The owner's file gets no rows, for the reason `why`, which later
    /// links find. Returns the paths that waited, which go nowhere either.
    fn discard(&self, identity: &Identity, why: Dropped) -> Vec<RelPath> {
        match self.settle(identity, FileState::Dropped(why)) {
            FileState::Pending(paths) => paths,
            _ => panic!("the result of a file was published twice"),
        }
    }

    /// Replace the state of a claimed file; returns the old state.
    fn settle(&self, identity: &Identity, state: FileState) -> FileState {
        let mut files = self.0.lock().unwrap();
        let entry = files
            .get_mut(identity)
            .expect("the owner of a file has claimed it");
        std::mem::replace(entry, state)
    }

    /// The number of claimed files whose owners have no result yet.
    fn pending(&self) -> usize {
        let files = self.0.lock().unwrap();
        files
            .values()
            .filter(|state| matches!(state, FileState::Pending(_)))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use rustix::fs::{Mode, OFlags};
    use tempfile::TempDir;

    use super::*;
    use crate::fsid::{FsIndex, Triple};
    use crate::hash::PREFIX_LEN;
    use crate::index::{self, CurrentRoot, tests as index_tests};

    /// Block rows enough that removing them commits mid-way: each row is
    /// two entries, and a transaction holds `COMMIT_WORK` changes.
    const MANY_BLOCKS: u64 = 51_200;

    /// An index, with the scan pool and the filesystem of the files that a
    /// test claims.
    struct Fixture {
        index: Index,
        fs_cache: FsCache,
        /// The filesystem of the test's directory.
        fs: FsIndex,
        /// One thread, so that one transaction collects all changes.
        pool: ThreadPool,
        /// The roots of the scan: none, unless a test reconciles some.
        reconciliation: Reconciliation,
        /// Declared last: it is removed after the index has closed.
        _dir: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let index = index_tests::new_index(dir.path());
            let fs_cache = FsCache::new();
            let triple = Triple {
                mnt_id: 0,
                dev_major: 0,
                dev_minor: 0,
            };
            let fs = fs_cache
                .resolve(triple, || {
                    rustix::fs::open(
                        dir.path(),
                        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                        Mode::empty(),
                    )
                })
                .unwrap();
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap();
            Fixture {
                index,
                fs_cache,
                fs,
                pool,
                reconciliation: Reconciliation::default(),
                _dir: dir,
            }
        }

        fn changes(&self) -> Changes<'_> {
            Changes::new(
                &self.index,
                &self.fs_cache,
                &self.pool,
                &self.reconciliation,
            )
        }

        /// A file on the test's filesystem.
        fn identity(&self, handle: u8) -> Identity {
            Identity {
                fs: self.fs,
                handle: Box::new([handle]),
            }
        }

        /// The `identities` key of `identity(handle)`.
        fn key(&self, handle: u8) -> Vec<u8> {
            layout::identity(&self.fs_cache.info(self.fs).fsid, &[handle])
        }

        /// The relative paths that have committed, in key order.
        fn committed_paths(&self) -> Vec<Vec<u8>> {
            let mut paths = Vec::new();
            self.index.shared.each(Cf::Paths, b"", |key, _| {
                paths.push(key[layout::ID_LEN..].to_vec());
            });
            paths
        }
    }

    fn path(name: &str) -> RelPath {
        RelPath {
            root_id: 1,
            rel_path: name.as_bytes().to_vec(),
        }
    }

    fn block(block_no: u64) -> BlockRow {
        BlockRow {
            block_no,
            weak: block_no,
            strong_prefix: [block_no as u8; PREFIX_LEN],
        }
    }

    /// An interrupted orphan cleanup must not leave a complete inode with
    /// missing blocks: a later scan of the returned file would reuse the
    /// remaining rows as unchanged. The injected failure interrupts the
    /// cleanup after its first commit; the committed state must hold an
    /// incomplete inode, which reconciliation removes. The injection is
    /// for the calling thread, so the test removes the orphans of all
    /// inode IDs as one range on its own thread.
    #[test]
    fn interrupted_orphan_cleanup_leaves_the_inode_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = index_tests::new_index(dir.path());
        // One complete inode without paths: an orphan.
        let blocks: Vec<_> = (0..MANY_BLOCKS)
            .map(|block_no| (1, block_no, block_no, [0; PREFIX_LEN]))
            .collect();
        index_tests::insert(&index, &[(1, b"\x01\x02", Some(4096))], &[], &[], &blocks);

        index_tests::interrupt_after_commits(Some(1));
        let result = catch_unwind(AssertUnwindSafe(|| {
            index.remove_orphans_in(1..index.next_inode_id())
        }));
        index_tests::interrupt_after_commits(None);
        assert!(result.is_err(), "the injected failure must abort cleanup");
        drop(index);

        let index = index::open_modifying(path.as_os_str().as_bytes());
        let row = index.inode_row(1);
        assert_eq!(row.size, None, "the committed inode must be incomplete");
        let blocks = index_tests::count(&index, Cf::Blocks) as u64;
        assert!(
            blocks > 0 && blocks < MANY_BLOCKS,
            "cleanup must stop between its commits: {blocks} rows left"
        );
        assert_eq!(index_tests::count(&index, Cf::Strong) as u64, blocks);

        // The next update's reconciliation removes the incomplete remains.
        index.reconcile_roots(&[], false);
        for cf in [Cf::Inodes, Cf::Identities, Cf::Blocks, Cf::Strong] {
            assert_eq!(index_tests::count(&index, cf), 0, "{cf:?}");
        }
    }

    /// The orphan removal splits the inode IDs into ranges for the threads
    /// of the pool: an orphan at either end of a range goes with its
    /// blocks, and an inode with paths stays, also next to the end of its
    /// range. The one thread of the fixture's pool takes four ranges:
    /// inodes 1-3, 4-6, 7-9, and 10-12.
    #[test]
    fn orphans_go_at_the_ends_of_ranges() {
        let fixture = Fixture::new();
        let identities: Vec<Vec<u8>> = (1..=12u8)
            .map(|id| layout::identity(b"\x01", &[id]))
            .collect();
        let inodes: Vec<(u64, &[u8], Option<u64>)> = (1..)
            .zip(&identities)
            .map(|(id, identity)| (id, &identity[..], Some(4096)))
            .collect();
        let linked = [1, 2, 5, 6, 7, 8, 11];
        let names: Vec<String> = linked.iter().map(|id| format!("f{id}")).collect();
        let mut paths: Vec<(u64, u64, &[u8])> = linked
            .iter()
            .zip(&names)
            .map(|(&id, name)| (id, 1, name.as_bytes()))
            .collect();
        paths.push((6, 2, b"g"));
        let blocks: Vec<_> = (1..=12)
            .map(|id| (id, 0, id, [id as u8; PREFIX_LEN]))
            .collect();
        index_tests::insert(
            &fixture.index,
            &inodes,
            &[(1, b"/r"), (2, b"/s")],
            &paths,
            &blocks,
        );
        assert_eq!(fixture.index.remove_orphans(&fixture.pool), 5);
        let mut left = Vec::new();
        fixture.index.shared.each(Cf::Inodes, b"", |key, _| {
            left.push(u64_at(key, 0));
        });
        assert_eq!(left, linked);
        let mut blocks_left = Vec::new();
        fixture.index.shared.each(Cf::Blocks, b"", |key, _| {
            blocks_left.push(u64_at(key, 0));
        });
        assert_eq!(blocks_left, linked);
        for (cf, n) in [(Cf::Identities, 7), (Cf::Strong, 7), (Cf::InodePaths, 8)] {
            assert_eq!(index_tests::count(&fixture.index, cf), n, "{cf:?}");
        }
    }

    /// Links found before the owner's row commits wait for it, and links
    /// found after it add their paths themselves; a file without rows
    /// takes no paths (DESIGN.md §4.6).
    #[test]
    fn links_wait_for_the_owner() {
        let identity = |n: u8| Identity {
            fs: 0,
            handle: Box::new([n]),
        };
        let files = Files::default();
        let one = identity(1);
        assert!(matches!(files.claim(&one, path("a")), Found::Owner(p) if p.rel_path == b"a"));
        assert!(matches!(files.claim(&one, path("b")), Found::Deferred));
        assert_eq!(files.pending(), 1, "the owner has no result yet");
        let waiting: Vec<_> = files
            .publish(&one, 7)
            .into_iter()
            .map(|p| p.rel_path)
            .collect();
        assert_eq!(waiting, [b"b".to_vec()]);
        assert!(matches!(files.claim(&one, path("c")), Found::Link(7, p) if p.rel_path == b"c"));

        // An owner whose file gets no rows drops the waiting paths and the
        // later ones, which find why: its processing failed, or its
        // permissions deny opening it for reading.
        for (handle, why) in [(2, Dropped::Failed), (4, Dropped::Denied)] {
            let file = identity(handle);
            assert!(matches!(files.claim(&file, path("a")), Found::Owner(_)));
            assert!(matches!(files.claim(&file, path("b")), Found::Deferred));
            let waited: Vec<_> = files
                .discard(&file, why)
                .into_iter()
                .map(|p| p.rel_path)
                .collect();
            assert_eq!(waited, [b"b".to_vec()]);
            let later = files.claim(&file, path("c"));
            assert!(
                matches!(later, Found::Dropped(found, p) if found == why && p.rel_path == b"c")
            );
        }

        // Only the first claim of a small file owns it, and no link of it
        // adds a path, even one that finds the file large.
        let three = identity(3);
        assert!(files.claim_small(&three));
        assert!(!files.claim_small(&three));
        let large = files.claim(&three, path("d"));
        assert!(matches!(large, Found::Dropped(Dropped::Small, _)));
        assert!(!files.claim_small(&one));
        assert_eq!(files.pending(), 0, "every claimed file has a result");
    }

    /// The first claim reuses the row of its identity only when the row is
    /// complete and stores the observed size and ctime; otherwise its task
    /// owns the file, and replaces the row. An index without inode rows
    /// has no identities, so nothing is looked up in it.
    #[test]
    fn existing_rows_are_reused_only_when_unchanged() {
        let fixture = Fixture::new();
        assert!(fixture.changes().empty, "a new index has no inode rows");
        // `insert` stores the ctime 123.
        index_tests::insert(
            &fixture.index,
            &[
                (7, &fixture.key(1), Some(4096)),
                (8, &fixture.key(2), Some(4096)),
                (9, &fixture.key(3), Some(4096)),
                (10, &fixture.key(4), None),
            ],
            &[],
            &[],
            &[],
        );
        let changes = fixture.changes();
        assert!(!changes.empty);
        /// The existing row that the owner of a claim replaces.
        fn replaces(claim: Claim<'_, '_>) -> Option<u64> {
            let Claim::Owner(owner) = claim else {
                panic!("the file is not owned");
            };
            let existing = owner.existing;
            owner.fail();
            existing
        }
        fixture.pool.install(|| {
            let claim = |handle, size, ctime_ns| {
                changes.claim(fixture.identity(handle), path("f"), size, ctime_ns)
            };
            assert!(matches!(claim(1, 4096, 123), Claim::Unchanged));
            assert_eq!(replaces(claim(2, 4096, 124)), Some(8), "a new ctime");
            assert_eq!(replaces(claim(3, 4095, 123)), Some(9), "a new size");
            assert_eq!(
                replaces(claim(4, 4096, 123)),
                Some(10),
                "an incomplete row is not reusable"
            );
            assert_eq!(replaces(claim(5, 4096, 123)), None, "a new identity");
        });
        let summary = changes.finish();
        assert_eq!(summary.paths, 1);
        assert_eq!(fixture.committed_paths(), [b"f".to_vec()]);
    }

    /// `end` commits the complete row and the owner's path before it
    /// publishes the row: a link that finds the row published adds its
    /// path at once, and a path must commit only after its inode row
    /// (DESIGN.md §4.6). Links found before and after `end` get their
    /// paths.
    #[test]
    fn end_commits_the_row_before_it_publishes() {
        let fixture = Fixture::new();
        let changes = fixture.changes();
        let claim = |name| changes.claim(fixture.identity(1), path(name), 4096, 123);
        fixture.pool.install(|| {
            let Claim::Owner(owner) = claim("a") else {
                panic!("the first claim owns the file");
            };
            assert!(matches!(claim("b"), Claim::Linked));
            let mut hashing = owner.begin();
            hashing.blocks(&[block(0)]);
            hashing.end();
        });
        // Before `finish` commits the transactions.
        let inode_id = fixture
            .index
            .inode_id(&fixture.key(1))
            .expect("the identity must have committed");
        assert_eq!(fixture.index.inode_row(inode_id).size, Some(4096));
        assert_eq!(index_tests::count(&fixture.index, Cf::Blocks), 1);
        assert!(fixture.committed_paths().contains(&b"a".to_vec()));
        fixture
            .pool
            .install(|| assert!(matches!(claim("c"), Claim::Linked)));
        let summary = changes.finish();
        assert_eq!((summary.paths, summary.blocks), (3, 1));
        assert_eq!(
            fixture.committed_paths(),
            [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );

        // When the commit of `end` fails, no link can see the row yet.
        let changes = fixture.changes();
        fixture.pool.install(|| {
            let Claim::Owner(owner) = changes.claim(fixture.identity(2), path("d"), 4096, 123)
            else {
                panic!("the first claim owns the file");
            };
            let hashing = owner.begin();
            index_tests::interrupt_after_commits(Some(0));
            let result = catch_unwind(AssertUnwindSafe(|| hashing.end()));
            index_tests::interrupt_after_commits(None);
            assert!(result.is_err(), "the injected failure must stop `end`");
        });
        assert_eq!(
            changes.files.pending(),
            1,
            "`end` must publish the row only after its commit"
        );
    }

    /// An owner whose hashing fails removes the rows that it wrote, and the
    /// rows of the inode that it replaced; no link of the file adds a path
    /// (DESIGN.md §4.6).
    #[test]
    fn abandoned_files_leave_no_rows() {
        let fixture = Fixture::new();
        // The row that the owner of file 1 replaces; file 2 is new.
        index_tests::insert(
            &fixture.index,
            &[(1, &fixture.key(1), Some(4096))],
            &[],
            &[],
            &[(1, 0, 5, [5; PREFIX_LEN])],
        );
        let changes = fixture.changes();
        fixture.pool.install(|| {
            for handle in [1, 2] {
                let claim =
                    |name: &str| changes.claim(fixture.identity(handle), path(name), 8192, 124);
                let Claim::Owner(owner) = claim(&format!("{handle}a")) else {
                    panic!("the first claim owns the file");
                };
                assert!(matches!(claim(&format!("{handle}b")), Claim::Linked));
                let mut hashing = owner.begin();
                hashing.blocks(&[block(0), block(1)]);
                hashing.abandon();
                assert!(matches!(claim(&format!("{handle}c")), Claim::Linked));
            }
        });
        let summary = changes.finish();
        assert_eq!(summary.paths, 0);
        for cf in [
            Cf::Inodes,
            Cf::Identities,
            Cf::Paths,
            Cf::InodePaths,
            Cf::Blocks,
            Cf::Strong,
        ] {
            assert_eq!(index_tests::count(&fixture.index, cf), 0, "{cf:?}");
        }
    }

    /// A current root at `path`: a copy of the group `identifier`, or a
    /// mutable root for `None`.
    fn current<'a>(path: &'a [u8], identifier: Option<&'a [u8]>) -> CurrentRoot<'a> {
        CurrentRoot { path, identifier }
    }

    /// The identifier of each root row, by root ID.
    fn identifiers(index: &Index) -> Vec<(u64, Option<Vec<u8>>)> {
        let rows = index.roots().into_iter();
        rows.map(|(root_id, row)| (root_id, row.identifier))
            .collect()
    }

    /// An owner that abandons a file removes the marker of every completed
    /// group with a path of the file, with the transaction that removes the
    /// paths, so that the next update scans the group again (DESIGN.md
    /// §4.2 and §4.6). Here a mutable root finds a file of a completed
    /// group changed, and its hashing fails.
    #[test]
    fn abandoning_a_file_reopens_the_groups_that_hold_it() {
        let mut fixture = Fixture::new();
        index_tests::insert(
            &fixture.index,
            &[(1, &fixture.key(1), Some(4096))],
            &[(1, b"/g")],
            &[(1, 1, b"f"), (1, 1, b"h")],
            &[(1, 0, 5, [5; PREFIX_LEN])],
        );
        index_tests::give_identifiers(&fixture.index, &[(1, b"g")]);
        let roots = [current(b"/g", Some(b"g")), current(b"/m", None)];
        fixture.reconciliation = fixture.index.reconcile_roots(&roots, false);
        assert_eq!(index_tests::scans(&fixture.reconciliation), [(2, vec![1])]);
        let changes = fixture.changes();
        fixture.pool.install(|| {
            let path = RelPath {
                root_id: 2,
                rel_path: b"f".to_vec(),
            };
            let Claim::Owner(owner) = changes.claim(fixture.identity(1), path, 4096, 124) else {
                panic!("the first claim owns the file");
            };
            let mut hashing = owner.begin();
            hashing.blocks(&[block(0)]);
            hashing.abandon();
        });
        let summary = changes.finish();
        assert_eq!((summary.groups_reused, summary.groups_incomplete), (0, 1));
        assert_eq!(identifiers(&fixture.index), [(1, None), (2, None)]);
        for cf in [
            Cf::Inodes,
            Cf::Identities,
            Cf::Paths,
            Cf::InodePaths,
            Cf::Blocks,
            Cf::Strong,
        ] {
            assert_eq!(index_tests::count(&fixture.index, cf), 0, "{cf:?}");
        }
    }

    /// After the scan, an unfinished group gets its marker only if no entry
    /// under its root failed and every file of it got its path: a link
    /// that waited for an owner that failed, a link that came later, and a
    /// failed entry of the traversal leave their groups unfinished. The row
    /// of a group whose scan took another copy than the first names that
    /// copy, also when the group stays unfinished, and the group keeps the
    /// paths that the scan found there, which a run with `--no-update`
    /// reads (DESIGN.md §4.2 and §4.6).
    #[test]
    fn groups_complete_only_without_failures() {
        let mut fixture = Fixture::new();
        let roots = [
            current(b"/one", Some(b"one")),
            current(b"/two", Some(b"two")),
            current(b"/three", Some(b"three")),
            current(b"/four", Some(b"four")),
            current(b"/m", None),
        ];
        fixture.reconciliation = fixture.index.reconcile_roots(&roots, false);
        let changes = fixture.changes();
        let path = |root_id, name: &str| RelPath {
            root_id,
            rel_path: name.as_bytes().to_vec(),
        };
        fixture.pool.install(|| {
            // The mutable root owns a file of groups one and two, and fails.
            let claim = |root_id| changes.claim(fixture.identity(1), path(root_id, "a"), 4096, 1);
            let Claim::Owner(owner) = claim(5) else {
                panic!("the first claim owns the file");
            };
            assert!(matches!(claim(1), Claim::Linked));
            owner.fail();
            assert!(matches!(claim(2), Claim::Linked));
            // Groups three and four have files of their own, and another
            // entry of four fails.
            for (handle, root_id, name) in [(2, 3, "b"), (3, 4, "c")] {
                let Claim::Owner(owner) =
                    changes.claim(fixture.identity(handle), path(root_id, name), 4096, 1)
                else {
                    panic!("the first claim owns the file");
                };
                owner.begin().end();
            }
        });
        changes.scan_copy(4, b"/four-b");
        changes.fail_root(4);
        let summary = changes.finish();
        assert_eq!(
            (summary.groups_completed, summary.groups_incomplete),
            (1, 3)
        );
        assert_eq!(fixture.committed_paths(), [b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(
            identifiers(&fixture.index),
            [
                (1, None),
                (2, None),
                (3, Some(b"three".to_vec())),
                (4, None),
                (5, None)
            ]
        );
        let (_, four) = &fixture.index.roots()[3];
        assert_eq!(four.path, b"/four-b");
    }

    /// A file that its permissions keep from being read leaves no group
    /// unfinished through a link below the group's root: the scan of a
    /// copy skips it as it skips any entry that the copy's permissions deny.
    /// A single-file root that is a link of the file cannot be read at all,
    /// so it stays unfinished, whether its link owns the file, waits for the
    /// owner, or comes later (DESIGN.md §4.5 and §4.6).
    #[test]
    fn denied_files_leave_only_their_single_file_roots_unfinished() {
        let mut fixture = Fixture::new();
        let roots = [
            current(b"/one", Some(b"one")),
            current(b"/two", Some(b"two")),
            current(b"/three", Some(b"three")),
            current(b"/four", Some(b"four")),
            current(b"/five", Some(b"five")),
            current(b"/m", None),
        ];
        fixture.reconciliation = fixture.index.reconcile_roots(&roots, false);
        let changes = fixture.changes();
        let path = |root_id, name: &str| RelPath {
            root_id,
            rel_path: name.as_bytes().to_vec(),
        };
        let owner = |handle, path| match changes.claim(fixture.identity(handle), path, 4096, 1) {
            Claim::Owner(owner) => owner,
            _ => panic!("the first claim owns the file"),
        };
        fixture.pool.install(|| {
            // The mutable root owns a file that groups one to four link:
            // one and three below their roots, two and four as single-file
            // roots. One and two wait for the owner; three and four come
            // after it.
            let claim =
                |root_id, name| changes.claim(fixture.identity(1), path(root_id, name), 4096, 1);
            let denied = owner(1, path(6, "f"));
            assert!(matches!(claim(1, "f"), Claim::Linked));
            assert!(matches!(claim(2, ""), Claim::Linked));
            denied.deny();
            assert!(matches!(claim(3, "f"), Claim::Linked));
            assert!(matches!(claim(4, ""), Claim::Linked));
            // Group five, a single-file root, owns its file.
            owner(2, path(5, "")).deny();
        });
        let summary = changes.finish();
        assert_eq!(summary.paths, 0);
        assert_eq!(
            (summary.groups_completed, summary.groups_incomplete),
            (2, 3)
        );
        assert_eq!(
            identifiers(&fixture.index),
            [
                (1, Some(b"one".to_vec())),
                (2, None),
                (3, Some(b"three".to_vec())),
                (4, None),
                (5, None),
                (6, None)
            ]
        );
    }

    /// An owner that ends without a result is a bug, which `finish`
    /// reports. `#[must_use]` does not catch an owner that an early return
    /// drops.
    #[test]
    #[should_panic(expected = "1 claimed files have no result")]
    fn finish_requires_a_result_for_every_claimed_file() {
        let fixture = Fixture::new();
        let changes = fixture.changes();
        fixture.pool.install(|| {
            drop(changes.claim(fixture.identity(1), path("a"), 4096, 123));
        });
        changes.finish();
    }

    /// A rayon call while a file is hashed can run another task on the
    /// thread of the hashing task. A task that writes changes there finds
    /// the transaction of the thread locked, which is a bug: `writer`
    /// reports it instead of waiting forever. With one thread, `join` runs
    /// its second closure on the thread of the hashing task.
    #[test]
    #[should_panic(expected = "one task of a thread holds its transaction at a time")]
    fn writing_inside_a_hashing_task_is_a_bug() {
        let fixture = Fixture::new();
        let changes = fixture.changes();
        let owner =
            |handle, name| match changes.claim(fixture.identity(handle), path(name), 4096, 123) {
                Claim::Owner(owner) => owner,
                _ => panic!("the first claim owns the file"),
            };
        fixture.pool.install(|| {
            let hashing = owner(1, "a").begin();
            rayon::join(|| {}, || owner(2, "b").begin().end());
            hashing.end();
        });
    }
}

//! The writer thread owns all index mutations during an update
//! (DESIGN.md §4.6).
//!
//! Messages are keyed by a scan-local `FileId` that the file visited set
//! assigns in claim order. Each claimed file sends exactly one terminal
//! message: `End`, `Unchanged`, `Omit`, or `Abandon`. A `Path` for a file
//! whose result is not yet known waits for it. After the last message,
//! the writer removes the inodes that no path references any more.
//!
//! The writer reads committed entries only. Only the messages of one file
//! change its inode, so a read of an inode that no earlier message of the
//! update changed needs no commit; `Abandon` commits before it removes
//! the rows that `Begin` and `Blocks` wrote.

use std::mem;

use crossbeam_channel::Receiver;

use super::layout::{self, Cf, InodeRow, id_key, u64_at};
use super::{Index, Transaction};
use crate::blocks::BlockRow;
use crate::fsid::{FsCache, Identity};

/// Scan-local file number, dense from zero.
pub type FileId = u32;

/// A path of a file under a root.
pub struct RelPath {
    pub root_id: u64,
    pub rel_path: Vec<u8>,
}

pub enum Msg {
    /// Mark the inode incomplete, storing the observed ctime, then remove
    /// its old blocks in bounded transactions.
    Begin {
        file: FileId,
        identity: Identity,
        ctime_ns: i64,
    },
    /// A bounded batch of block rows.
    Blocks { file: FileId, rows: Vec<BlockRow> },
    /// Hashing succeeded: store the size and the owner's path. The inode
    /// becomes complete even when no block rows were inserted
    /// (DESIGN.md §4.4).
    End {
        file: FileId,
        size: u64,
        path: RelPath,
    },
    /// The stored content is current (DESIGN.md §4.2): record the owner's
    /// path against the reused complete inode row. The row survives until
    /// this message is applied: reconciliation removed incomplete rows
    /// before the scan, only the owning task replaces an inode, and
    /// orphan removal starts after the last message.
    Unchanged {
        file: FileId,
        inode_id: u64,
        path: RelPath,
    },
    /// The file is too small to index; remove its rows.
    Omit { file: FileId, identity: Identity },
    /// An additional hard link.
    Path { file: FileId, path: RelPath },
    /// Hashing failed; remove partial data and discard pending paths.
    Abandon { file: FileId },
}

#[derive(Default, Debug)]
pub struct Summary {
    pub paths: u64,
    pub blocks: u64,
    /// Inodes that ended the update without a path and were removed.
    pub orphans: u64,
}

/// What the writer knows about one claimed file.
enum FileState {
    /// Neither `Begin` nor a terminal message yet: paths wait.
    Pending(Vec<RelPath>),
    /// `Begin` arrived: block rows are being appended to this incomplete
    /// inode row.
    Hashing {
        inode_id: u64,
        row: InodeRow,
        paths: Vec<RelPath>,
    },
    /// Indexed: paths link to this inode row.
    Indexed(u64),
    /// Omitted or abandoned: paths are dropped.
    Dropped,
}

struct Writer<'a> {
    index: &'a Index,
    txn: Transaction<'a>,
    fs_cache: &'a FsCache,
    /// The state of every file, by `FileId`.
    files: Vec<FileState>,
    next_inode_id: u64,
    summary: Summary,
}

/// Apply messages until every sender is gone, then remove the orphaned
/// inodes.
pub fn run(index: &Index, fs_cache: &FsCache, rx: Receiver<Msg>) -> Summary {
    let mut writer = Writer {
        index,
        txn: index.transaction(),
        fs_cache,
        files: Vec::new(),
        next_inode_id: index.next_inode_id(),
        summary: Summary::default(),
    };
    while let Ok(msg) = rx.recv() {
        writer.handle(msg);
    }
    writer.txn.commit();
    assert!(
        writer
            .files
            .iter()
            .all(|state| matches!(state, FileState::Indexed(_) | FileState::Dropped)),
        "files without a terminal message"
    );
    writer.remove_orphans();
    writer.summary
}

impl Writer<'_> {
    fn file(&mut self, file: FileId) -> &mut FileState {
        let index = file as usize;
        if index >= self.files.len() {
            self.files
                .resize_with(index + 1, || FileState::Pending(Vec::new()));
        }
        &mut self.files[index]
    }

    /// The `identities` key of a scan identity.
    fn identity(&self, identity: &Identity) -> Vec<u8> {
        layout::identity(&self.fs_cache.info(identity.fs).fsid, &identity.handle)
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Begin {
                file,
                identity,
                ctime_ns,
            } => {
                let paths = match self.file(file) {
                    FileState::Pending(paths) => mem::take(paths),
                    _ => panic!("Begin after Begin or after the terminal message"),
                };
                let row = InodeRow {
                    size: None,
                    ctime_ns,
                    identity: self.identity(&identity),
                };
                let inode_id = self.begin_inode(&row);
                *self.file(file) = FileState::Hashing {
                    inode_id,
                    row,
                    paths,
                };
            }
            Msg::Blocks { file, rows } => {
                let inode_id = match self.file(file) {
                    FileState::Hashing { inode_id, .. } => *inode_id,
                    _ => panic!("Blocks without Begin"),
                };
                for row in &rows {
                    self.txn.put(
                        Cf::Blocks,
                        &layout::block_key(inode_id, row.block_no),
                        &layout::block_value(row.weak, &row.strong_prefix),
                    );
                    self.txn.put(
                        Cf::Strong,
                        &layout::strong_key(&row.strong_prefix, inode_id, row.block_no),
                        &[],
                    );
                }
                self.summary.blocks += rows.len() as u64;
            }
            Msg::End { file, size, path } => {
                let FileState::Hashing {
                    inode_id,
                    mut row,
                    paths,
                } = mem::replace(self.file(file), FileState::Dropped)
                else {
                    panic!("End without Begin");
                };
                // The ctime is already there from Begin's incomplete mark.
                // Also with no block rows, the complete inode lets the
                // next update reuse the unchanged file without hashing
                // it again (DESIGN.md §4.4).
                row.size = Some(size);
                self.txn.put(Cf::Inodes, &id_key(inode_id), &row.encode());
                for path in std::iter::once(&path).chain(&paths) {
                    self.insert_path(inode_id, path);
                }
                *self.file(file) = FileState::Indexed(inode_id);
            }
            Msg::Unchanged {
                file,
                inode_id,
                path,
            } => {
                let paths = match self.file(file) {
                    FileState::Pending(paths) => mem::take(paths),
                    _ => panic!("Unchanged after Begin or after the terminal message"),
                };
                for path in std::iter::once(&path).chain(&paths) {
                    self.insert_path(inode_id, path);
                }
                *self.file(file) = FileState::Indexed(inode_id);
            }
            Msg::Omit { file, identity } => {
                assert!(
                    matches!(self.file(file), FileState::Pending(_)),
                    "Omit after Begin or after the terminal message"
                );
                let identity = self.identity(&identity);
                if let Some(inode_id) = self.index.inode_id(&identity) {
                    let row = self.index.inode_row(inode_id);
                    self.delete_inode(inode_id, row);
                }
                *self.file(file) = FileState::Dropped;
            }
            Msg::Path { file, path } => match self.file(file) {
                FileState::Pending(paths) | FileState::Hashing { paths, .. } => paths.push(path),
                FileState::Indexed(inode_id) => {
                    let inode_id = *inode_id;
                    self.insert_path(inode_id, &path);
                }
                FileState::Dropped => {}
            },
            Msg::Abandon { file } => match mem::replace(self.file(file), FileState::Dropped) {
                FileState::Hashing { inode_id, row, .. } => {
                    // The removal reads committed rows, and the pending
                    // changes can hold this inode's new blocks.
                    self.txn.commit();
                    self.delete_inode(inode_id, row);
                }
                FileState::Pending(_) => {}
                _ => panic!("Abandon after the terminal message"),
            },
        }
        self.txn.checkpoint();
    }

    /// Start replacing the blocks of a file: mark its inode incomplete,
    /// storing the observed ctime, then remove the old blocks in bounded
    /// transactions (DESIGN.md §4.4). A new identity gets a new inode row.
    /// Returns the inode ID.
    fn begin_inode(&mut self, row: &InodeRow) -> u64 {
        match self.index.inode_id(&row.identity) {
            Some(inode_id) => {
                // The mark may commit before any block is removed; recovery
                // then removes the blocks of the incomplete inode.
                self.txn.put(Cf::Inodes, &id_key(inode_id), &row.encode());
                self.txn.remove_blocks(inode_id);
                inode_id
            }
            None => {
                let inode_id = self.next_inode_id;
                self.next_inode_id += 1;
                self.txn
                    .put(Cf::Identities, &row.identity, &id_key(inode_id));
                self.txn.put(Cf::Inodes, &id_key(inode_id), &row.encode());
                inode_id
            }
        }
    }

    /// Remove inodes without paths, and their blocks, in bounded
    /// transactions, once all results are known (DESIGN.md §4.6). This
    /// also removes orphans left by earlier interrupted cleanup.
    fn remove_orphans(&mut self) {
        let index = self.index;
        // Both scans read the snapshot of their start, which the removals
        // do not change.
        let mut inodes = index.shared.scan(Cf::Inodes, b"");
        let mut paths = index.shared.scan(Cf::InodePaths, b"");
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
                self.delete_inode(inode_id, InodeRow::decode(value));
                self.summary.orphans += 1;
                self.txn.checkpoint();
            }
            inodes.next();
        }
        self.txn.commit();
    }

    /// Remove an inode with its paths and blocks. The inode is marked
    /// incomplete first, in the same transaction as the removal of its
    /// paths and first blocks: block removal can commit between batches,
    /// and an interrupted removal must not leave a complete inode with
    /// missing blocks — a later scan would reuse it as unchanged. Recovery
    /// removes incomplete inodes instead (DESIGN.md §4.2). The removal
    /// reads committed entries.
    fn delete_inode(&mut self, inode_id: u64, mut row: InodeRow) {
        row.size = None;
        self.txn.put(Cf::Inodes, &id_key(inode_id), &row.encode());
        self.txn.remove_inode_paths(inode_id);
        self.txn.remove_blocks(inode_id);
        self.txn.delete(Cf::Inodes, &id_key(inode_id));
        self.txn.delete(Cf::Identities, &row.identity);
    }

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
        self.summary.paths += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use crossbeam_channel::bounded;

    use super::*;
    use crate::hash::PREFIX_LEN;
    use crate::index::{self, tests as index_tests};

    /// Block rows enough that removing them commits mid-way: each row is
    /// two entries, and a transaction holds `COMMIT_WORK` changes.
    const MANY_BLOCKS: u64 = 51_200;

    /// An interrupted orphan cleanup must not leave a complete inode with
    /// missing blocks: a later scan of the returned file would reuse the
    /// remaining rows as unchanged. The injected failure interrupts the
    /// cleanup after its first commit; the committed state must hold an
    /// incomplete inode, which reconciliation removes.
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

        let fs_cache = FsCache::new();
        let (tx, rx) = bounded(1);
        drop(tx);
        index_tests::interrupt_after_commits(Some(1));
        let result = catch_unwind(AssertUnwindSafe(|| run(&index, &fs_cache, rx)));
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
        index.reconcile_roots(&[]);
        for cf in [Cf::Inodes, Cf::Identities, Cf::Blocks, Cf::Strong] {
            assert_eq!(index_tests::count(&index, cf), 0, "{cf:?}");
        }
    }
}

//! The writer thread owns all index mutations during an update
//! (DESIGN.md §4.6).
//!
//! Messages are keyed by a scan-local `FileId` that the file visited set
//! assigns in claim order. Each claimed file sends exactly one terminal
//! message: `End`, `Unchanged`, `Omit`, or `Abandon`. A `Path` for a file
//! whose result is not yet known waits for it. After the last message,
//! the writer removes the inodes that no path references any more.

use std::collections::HashMap;
use std::mem;

use crossbeam_channel::Receiver;
use rusqlite::{Connection, OptionalExtension, params};

use super::{Index, db};
use crate::blocks::{BATCH_ROWS, BlockRow};
use crate::fsid::{FsCache, FsIndex, Identity};

/// Scan-local file number, dense from zero.
pub type FileId = u32;

/// A path of a file under a root.
pub struct RelPath {
    pub root_id: i64,
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
        inode_id: i64,
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

/// Work units (messages plus rows) per transaction.
const COMMIT_WORK: u64 = 50_000;

/// What the writer knows about one claimed file.
enum FileState {
    /// Neither `Begin` nor a terminal message yet: paths wait.
    Pending(Vec<RelPath>),
    /// `Begin` arrived: block rows are being appended to this inode.
    Hashing { inode_id: i64, paths: Vec<RelPath> },
    /// Indexed: paths link to this inode row.
    Indexed(i64),
    /// Omitted or abandoned: paths are dropped.
    Dropped,
}

struct Writer<'a> {
    conn: &'a Connection,
    fs_cache: &'a FsCache,
    /// The state of every file, by `FileId`.
    files: Vec<FileState>,
    fs_ids: HashMap<FsIndex, i64>,
    in_transaction: bool,
    work: u64,
    summary: Summary,
}

/// Apply messages until every sender is gone, then hand the index back.
pub fn run(index: Index, fs_cache: &FsCache, rx: Receiver<Msg>) -> (Index, Summary) {
    let summary = write_all(&index.conn, fs_cache, rx);
    (index, summary)
}

fn write_all(conn: &Connection, fs_cache: &FsCache, rx: Receiver<Msg>) -> Summary {
    let mut writer = Writer {
        conn,
        fs_cache,
        files: Vec::new(),
        fs_ids: HashMap::new(),
        in_transaction: false,
        work: 0,
        summary: Summary::default(),
    };
    while let Ok(msg) = rx.recv() {
        writer.handle(msg);
    }
    writer.commit();
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
    fn begin(&mut self) {
        if !self.in_transaction {
            db(self.conn.execute_batch("BEGIN IMMEDIATE"));
            self.in_transaction = true;
        }
    }

    fn commit(&mut self) {
        if self.in_transaction {
            db(self.conn.execute_batch("COMMIT"));
            self.in_transaction = false;
        }
        self.work = 0;
    }

    /// Commit once the transaction holds enough work.
    fn checkpoint(&mut self) {
        if self.work >= COMMIT_WORK {
            self.commit();
        }
    }

    fn file(&mut self, file: FileId) -> &mut FileState {
        let index = file as usize;
        if index >= self.files.len() {
            self.files
                .resize_with(index + 1, || FileState::Pending(Vec::new()));
        }
        &mut self.files[index]
    }

    fn handle(&mut self, msg: Msg) {
        self.begin();
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
                let inode_id = self.begin_inode(&identity, ctime_ns);
                *self.file(file) = FileState::Hashing { inode_id, paths };
            }
            Msg::Blocks { file, rows } => {
                let inode_id = match self.file(file) {
                    FileState::Hashing { inode_id, .. } => *inode_id,
                    _ => panic!("Blocks without Begin"),
                };
                let mut insert = db(self.conn.prepare_cached(
                    "INSERT INTO blocks (inode_id, block_no, weak, strong) VALUES (?, ?, ?, ?)",
                ));
                for row in &rows {
                    db(insert.execute(params![
                        inode_id,
                        row.block_no as i64,
                        row.weak as i64,
                        &row.strong_prefix[..]
                    ]));
                }
                self.summary.blocks += rows.len() as u64;
                self.work += rows.len() as u64;
            }
            Msg::End { file, size, path } => {
                let (inode_id, paths) = match self.file(file) {
                    FileState::Hashing { inode_id, paths } => (*inode_id, mem::take(paths)),
                    _ => panic!("End without Begin"),
                };
                // The ctime is already there from Begin's incomplete mark.
                // Also with no block rows, the complete inode lets the
                // next update reuse the unchanged file without hashing
                // it again (DESIGN.md §4.4).
                db(self.conn.execute(
                    "UPDATE inodes SET size = ? WHERE id = ?",
                    params![size as i64, inode_id],
                ));
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
                let fs_id = self.fs_id(identity.fs);
                if let Some(id) = self.select_inode(fs_id, &identity.handle) {
                    self.delete_inode(id);
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
                FileState::Hashing { inode_id, .. } => self.delete_inode(inode_id),
                FileState::Pending(_) => {}
                _ => panic!("Abandon after the terminal message"),
            },
        }
        self.work += 1;
        self.checkpoint();
    }

    /// The `filesystems` row of a scan-local filesystem number.
    fn fs_id(&mut self, fs: FsIndex) -> i64 {
        if let Some(&id) = self.fs_ids.get(&fs) {
            return id;
        }
        let fsid = &self.fs_cache.info(fs).fsid;
        db(self.conn.execute(
            "INSERT OR IGNORE INTO filesystems (fsid) VALUES (?)",
            params![fsid],
        ));
        let id: i64 = db(self.conn.query_row(
            "SELECT id FROM filesystems WHERE fsid = ?",
            params![fsid],
            |row| row.get(0),
        ));
        self.fs_ids.insert(fs, id);
        id
    }

    fn select_inode(&self, fs_id: i64, handle: &[u8]) -> Option<i64> {
        db(self
            .conn
            .query_row(
                "SELECT id FROM inodes WHERE fs_id = ? AND handle = ?",
                params![fs_id, handle],
                |row| row.get(0),
            )
            .optional())
    }

    /// Start replacing the blocks of a file: mark its inode incomplete,
    /// storing the observed ctime, then remove the old blocks in bounded
    /// transactions (DESIGN.md §4.4). Returns the inode row.
    fn begin_inode(&mut self, identity: &Identity, ctime_ns: i64) -> i64 {
        let fs_id = self.fs_id(identity.fs);
        match self.select_inode(fs_id, &identity.handle) {
            Some(id) => {
                // The mark may commit before any block is removed; recovery
                // then removes the blocks of the incomplete inode.
                db(self.conn.execute(
                    "UPDATE inodes SET size = NULL, ctime_ns = ? WHERE id = ?",
                    params![ctime_ns, id],
                ));
                self.remove_blocks(id);
                id
            }
            None => {
                db(self.conn.execute(
                    "INSERT INTO inodes (fs_id, handle, size, ctime_ns) VALUES (?, ?, NULL, ?)",
                    params![fs_id, &*identity.handle, ctime_ns],
                ));
                self.conn.last_insert_rowid()
            }
        }
    }

    /// Remove the blocks of an inode, `BATCH_ROWS` at a time, committing
    /// between batches when the work limit requires it.
    fn remove_blocks(&mut self, inode_id: i64) {
        loop {
            self.begin();
            let removed = db(self.conn.execute(
                "DELETE FROM blocks WHERE inode_id = ?1 AND block_no IN
                 (SELECT block_no FROM blocks WHERE inode_id = ?1 ORDER BY block_no LIMIT ?2)",
                params![inode_id, BATCH_ROWS as i64],
            ));
            self.work += removed as u64;
            if removed < BATCH_ROWS {
                return;
            }
            self.checkpoint();
        }
    }

    /// Remove inodes without paths, and their blocks, in bounded
    /// transactions, once all results are known (DESIGN.md §4.6). This
    /// also removes orphans left by earlier interrupted cleanup.
    fn remove_orphans(&mut self) {
        let mut after = 0i64;
        loop {
            let ids: Vec<i64> = {
                let mut select = db(self.conn.prepare_cached(
                    "SELECT id FROM inodes
                     WHERE id > ? AND NOT EXISTS
                       (SELECT 1 FROM paths WHERE paths.inode_id = inodes.id)
                     ORDER BY id LIMIT 1000",
                ));
                let rows = db(select.query_map(params![after], |row| row.get(0)));
                rows.map(db).collect()
            };
            let Some(&last) = ids.last() else {
                break;
            };
            for id in ids {
                self.begin();
                self.delete_inode(id);
                self.summary.orphans += 1;
                self.work += 1;
                self.checkpoint();
            }
            after = last;
        }
        self.commit();
    }

    /// Remove an inode with its paths and blocks. The inode is marked
    /// incomplete first, in the same transaction as the first removed
    /// blocks: block removal can commit between batches, and an
    /// interrupted removal must not leave a complete inode with missing
    /// blocks — a later scan would reuse it as unchanged. Recovery
    /// removes incomplete inodes instead (DESIGN.md §4.2).
    fn delete_inode(&mut self, id: i64) {
        db(self
            .conn
            .execute("DELETE FROM paths WHERE inode_id = ?", params![id]));
        db(self
            .conn
            .execute("UPDATE inodes SET size = NULL WHERE id = ?", params![id]));
        self.remove_blocks(id);
        db(self
            .conn
            .execute("DELETE FROM inodes WHERE id = ?", params![id]));
    }

    fn insert_path(&mut self, inode_id: i64, path: &RelPath) {
        let mut insert = db(self
            .conn
            .prepare_cached("INSERT INTO paths (root_id, rel_path, inode_id) VALUES (?, ?, ?)"));
        db(insert.execute(params![path.root_id, &path.rel_path, inode_id]));
        self.summary.paths += 1;
        self.work += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use crossbeam_channel::bounded;

    use super::*;
    use crate::index::{self, Settings};

    /// Rows enough that removing them commits mid-way (`COMMIT_WORK`)
    /// while a full deletion batch (`BATCH_ROWS`) remains.
    const MANY_BLOCKS: usize = 51_200;

    /// An interrupted orphan cleanup must not leave a complete inode with
    /// missing blocks: a later scan of the returned file would reuse the
    /// remaining rows as unchanged. The injected failure aborts cleanup
    /// inside the transaction after its first commit; the committed state
    /// must hold an incomplete inode, which reconciliation removes.
    #[test]
    fn interrupted_orphan_cleanup_leaves_the_inode_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.sqlite");
        let path_bytes = path.as_os_str().as_bytes().to_vec();

        let index = index::open_modifying(&path_bytes);
        index.reset(&Settings {
            block_size: 4096,
            weak_hash_id: "test".into(),
            strong_hash_id: "test".into(),
        });
        // One complete inode without paths: an orphan. Deletion removes
        // blocks in ascending order; the last row fails it.
        db(index.conn.execute_batch(&format!(
            "INSERT INTO filesystems (id, fsid) VALUES (1, x'01');
             INSERT INTO inodes (id, fs_id, handle, size, ctime_ns)
             VALUES (1, 1, x'02', 4096, 123);
             WITH RECURSIVE c(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM c WHERE n + 1 < {MANY_BLOCKS})
             INSERT INTO blocks (inode_id, block_no, weak, strong)
             SELECT 1, n, n, zeroblob(16) FROM c;
             CREATE TRIGGER interrupt BEFORE DELETE ON blocks
             WHEN old.block_no = {}
             BEGIN SELECT RAISE(ABORT, 'injected interruption'); END;",
            MANY_BLOCKS - 1
        )));

        let fs_cache = FsCache::new();
        let (tx, rx) = bounded(1);
        drop(tx);
        let result = catch_unwind(AssertUnwindSafe(|| run(index, &fs_cache, rx)));
        assert!(result.is_err(), "the injected failure must abort cleanup");

        let conn = Connection::open(&path).unwrap();
        let (size, blocks): (Option<i64>, i64) = conn
            .query_row(
                "SELECT (SELECT size FROM inodes WHERE id = 1),
                        (SELECT count(*) FROM blocks)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(size, None, "the committed inode must be incomplete");
        assert!(
            blocks > 0 && blocks < MANY_BLOCKS as i64,
            "cleanup must stop between its commits: {blocks} rows left"
        );
        conn.execute_batch("DROP TRIGGER interrupt").unwrap();
        drop(conn);

        // The next update's reconciliation removes the incomplete remains.
        let index = index::open_modifying(&path_bytes);
        index.reconcile_roots(&[]);
        let rows: i64 = db(index.conn.query_row(
            "SELECT (SELECT count(*) FROM inodes) + (SELECT count(*) FROM blocks)",
            [],
            |row| row.get(0),
        ));
        assert_eq!(rows, 0);
    }
}

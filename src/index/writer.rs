//! The writer thread owns all index mutations during an update
//! (DESIGN.md §4.6).
//!
//! Messages are keyed by a scan-local `FileId` that the file visited set
//! assigns in claim order. Each claimed file sends exactly one terminal
//! message: `End`, `Omit`, or `Abandon`. A `Path` for a file whose result
//! is not yet known waits for it.

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
    /// Mark the inode incomplete, then remove its old blocks in bounded
    /// transactions.
    Begin {
        file: FileId,
        identity: Identity,
        ctime_ns: i64,
    },
    /// A bounded batch of block rows.
    Blocks { file: FileId, rows: Vec<BlockRow> },
    /// Hashing succeeded: store metadata and the owner's path, or remove
    /// an inode with no useful blocks.
    End {
        file: FileId,
        size: u64,
        ctime_ns: i64,
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
}

/// Work units (messages plus rows) per transaction.
const COMMIT_WORK: u64 = 50_000;

/// What the writer knows about one claimed file.
enum FileState {
    /// Neither `Begin` nor a terminal message yet: paths wait.
    Pending(Vec<RelPath>),
    /// `Begin` arrived: block rows are being appended to this inode.
    Hashing {
        inode_id: i64,
        blocks: u64,
        paths: Vec<RelPath>,
    },
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
                *self.file(file) = FileState::Hashing {
                    inode_id,
                    blocks: 0,
                    paths,
                };
            }
            Msg::Blocks { file, rows } => {
                let inode_id = match self.file(file) {
                    FileState::Hashing {
                        inode_id, blocks, ..
                    } => {
                        *blocks += rows.len() as u64;
                        *inode_id
                    }
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
            Msg::End {
                file,
                size,
                ctime_ns,
                path,
            } => {
                let (inode_id, blocks, paths) =
                    match mem::replace(self.file(file), FileState::Dropped) {
                        FileState::Hashing {
                            inode_id,
                            blocks,
                            paths,
                        } => (inode_id, blocks, paths),
                        _ => panic!("End without Begin"),
                    };
                if blocks == 0 {
                    // No useful blocks: nothing persists (DESIGN.md §4.4).
                    db(self
                        .conn
                        .execute("DELETE FROM inodes WHERE id = ?", params![inode_id]));
                } else {
                    db(self.conn.execute(
                        "UPDATE inodes SET size = ?, ctime_ns = ? WHERE id = ?",
                        params![size as i64, ctime_ns, inode_id],
                    ));
                    for path in std::iter::once(&path).chain(&paths) {
                        self.insert_path(inode_id, path);
                    }
                    *self.file(file) = FileState::Indexed(inode_id);
                }
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
    /// then remove the old blocks in bounded transactions (DESIGN.md §4.4).
    /// Returns the inode row.
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

    /// Remove an inode with its paths and blocks.
    fn delete_inode(&mut self, id: i64) {
        db(self
            .conn
            .execute("DELETE FROM paths WHERE inode_id = ?", params![id]));
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

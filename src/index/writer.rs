//! The writer thread owns all index mutations during an update
//! (DESIGN.md §4.6).
//!
//! Messages are keyed by a scan-local `FileId` that the file visited set
//! assigns in claim order. Each claimed file sends exactly one terminal
//! message: `End`, `Omit`, or `Abandon`. A `Path` for a file whose result
//! is not yet known waits for it.

use std::collections::HashMap;

use crossbeam_channel::Receiver;
use rusqlite::{Connection, OptionalExtension, params};

use super::{Index, db};
use crate::blocks::BlockRow;
use crate::fsid::{FsCache, FsIndex, Identity};

/// Scan-local file number, dense from zero.
pub type FileId = u32;

/// A path of a file under a root.
pub struct RelPath {
    pub root_id: i64,
    pub rel_path: Vec<u8>,
}

pub enum Msg {
    /// Mark the inode incomplete and remove its old blocks.
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

const RESULT_UNKNOWN: i64 = 0;
const RESULT_OMITTED: i64 = -1;
const RESULT_FAILED: i64 = -2;

struct Inflight {
    inode_id: i64,
    blocks: u64,
}

struct Writer<'a> {
    conn: &'a Connection,
    fs_cache: &'a FsCache,
    /// Per file: the inode ID, or one of the `RESULT_*` values.
    results: Vec<i64>,
    inflight: HashMap<FileId, Inflight>,
    pending: HashMap<FileId, Vec<RelPath>>,
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
        results: Vec::new(),
        inflight: HashMap::new(),
        pending: HashMap::new(),
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
        writer.inflight.is_empty(),
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

    fn handle(&mut self, msg: Msg) {
        self.begin();
        match msg {
            Msg::Begin {
                file,
                identity,
                ctime_ns,
            } => {
                let fs_id = self.fs_id(identity.fs);
                let inode_id = match self.select_inode(fs_id, &identity.handle) {
                    Some(id) => {
                        db(self.conn.execute(
                            "UPDATE inodes SET size = NULL, ctime_ns = ? WHERE id = ?",
                            params![ctime_ns, id],
                        ));
                        db(self
                            .conn
                            .execute("DELETE FROM blocks WHERE inode_id = ?", params![id]));
                        id
                    }
                    None => {
                        db(self.conn.execute(
                            "INSERT INTO inodes (fs_id, handle, size, ctime_ns) VALUES (?, ?, NULL, ?)",
                            params![fs_id, &*identity.handle, ctime_ns],
                        ));
                        self.conn.last_insert_rowid()
                    }
                };
                self.inflight.insert(
                    file,
                    Inflight {
                        inode_id,
                        blocks: 0,
                    },
                );
            }
            Msg::Blocks { file, rows } => {
                let inflight = self.inflight.get_mut(&file).expect("Blocks without Begin");
                let mut insert = db(self.conn.prepare_cached(
                    "INSERT INTO blocks (inode_id, block_no, weak, strong) VALUES (?, ?, ?, ?)",
                ));
                for row in &rows {
                    db(insert.execute(params![
                        inflight.inode_id,
                        row.block_no as i64,
                        row.weak as i64,
                        &row.strong_prefix[..]
                    ]));
                }
                inflight.blocks += rows.len() as u64;
                self.summary.blocks += rows.len() as u64;
                self.work += rows.len() as u64;
            }
            Msg::End {
                file,
                size,
                ctime_ns,
                path,
            } => {
                let inflight = self.inflight.remove(&file).expect("End without Begin");
                if inflight.blocks == 0 {
                    db(self.conn.execute(
                        "DELETE FROM inodes WHERE id = ?",
                        params![inflight.inode_id],
                    ));
                    self.finish(file, RESULT_OMITTED);
                } else {
                    db(self.conn.execute(
                        "UPDATE inodes SET size = ?, ctime_ns = ? WHERE id = ?",
                        params![size as i64, ctime_ns, inflight.inode_id],
                    ));
                    self.insert_path(inflight.inode_id, &path);
                    self.finish(file, inflight.inode_id);
                }
            }
            Msg::Omit { file, identity } => {
                let fs_id = self.fs_id(identity.fs);
                if let Some(id) = self.select_inode(fs_id, &identity.handle) {
                    self.delete_inode(id);
                }
                self.finish(file, RESULT_OMITTED);
            }
            Msg::Path { file, path } => match self.result(file) {
                RESULT_UNKNOWN => self.pending.entry(file).or_default().push(path),
                RESULT_OMITTED | RESULT_FAILED => {}
                inode_id => self.insert_path(inode_id, &path),
            },
            Msg::Abandon { file } => {
                if let Some(inflight) = self.inflight.remove(&file) {
                    self.delete_inode(inflight.inode_id);
                }
                self.finish(file, RESULT_FAILED);
            }
        }
        self.work += 1;
        if self.work >= COMMIT_WORK {
            self.commit();
        }
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

    /// Remove an inode with its paths and blocks.
    fn delete_inode(&self, id: i64) {
        db(self
            .conn
            .execute("DELETE FROM paths WHERE inode_id = ?", params![id]));
        db(self
            .conn
            .execute("DELETE FROM blocks WHERE inode_id = ?", params![id]));
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

    fn result(&self, file: FileId) -> i64 {
        self.results
            .get(file as usize)
            .copied()
            .unwrap_or(RESULT_UNKNOWN)
    }

    /// Record a file's terminal result and apply its pending paths.
    fn finish(&mut self, file: FileId, result: i64) {
        let index = file as usize;
        if index >= self.results.len() {
            self.results.resize(index + 1, RESULT_UNKNOWN);
        }
        assert_eq!(
            self.results[index], RESULT_UNKNOWN,
            "two terminal messages for one file"
        );
        self.results[index] = result;
        if result > 0 {
            for path in self.pending.remove(&file).unwrap_or_default() {
                self.insert_path(result, &path);
            }
        } else {
            self.pending.remove(&file);
        }
    }
}

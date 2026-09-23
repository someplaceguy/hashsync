//! Traversal and hashing of mutable roots (DESIGN.md §4.5 and §4.6).
//!
//! One rayon pool runs directory and file tasks. A task holds the `O_PATH`
//! descriptor of its parent directory and opens its own entry when it
//! starts; root tasks hold the directory that root expansion left open. A
//! directory task claims the directory's identity in a shared visited set,
//! where a repeat is fatal, then lists its entries and queues one task per
//! relevant child. A file task claims the file's identity: the first claimer
//! reuses the file's unchanged stored content or hashes it, and later
//! claimers add hard-link paths. A root that is not a
//! directory or a regular file is skipped like any other such entry, but
//! with a warning, since it is configuration. Results go to the writer
//! thread through a bounded channel.

use std::cell::RefCell;
use std::collections::HashMap;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use bstr::ByteSlice;
use crossbeam_channel::Sender;
use rayon::Scope;
use rustix::fs::FileType;
use rustix::io::Errno;

use crate::blocks::BlockHasher;
use crate::config::{Config, RootConfig};
use crate::die::{die, warn};
use crate::fsid::{FsCache, Identity, identify};
use crate::fsops::{self, Meta};
use crate::index;
use crate::index::writer::{FileId, Msg, RelPath};
use crate::roots::{self, Location};

/// A root location to scan and its index row.
pub struct ScanRoot<'a> {
    pub location: &'a Location,
    pub root_id: i64,
}

#[derive(Default, Debug)]
pub struct Counters {
    pub directories: AtomicU64,
    pub files_hashed: AtomicU64,
    pub files_unchanged: AtomicU64,
    pub files_linked: AtomicU64,
    pub files_small: AtomicU64,
    pub bytes_hashed: AtomicU64,
    pub blocks_periodic: AtomicU64,
    pub blocks_in_holes: AtomicU64,
}

/// Scan every root on a dedicated pool, then close `tx`. `jobs` is the
/// `-j` override.
pub fn run(
    config: &Config,
    roots: Vec<ScanRoot<'_>>,
    fs_cache: &FsCache,
    tx: Sender<Msg>,
    jobs: Option<u32>,
) -> Counters {
    let mut shared = Vec::with_capacity(roots.len());
    let mut starts = Vec::with_capacity(roots.len());
    for ScanRoot { location, root_id } in roots {
        shared.push(Root {
            path: location.path.clone(),
            config: &config.roots[location.root],
            root_id,
        });
        starts.push((Arc::clone(&location.dir), location.name.clone()));
    }
    let scanner = Scanner {
        roots: shared,
        fs_cache,
        hasher: BlockHasher::new(config.block_size, config.strong_hash),
        index_path: &config.index_path,
        visited_dirs: Mutex::default(),
        visited_files: Mutex::default(),
        tx,
        counters: Counters::default(),
    };
    // Each worker closes its reader inside the wrapper, which the scoped
    // pool waits for: worker readers close before the writer connection
    // does (DESIGN.md §4.4). Neither a dropped unscoped pool (it signals
    // termination without joining) nor the scoped join alone (it can
    // return before thread-local destructors run) guarantees that order.
    rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count(config.threads, jobs))
        .thread_name(|i| format!("scan{i}"))
        .build_scoped(
            |thread| {
                thread.run();
                READER.with_borrow_mut(|reader| drop(reader.take()));
            },
            |pool| {
                pool.scope(|scope| {
                    for (root, (dir, name)) in starts.into_iter().enumerate() {
                        let scanner = &scanner;
                        scope.spawn(move |scope| scanner.root_task(scope, root, dir, name));
                    }
                });
            },
        )
        .expect("cannot start the scan pool");
    scanner.counters
}

/// The scan pool size: `-j`, else the `threads` option, else the default
/// `4 × cores` clamped to 16–64 (DESIGN.md §4.6). Zero selects the default.
fn thread_count(configured: u32, jobs: Option<u32>) -> usize {
    match jobs.unwrap_or(configured) {
        0 => {
            let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
            (cores * 4).clamp(16, 64)
        }
        n => n as usize,
    }
}

/// What every task under one root shares.
struct Root<'a> {
    /// The root's absolute path.
    path: Vec<u8>,
    config: &'a RootConfig,
    root_id: i64,
}

/// Shared context of a traversed directory.
struct DirCtx {
    /// The directory's `O_PATH` descriptor, the parent of relative opens.
    fd: OwnedFd,
    root: usize,
    /// Path relative to the root; empty for the root directory.
    rel: Vec<u8>,
    mnt_id: u64,
}

/// Why an entry is not indexed. Entries that vanish before they are
/// opened, and symlinks and special files, are normal and silent; failures
/// are reported.
enum Skip {
    Vanished,
    OtherKind,
    Failed(String),
}

type LocalResult<T> = Result<T, Skip>;

fn io(what: &'static str) -> impl Fn(Errno) -> Skip {
    move |e| match e {
        Errno::NOENT => Skip::Vanished,
        e => Skip::Failed(format!("{what}: {e}")),
    }
}

thread_local! {
    /// Each worker's reusable block buffer.
    static BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    /// Each worker's index reader connection (DESIGN.md §4.6), opened on
    /// first use. The worker wrapper closes it explicitly before the scan
    /// returns (DESIGN.md §4.4); a thread-local destructor can run after
    /// the scoped pool's join.
    static READER: RefCell<Option<index::Reader>> = const { RefCell::new(None) };
}

struct Scanner<'a> {
    roots: Vec<Root<'a>>,
    fs_cache: &'a FsCache,
    hasher: BlockHasher,
    index_path: &'a [u8],
    /// Every claimed directory identity, with the path that claimed it.
    visited_dirs: Mutex<HashMap<Identity, Vec<u8>>>,
    visited_files: Mutex<HashMap<Identity, FileId>>,
    tx: Sender<Msg>,
    counters: Counters,
}

impl Scanner<'_> {
    fn count(&self, counter: impl Fn(&Counters) -> &AtomicU64, n: u64) {
        counter(&self.counters).fetch_add(n, Relaxed);
    }

    fn send(&self, msg: Msg) {
        self.tx.send(msg).expect("the writer thread ended early");
    }

    fn abs_path(&self, root: usize, rel: &[u8]) -> Vec<u8> {
        let base = &self.roots[root].path;
        if rel.is_empty() {
            base.clone()
        } else {
            roots::join(base, rel)
        }
    }

    fn root_task<'s>(&'s self, scope: &Scope<'s>, root: usize, dir: Arc<OwnedFd>, name: Vec<u8>) {
        let result = self.open_and_visit(scope, dir.as_fd(), &name, root, Vec::new(), None);
        let message = match result {
            Ok(()) => return,
            Err(Skip::Vanished) => "root does not exist".to_string(),
            Err(Skip::OtherKind) => "root is not a directory or a regular file".to_string(),
            Err(Skip::Failed(message)) => message,
        };
        warn(format!("{}: {message}", self.abs_path(root, b"").as_bstr()));
    }

    fn entry_task<'s>(&'s self, scope: &Scope<'s>, ctx: Arc<DirCtx>, name: Vec<u8>) {
        let rel = if ctx.rel.is_empty() {
            name.clone()
        } else {
            roots::join(&ctx.rel, &name)
        };
        let result = self.open_and_visit(
            scope,
            ctx.fd.as_fd(),
            &name,
            ctx.root,
            rel.clone(),
            Some(&ctx),
        );
        if let Err(Skip::Failed(message)) = result {
            warn(format!(
                "{}: {message}",
                self.abs_path(ctx.root, &rel).as_bstr()
            ));
        }
    }

    /// Open the entry `name` of `dir`, then traverse it as a directory or
    /// index it as a regular file. Anything else is not opened for I/O.
    fn open_and_visit<'s>(
        &'s self,
        scope: &Scope<'s>,
        dir: BorrowedFd<'_>,
        name: &[u8],
        root: usize,
        rel: Vec<u8>,
        parent: Option<&DirCtx>,
    ) -> LocalResult<()> {
        let fd = fsops::open_path(dir, name).map_err(io("open"))?;
        let meta = fsops::stat(fd.as_fd()).map_err(io("statx"))?;
        // `one_filesystem` skips an entry on another mount whatever its
        // kind, a bind-mounted file included. A root itself is never
        // skipped: it has no parent here.
        if let Some(parent) = parent
            && self.roots[root].config.one_filesystem
            && parent.mnt_id != meta.mnt_id
        {
            return Ok(());
        }
        match meta.kind {
            FileType::Directory => self.visit_dir(scope, fd, &meta, root, rel),
            FileType::RegularFile => self.visit_file(fd, &meta, root, rel),
            _ => Err(Skip::OtherKind),
        }
    }

    fn visit_dir<'s>(
        &'s self,
        scope: &Scope<'s>,
        fd: OwnedFd,
        meta: &Meta,
        root: usize,
        rel: Vec<u8>,
    ) -> LocalResult<()> {
        let scan_root = &self.roots[root];
        let identity = identify(self.fs_cache, fd.as_fd(), meta).map_err(io("identity"))?;
        self.claim_dir(identity, root, &rel);
        let entries = fsops::list_dir(fd.as_fd()).map_err(io("reopen"))?;
        self.count(|c| &c.directories, 1);
        let ctx = Arc::new(DirCtx {
            fd,
            root,
            rel,
            mnt_id: meta.mnt_id,
        });
        let exclude = &scan_root.config.exclude;
        let dir_abs = self.abs_path(root, &ctx.rel);
        for entry in entries {
            let entry = entry.map_err(io("readdir"))?;
            let name = entry.file_name().to_bytes();
            if exclude.excludes(name, || roots::join(&dir_abs, name)) {
                continue;
            }
            let name = name.to_vec();
            let ctx = Arc::clone(&ctx);
            scope.spawn(move |scope| self.entry_task(scope, ctx, name));
        }
        Ok(())
    }

    /// Claim a directory identity before traversing the directory. A
    /// directory reached twice, through a cycle or a mount alias, is fatal
    /// whatever roots are involved (DESIGN.md §4.5).
    fn claim_dir(&self, identity: Identity, root: usize, rel: &[u8]) {
        let path = self.abs_path(root, rel);
        if let Some(earlier) = self.visited_dirs.lock().unwrap().insert(identity, path) {
            die(format!(
                "{} and {} are the same directory (cycle or mount alias)",
                earlier.as_bstr(),
                self.abs_path(root, rel).as_bstr()
            ));
        }
    }

    fn visit_file(&self, fd: OwnedFd, meta: &Meta, root: usize, rel: Vec<u8>) -> LocalResult<()> {
        let identity = identify(self.fs_cache, fd.as_fd(), meta).map_err(io("identity"))?;
        let (file, first) = self.claim_file(&identity);
        let root_id = self.roots[root].root_id;
        if meta.size < u64::from(self.hasher.block_size()) {
            if first {
                self.send(Msg::Omit { file, identity });
            }
            self.count(|c| &c.files_small, 1);
            return Ok(());
        }
        let path = RelPath {
            root_id,
            rel_path: rel,
        };
        if !first {
            self.send(Msg::Path { file, path });
            self.count(|c| &c.files_linked, 1);
            return Ok(());
        }
        if let Some(inode_id) = self.reusable_inode(&identity, meta) {
            self.send(Msg::Unchanged {
                file,
                inode_id,
                path,
            });
            self.count(|c| &c.files_unchanged, 1);
            return Ok(());
        }
        self.hash_owner(fd, meta, file, identity, path)
    }

    /// The complete inode row that holds `identity` with the observed
    /// size and ctime, so the file's content can be reused without
    /// hashing or reopening it (DESIGN.md §4.2). The row stays valid for
    /// the writer: only this task, its owner, can replace it.
    fn reusable_inode(&self, identity: &Identity, meta: &Meta) -> Option<i64> {
        READER.with_borrow_mut(|reader| {
            let reader = reader.get_or_insert_with(|| index::open_worker_reader(self.index_path));
            let fsid = &self.fs_cache.info(identity.fs).fsid;
            reader.reusable_inode(fsid, &identity.handle, meta.size, meta.ctime_ns)
        })
    }

    /// Claim a file identity. Returns its ID and whether this is the first
    /// claim.
    fn claim_file(&self, identity: &Identity) -> (FileId, bool) {
        let mut files = self.visited_files.lock().unwrap();
        if let Some(&file) = files.get(identity) {
            return (file, false);
        }
        let file = FileId::try_from(files.len()).expect("too many files");
        files.insert(identity.clone(), file);
        (file, true)
    }

    /// Hash a claimed file. Sends exactly one terminal message.
    fn hash_owner(
        &self,
        fd: OwnedFd,
        meta: &Meta,
        file: FileId,
        identity: Identity,
        path: RelPath,
    ) -> LocalResult<()> {
        let readable = match fsops::reopen_file(fd.as_fd()) {
            Ok(readable) => readable,
            Err(e) => {
                self.send(Msg::Abandon { file });
                return Err(io("reopen")(e));
            }
        };
        drop(fd);
        self.send(Msg::Begin {
            file,
            identity,
            ctime_ns: meta.ctime_ns,
        });
        let outcome = BUFFER.with_borrow_mut(|buffer| {
            self.hasher
                .hash_file(readable.as_fd(), buffer, &mut |rows| {
                    self.send(Msg::Blocks { file, rows });
                })
        });
        match outcome {
            Ok(outcome) => {
                // The size observed before hashing, like the ctime sent
                // with Begin: a file that changed meanwhile has a later
                // ctime, so the next update rehashes it.
                self.send(Msg::End {
                    file,
                    size: meta.size,
                    path,
                });
                self.count(|c| &c.files_hashed, 1);
                self.count(
                    |c| &c.bytes_hashed,
                    outcome.blocks_read * u64::from(self.hasher.block_size()),
                );
                self.count(
                    |c| &c.blocks_periodic,
                    outcome.blocks_read - outcome.blocks_indexed,
                );
                self.count(|c| &c.blocks_in_holes, outcome.blocks_in_holes);
                Ok(())
            }
            Err(e) => {
                self.send(Msg::Abandon { file });
                Err(Skip::Failed(e.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_count_overrides() {
        assert_eq!(thread_count(5, None), 5);
        assert_eq!(thread_count(5, Some(2)), 2);
        assert_eq!(thread_count(0, Some(2)), 2);
        // Zero selects the default, wherever it comes from.
        assert!((16..=64).contains(&thread_count(0, None)));
        assert!((16..=64).contains(&thread_count(5, Some(0))));
    }
}

//! Traversal and hashing of mutable roots (DESIGN.md §4.5 and §4.6).
//!
//! One rayon pool runs directory and file tasks. A task holds the `O_PATH`
//! descriptor of its parent directory and opens its own entry when it
//! starts; root tasks hold the directory that root expansion left open. A
//! directory task claims the directory's identity in a shared visited set,
//! where a repeat is fatal, then lists its entries and queues one task per
//! relevant child. A file task claims the file's identity in the index
//! changes (`index::changes`): the first claim owns the file and reuses its
//! unchanged stored content or hashes it, and later claims add hard-link
//! paths. A root that is not a directory or a regular file is skipped like
//! any other such entry, but with a warning, since it is configuration.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use bstr::ByteSlice;
use rayon::Scope;
use rustix::fs::FileType;
use rustix::io::Errno;

use crate::blocks::BlockHasher;
use crate::config::{Config, RootConfig};
use crate::die::{die, warn};
use crate::fsid::{FsCache, Identity, identify};
use crate::fsops::{self, Meta};
use crate::index::Index;
use crate::index::changes::{Changes, Claim, Owner, RelPath, Summary};
use crate::roots::{self, Location};

/// A root location to scan and its index row.
pub struct ScanRoot<'a> {
    pub location: &'a Location,
    pub root_id: u64,
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

/// Scan every root on a dedicated pool, writing the changes to `index`,
/// then remove the orphaned inodes. `jobs` is the `-j` override.
pub fn run(
    config: &Config,
    roots: Vec<ScanRoot<'_>>,
    fs_cache: &FsCache,
    index: &Index,
    jobs: Option<u32>,
) -> (Counters, Summary) {
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
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count(config.threads, jobs))
        .thread_name(|i| format!("scan{i}"))
        .build()
        .expect("cannot start the scan pool");
    let scanner = Scanner {
        roots: shared,
        fs_cache,
        hasher: BlockHasher::new(config.block_size, config.strong_hash),
        changes: Changes::new(index, fs_cache, &pool),
        visited_dirs: Mutex::default(),
        counters: Counters::default(),
    };
    pool.scope(|scope| {
        for (root, (dir, name)) in starts.into_iter().enumerate() {
            let scanner = &scanner;
            scope.spawn(move |scope| scanner.root_task(scope, root, dir, name));
        }
    });
    let Scanner {
        changes, counters, ..
    } = scanner;
    (counters, changes.finish())
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
    root_id: u64,
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
}

struct Scanner<'a> {
    roots: Vec<Root<'a>>,
    fs_cache: &'a FsCache,
    hasher: BlockHasher,
    /// The claimed files, and the index changes that the tasks write
    /// (DESIGN.md §4.6).
    changes: Changes<'a>,
    /// Every claimed directory identity, with the path that claimed it.
    visited_dirs: Mutex<HashMap<Identity, Vec<u8>>>,
    counters: Counters,
}

impl Scanner<'_> {
    fn count(&self, counter: impl Fn(&Counters) -> &AtomicU64, n: u64) {
        counter(&self.counters).fetch_add(n, Relaxed);
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
        // A listing can return a name twice when the directory changes
        // during the listing. Each path must have one task: the index keeps
        // one inode per path (DESIGN.md §4.5).
        let mut names = HashSet::new();
        for entry in entries {
            let entry = entry.map_err(io("readdir"))?;
            let name = entry.file_name().to_bytes();
            if exclude.excludes(name, || roots::join(&dir_abs, name))
                || !names.insert(name.to_vec())
            {
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
        if meta.size < u64::from(self.hasher.block_size()) {
            self.changes.claim_small(identity);
            self.count(|c| &c.files_small, 1);
            return Ok(());
        }
        let path = RelPath {
            root_id: self.roots[root].root_id,
            rel_path: rel,
        };
        match self.changes.claim(identity, path, meta.size, meta.ctime_ns) {
            Claim::Linked => {
                self.count(|c| &c.files_linked, 1);
                Ok(())
            }
            Claim::Unchanged => {
                self.count(|c| &c.files_unchanged, 1);
                Ok(())
            }
            Claim::Owner(owner) => self.hash_owner(fd, owner),
        }
    }

    /// Hash an owned file into its inode row, and publish the file's
    /// result.
    fn hash_owner(&self, fd: OwnedFd, owner: Owner<'_, '_>) -> LocalResult<()> {
        let readable = match fsops::reopen_file(fd.as_fd()) {
            Ok(readable) => readable,
            Err(e) => {
                owner.fail();
                return Err(io("reopen")(e));
            }
        };
        drop(fd);
        let mut hashing = owner.begin();
        let outcome = BUFFER.with_borrow_mut(|buffer| {
            self.hasher
                .hash_file(readable.as_fd(), buffer, &mut |rows| hashing.blocks(rows))
        });
        match outcome {
            Ok(outcome) => {
                hashing.end();
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
                hashing.abandon();
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

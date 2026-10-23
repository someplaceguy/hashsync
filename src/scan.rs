//! Traversal and hashing of the roots that an update scans (DESIGN.md §4.5
//! and §4.6): every mutable root, and one copy of each unfinished immutable
//! group.
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
//! Every entry that cannot be indexed because of a failure makes its root
//! unsuccessful, so that an immutable group that the root scans is scanned
//! again by the next update. An entry below the root that its permissions
//! keep from being opened, examined or listed is skipped with a warning
//! but is no failure: a group's copy keeps its permissions as it keeps its
//! content, so a scan of the group would be denied again on every update.
//! A read that fails is a failure whatever its errno, and the root itself
//! must be readable.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::os::fd::{AsFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use rayon::Scope;
use rustix::fs::FileType;
use rustix::io::Errno;

use crate::blocks::BlockHasher;
use crate::config::{Config, RootConfig};
use crate::die::die;
use crate::escapes::quoted;
use crate::fsid::{FsCache, Identity, identify};
use crate::fsops::{self, Meta};
use crate::index::changes::{Changes, Claim, Owner, RelPath, Summary};
use crate::index::{Index, Reconciliation};
use crate::pool::Pool;
use crate::report::warn;
use crate::roots::{self, Location};

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

/// Scan the roots that `reconciliation` chose among `locations`, the
/// current roots, on a dedicated pool, writing the changes to `index`.
/// Then mark the immutable groups that the scan completed, and remove the
/// orphaned inodes. The pool ends when this returns (DESIGN.md §3 and
/// §4.6). `jobs` is the `-j` override.
pub fn run(
    config: &Config,
    locations: &[Location],
    reconciliation: &Reconciliation,
    fs_cache: &FsCache,
    index: &Index,
    jobs: Option<u32>,
) -> (Counters, Summary) {
    let pool = Pool::start("scan", thread_count(config.threads, jobs));
    let scanner = Scanner {
        fs_cache,
        hasher: BlockHasher::new(config.block_size, config.strong_hash),
        changes: Changes::new(index, fs_cache, &pool, reconciliation),
        visited_dirs: Mutex::default(),
        counters: Counters::default(),
    };
    pool.scope(|scope| {
        for scan in reconciliation.scans() {
            let copies: Vec<(&Location, &RootConfig)> = scan
                .locations
                .iter()
                .map(|&i| (&locations[i], &config.roots[locations[i].root]))
                .collect();
            let (scanner, root_id) = (&scanner, scan.root_id);
            scope.spawn(move |scope| scanner.root_task(scope, root_id, copies));
        }
    });
    let Scanner {
        changes, counters, ..
    } = scanner;
    (counters, changes.finish())
}

/// The scan pool size: `-j`, else the `threads` option, else the default
/// `4 × cores` clamped to 16–64 (DESIGN.md §4.6). Zero selects the default.
/// The lookups of a prune use it too.
pub fn thread_count(configured: u32, jobs: Option<u32>) -> usize {
    match jobs.unwrap_or(configured) {
        0 => {
            let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
            (cores * 4).clamp(16, 64)
        }
        n => n as usize,
    }
}

/// What every task under one root shares: the copy of the root that its
/// root task opened, and the root's row.
struct Root<'a> {
    /// The copy's absolute path.
    path: Vec<u8>,
    config: &'a RootConfig,
    root_id: u64,
}

impl Root<'_> {
    /// The absolute path of the entry at `rel` under the root.
    fn abs_path(&self, rel: &[u8]) -> Vec<u8> {
        if rel.is_empty() {
            self.path.clone()
        } else {
            roots::join(&self.path, rel)
        }
    }
}

/// Shared context of a traversed directory.
struct DirCtx<'a> {
    /// The directory's `O_PATH` descriptor, the parent of relative opens.
    fd: OwnedFd,
    root: Arc<Root<'a>>,
    /// Path relative to the root; empty for the root directory.
    rel: Vec<u8>,
    mnt_id: u64,
}

/// Why an entry is not indexed. Entries that vanish before they are
/// opened, and symlinks and special files, are normal and silent; the
/// others are reported (`Scanner::skipped`).
enum Skip {
    Vanished,
    OtherKind,
    /// A permission error (`EACCES` or `EPERM`) of a call that opens,
    /// examines or lists the entry. The copy's own permissions deny the
    /// entry, and would deny it again in every scan of an immutable group,
    /// so it does not make the root unsuccessful, unless they deny the root
    /// itself (DESIGN.md §4.5).
    Denied(String),
    Failed(String),
}

type LocalResult<T> = Result<T, Skip>;

fn io(what: &'static str) -> impl Fn(Errno) -> Skip {
    move |e| match e {
        Errno::NOENT => Skip::Vanished,
        Errno::ACCESS | Errno::PERM => Skip::Denied(format!("{what}: {e}")),
        e => Skip::Failed(format!("{what}: {e}")),
    }
}

thread_local! {
    /// Each worker's reusable block buffer.
    static BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

struct Scanner<'a> {
    fs_cache: &'a FsCache,
    hasher: BlockHasher,
    /// The claimed files, and the index changes that the tasks write
    /// (DESIGN.md §4.6).
    changes: Changes<'a>,
    /// Every claimed directory identity, with the path that claimed it.
    visited_dirs: Mutex<HashMap<Identity, Vec<u8>>>,
    counters: Counters,
}

impl<'a> Scanner<'a> {
    fn count(&self, counter: impl Fn(&Counters) -> &AtomicU64, n: u64) {
        counter(&self.counters).fetch_add(n, Relaxed);
    }

    /// Report an entry under `root`, at `rel`, that cannot be indexed, and
    /// make the root unsuccessful (DESIGN.md §4.5).
    fn fail(&self, root: &Root<'_>, rel: &[u8], message: &str) {
        warn(format!("{}: {message}", quoted(&root.abs_path(rel))));
        self.changes.fail_root(root.root_id);
    }

    /// Report an entry below `root`, at `rel`, that is not indexed because
    /// of `skip`, unless that is a normal skip, and make the root
    /// unsuccessful when it is a failure (`Skip`).
    fn skipped(&self, root: &Root<'_>, rel: &[u8], skip: Skip) {
        match skip {
            Skip::Vanished | Skip::OtherKind => {}
            Skip::Denied(message) => warn(format!("{}: {message}", quoted(&root.abs_path(rel)))),
            Skip::Failed(message) => self.fail(root, rel, &message),
        }
    }

    /// Scan the root of the row `root_id` at the first of `copies` that is
    /// still there (DESIGN.md §4.2). Root expansion found every copy, so a
    /// copy that is not there when the task opens it has vanished since,
    /// and it gives its place to the next one only when it may vanish. A
    /// root that vanished and may not vanish is a failure: the copy of a
    /// group that an update scans must be there, or the group would
    /// complete without its files. When every copy is gone, and each one
    /// may vanish, the root has no scan and no warning, and its group stays
    /// unfinished.
    fn root_task<'s>(
        &'s self,
        scope: &Scope<'s>,
        root_id: u64,
        copies: Vec<(&'a Location, &'a RootConfig)>,
    ) where
        'a: 's,
    {
        for (i, (location, config)) in copies.into_iter().enumerate() {
            let root = Arc::new(Root {
                path: location.path.clone(),
                config,
                root_id,
            });
            let fd = match fsops::open_path(location.dir.as_fd(), &location.name) {
                Ok(fd) => fd,
                Err(Errno::NOENT) if config.may_vanish => continue,
                Err(Errno::NOENT) => return self.fail(&root, b"", "root does not exist"),
                Err(e) => return self.fail(&root, b"", &format!("open: {e}")),
            };
            // The row names the first copy until its group completes.
            if i > 0 {
                self.changes.scan_copy(root_id, &location.path);
            }
            let message = match self.visit(scope, fd, &root, Vec::new(), None) {
                Ok(()) => return,
                Err(Skip::Vanished) => "root does not exist".to_string(),
                Err(Skip::OtherKind) => "root is not a directory or a regular file".to_string(),
                // A root that its permissions deny cannot be read at all, and
                // its group must not complete without its files.
                Err(Skip::Denied(message) | Skip::Failed(message)) => message,
            };
            return self.fail(&root, b"", &message);
        }
        self.changes.fail_root(root_id);
    }

    fn entry_task<'s>(&'s self, scope: &Scope<'s>, ctx: Arc<DirCtx<'a>>, name: Vec<u8>)
    where
        'a: 's,
    {
        let rel = if ctx.rel.is_empty() {
            name.clone()
        } else {
            roots::join(&ctx.rel, &name)
        };
        let result = match fsops::open_path(ctx.fd.as_fd(), &name).map_err(io("open")) {
            // Opening an entry needs the search permission of its directory.
            // So a root that cannot be searched denies every entry in it, and
            // cannot be read at all, as a root that cannot be listed.
            Err(Skip::Denied(message)) if ctx.rel.is_empty() => Err(Skip::Failed(message)),
            opened => {
                opened.and_then(|fd| self.visit(scope, fd, &ctx.root, rel.clone(), Some(&ctx)))
            }
        };
        if let Err(skip) = result {
            self.skipped(&ctx.root, &rel, skip);
        }
    }

    /// Traverse the entry of `fd`, its `O_PATH` descriptor, as a directory,
    /// or index it as a regular file. Anything else is not opened for I/O.
    fn visit<'s>(
        &'s self,
        scope: &Scope<'s>,
        fd: OwnedFd,
        root: &Arc<Root<'a>>,
        rel: Vec<u8>,
        parent: Option<&DirCtx<'a>>,
    ) -> LocalResult<()>
    where
        'a: 's,
    {
        let meta = fsops::stat(fd.as_fd()).map_err(io("statx"))?;
        // `one_filesystem` skips an entry on another mount whatever its
        // kind, a bind-mounted file included. A root itself is never
        // skipped: it has no parent here.
        if let Some(parent) = parent
            && root.config.one_filesystem
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
        root: &Arc<Root<'a>>,
        rel: Vec<u8>,
    ) -> LocalResult<()>
    where
        'a: 's,
    {
        let identity = identify(self.fs_cache, fd.as_fd(), meta).map_err(io("identity"))?;
        self.claim_dir(identity, root, &rel);
        let entries = fsops::list_dir(fd.as_fd()).map_err(io("reopen"))?;
        self.count(|c| &c.directories, 1);
        let ctx = Arc::new(DirCtx {
            fd,
            root: Arc::clone(root),
            rel,
            mnt_id: meta.mnt_id,
        });
        let exclude = &root.config.exclude;
        let dir_abs = root.abs_path(&ctx.rel);
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
    fn claim_dir(&self, identity: Identity, root: &Root<'_>, rel: &[u8]) {
        let path = root.abs_path(rel);
        if let Some(earlier) = self.visited_dirs.lock().unwrap().insert(identity, path) {
            die(format!(
                "{} and {} are the same directory (cycle or mount alias)",
                quoted(&earlier),
                quoted(&root.abs_path(rel))
            ));
        }
    }

    fn visit_file(
        &self,
        fd: OwnedFd,
        meta: &Meta,
        root: &Root<'_>,
        rel: Vec<u8>,
    ) -> LocalResult<()> {
        let identity = identify(self.fs_cache, fd.as_fd(), meta).map_err(io("identity"))?;
        if meta.size < u64::from(self.hasher.block_size()) {
            self.changes.claim_small(identity);
            self.count(|c| &c.files_small, 1);
            return Ok(());
        }
        let path = RelPath {
            root_id: root.root_id,
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
                let skip = io("reopen")(e);
                if let Skip::Denied(_) = skip {
                    owner.deny();
                } else {
                    owner.fail();
                }
                return Err(skip);
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
            // A read that fails is a failure, also for a permission error:
            // the file opened, so its permissions are not the cause, and
            // the cause may not last (DESIGN.md §4.5).
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

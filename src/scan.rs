//! Traversal and hashing of mutable roots (DESIGN.md §4.5 and §4.6).
//!
//! One rayon pool runs directory and file tasks. A directory task lists
//! its entries and queues one task per relevant child, sharing its `O_PATH`
//! descriptor. A file task claims the file's identity: the first claimer
//! hashes it and later claimers add hard-link paths. Results go to the
//! writer thread through a bounded channel.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::os::fd::{AsFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use bstr::ByteSlice;
use crossbeam_channel::Sender;
use rayon::Scope;
use rustix::fs::{Dir, FileType};
use rustix::io::Errno;

use crate::blocks::BlockHasher;
use crate::config::RootConfig;
use crate::die::{EXIT_FAILURE, die, warn};
use crate::fsid::{FsCache, Identity, identify};
use crate::fsops::{self, Meta};
use crate::glob;
use crate::index::writer::{FileId, Msg, RelPath};
use crate::roots::{self, Location, LocationKind, Registry};

/// A root location to scan and its index row.
pub struct ScanRoot<'a> {
    pub location: &'a Location,
    pub config: &'a RootConfig,
    pub root_id: i64,
}

#[derive(Default, Debug)]
pub struct Counters {
    pub directories: AtomicU64,
    pub files_hashed: AtomicU64,
    pub files_linked: AtomicU64,
    pub files_small: AtomicU64,
    pub bytes_hashed: AtomicU64,
    pub blocks_periodic: AtomicU64,
    pub blocks_in_holes: AtomicU64,
}

/// Shared context of a traversed directory.
struct DirCtx {
    /// The directory's `O_PATH` descriptor, the parent of relative opens.
    fd: OwnedFd,
    root: usize,
    /// Path relative to the root; empty for the root directory.
    rel: Vec<u8>,
    mnt_id: u64,
    identity: Identity,
}

/// A root location, reopened and verified.
enum Opened {
    Directory {
        fd: OwnedFd,
        meta: Meta,
    },
    File {
        fd: OwnedFd,
        meta: Meta,
        name: Vec<u8>,
        parent: Identity,
    },
}

/// Why an entry is not indexed. Entries that vanish before they are
/// opened are normal and silent; other failures are reported.
enum Skip {
    Vanished,
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

pub struct Scanner<'a> {
    roots: &'a [ScanRoot<'a>],
    registry: &'a Registry,
    fs_cache: &'a FsCache,
    hasher: BlockHasher,
    visited_dirs: Mutex<HashSet<Identity>>,
    visited_files: Mutex<HashMap<Identity, FileId>>,
    tx: Sender<Msg>,
    counters: Counters,
}

impl<'a> Scanner<'a> {
    pub fn new(
        roots: &'a [ScanRoot<'a>],
        registry: &'a Registry,
        fs_cache: &'a FsCache,
        hasher: BlockHasher,
        tx: Sender<Msg>,
    ) -> Self {
        Scanner {
            roots,
            registry,
            fs_cache,
            hasher,
            visited_dirs: Mutex::new(HashSet::new()),
            visited_files: Mutex::new(HashMap::new()),
            tx,
            counters: Counters::default(),
        }
    }

    /// Scan every root on `pool`, then close the writer channel.
    pub fn run(self, pool: &rayon::ThreadPool) -> Counters {
        pool.scope(|scope| {
            for index in 0..self.roots.len() {
                let scanner = &self;
                scope.spawn(move |scope| scanner.root_task(scope, index));
            }
        });
        self.counters
    }

    fn count(&self, counter: impl Fn(&Counters) -> &AtomicU64, n: u64) {
        counter(&self.counters).fetch_add(n, Relaxed);
    }

    fn send(&self, msg: Msg) {
        self.tx.send(msg).expect("the writer thread ended early");
    }

    fn abs_path(&self, root: usize, rel: &[u8]) -> Vec<u8> {
        let base = &self.roots[root].location.path;
        if rel.is_empty() {
            base.clone()
        } else {
            roots::join(base, rel)
        }
    }

    /// Report an entry that is not indexed, and continue.
    fn report(&self, root: usize, rel: &[u8], skip: Skip) {
        if let Skip::Failed(message) = skip {
            warn(format!("{}: {message}", self.abs_path(root, rel).as_bstr()));
        }
    }

    fn root_task<'s>(&'s self, scope: &Scope<'s>, index: usize) {
        let result = match self.open_root(index) {
            Ok(Opened::Directory { fd, meta }) => {
                self.visit_dir(scope, fd, &meta, index, Vec::new(), None)
            }
            Ok(Opened::File {
                fd,
                meta,
                name,
                parent,
            }) => self.visit_file(fd, &meta, index, Vec::new(), &name, &parent),
            Err(e) => Err(e),
        };
        if let Err(skip) = result {
            self.report(index, b"", skip);
        }
    }

    /// Reopen a root location by walking from `/` and require the
    /// registered identity to be unchanged.
    fn open_root(&self, index: usize) -> LocalResult<Opened> {
        let location = self.roots[index].location;
        let components: Vec<&[u8]> = glob::components(&location.path).collect();
        let mut dir = fsops::open_root_dir().map_err(io("open /"))?;
        let mut dir_meta = fsops::stat(dir.as_fd()).map_err(io("statx"))?;
        let Some((&name, parents)) = components.split_last() else {
            // The root is `/` itself.
            let LocationKind::Directory { identity } = &location.kind else {
                return Err(Skip::Failed("root is not a directory".into()));
            };
            let found = identify(self.fs_cache, dir.as_fd(), &dir_meta).map_err(io("identity"))?;
            if found != *identity {
                return Err(Skip::Failed("root location changed".into()));
            }
            return Ok(Opened::Directory {
                fd: dir,
                meta: dir_meta,
            });
        };
        for component in parents {
            let next = fsops::open_path(dir.as_fd(), component).map_err(io("open"))?;
            let meta = fsops::stat(next.as_fd()).map_err(io("statx"))?;
            if meta.kind != FileType::Directory {
                return Err(Skip::Failed(format!(
                    "{}: not a directory",
                    component.as_bstr()
                )));
            }
            dir = next;
            dir_meta = meta;
        }
        let fd = fsops::open_path(dir.as_fd(), name).map_err(io("open"))?;
        let meta = fsops::stat(fd.as_fd()).map_err(io("statx"))?;
        let found = identify(self.fs_cache, fd.as_fd(), &meta).map_err(io("identity"))?;
        match &location.kind {
            LocationKind::Directory { identity } => {
                if meta.kind != FileType::Directory {
                    return Err(Skip::Failed("root is no longer a directory".into()));
                }
                if found != *identity {
                    return Err(Skip::Failed("root location changed".into()));
                }
                Ok(Opened::Directory { fd, meta })
            }
            LocationKind::File {
                identity, parent, ..
            } => {
                if meta.kind != FileType::RegularFile {
                    return Err(Skip::Failed("root is no longer a regular file".into()));
                }
                let parent_found =
                    identify(self.fs_cache, dir.as_fd(), &dir_meta).map_err(io("identity"))?;
                if found != *identity || parent_found != *parent {
                    return Err(Skip::Failed("root location changed".into()));
                }
                Ok(Opened::File {
                    fd,
                    meta,
                    name: name.to_vec(),
                    parent: parent_found,
                })
            }
        }
    }

    fn entry_task<'s>(&'s self, scope: &Scope<'s>, ctx: Arc<DirCtx>, name: Vec<u8>) {
        let rel = if ctx.rel.is_empty() {
            name.clone()
        } else {
            roots::join(&ctx.rel, &name)
        };
        let result = (|| {
            let fd = fsops::open_path(ctx.fd.as_fd(), &name).map_err(io("open"))?;
            let meta = fsops::stat(fd.as_fd()).map_err(io("statx"))?;
            match meta.kind {
                FileType::Directory => {
                    self.visit_dir(scope, fd, &meta, ctx.root, rel.clone(), Some(&ctx))
                }
                FileType::RegularFile => {
                    self.visit_file(fd, &meta, ctx.root, rel.clone(), &name, &ctx.identity)
                }
                // Symlinks and special files are skipped without I/O.
                _ => Ok(()),
            }
        })();
        if let Err(skip) = result {
            self.report(ctx.root, &rel, skip);
        }
    }

    fn visit_dir<'s>(
        &'s self,
        scope: &Scope<'s>,
        fd: OwnedFd,
        meta: &Meta,
        root: usize,
        rel: Vec<u8>,
        parent: Option<&DirCtx>,
    ) -> LocalResult<()> {
        let scan_root = &self.roots[root];
        if let Some(parent) = parent
            && scan_root.config.one_filesystem
            && parent.mnt_id != meta.mnt_id
        {
            return Ok(());
        }
        let identity = identify(self.fs_cache, fd.as_fd(), meta).map_err(io("identity"))?;
        if let Some(owner) = self.registry.directory_owner(&identity)
            && owner != root
        {
            self.die_entered_root(root, &rel, owner);
        }
        if !self.visited_dirs.lock().unwrap().insert(identity.clone()) {
            die(
                EXIT_FAILURE,
                format!(
                    "{}: directory reached twice (cycle or mount alias)",
                    self.abs_path(root, &rel).as_bstr()
                ),
            );
        }
        let readable = fsops::reopen_dir(fd.as_fd()).map_err(io("reopen"))?;
        self.count(|c| &c.directories, 1);
        let ctx = Arc::new(DirCtx {
            fd,
            root,
            rel,
            mnt_id: meta.mnt_id,
            identity,
        });
        let exclude = &scan_root.config.exclude;
        let dir_abs = self.abs_path(root, &ctx.rel);
        let dir = Dir::new(readable).map_err(io("opendir"))?;
        for entry in dir {
            let entry = entry.map_err(io("readdir"))?;
            let name = entry.file_name().to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            // Entry types are hints; they only skip entries that are
            // never indexed, without opening them.
            if matches!(
                entry.file_type(),
                FileType::Symlink
                    | FileType::Fifo
                    | FileType::Socket
                    | FileType::CharacterDevice
                    | FileType::BlockDevice
            ) {
                continue;
            }
            if exclude.excludes(name, || roots::join(&dir_abs, name)) {
                continue;
            }
            let name = name.to_vec();
            let ctx = Arc::clone(&ctx);
            scope.spawn(move |scope| self.entry_task(scope, ctx, name));
        }
        Ok(())
    }

    fn visit_file(
        &self,
        fd: OwnedFd,
        meta: &Meta,
        root: usize,
        rel: Vec<u8>,
        name: &[u8],
        parent: &Identity,
    ) -> LocalResult<()> {
        if let Some(owner) = self.registry.file_owner(parent, name)
            && owner != root
        {
            self.die_entered_root(root, &rel, owner);
        }
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
        if !first {
            self.send(Msg::Path {
                file,
                path: RelPath {
                    root_id,
                    rel_path: rel,
                },
            });
            self.count(|c| &c.files_linked, 1);
            return Ok(());
        }
        self.hash_owner(
            fd,
            meta,
            file,
            identity,
            RelPath {
                root_id,
                rel_path: rel,
            },
        )
    }

    fn die_entered_root(&self, root: usize, rel: &[u8], owner: usize) -> ! {
        die(
            EXIT_FAILURE,
            format!(
                "{}: traversal of root {} entered root {}",
                self.abs_path(root, rel).as_bstr(),
                self.roots[root].location.path.as_bstr(),
                self.roots[owner].location.path.as_bstr()
            ),
        )
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
                .hash_file(readable.as_fd(), meta, buffer, &mut |rows| {
                    self.send(Msg::Blocks { file, rows });
                })
        });
        match outcome {
            Ok(outcome) => {
                self.send(Msg::End {
                    file,
                    size: meta.size,
                    ctime_ns: meta.ctime_ns,
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

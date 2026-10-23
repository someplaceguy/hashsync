//! `hashsync prune [--scan-immutable]` (DESIGN.md §4.2 and §8): remove from
//! the index what no current copy of a root holds any more, without hashing
//! a file, then compact the database, as `hashsync compact` does.
//!
//! Prune resolves the roots as an update does, so a root that the
//! configuration no longer names, or whose copies are all missing, has no
//! current location, and goes with its paths. Prune looks into the other
//! mutable roots, and with `--scan-immutable` into the immutable ones too:
//! it opens the copies of each such root, and looks up each path of the root
//! in them, as a candidate read reaches a file (DESIGN.md §6.4), until one
//! of them holds a regular file there that a scan of the copy would reach.
//! So no copy holds a path that the root's `exclude` patterns or
//! `one_filesystem` now leave out: a scan would skip it. A path that no
//! copy holds goes. So does a path whose lookup fails, as an update drops
//! what it cannot reach: no candidate read could reach it either. Such a
//! failure leaves a completed group unfinished, as a failure in the scan of
//! a group does (DESIGN.md §4.5), so that the next update scans it again,
//! unless the permissions of a directory or a file below the root of the
//! copy deny the lookup: the scan of a copy skips such an entry without a
//! failure too. A directory below which lookups fail gets one warning, as a
//! directory that an update cannot read does, and not one for each indexed
//! path below it.
//! A root none of whose copies opens as a directory or a regular file goes
//! too.

use std::collections::{HashMap, HashSet};
use std::os::fd::{AsFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::slice::ParallelSlice;
use rustix::fs::FileType;
use rustix::io::Errno;

use crate::compact;
use crate::config::{Config, RootConfig};
use crate::escapes::quoted;
use crate::fsid::FsCache;
use crate::fsops;
use crate::index::{self, Index, Pruning, RootRow};
use crate::pool::Pool;
use crate::report::{note, warn};
use crate::roots::{self, Location};
use crate::scan;

/// The paths that one task looks up: enough that a task costs little next
/// to its lookups, and few enough that the paths of a large root spread
/// over the threads.
const PATHS_PER_TASK: usize = 256;

pub fn run(config: &Config, scan_immutable: bool) {
    // RocksDB keeps every file of the index open (DESIGN.md §4.4).
    fsops::raise_descriptor_limit();

    // Resolve and validate the roots before the index is opened: a rejected
    // configuration must not change the index (DESIGN.md §4.2).
    let fs_cache = FsCache::new();
    let locations = roots::resolve(config, &fs_cache);

    let index = index::open_existing(config);
    // As for an update, the time counts from the moment the index is
    // locked.
    let start = Instant::now();
    let pool = Pool::start("prune", scan::thread_count(config.threads, None));
    let rows = index.roots();
    let current = roots::current_locations(&locations, &rows);
    let pruning = pool.install(|| find(&index, config, &rows, &current, scan_immutable));
    let pruned = index.prune(&pool, &pruning);
    drop(pool);
    note(format_args!(
        "pruned index in {:.1}s: {} roots, {} paths, and {} stale inodes removed",
        start.elapsed().as_secs_f64(),
        pruning.roots.len(),
        pruned.paths,
        pruned.orphans
    ));
    if !pruning.unfinished.is_empty() {
        note(format_args!(
            "immutable roots: {} incomplete, which the next update scans again",
            pruning.unfinished.len()
        ));
    }
    compact::perform(&index);
    index.close();
}

/// What prune removes from the index, of the root rows `rows`, whose
/// current copies `current` gives (module documentation). Runs on the
/// threads of the current pool.
fn find(
    index: &Index,
    config: &Config,
    rows: &[(u64, RootRow)],
    current: &HashMap<u64, Vec<Location>>,
    scan_immutable: bool,
) -> Pruning {
    let found: Vec<Found<'_>> = rows
        .par_iter()
        .map(|(root_id, _)| match current.get(root_id) {
            None => Found::Gone,
            // The configuration gives a root its kind (DESIGN.md §4.2):
            // every copy of an immutable group has its identifier.
            Some(copies) if copies[0].identifier.is_some() && !scan_immutable => Found::Kept,
            Some(copies) => look_into(index, config, *root_id, copies),
        })
        .collect();
    let mut pruning = Pruning::default();
    for ((root_id, row), found) in rows.iter().zip(found) {
        match found {
            Found::Kept => {}
            Found::Gone => pruning.roots.push(*root_id),
            Found::Paths {
                paths,
                failed,
                opened,
            } => {
                // A run with `--no-update` finds the copies of an unfinished
                // group by the path of its row, so the row takes the path of
                // a copy that is there: the first one that opened. That is
                // the copy that the row names while it is there, since it
                // comes first, but not one that vanished after root
                // resolution.
                if failed && row.identifier.is_some() {
                    pruning.unfinished.push((*root_id, opened.path.clone()));
                }
                pruning.paths.extend(paths);
            }
        }
    }
    pruning
}

/// What prune finds for one root, whose current copies live for `'a`.
enum Found<'a> {
    /// The root stays as it is: prune does not look into it.
    Kept,
    /// No current copy holds the root: it goes, with all its paths.
    Gone,
    /// The paths of the root that go, as `Pruning::paths` holds them,
    /// whether the lookup of one of them failed, and the first copy of the
    /// root that opened.
    Paths {
        paths: Vec<(u64, Vec<u8>, u64)>,
        failed: bool,
        opened: &'a Location,
    },
}

/// Look up the paths of the root `root_id` in `locations`, its current
/// copies, which `config` configures, on the threads of the current pool.
fn look_into<'a>(
    index: &Index,
    config: &'a Config,
    root_id: u64,
    locations: &'a [Location],
) -> Found<'a> {
    let copies: Vec<CurrentCopy<'a>> = locations
        .iter()
        .map(|location| CurrentCopy::new(location, &config.roots[location.root]))
        .collect();
    // The copies open in their order until one opens.
    let Some(opened) = copies.iter().find(|copy| copy.opens()) else {
        return Found::Gone;
    };
    let opened = opened.location;
    let indexed = index.root_paths(root_id);
    let failed = AtomicBool::new(false);
    let paths = indexed
        .par_chunks(PATHS_PER_TASK)
        .flat_map_iter(|chunk| {
            chunk.iter().filter_map(|(rel_path, inode_id)| {
                match lookup(&copies, rel_path) {
                    Lookup::Held => return None,
                    Lookup::Absent => {}
                    Lookup::Failed => failed.store(true, Relaxed),
                }
                Some((root_id, rel_path.clone(), *inode_id))
            })
        })
        .collect();
    Found::Paths {
        paths,
        failed: failed.into_inner(),
        opened,
    }
}

/// What the lookup of a path finds in the copies of its root.
enum Lookup {
    /// A copy holds the path (`CurrentCopy::holds`).
    Held,
    /// No copy holds one. A copy where the permissions of an entry below its
    /// root deny the lookup holds none either, since a scan of the copy
    /// skips the entry without a failure (DESIGN.md §4.5).
    Absent,
    /// No copy holds one, but a lookup failed, so a copy may. The path goes
    /// all the same: no candidate read could reach it either.
    Failed,
}

/// Look `rel_path` up in `copies`, in their order, until one holds it
/// (`CurrentCopy::holds`). Only a path that goes reports the failures of
/// its lookups: where a later copy holds the path, they change nothing.
fn lookup(copies: &[CurrentCopy<'_>], rel_path: &[u8]) -> Lookup {
    let mut failures = Vec::new();
    for copy in copies {
        match copy.holds(rel_path) {
            Ok(true) => return Lookup::Held,
            Ok(false) => {}
            Err(failure) => failures.push(failure),
        }
    }
    let mut lookup = Lookup::Absent;
    for failure in failures {
        // A failed copy has had its own warning.
        let denied = failure.is_some_and(|failure| failure.report(rel_path));
        if !denied {
            lookup = Lookup::Failed;
        }
    }
    lookup
}

/// A current copy of a root, which prune opens when a lookup first needs
/// it, once for all the lookups of the root.
struct CurrentCopy<'a> {
    location: &'a Location,
    /// The configuration of the copy's root: its indexing policy, and
    /// whether the copy need not be there (`may_vanish`).
    config: &'a RootConfig,
    root: OnceLock<CopyRoot>,
}

/// The root of a copy, as prune finds it.
enum CopyRoot {
    /// A directory.
    Dir(CopyDir),
    /// A regular file: the copy of a single-file root.
    File,
    /// Nothing: the copy has vanished since root expansion. A warning has
    /// said so, unless the copy may vanish.
    Missing,
    /// It cannot be opened, or it is neither a directory nor a regular
    /// file. A warning has said so.
    Failed,
}

/// The root directory of a copy.
struct CopyDir {
    /// Its `O_PATH` descriptor, where the lookups start.
    fd: OwnedFd,
    /// The root has `one_filesystem`, so the lookups do not cross mount
    /// points.
    one_filesystem: bool,
    /// The directories, relative to the root, below which a lookup failed
    /// and a warning said so. The root itself is the empty path.
    failed: Mutex<HashSet<Vec<u8>>>,
}

impl CopyDir {
    /// Open `rel_path` below the root the way a scan of the copy reaches
    /// it: without a symlink, and with `one_filesystem`, without crossing a
    /// mount point. The scan skips an entry on another mount, where this
    /// open fails with `EXDEV` (DESIGN.md §4.2).
    fn open(&self, rel_path: &[u8]) -> Result<OwnedFd, Errno> {
        fsops::open_beneath(self.fd.as_fd(), rel_path, self.one_filesystem)
    }

    /// The deepest directory above `rel_path` that opens, relative to the
    /// root: the directory in which the lookup of `rel_path` failed. The
    /// root itself is the empty path.
    fn deepest_open<'p>(&self, rel_path: &'p [u8]) -> &'p [u8] {
        let mut dir = rel_path;
        while let Some(slash) = dir.iter().rposition(|&b| b == b'/') {
            dir = &dir[..slash];
            if self.open(dir).is_ok() {
                return dir;
            }
        }
        b""
    }
}

/// A lookup that failed below the root directory of a copy, which prune
/// reports when the path goes.
struct Failure<'c> {
    dir: &'c CopyDir,
    /// The warning that reports the failure.
    message: String,
    /// What denied the lookup by its permissions (`EACCES` or `EPERM`), if
    /// that is why it failed.
    denied: Option<Denier>,
}

/// What denied a lookup by its permissions.
enum Denier {
    /// A directory on the way to the path, which cannot be searched: the
    /// deepest one that still opens, which can be the root.
    Directory,
    /// The file at the path, which opened. Examining it needs no
    /// permission, but a security module can still deny it.
    File,
}

impl Failure<'_> {
    /// Warn about the failed lookup of `rel_path`, unless a warning has
    /// already reported a failure in the same directory: the deepest one
    /// above the path that still opens. So a directory that cannot be
    /// searched gets one warning, as a directory that an update cannot
    /// read does, and not one for each indexed path below it. Returns true
    /// when the permissions of an entry below the root denied the lookup,
    /// which the scan of the copy skips without a failure: the root itself
    /// must be readable (DESIGN.md §4.5).
    fn report(self, rel_path: &[u8]) -> bool {
        let dir = self.dir.deepest_open(rel_path);
        if self.dir.failed.lock().unwrap().insert(dir.to_vec()) {
            warn(self.message);
        }
        match self.denied {
            Some(Denier::Directory) => !dir.is_empty(),
            Some(Denier::File) => true,
            None => false,
        }
    }
}

impl<'a> CurrentCopy<'a> {
    fn new(location: &'a Location, config: &'a RootConfig) -> Self {
        CurrentCopy {
            location,
            config,
            root: OnceLock::new(),
        }
    }

    /// The root of the copy, which the first call opens.
    fn root(&self) -> &CopyRoot {
        self.root
            .get_or_init(|| open_root(self.location, self.config))
    }

    /// True when the root of the copy opens as a directory or a regular
    /// file.
    fn opens(&self) -> bool {
        matches!(self.root(), CopyRoot::Dir(_) | CopyRoot::File)
    }

    /// Whether the copy holds a regular file at `rel_path` that a scan of
    /// the copy would reach (DESIGN.md §4.2): the copy's exclusions leave
    /// out neither the file nor a directory above it (`excludes`), and the
    /// path reaches the file without a symlink, as a candidate read does
    /// (DESIGN.md §6.4), and with `one_filesystem`, without crossing a
    /// mount point (`CopyDir::open`). A failed lookup is an error, with the
    /// failure to report if the path goes; a failed copy is one too, with
    /// nothing to report, since it has had its own warning.
    fn holds(&self, rel_path: &[u8]) -> Result<bool, Option<Failure<'_>>> {
        // An excluded path needs no lookup, nor even an open copy.
        if self.excludes(rel_path) {
            return Ok(false);
        }
        let dir = match self.root() {
            CopyRoot::Dir(dir) => dir,
            // The one path of a single-file root is empty.
            CopyRoot::File => return Ok(rel_path.is_empty()),
            CopyRoot::Missing => return Ok(false),
            CopyRoot::Failed => return Err(None),
        };
        // The root of a single-file root is a directory now.
        if rel_path.is_empty() {
            return Ok(false);
        }
        let failure = |what: &str, e: Errno, denier: Denier| {
            let path = roots::join(&self.location.path, rel_path);
            let message = format!("{}: {what}: {e}", quoted(&path));
            let denied = matches!(e, Errno::ACCESS | Errno::PERM).then_some(denier);
            Err(Some(Failure {
                dir,
                message,
                denied,
            }))
        };
        let file = match dir.open(rel_path) {
            Ok(file) => file,
            // Nothing there, or nothing that a scan of the copy would
            // reach: the path goes through a symlink, or with
            // `one_filesystem`, across a mount point.
            Err(Errno::NOENT | Errno::NOTDIR | Errno::LOOP | Errno::XDEV) => return Ok(false),
            Err(e) => return failure("open", e, Denier::Directory),
        };
        match fsops::stat(file.as_fd()) {
            Ok(meta) => Ok(meta.kind == FileType::RegularFile),
            Err(e) => failure("statx", e, Denier::File),
        }
    }

    /// True when an `exclude` pattern of the copy's root matches the entry
    /// at `rel_path` or a directory above it, below the root. A scan
    /// matches the patterns against each entry that it lists, and does not
    /// enter an excluded directory, so it would not reach such an entry
    /// (DESIGN.md §4.2). Absolute patterns match the path of the entry in
    /// this copy. The root itself is not matched here: root expansion has
    /// matched it already.
    fn excludes(&self, rel_path: &[u8]) -> bool {
        // The one path of a single-file root is empty, and names the root.
        if rel_path.is_empty() {
            return false;
        }
        let mut end = 0;
        for name in rel_path.split(|&b| b == b'/') {
            end += name.len();
            let path = || roots::join(&self.location.path, &rel_path[..end]);
            if self.config.exclude.excludes(name, path) {
                return true;
            }
            end += 1;
        }
        false
    }
}

/// Open the root of a copy in the directory that root expansion left open,
/// as a scan task opens it (DESIGN.md §4.5), and say in a warning when it
/// fails, or when it is missing and may not vanish (`may_vanish`): once for
/// all the lookups of the root. `config` configures the copy's root.
fn open_root(location: &Location, config: &RootConfig) -> CopyRoot {
    let shown = || quoted(&location.path);
    let root = match fsops::open_path(location.dir.as_fd(), &location.name) {
        Ok(root) => root,
        // A copy that vanished after root expansion gets the warning that
        // root expansion gives a copy that was gone before it, as in the
        // scan of an update (DESIGN.md §4.2).
        Err(Errno::NOENT) => {
            if !config.may_vanish {
                warn(format!("{}: root does not exist", shown()));
            }
            return CopyRoot::Missing;
        }
        Err(e) => {
            warn(format!("{}: open: {e}", shown()));
            return CopyRoot::Failed;
        }
    };
    match fsops::stat(root.as_fd()) {
        Ok(meta) if meta.kind == FileType::Directory => CopyRoot::Dir(CopyDir {
            fd: root,
            one_filesystem: config.one_filesystem,
            failed: Mutex::default(),
        }),
        Ok(meta) if meta.kind == FileType::RegularFile => CopyRoot::File,
        Ok(_) => {
            warn(format!(
                "{}: root is not a directory or a regular file",
                shown()
            ));
            CopyRoot::Failed
        }
        Err(e) => {
            warn(format!("{}: statx: {e}", shown()));
            CopyRoot::Failed
        }
    }
}

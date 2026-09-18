//! Root expansion and validation (DESIGN.md §4.2).
//!
//! Root patterns are expanded by descriptor-relative walking that never
//! follows symlinks. A pattern without wildcards names its root as written.
//! Otherwise names come from directory listings, and a literal name after a
//! wildcard is looked up for existence only. A match is not examined: its
//! `Location` keeps the directory holding it open, and the root's scan task
//! opens and classifies the match when it starts (DESIGN.md §4.6). Only the
//! directories that a pattern continues into are opened here.

use std::collections::HashMap;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::Arc;

use bstr::ByteSlice;
use rustix::fs::{Dir, FileType};
use rustix::io::Errno;

use crate::config::{Config, RootConfig};
use crate::die::{EXIT_FAILURE, die, warn};
use crate::fsid::{FsCache, Identity, identify};
use crate::fsops::{self, Meta};
use crate::glob::{self, Component};

/// One current location of a configured root: a directory entry whose
/// name matched the pattern. Its scan task finds out whether it is a
/// directory, a regular file, or not a root at all.
pub struct Location {
    /// Index of the `[[roots]]` entry.
    pub root: usize,
    /// Absolute path bytes, normalized.
    pub path: Vec<u8>,
    /// The directory holding the root, shared by every root inside it.
    pub dir: Arc<OwnedFd>,
    /// The root's name in `dir`; `.` when the root is `/` itself.
    pub name: Vec<u8>,
}

/// Expand every root pattern. Dies on duplicate or nested roots.
pub fn expand(config: &Config, fs_cache: &FsCache) -> Vec<Location> {
    let mut locations = Vec::new();
    for (index, root) in config.roots.iter().enumerate() {
        let components: Vec<Component> = glob::components(&root.pattern)
            .map(glob::classify)
            .collect();
        let before = locations.len();
        let mut walker = Walker {
            fs_cache,
            root,
            root_index: index,
            exact: components
                .iter()
                .all(|c| matches!(c, Component::Literal(_))),
            locations: &mut locations,
            visited: HashMap::new(),
        };
        match open_slash() {
            Ok(node) => match components.split_first() {
                None => walker.emit(&node, b".", node.path.clone()),
                Some((first, rest)) => walker.walk(&node, first, rest),
            },
            Err(e) => walker.error(b"/", "open", e),
        }
        if locations.len() == before {
            warn(format!(
                "root pattern {} matched nothing",
                root.pattern.as_bstr()
            ));
        }
    }
    check_nesting(&locations, config);
    locations
}

/// An opened directory during expansion.
struct Node {
    fd: Arc<OwnedFd>,
    meta: Meta,
    path: Vec<u8>,
}

impl Node {
    fn child_path(&self, name: &[u8]) -> Vec<u8> {
        join(&self.path, name)
    }
}

/// `base/name`, where `base` is an absolute path.
pub fn join(base: &[u8], name: &[u8]) -> Vec<u8> {
    let mut path = Vec::with_capacity(base.len() + 1 + name.len());
    path.extend_from_slice(base);
    if base != b"/" {
        path.push(b'/');
    }
    path.extend_from_slice(name);
    path
}

fn open_slash() -> Result<Node, Errno> {
    let fd = fsops::open_root_dir()?;
    let meta = fsops::stat(fd.as_fd())?;
    Ok(Node {
        fd: Arc::new(fd),
        meta,
        path: b"/".to_vec(),
    })
}

struct Walker<'a> {
    fs_cache: &'a FsCache,
    root: &'a RootConfig,
    root_index: usize,
    /// The pattern has no wildcards, so it names one root, existing or not.
    exact: bool,
    locations: &'a mut Vec<Location>,
    /// Directories entered under `**`, for cycle detection.
    visited: HashMap<Identity, Vec<u8>>,
}

impl Walker<'_> {
    /// Report an entry that cannot be examined, and continue.
    fn error(&mut self, path: &[u8], what: &str, e: Errno) {
        warn(format!("{}: {what}: {e}", path.as_bstr()));
    }

    /// Match `first`, then `rest`, against the entries of `node`.
    fn walk(&mut self, node: &Node, first: &Component, rest: &[Component]) {
        match first {
            Component::Literal(name) => match self.literal_matches(node, name, rest) {
                Ok(true) => self.consider(node, name, rest, false),
                Ok(false) => {}
                Err(e) => self.error(&node.child_path(name), "stat", e),
            },
            Component::Wild(pattern) => {
                self.for_each_entry(node, |walker, name| {
                    if glob::matches(pattern, name) {
                        walker.consider(node, name, rest, false);
                    }
                });
            }
            Component::AnyDepth => {
                if let Some((next, rest)) = rest.split_first() {
                    // Zero directories.
                    self.walk(node, next, rest);
                }
                self.for_each_entry(node, |walker, name| {
                    walker.consider(node, name, rest, true);
                });
            }
        }
    }

    /// Whether the literal component `name` matches under `node`. Only the
    /// final literal of a wildcard pattern is looked up: it stands where a
    /// listing stands for a wildcard and decides which names the pattern
    /// matches, or `/srv/**/data` would name a root under every directory.
    /// It decides nothing about the entry itself; the scan task opens the
    /// root and classifies it from its own descriptor. An exact pattern
    /// names its root as written, and a literal that the pattern continues
    /// into is opened by `descend`, which finds out whether it exists.
    fn literal_matches(&self, node: &Node, name: &[u8], rest: &[Component]) -> Result<bool, Errno> {
        if self.exact || !rest.is_empty() {
            return Ok(true);
        }
        fsops::entry_exists(node.fd.as_fd(), name)
    }

    /// Handle the entry `name` of `parent`. `rest` is what follows it in
    /// the pattern; `any_depth` says that it was reached under `**`.
    fn consider(&mut self, parent: &Node, name: &[u8], rest: &[Component], any_depth: bool) {
        let path = parent.child_path(name);
        if any_depth || !rest.is_empty() {
            self.descend(parent, name, &path, rest, any_depth);
        }
        if rest.is_empty() {
            self.emit(parent, name, path);
        }
    }

    /// Continue the pattern inside the entry `name` of `parent`, if it is
    /// a directory.
    fn descend(
        &mut self,
        parent: &Node,
        name: &[u8],
        path: &[u8],
        rest: &[Component],
        any_depth: bool,
    ) {
        let fd = match fsops::open_path(parent.fd.as_fd(), name) {
            Ok(fd) => fd,
            Err(Errno::NOENT) => return,
            Err(e) => return self.error(path, "open", e),
        };
        let meta = match fsops::stat(fd.as_fd()) {
            Ok(meta) => meta,
            Err(e) => return self.error(path, "statx", e),
        };
        if meta.kind != FileType::Directory {
            return;
        }
        let node = Node {
            fd: Arc::new(fd),
            meta,
            path: path.to_vec(),
        };
        if any_depth {
            if self.enter_any_depth(&node) {
                self.walk(&node, &Component::AnyDepth, rest);
            }
        } else if let Some((next, rest)) = rest.split_first() {
            self.walk(&node, next, rest);
        }
    }

    /// Record a directory entered under `**`; dies on a repeated identity.
    fn enter_any_depth(&mut self, node: &Node) -> bool {
        let identity = match identify(self.fs_cache, node.fd.as_fd(), &node.meta) {
            Ok(identity) => identity,
            Err(e) => {
                self.error(&node.path, "identity", e);
                return false;
            }
        };
        if let Some(earlier) = self.visited.insert(identity, node.path.clone()) {
            die(
                EXIT_FAILURE,
                format!(
                    "root pattern {}: {} and {} are the same directory (cycle or mount alias)",
                    self.root.pattern.as_bstr(),
                    earlier.as_bstr(),
                    node.path.as_bstr()
                ),
            );
        }
        true
    }

    /// Call `f` with the name of every entry of `node` that can be a root
    /// or lead to one.
    fn for_each_entry(&mut self, node: &Node, mut f: impl FnMut(&mut Self, &[u8])) {
        let readable = match fsops::reopen_dir(node.fd.as_fd()) {
            Ok(fd) => fd,
            Err(e) => return self.error(&node.path, "reopen", e),
        };
        let dir = match Dir::new(readable) {
            Ok(dir) => dir,
            Err(e) => return self.error(&node.path, "opendir", e),
        };
        for entry in dir {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => return self.error(&node.path, "readdir", e),
            };
            let name = entry.file_name().to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            // Directory-entry types are hints; they only skip entries
            // that can never be roots without examining them.
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
            f(self, name);
        }
    }

    fn excluded(&self, path: &[u8]) -> bool {
        let name = path.rsplit(|&b| b == b'/').next().unwrap_or(path);
        self.root.exclude.excludes(name, || path.to_vec())
    }

    /// Record the match `name` of `parent`, unless it is excluded.
    fn emit(&mut self, parent: &Node, name: &[u8], path: Vec<u8>) {
        if self.excluded(&path) {
            return;
        }
        self.locations.push(Location {
            root: self.root_index,
            path,
            dir: Arc::clone(&parent.fd),
            name: name.to_vec(),
        });
    }
}

/// Reject duplicate and nested root paths. The check is lexical.
fn check_nesting(locations: &[Location], config: &Config) {
    let mut by_path: HashMap<&[u8], &Location> = HashMap::with_capacity(locations.len());
    for location in locations {
        if let Some(earlier) = by_path.insert(&location.path, location) {
            die(
                EXIT_FAILURE,
                format!(
                    "{} is matched by both root patterns {} and {}",
                    location.path.as_bstr(),
                    config.roots[earlier.root].pattern.as_bstr(),
                    config.roots[location.root].pattern.as_bstr()
                ),
            );
        }
    }
    for location in locations {
        let path = &location.path;
        let ancestors = path
            .iter()
            .enumerate()
            .skip(1)
            .filter(|&(_, &b)| b == b'/')
            .map(|(i, _)| &path[..i])
            .chain((path.len() > 1).then_some(&b"/"[..]));
        for ancestor in ancestors {
            if let Some(outer) = by_path.get(ancestor) {
                die(
                    EXIT_FAILURE,
                    format!(
                        "root {} is inside root {}",
                        path.as_bstr(),
                        outer.path.as_bstr()
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_paths() {
        assert_eq!(join(b"/", b"a"), b"/a");
        assert_eq!(join(b"/a", b"b"), b"/a/b");
    }
}

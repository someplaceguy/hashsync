//! Root expansion, validation, and registration (DESIGN.md §4.2 and §4.5).
//!
//! Root patterns are expanded by descriptor-relative walking that never
//! follows symlinks. Each match becomes a `Location` with the identity of
//! its object, so that traversal can later check that the location is
//! unchanged and detect entry into another root through a mount alias.

use std::collections::HashMap;
use std::os::fd::{AsFd, OwnedFd};

use bstr::ByteSlice;
use rustix::fs::{Dir, FileType};
use rustix::io::Errno;

use crate::config::{Config, RootConfig};
use crate::die::{EXIT_FAILURE, die, warn};
use crate::fsid::{FsCache, Identity, identify};
use crate::fsops::{self, Meta};
use crate::glob::{self, Component};

/// One current location of a configured root.
pub struct Location {
    /// Index of the `[[roots]]` entry.
    pub root: usize,
    /// Absolute path bytes, normalized.
    pub path: Vec<u8>,
    pub kind: LocationKind,
}

pub enum LocationKind {
    Directory {
        identity: Identity,
    },
    File {
        identity: Identity,
        /// Identity of the directory holding the entry.
        parent: Identity,
        /// The entry's basename.
        name: Vec<u8>,
    },
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
            locations: &mut locations,
            visited: HashMap::new(),
        };
        match open_slash() {
            Ok(node) => walker.walk(&node, &components),
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
    fd: OwnedFd,
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
        fd,
        meta,
        path: b"/".to_vec(),
    })
}

struct Walker<'a> {
    fs_cache: &'a FsCache,
    root: &'a RootConfig,
    root_index: usize,
    locations: &'a mut Vec<Location>,
    /// Directories entered under `**`, for cycle detection.
    visited: HashMap<Identity, Vec<u8>>,
}

impl Walker<'_> {
    /// Report an entry that cannot be examined, and continue.
    fn error(&mut self, path: &[u8], what: &str, e: Errno) {
        warn(format!("{}: {what}: {e}", path.as_bstr()));
    }

    fn walk(&mut self, node: &Node, components: &[Component]) {
        match components.first() {
            None => self.emit_directory(node),
            Some(Component::Literal(name)) => match fsops::open_path(node.fd.as_fd(), name) {
                Ok(fd) => self.consider(node, name, fd, &components[1..], None),
                Err(Errno::NOENT) => {}
                Err(e) => self.error(&node.child_path(name), "open", e),
            },
            Some(Component::Wild(pattern)) => {
                self.for_each_entry(node, |walker, name, fd| {
                    if glob::matches(pattern, name) {
                        walker.consider(node, name, fd, &components[1..], None);
                    }
                });
            }
            Some(Component::AnyDepth) => {
                let rest = &components[1..];
                if !rest.is_empty() {
                    // Zero directories.
                    self.walk(node, rest);
                }
                self.for_each_entry(node, |walker, name, fd| {
                    walker.consider(node, name, fd, rest, Some(components));
                });
            }
        }
    }

    /// Classify an opened entry. `rest` is what follows it in the pattern;
    /// `any_depth` is the pattern from its `**` when the entry was reached
    /// under one.
    fn consider(
        &mut self,
        parent: &Node,
        name: &[u8],
        fd: OwnedFd,
        rest: &[Component],
        any_depth: Option<&[Component]>,
    ) {
        let path = parent.child_path(name);
        let meta = match fsops::stat(fd.as_fd()) {
            Ok(meta) => meta,
            Err(e) => return self.error(&path, "statx", e),
        };
        match meta.kind {
            FileType::Directory => {
                let child = Node { fd, meta, path };
                match any_depth {
                    Some(components) => {
                        if rest.is_empty() {
                            self.emit_directory(&child);
                        }
                        if self.enter_any_depth(&child) {
                            self.walk(&child, components);
                        }
                    }
                    None if rest.is_empty() => self.emit_directory(&child),
                    None => self.walk(&child, rest),
                }
            }
            FileType::RegularFile if rest.is_empty() => {
                self.emit_file(parent, name, fd, &meta, path)
            }
            _ => {}
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

    fn for_each_entry(&mut self, node: &Node, mut f: impl FnMut(&mut Self, &[u8], OwnedFd)) {
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
            // that can never be roots without opening them.
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
            match fsops::open_path(node.fd.as_fd(), name) {
                Ok(fd) => f(self, name, fd),
                Err(Errno::NOENT) => {}
                Err(e) => self.error(&node.child_path(name), "open", e),
            }
        }
    }

    fn excluded(&self, path: &[u8]) -> bool {
        let name = path.rsplit(|&b| b == b'/').next().unwrap_or(path);
        self.root.exclude.excludes(name, || path.to_vec())
    }

    fn emit_directory(&mut self, node: &Node) {
        if self.excluded(&node.path) {
            return;
        }
        match identify(self.fs_cache, node.fd.as_fd(), &node.meta) {
            Ok(identity) => self.locations.push(Location {
                root: self.root_index,
                path: node.path.clone(),
                kind: LocationKind::Directory { identity },
            }),
            Err(e) => self.error(&node.path, "identity", e),
        }
    }

    fn emit_file(&mut self, parent: &Node, name: &[u8], fd: OwnedFd, meta: &Meta, path: Vec<u8>) {
        if self.excluded(&path) {
            return;
        }
        let identity = match identify(self.fs_cache, fd.as_fd(), meta) {
            Ok(identity) => identity,
            Err(e) => return self.error(&path, "identity", e),
        };
        let parent_identity = match identify(self.fs_cache, parent.fd.as_fd(), &parent.meta) {
            Ok(identity) => identity,
            Err(e) => return self.error(&parent.path, "identity", e),
        };
        self.locations.push(Location {
            root: self.root_index,
            path,
            kind: LocationKind::File {
                identity,
                parent: parent_identity,
                name: name.to_vec(),
            },
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

/// Registered root locations, for detecting entry into another root
/// during traversal.
pub struct Registry {
    directories: HashMap<Identity, usize>,
    /// Single-file roots by parent directory identity, then basename.
    files: HashMap<Identity, HashMap<Vec<u8>, usize>>,
}

impl Registry {
    /// Dies when two locations are the same directory.
    pub fn new(locations: &[Location]) -> Self {
        let mut directories = HashMap::new();
        let mut files: HashMap<Identity, HashMap<Vec<u8>, usize>> = HashMap::new();
        for (index, location) in locations.iter().enumerate() {
            let earlier = match &location.kind {
                LocationKind::Directory { identity } => directories.insert(identity.clone(), index),
                LocationKind::File { parent, name, .. } => files
                    .entry(parent.clone())
                    .or_default()
                    .insert(name.clone(), index),
            };
            if let Some(earlier) = earlier {
                die(
                    EXIT_FAILURE,
                    format!(
                        "roots {} and {} are the same object (mount alias)",
                        locations[earlier].path.as_bstr(),
                        location.path.as_bstr()
                    ),
                );
            }
        }
        Registry { directories, files }
    }

    /// The location index registered for a directory identity.
    pub fn directory_owner(&self, identity: &Identity) -> Option<usize> {
        self.directories.get(identity).copied()
    }

    /// The location index registered for the entry `name` of the directory
    /// with identity `parent`.
    pub fn file_owner(&self, parent: &Identity, name: &[u8]) -> Option<usize> {
        self.files.get(parent)?.get(name).copied()
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

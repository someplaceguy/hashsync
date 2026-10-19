//! Root expansion and validation (DESIGN.md §4.2).
//!
//! Root patterns are expanded by descriptor-relative walking that never
//! follows symlinks. The walk enters every directory once per pattern and
//! carries the set of pattern positions that can be active there, so a
//! pattern with several `**` components neither lists a directory twice nor
//! mistakes the second visit for a cycle. A pattern without wildcards names
//! its root as written. Otherwise names come from directory listings. A
//! literal name is looked up instead, for existence only, when no wildcard
//! is active in its directory. A directory that cannot be listed is
//! abandoned with its contents (DESIGN.md §4.5). A match is not examined:
//! its `Location` keeps the directory holding it open, and the root's scan
//! task opens and classifies the match when it starts (DESIGN.md §4.6).
//! Only the directories that a pattern continues into are opened here. A
//! directory is identified before it is listed under `**`, so that a cycle
//! is fatal; one that cannot be identified is abandoned too. Exclusions
//! apply to every name that a wildcard pattern selects and to the matches
//! of every pattern; an excluded directory is not entered.

use std::collections::HashMap;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::Arc;

use bstr::ByteSlice;
use rustix::fs::FileType;
use rustix::io::Errno;

use crate::config::{Config, RootConfig};
use crate::die::die;
use crate::fsid::{FsCache, Identity, identify};
use crate::fsops::{self, Meta};
use crate::glob::{self, Component};
use crate::report::warn;

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

/// Positions in a pattern's component list, sorted and without duplicates.
/// Position `i` means that the components before `i` matched the path so
/// far; the position after the last component means that the whole pattern
/// matched.
type Positions = Vec<usize>;

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
            components: &components,
            exact: components
                .iter()
                .all(|c| matches!(c, Component::Literal(_))),
            locations: &mut locations,
            visited: HashMap::new(),
        };
        match open_slash() {
            Ok(node) if components.is_empty() => {
                // `/` itself is not an entry of any directory: it has no
                // name for basename exclusions, but an absolute exclusion
                // can match it.
                if !walker.excluded(b"", b"/") {
                    walker.emit(&node, b".", b"/");
                }
            }
            Ok(node) => {
                let mut positions = vec![0];
                close(&components, &mut positions);
                walker.visit(&node, &positions);
            }
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

/// The positions reachable from `positions` by matching one path
/// component `name`.
fn step(components: &[Component], positions: &[usize], name: &[u8]) -> Positions {
    let mut next = Vec::with_capacity(positions.len() + 1);
    for &i in positions {
        match components.get(i) {
            Some(Component::Literal(literal)) if literal == name => next.push(i + 1),
            Some(Component::Wild(pattern)) if glob::matches(pattern, name) => next.push(i + 1),
            // `**` took this name: it can take more, or it is complete.
            Some(Component::AnyDepth) => next.extend([i, i + 1]),
            _ => {}
        }
    }
    close(components, &mut next);
    next
}

/// Add the position after every `**` that another component follows, since
/// such a `**` also matches zero directories; then sort and remove
/// duplicates. A trailing `**` matches everything inside a directory and
/// never the directory itself, so it must take at least one name.
fn close(components: &[Component], positions: &mut Positions) {
    let mut i = 0;
    while i < positions.len() {
        let p = positions[i];
        if components.get(p) == Some(&Component::AnyDepth)
            && p + 1 < components.len()
            && !positions.contains(&(p + 1))
        {
            positions.push(p + 1);
        }
        i += 1;
    }
    positions.sort_unstable();
    positions.dedup();
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
    components: &'a [Component],
    /// The pattern has no wildcards, so it names one root, existing or not.
    exact: bool,
    locations: &'a mut Vec<Location>,
    /// Directories listed under `**`, for cycle detection.
    visited: HashMap<Identity, Vec<u8>>,
}

impl Walker<'_> {
    /// Report an entry that cannot be examined, and continue.
    fn error(&mut self, path: &[u8], what: &str, e: Errno) {
        warn(format!("{}: {what}: {e}", path.as_bstr()));
    }

    /// Match the pattern positions against the entries of `node`.
    fn visit(&mut self, node: &Node, positions: &[usize]) {
        let components = self.components;
        let mut wild = false;
        let mut any_depth = false;
        for &i in positions {
            match components.get(i) {
                Some(Component::Literal(_)) | None => {}
                Some(Component::Wild(_)) => wild = true,
                Some(Component::AnyDepth) => {
                    wild = true;
                    any_depth = true;
                }
            }
        }
        if !wild {
            // Only literal names can continue: try them without a listing.
            for &i in positions {
                if let Some(Component::Literal(name)) = components.get(i) {
                    self.consider(node, name, positions, false);
                }
            }
            return;
        }
        // `**` stays active below a directory where it is active, so the
        // walk recurses without bound there and a cycle is fatal: record
        // the directory before listing it.
        if any_depth && !self.enter_any_depth(node) {
            return;
        }
        // The listing supplies the names; a literal position matches a
        // listed name and needs no lookup of its own. A listing failure
        // is reported and abandons the entries not yet listed.
        let entries = match fsops::list_dir(node.fd.as_fd()) {
            Ok(entries) => entries,
            Err(e) => return self.error(&node.path, "reopen", e),
        };
        for entry in entries {
            match entry {
                Ok(entry) => self.consider(node, entry.file_name().to_bytes(), positions, true),
                Err(e) => return self.error(&node.path, "readdir", e),
            }
        }
    }

    /// Handle the entry `name` of `node`. `listed` says that the name came
    /// from a directory listing, so the entry exists.
    fn consider(&mut self, node: &Node, name: &[u8], positions: &[usize], listed: bool) {
        let next = step(self.components, positions, name);
        let end = self.components.len();
        let matched = next.contains(&end);
        let continues = next.iter().any(|&i| i < end);
        if !matched && !continues {
            return;
        }
        let path = node.child_path(name);
        // A wildcard pattern selects the names it matches, and an excluded
        // name is not selected, neither as a root nor as a directory to
        // enter. An exact pattern names its root as written; only that
        // root can be excluded.
        if (matched || !self.exact) && self.excluded(name, &path) {
            return;
        }
        if matched {
            // A listed name exists, and an exact pattern names its root
            // whether it exists or not. Only the final literal of a
            // wildcard pattern is looked up: the lookup decides whether
            // the pattern matches here, as a listing decides for a
            // wildcard, or `/srv/**/data` would name a root under every
            // directory. It decides nothing about the entry itself; the
            // scan task opens the root and classifies it from its own
            // descriptor.
            let exists = if listed || self.exact {
                Ok(true)
            } else {
                fsops::entry_exists(node.fd.as_fd(), name)
            };
            match exists {
                Ok(true) => self.emit(node, name, &path),
                Ok(false) => {}
                Err(e) => self.error(&path, "stat", e),
            }
        }
        if continues && let Some(child) = self.open_dir(node, name, path) {
            self.visit(&child, &next);
        }
    }

    /// Open the entry `name` of `parent` as a directory. `None` when it is
    /// not a directory, has vanished, or cannot be examined.
    fn open_dir(&mut self, parent: &Node, name: &[u8], path: Vec<u8>) -> Option<Node> {
        let fd = match fsops::open_path(parent.fd.as_fd(), name) {
            Ok(fd) => fd,
            Err(Errno::NOENT) => return None,
            Err(e) => {
                self.error(&path, "open", e);
                return None;
            }
        };
        let meta = match fsops::stat(fd.as_fd()) {
            Ok(meta) => meta,
            Err(e) => {
                self.error(&path, "statx", e);
                return None;
            }
        };
        (meta.kind == FileType::Directory).then(|| Node {
            fd: Arc::new(fd),
            meta,
            path,
        })
    }

    /// Record a directory before it is listed under `**`; dies on a
    /// repeated identity. A directory without an identity is abandoned.
    fn enter_any_depth(&mut self, node: &Node) -> bool {
        let identity = match identify(self.fs_cache, node.fd.as_fd(), &node.meta) {
            Ok(identity) => identity,
            Err(e) => {
                self.error(&node.path, "identity", e);
                return false;
            }
        };
        if let Some(earlier) = self.visited.insert(identity, node.path.clone()) {
            die(format!(
                "root pattern {}: {} and {} are the same directory (cycle or mount alias)",
                self.root.pattern.as_bstr(),
                earlier.as_bstr(),
                node.path.as_bstr()
            ));
        }
        true
    }

    fn excluded(&self, name: &[u8], path: &[u8]) -> bool {
        self.root.exclude.excludes(name, || path.to_vec())
    }

    /// Record the match `name` of `parent`.
    fn emit(&mut self, parent: &Node, name: &[u8], path: &[u8]) {
        self.locations.push(Location {
            root: self.root_index,
            path: path.to_vec(),
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
            die(format!(
                "{} is matched by both root patterns {} and {}",
                location.path.as_bstr(),
                config.roots[earlier.root].pattern.as_bstr(),
                config.roots[location.root].pattern.as_bstr()
            ));
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
                die(format!(
                    "root {} is inside root {}",
                    path.as_bstr(),
                    outer.path.as_bstr()
                ));
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

    fn pattern(text: &str) -> Vec<Component> {
        glob::components(text.as_bytes())
            .map(glob::classify)
            .collect()
    }

    fn start(components: &[Component]) -> Positions {
        let mut positions = vec![0];
        close(components, &mut positions);
        positions
    }

    #[test]
    fn positions_follow_the_pattern() {
        let p = pattern("/srv/**/data");
        let at_srv = step(&p, &start(&p), b"srv");
        assert_eq!(at_srv, vec![1, 2]);
        assert_eq!(step(&p, &at_srv, b"x"), vec![1, 2]);
        assert_eq!(step(&p, &at_srv, b"data"), vec![1, 2, 3]);
        assert_eq!(step(&p, &start(&p), b"other"), Vec::<usize>::new());
    }

    #[test]
    fn repeated_any_depth_merges_into_one_position_set() {
        let p = pattern("/**/**/x");
        assert_eq!(start(&p), vec![0, 1, 2]);
        assert_eq!(step(&p, &start(&p), b"y"), vec![0, 1, 2]);
        assert_eq!(step(&p, &start(&p), b"x"), vec![0, 1, 2, 3]);

        let p = pattern("/t/**/a/**/leaf");
        let at_t = step(&p, &start(&p), b"t");
        let at_a = step(&p, &at_t, b"a");
        assert_eq!(at_a, vec![1, 2, 3, 4]);
        assert_eq!(step(&p, &at_a, b"b"), vec![1, 2, 3, 4]);
        assert_eq!(step(&p, &at_a, b"leaf"), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn trailing_any_depth_matches_every_descendant_but_not_the_directory() {
        let p = pattern("/a/**");
        let at_a = step(&p, &start(&p), b"a");
        assert_eq!(at_a, vec![1]);
        let at_b = step(&p, &at_a, b"b");
        assert_eq!(at_b, vec![1, 2]);
        assert_eq!(step(&p, &at_b, b"c"), vec![1, 2]);
        assert_eq!(start(&pattern("/**")), vec![0]);
    }
}

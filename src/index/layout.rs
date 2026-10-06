//! The keys and values of the index's column families (DESIGN.md §4.4).
//!
//! Integers are big endian, so the byte order of keys is their numeric
//! order. A malformed entry is a database error, which panics
//! (DESIGN.md §6.7).

use crate::hash::PREFIX_LEN;

/// The column families of the index, in the order of DESIGN.md §4.4.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cf {
    /// The metadata, by name. It is RocksDB's `default` column family.
    Meta,
    Roots,
    Inodes,
    Identities,
    Paths,
    InodePaths,
    Blocks,
    Strong,
}

impl Cf {
    pub const ALL: [Cf; 8] = [
        Cf::Meta,
        Cf::Roots,
        Cf::Inodes,
        Cf::Identities,
        Cf::Paths,
        Cf::InodePaths,
        Cf::Blocks,
        Cf::Strong,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Cf::Meta => "default",
            Cf::Roots => "roots",
            Cf::Inodes => "inodes",
            Cf::Identities => "identities",
            Cf::Paths => "paths",
            Cf::InodePaths => "inode_paths",
            Cf::Blocks => "blocks",
            Cf::Strong => "strong",
        }
    }
}

/// The length of an encoded ID or block number.
pub const ID_LEN: usize = 8;

/// The big-endian `u64` at `at` in `bytes`.
pub fn u64_at(bytes: &[u8], at: usize) -> u64 {
    match bytes.get(at..at + ID_LEN) {
        Some(field) => u64::from_be_bytes(field.try_into().unwrap()),
        None => panic!("database error: an index entry is too short"),
    }
}

/// The key of a root or inode row, which is also the prefix of the
/// entries of that root in `paths` and of that inode in `inode_paths` and
/// `blocks`.
pub fn id_key(id: u64) -> [u8; ID_LEN] {
    id.to_be_bytes()
}

/// An `identities` key: the length of the filesystem ID as one byte, the
/// filesystem ID, and the inode handle. `inodes` values end with it.
pub fn identity(fsid: &[u8], handle: &[u8]) -> Vec<u8> {
    let len = u8::try_from(fsid.len()).expect("a filesystem ID is longer than 255 bytes");
    let mut key = Vec::with_capacity(1 + fsid.len() + handle.len());
    key.push(len);
    key.extend_from_slice(fsid);
    key.extend_from_slice(handle);
    key
}

/// The filesystem ID in an `identities` key.
pub fn identity_fsid(identity: &[u8]) -> &[u8] {
    let len = usize::from(*identity.first().expect("database error: an empty identity"));
    match identity.get(1..1 + len) {
        Some(fsid) => fsid,
        None => panic!("database error: an identity is too short"),
    }
}

/// An `inodes` value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InodeRow {
    /// `stx_size` observed before hashing; `None` while processing is
    /// incomplete.
    pub size: Option<u64>,
    pub ctime_ns: i64,
    /// The `identities` key of the inode.
    pub identity: Vec<u8>,
}

/// The state byte of an `inodes` value.
const INCOMPLETE: u8 = 0;
const COMPLETE: u8 = 1;

impl InodeRow {
    /// A state byte (0 incomplete, 1 complete), the size (0 while
    /// incomplete), the ctime as the bits of an `i64`, and the identity.
    pub fn encode(&self) -> Vec<u8> {
        let mut value = Vec::with_capacity(1 + 2 * ID_LEN + self.identity.len());
        value.push(if self.size.is_some() {
            COMPLETE
        } else {
            INCOMPLETE
        });
        value.extend_from_slice(&self.size.unwrap_or(0).to_be_bytes());
        value.extend_from_slice(&self.ctime_ns.to_be_bytes());
        value.extend_from_slice(&self.identity);
        value
    }

    pub fn decode(value: &[u8]) -> InodeRow {
        let size = u64_at(value, 1);
        InodeRow {
            size: match value[0] {
                COMPLETE => Some(size),
                INCOMPLETE => None,
                state => panic!("database error: inode state {state}"),
            },
            ctime_ns: u64_at(value, 1 + ID_LEN) as i64,
            identity: value[1 + 2 * ID_LEN..].to_vec(),
        }
    }
}

/// A `roots` value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootRow {
    /// Present only for a completely indexed immutable group
    /// (DESIGN.md §4.2). Identifiers are never empty.
    pub identifier: Option<Vec<u8>>,
    /// One representative location.
    pub path: Vec<u8>,
}

impl RootRow {
    /// The identifier's length as a `u32` (0 for no identifier), the
    /// identifier, and the path.
    pub fn encode(&self) -> Vec<u8> {
        let identifier = self.identifier.as_deref().unwrap_or_default();
        assert!(
            self.identifier.is_none() || !identifier.is_empty(),
            "an empty root identifier"
        );
        let len = u32::try_from(identifier.len()).expect("a root identifier is too long");
        let mut value = Vec::with_capacity(4 + identifier.len() + self.path.len());
        value.extend_from_slice(&len.to_be_bytes());
        value.extend_from_slice(identifier);
        value.extend_from_slice(&self.path);
        value
    }

    pub fn decode(value: &[u8]) -> RootRow {
        let Some((len, rest)) = value.split_first_chunk::<4>() else {
            panic!("database error: a root entry is too short");
        };
        let len = u32::from_be_bytes(*len) as usize;
        let Some((identifier, path)) = rest.split_at_checked(len) else {
            panic!("database error: a root identifier is too long");
        };
        RootRow {
            identifier: (len > 0).then(|| identifier.to_vec()),
            path: path.to_vec(),
        }
    }
}

/// A `paths` key: the root ID and the relative path. The value is the
/// inode ID.
pub fn path_key(root_id: u64, rel_path: &[u8]) -> Vec<u8> {
    [&id_key(root_id)[..], rel_path].concat()
}

/// An `inode_paths` key: the inode ID, the root ID, and the relative path.
/// The value is empty.
pub fn inode_path_key(inode_id: u64, root_id: u64, rel_path: &[u8]) -> Vec<u8> {
    [&id_key(inode_id)[..], &id_key(root_id)[..], rel_path].concat()
}

/// A `blocks` key: the inode ID and the block number.
pub fn block_key(inode_id: u64, block_no: u64) -> [u8; 2 * ID_LEN] {
    let mut key = [0; 2 * ID_LEN];
    key[..ID_LEN].copy_from_slice(&id_key(inode_id));
    key[ID_LEN..].copy_from_slice(&block_no.to_be_bytes());
    key
}

/// A `blocks` value: the weak hash and the strong-hash prefix.
pub fn block_value(weak: u64, strong_prefix: &[u8; PREFIX_LEN]) -> [u8; ID_LEN + PREFIX_LEN] {
    let mut value = [0; ID_LEN + PREFIX_LEN];
    value[..ID_LEN].copy_from_slice(&weak.to_be_bytes());
    value[ID_LEN..].copy_from_slice(strong_prefix);
    value
}

/// The strong-hash prefix in a `blocks` value.
pub fn block_strong_prefix(value: &[u8]) -> &[u8; PREFIX_LEN] {
    match value.get(ID_LEN..ID_LEN + PREFIX_LEN) {
        Some(prefix) => prefix.try_into().unwrap(),
        None => panic!("database error: a block entry is too short"),
    }
}

/// A `strong` key: the strong-hash prefix, the inode ID, and the block
/// number. The value is empty.
pub fn strong_key(
    strong_prefix: &[u8; PREFIX_LEN],
    inode_id: u64,
    block_no: u64,
) -> [u8; PREFIX_LEN + 2 * ID_LEN] {
    let mut key = [0; PREFIX_LEN + 2 * ID_LEN];
    key[..PREFIX_LEN].copy_from_slice(strong_prefix);
    key[PREFIX_LEN..].copy_from_slice(&block_key(inode_id, block_no));
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_survive_encoding() {
        for row in [
            InodeRow {
                size: Some(u64::MAX),
                ctime_ns: -5,
                identity: identity(b"\x01uuid", b"\x02handle"),
            },
            InodeRow {
                size: None,
                ctime_ns: i64::MAX,
                identity: identity(b"", b""),
            },
        ] {
            assert_eq!(InodeRow::decode(&row.encode()), row);
        }
        for row in [
            RootRow {
                identifier: Some(b"id\0".to_vec()),
                path: b"/r\xff".to_vec(),
            },
            RootRow {
                identifier: None,
                path: b"/".to_vec(),
            },
        ] {
            assert_eq!(RootRow::decode(&row.encode()), row);
        }
        assert_eq!(identity_fsid(&identity(b"\x02fsid", b"h")), b"\x02fsid");
    }

    /// Keys sort by their integers first: the entries of one root or
    /// inode are contiguous, and blocks follow their numbers.
    #[test]
    fn keys_sort_by_numbers() {
        assert!(block_key(1, u64::MAX) < block_key(2, 0));
        assert!(block_key(1, 255) < block_key(1, 256));
        assert!(path_key(1, b"\xff\xff") < path_key(2, b""));
        assert!(inode_path_key(1, 9, b"z") < inode_path_key(2, 0, b""));
        let prefix = [7; PREFIX_LEN];
        assert!(strong_key(&prefix, 1, 256) < strong_key(&prefix, 2, 0));
        assert_eq!(&strong_key(&prefix, 3, 4)[PREFIX_LEN..], &block_key(3, 4));
        assert_eq!(block_strong_prefix(&block_value(9, &prefix)), &prefix);
    }
}

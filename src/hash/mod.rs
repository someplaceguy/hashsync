//! Strong hashes (DESIGN.md §5.3) and the weak hash (§5.1–5.2).

pub mod weak;

use sha2::Digest;

/// Output length of every supported strong hash, in bytes.
pub const STRONG_LEN: usize = 32;

/// Length of the strong-hash prefix stored in the index, in bytes.
pub const PREFIX_LEN: usize = 16;

/// A full strong-hash digest.
pub type Strong = [u8; STRONG_LEN];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrongHash {
    Blake3,
    Sha256,
}

impl StrongHash {
    pub const DEFAULT: StrongHash = StrongHash::Blake3;

    /// Every supported strong hash, the default first. `from_id` looks names
    /// up here, so a hash missing from this list cannot be selected.
    pub const ALL: [StrongHash; 2] = [StrongHash::Blake3, StrongHash::Sha256];

    /// The algorithm name used in the index metadata and the protocol.
    pub fn id(self) -> &'static str {
        match self {
            StrongHash::Blake3 => "blake3",
            StrongHash::Sha256 => "sha256",
        }
    }

    /// The algorithm with this name; `None` for unsupported names.
    pub fn from_id(id: &str) -> Option<StrongHash> {
        Self::ALL.into_iter().find(|hash| hash.id() == id)
    }

    pub fn digest(self, data: &[u8]) -> Strong {
        match self {
            StrongHash::Blake3 => *blake3::hash(data).as_bytes(),
            StrongHash::Sha256 => sha2::Sha256::digest(data).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digests_are_full_length_and_differ_by_algorithm() {
        let data = b"hashsync";
        let b = StrongHash::Blake3.digest(data);
        let s = StrongHash::Sha256.digest(data);
        assert_eq!(b.len(), STRONG_LEN);
        assert_ne!(b, s);
        // Known SHA-256 test vector.
        let empty = StrongHash::Sha256.digest(b"");
        assert_eq!(
            empty[..4],
            [0xe3, 0xb0, 0xc4, 0x42],
            "SHA-256 of the empty string starts with e3b0c442"
        );
    }
}

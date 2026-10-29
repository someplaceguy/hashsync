// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! The names that this program makes: hexadecimal text, random bytes for
//! identities, tokens and names, and the names of temporary files
//! (DESIGN.md §11).

use rustix::io::retry_on_intr;

/// `bytes` as lowercase hexadecimal text, two digits for each byte.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Exactly `2 × N` hexadecimal digits, in either case, as `N` bytes.
pub fn parse_hex<const N: usize>(digits: &[u8]) -> Option<[u8; N]> {
    if digits.len() != 2 * N {
        return None;
    }
    let mut bytes = [0u8; N];
    for (byte, pair) in bytes.iter_mut().zip(digits.chunks_exact(2)) {
        // Unlike `u8::from_str_radix`, `to_digit` accepts no `+` sign.
        let high = char::from(pair[0]).to_digit(16)?;
        let low = char::from(pair[1]).to_digit(16)?;
        *byte = ((high << 4) | low) as u8;
    }
    Some(bytes)
}

/// `N` random bytes from the kernel.
pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    let mut filled = 0;
    // Reads up to 256 bytes are not split, but handle a short read anyway.
    while filled < N {
        let n = retry_on_intr(|| {
            rustix::rand::getrandom(&mut bytes[filled..], rustix::rand::GetRandomFlags::empty())
        })
        .unwrap_or_else(|e| panic!("getrandom: {e}"));
        filled += n;
    }
    bytes
}

/// The start of the name of every temporary file of this program.
const TEMPORARY_PREFIX: &str = ".hashsync-";

/// A new name for a temporary file: `.hashsync-` and 16 random
/// hexadecimal digits. Every temporary file of this program gets such a
/// name, so that a file that a failed run leaves shows its owner, and so
/// that this program can tell its temporaries apart from the hidden files
/// of other programs (`is_temporary`, DESIGN.md §11).
pub fn temporary_name() -> String {
    format!("{TEMPORARY_PREFIX}{}", hex(&random_bytes::<8>()))
}

/// True if `name` is the name of a temporary file of this program: it
/// starts with `.hashsync-`, as every name from `temporary_name` does.
///
/// This function exists so that this program removes only its own
/// temporaries, and no other hidden name: those that an exchange lists in
/// its peer/index directory, and those that expiry finds next to the
/// account ID and in the tmp_data directory. For example, an NFS client
/// gives a hidden `.nfs` name to a removed file that a process still has
/// open, and the removal of that name fails with EBUSY until the file
/// closes (DESIGN.md §5.6 and §6.5).
pub fn is_temporary(name: &[u8]) -> bool {
    name.starts_with(TEMPORARY_PREFIX.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_text_round_trips_and_parses_strictly() {
        let bytes = [0x00, 0x7f, 0xab, 0xff];
        assert_eq!(hex(&bytes), "007fabff");
        assert_eq!(parse_hex(hex(&bytes).as_bytes()), Some(bytes));
        assert_eq!(parse_hex(b"007FABFF"), Some(bytes), "either case");
        assert_eq!(parse_hex::<4>(b"007fabf"), None, "too short");
        assert_eq!(parse_hex::<4>(b"007fabff0"), None, "too long");
        assert_eq!(parse_hex::<4>(b"007fabfg"), None, "not a digit");
        // `u8::from_str_radix` alone would accept `+f` as 15.
        assert_eq!(parse_hex::<4>(b"+f7fabff"), None, "a sign");
    }

    #[test]
    fn only_the_hidden_names_of_this_program_are_temporaries() {
        // Every temporary file of this program has a name that starts with
        // `.hashsync-` (DESIGN.md §11). The cache removes only such names:
        // other hidden names, such as the `.nfs` names of an NFS client,
        // can belong to files that are still open (§5.6).
        let name = temporary_name();
        assert!(name.starts_with(".hashsync-"), "{name}");
        assert_eq!(name.len(), ".hashsync-".len() + 16, "{name}");
        assert!(is_temporary(name.as_bytes()));
        for name in [
            ".0123456789abcdef",
            "hashsync-0123456789abcdef",
            ".nfs000000000123456700000001",
        ] {
            assert!(!is_temporary(name.as_bytes()), "{name}");
        }
    }
}

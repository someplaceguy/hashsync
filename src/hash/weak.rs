//! Weak hash: a mixed polynomial rolling hash modulo 2^64 (DESIGN.md §5.1).
//!
//! For a window `w[0..B)`, `P = sum(w[i] * a^(B-1-i))`, computed with
//! Horner's rule. The weak hash is `mix64(P)`. All arithmetic wraps modulo
//! 2^64. The sender's rolling update belongs to a later milestone.

/// The fixed odd multiplier `a`, with `a ≡ 5 (mod 8)`.
pub const MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;

/// Identifies the algorithm, multiplier, and mixer in index metadata and
/// in HELLO. Changing the weak hash changes this string.
pub const ID: &str = "poly64-9e3779b97f4a7c15-splitmix64";

/// The splitmix64 finaliser (DESIGN.md §5.2). A bijection on `u64`.
pub fn mix64(mut z: u64) -> u64 {
    z ^= z >> 30;
    z = z.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z ^= z >> 27;
    z = z.wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    z
}

/// Weak-hash computation for one block size.
pub struct WeakHasher {
    block_size: usize,
}

impl WeakHasher {
    pub fn new(block_size: usize) -> Self {
        Self { block_size }
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// The unmixed polynomial `P` of a full window.
    pub fn polynomial(&self, window: &[u8]) -> u64 {
        debug_assert_eq!(window.len(), self.block_size);
        window.iter().fold(0u64, |p, &b| {
            p.wrapping_mul(MULTIPLIER).wrapping_add(u64::from(b))
        })
    }

    /// The weak hash of a full window.
    pub fn hash(&self, window: &[u8]) -> u64 {
        mix64(self.polynomial(window))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn multiplier_is_5_mod_8() {
        assert_eq!(MULTIPLIER % 8, 5);
    }

    #[test]
    fn mix64_of_zero_is_zero_and_small_inputs_differ() {
        assert_eq!(mix64(0), 0);
        let outputs: std::collections::HashSet<u64> = (0..10_000).map(mix64).collect();
        assert_eq!(outputs.len(), 10_000);
    }

    /// The definition `sum(w[i] * a^(B-1-i))`, term by term.
    fn polynomial_by_definition(window: &[u8]) -> u64 {
        let n = window.len();
        window.iter().enumerate().fold(0u64, |sum, (i, &b)| {
            let power = (0..n - 1 - i).fold(1u64, |p, _| p.wrapping_mul(MULTIPLIER));
            sum.wrapping_add(u64::from(b).wrapping_mul(power))
        })
    }

    proptest! {
        #[test]
        fn horner_matches_the_definition(
            window in proptest::collection::vec(any::<u8>(), 1..64),
        ) {
            let hasher = WeakHasher::new(window.len());
            let p = hasher.polynomial(&window);
            prop_assert_eq!(p, polynomial_by_definition(&window));
            prop_assert_eq!(hasher.hash(&window), mix64(p));
        }
    }
}

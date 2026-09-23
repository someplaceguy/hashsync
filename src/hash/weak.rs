//! Weak hash: a mixed polynomial rolling hash modulo 2^64 (DESIGN.md §5.1).
//!
//! For a window `w[0..B)`, `P = sum(w[i] * a^(B-1-i))`, computed with
//! Horner's rule. The weak hash is `mix64(P)`. All arithmetic wraps modulo
//! 2^64. The sender rolls the unmixed `P` from one window to the next with
//! `P' = a*P - Tout[out] + in`, where `Tout[b] = b * a^B`.

/// The fixed odd multiplier `a`, with `a ≡ 5 (mod 8)`.
pub const MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;

/// Identifies the algorithm, multiplier, and mixer in index metadata and
/// in HELLO. Changing the weak hash changes this string.
pub const ID: &str = "poly64-9e3779b97f4a7c15-splitmix64";

/// `a^0` to `a^8`.
const POWERS: [u64; 9] = {
    let mut powers = [1u64; 9];
    let mut i = 1;
    while i < powers.len() {
        powers[i] = powers[i - 1].wrapping_mul(MULTIPLIER);
        i += 1;
    }
    powers
};

/// The splitmix64 finaliser (DESIGN.md §5.2). A bijection on `u64`.
#[inline]
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
    /// `Tout[b] = b * a^B`: what a byte contributes when it leaves the
    /// window, after the multiplication by `a`.
    out: Box<[u64; 256]>,
}

impl WeakHasher {
    pub fn new(block_size: usize) -> Self {
        let power =
            MULTIPLIER.wrapping_pow(u32::try_from(block_size).expect("block size above u32"));
        let mut out = Box::new([0u64; 256]);
        for (byte, value) in out.iter_mut().enumerate() {
            *value = (byte as u64).wrapping_mul(power);
        }
        Self { block_size, out }
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// The unmixed polynomial `P` of a full window, by Horner's rule over
    /// eight bytes per step: `P = P*a^8 + sum(w[i+k] * a^(7-k))`. The
    /// products of a step are independent, so a step waits for one
    /// multiplication instead of eight.
    pub fn polynomial(&self, window: &[u8]) -> u64 {
        debug_assert_eq!(window.len(), self.block_size);
        let mut steps = window.chunks_exact(8);
        let p = (&mut steps).fold(0u64, |p, step| {
            let sum = step
                .iter()
                .zip(POWERS[..8].iter().rev())
                .fold(0u64, |sum, (&b, &power)| {
                    sum.wrapping_add(u64::from(b).wrapping_mul(power))
                });
            p.wrapping_mul(POWERS[8]).wrapping_add(sum)
        });
        steps.remainder().iter().fold(p, |p, &b| {
            p.wrapping_mul(MULTIPLIER).wrapping_add(u64::from(b))
        })
    }

    /// The polynomial of the window one byte further: `poly` is that of
    /// the window that starts with `out`, and `new` is the byte after it.
    #[inline]
    pub fn roll(&self, poly: u64, out: u8, new: u8) -> u64 {
        poly.wrapping_mul(MULTIPLIER)
            .wrapping_sub(self.out[usize::from(out)])
            .wrapping_add(u64::from(new))
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

        /// Rolling across the data gives every window's polynomial.
        #[test]
        fn rolling_matches_direct_computation(
            data in proptest::collection::vec(any::<u8>(), 2..300),
            block_size in 1usize..64,
        ) {
            prop_assume!(block_size < data.len());
            let hasher = WeakHasher::new(block_size);
            let mut poly = hasher.polynomial(&data[..block_size]);
            for start in 1..=data.len() - block_size {
                poly = hasher.roll(poly, data[start - 1], data[start + block_size - 1]);
                prop_assert_eq!(poly, hasher.polynomial(&data[start..start + block_size]));
            }
        }
    }
}

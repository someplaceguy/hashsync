// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! The polynomial of a whole window, `P = sum(w[i] * a^(B-1-i))`, as dot
//! products that vector instructions compute in parallel (DESIGN.md §5.1).
//!
//! The window is split into steps of `STEP` bytes. The products of a step's
//! sum, `sum(s[i] * a^(STEP-1-i))`, are independent, and Horner's rule
//! combines the steps with `a^STEP`, so that only one multiplication per
//! step waits for the one before. The bytes before the first whole step
//! make a shorter first step, with the last powers of a whole step.
//!
//! Each target computes a step's sum in its fastest form:
//!
//! - x86-64 with AVX2 or AVX-512: two 16-bit parts and one 32-bit part of
//!   the powers, in 32-bit sums that the compiler vectorizes;
//!   `fearless_simd` selects the level at run time.
//! - aarch64: 32-bit halves of the powers, for which the compiler uses
//!   NEON multiply-accumulates.
//!
//! Other x86-64 CPUs, whose 32-bit vector products are slow or absent, and
//! other targets use Horner's rule over groups of eight bytes instead
//! (`horner8`), with scalar 64-bit products. Scalar products over whole
//! steps would load a power for each byte, but the nine powers of
//! `horner8` stay in registers.

use super::MULTIPLIER;

/// The bytes of a step. At most 256, so that the 32-bit sums on x86-64 are
/// exact.
const STEP: usize = 256;

/// `a^(STEP-1-i)`: the power of byte `i` of a step.
const POWERS: [u64; STEP] = {
    let mut powers = [0u64; STEP];
    let mut power = 1u64;
    let mut i = STEP;
    while i > 0 {
        i -= 1;
        powers[i] = power;
        power = power.wrapping_mul(MULTIPLIER);
    }
    powers
};

/// `a^STEP`.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
const STEP_POWER: u64 = POWERS[0].wrapping_mul(MULTIPLIER);

/// The polynomial of `window`.
#[cfg(target_arch = "x86_64")]
pub fn polynomial(window: &[u8]) -> u64 {
    x86::polynomial(fearless_simd::Level::new(), window)
}

/// The polynomial of `window`.
#[cfg(target_arch = "aarch64")]
pub fn polynomial(window: &[u8]) -> u64 {
    by_steps(window, halves)
}

/// The polynomial of `window`.
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn polynomial(window: &[u8]) -> u64 {
    horner8(window)
}

/// The polynomial from the bytes before the first whole step, with the
/// last powers, and then from the sums of the whole steps, by Horner's
/// rule with `a^STEP`.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[inline(always)]
fn by_steps(window: &[u8], sum: impl Fn(&[u8; STEP]) -> u64) -> u64 {
    let (first, steps) = window.as_rchunks::<STEP>();
    let p = first
        .iter()
        .zip(&POWERS[STEP - first.len()..])
        .fold(0u64, |p, (&b, &power)| {
            p.wrapping_add(u64::from(b).wrapping_mul(power))
        });
    steps.iter().fold(p, |p, step| {
        p.wrapping_mul(STEP_POWER).wrapping_add(sum(step))
    })
}

/// The polynomial by Horner's rule over groups of eight bytes,
/// `P = P*a^8 + sum(g[k] * a^(7-k))`, and then over the remaining bytes
/// one at a time. The products of a group are independent, so a group
/// waits for one multiplication instead of eight.
#[cfg(not(target_arch = "aarch64"))]
fn horner8(window: &[u8]) -> u64 {
    // The last powers of a step, `a^7` to `a^0`, and `a^8`.
    let (powers, group_power) = (&POWERS[STEP - 8..], POWERS[STEP - 9]);
    let (groups, rest) = window.as_chunks::<8>();
    let p = groups.iter().fold(0u64, |p, group| {
        let sum = group.iter().zip(powers).fold(0u64, |sum, (&b, &power)| {
            sum.wrapping_add(u64::from(b).wrapping_mul(power))
        });
        p.wrapping_mul(group_power).wrapping_add(sum)
    });
    rest.iter().fold(p, |p, &b| {
        p.wrapping_mul(MULTIPLIER).wrapping_add(u64::from(b))
    })
}

/// The bits of each power that `mask` selects after a right shift by
/// `shift`.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
const fn bits(shift: u32, mask: u64) -> [u32; STEP] {
    let mut bits = [0u32; STEP];
    let mut i = 0;
    while i < STEP {
        bits[i] = ((POWERS[i] >> shift) & mask) as u32;
        i += 1;
    }
    bits
}

/// A step's sum from the 32-bit halves of the powers: 32x32->64-bit
/// products of the low halves, and only 32-bit products of the high
/// halves, which count from bit 32.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn halves(step: &[u8; STEP]) -> u64 {
    const LOW: [u32; STEP] = bits(0, 0xFFFF_FFFF);
    const HIGH: [u32; STEP] = bits(32, 0xFFFF_FFFF);
    let (low, high) =
        step.iter()
            .zip(&LOW)
            .zip(&HIGH)
            .fold((0u64, 0u32), |(low, high), ((&b, &l), &h)| {
                (
                    low.wrapping_add(u64::from(b) * u64::from(l)),
                    high.wrapping_add(u32::from(b).wrapping_mul(h)),
                )
            });
    low.wrapping_add(u64::from(high) << 32)
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use fearless_simd::{Level, Simd};
    use fearless_simd_macros::simd;

    use super::{STEP, bits, by_steps, horner8};

    /// The powers as `LOW + MID * 2^16 + HIGH * 2^32`, with 16-bit `LOW`
    /// and `MID`.
    const LOW: [u32; STEP] = bits(0, 0xFFFF);
    const MID: [u32; STEP] = bits(16, 0xFFFF);
    const HIGH: [u32; STEP] = bits(32, 0xFFFF_FFFF);
    // The first two sums of `sum` are exact. The bound is a `u64`, because
    // `usize` has 32 bits on x32.
    const _: () = assert!(STEP as u64 * 255 * 0xFFFF < 1 << 32);

    /// The polynomial at `level`: vectors with AVX2 or AVX-512, `horner8`
    /// without them.
    pub fn polynomial(level: Level, window: &[u8]) -> u64 {
        if let Some(simd) = level.as_avx512() {
            vectors(simd, window)
        } else if let Some(simd) = level.as_avx2() {
            vectors(simd, window)
        } else {
            horner8(window)
        }
    }

    /// The polynomial, compiled with the instructions of `simd`.
    #[simd]
    fn vectors<S: Simd>(simd: S, window: &[u8]) -> u64 {
        by_steps(window, sum)
    }

    /// A step's sum, `sum(w*LOW) + sum(w*MID) * 2^16 + sum(w*HIGH) * 2^32`,
    /// in 32-bit sums that the compiler vectorizes. The first two sums are
    /// exact, because `STEP * 255 * (2^16 - 1) < 2^32`, and the last one
    /// only counts modulo 2^32.
    #[inline(always)]
    fn sum(step: &[u8; STEP]) -> u64 {
        let (mut low, mut mid, mut high) = (0u32, 0u32, 0u32);
        for (i, &byte) in step.iter().enumerate() {
            let byte = u32::from(byte);
            low = low.wrapping_add(byte.wrapping_mul(LOW[i]));
            mid = mid.wrapping_add(byte.wrapping_mul(MID[i]));
            high = high.wrapping_add(byte.wrapping_mul(HIGH[i]));
        }
        u64::from(low)
            .wrapping_add(u64::from(mid) << 16)
            .wrapping_add(u64::from(high) << 32)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Horner's rule, one byte at a time.
    fn horner(window: &[u8]) -> u64 {
        window.iter().fold(0u64, |p, &b| {
            p.wrapping_mul(MULTIPLIER).wrapping_add(u64::from(b))
        })
    }

    /// Windows of whole steps and remainders, of the largest bytes, whose
    /// sums are the largest, and of zeros.
    fn windows() -> Vec<Vec<u8>> {
        let noise = crate::scanner::tests::noise(40_000, 11);
        let mut windows: Vec<Vec<u8>> = [0, 1, STEP - 1, STEP, STEP + 1, 3 * STEP + 17, 32_768]
            .into_iter()
            .map(|n| noise[..n].to_vec())
            .collect();
        windows.push(vec![0xFF; 32_768]);
        windows.push(vec![0xFF; 3 * STEP + 5]);
        windows.push(vec![0; 4096]);
        windows
    }

    #[test]
    fn fixed_windows_match_horner() {
        for window in windows() {
            assert_eq!(
                polynomial(&window),
                horner(&window),
                "{} bytes",
                window.len()
            );
        }
    }

    /// Every level that this CPU has, not only the best one.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn every_level_matches_horner() {
        use fearless_simd::{Level, Simd};

        let best = Level::new();
        let levels = [
            best.as_avx512().map(|simd| simd.level()),
            best.as_avx2().map(|simd| simd.level()),
            best.as_sse4_2().map(|simd| simd.level()),
            best.as_sse2().map(|simd| simd.level()),
        ];
        for level in levels.into_iter().flatten() {
            for window in windows() {
                assert_eq!(
                    x86::polynomial(level, &window),
                    horner(&window),
                    "{level:?}, {} bytes",
                    window.len()
                );
            }
        }
    }

    proptest! {
        /// Whole steps and remainders of any length and content.
        #[test]
        fn steps_and_remainders_match_horner(
            window in proptest::collection::vec(any::<u8>(), 0..4 * STEP),
        ) {
            prop_assert_eq!(polynomial(&window), horner(&window));
        }
    }
}

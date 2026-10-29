// Copyright 2026 Ricardo Correia
// SPDX-License-Identifier: GPL-3.0-or-later

//! Amounts of bytes in summaries and statistics, and the block and list
//! sizes in the tables of `bench` (DESIGN.md §8).
//!
//! Settings, limits, and messages that help to debug, such as panic
//! messages, show exact byte counts without this module: rounding could
//! make them unclear. The sizes in the tables of `bench` are settings too,
//! so their units never round.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

/// Cleared by `--no-human-readable`, before other threads start.
static HUMAN_READABLE: AtomicBool = AtomicBool::new(true);

/// Show every amount as whole bytes, without scale units.
pub fn disable_human_readable() {
    HUMAN_READABLE.store(false, Relaxed);
}

/// Whether amounts show scale units, which `--no-human-readable` disables.
pub fn human_readable() -> bool {
    HUMAN_READABLE.load(Relaxed)
}

/// An amount of bytes as summaries and statistics show it. An amount below
/// 1 KiB shows as whole bytes: `512 B`. A larger amount shows with two
/// decimal digits, in the first of KiB, MiB, GiB, TiB, and PiB in which
/// its rounded value is below 1024, else in PiB: `1.50 KiB`. Without
/// human-readable output, every amount shows as whole bytes.
pub struct Bytes(pub u64);

const UNITS: [&str; 5] = ["KiB", "MiB", "GiB", "TiB", "PiB"];

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.0;
        if count < 1024 || !HUMAN_READABLE.load(Relaxed) {
            return write!(f, "{count} B");
        }
        let mut unit = 0;
        loop {
            // Hundredths of the unit, rounded half up.
            let divisor = 1u128 << (10 * (unit + 1));
            let hundredths = (u128::from(count) * 100 + divisor / 2) / divisor;
            if hundredths < 1024 * 100 || unit == UNITS.len() - 1 {
                let (whole, fraction) = (hundredths / 100, hundredths % 100);
                return write!(f, "{whole}.{fraction:02} {}", UNITS[unit]);
            }
            unit += 1;
        }
    }
}

/// An exact amount of bytes, such as a block size in the tables of
/// `bench`: in the largest of KiB, MiB, GiB, TiB, and PiB of which it is a
/// whole number, `16 KiB`, else in bytes, `4100 B`. Unlike `Bytes`, it
/// never rounds. Without human-readable output, it shows as whole bytes.
pub struct ExactBytes(pub u64);

impl fmt::Display for ExactBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match whole_units(self.0, 1024, &UNITS) {
            Some((whole, unit)) => write!(f, "{whole} {unit}"),
            None => write!(f, "{} B", self.0),
        }
    }
}

/// An exact count, such as the size of a list in the tables of `bench`: in
/// the largest of thousands, millions, billions, trillions, and
/// quadrillions of which it is a whole number, `10K`, else as it is. It
/// never rounds. Without human-readable output, it shows as it is.
pub struct ExactCount(pub u64);

const COUNT_UNITS: [&str; 5] = ["K", "M", "G", "T", "P"];

impl fmt::Display for ExactCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match whole_units(self.0, 1000, &COUNT_UNITS) {
            Some((whole, unit)) => write!(f, "{whole}{unit}"),
            None => write!(f, "{}", self.0),
        }
    }
}

/// `count` as a whole number of the largest of `units` of which it is one,
/// where the first unit is `base` and each later one is `base` times the
/// one before it. `None` when `count` is zero or no whole number of the
/// first unit, and without human-readable output.
fn whole_units(count: u64, base: u64, units: &[&'static str]) -> Option<(u64, &'static str)> {
    if !HUMAN_READABLE.load(Relaxed) {
        return None;
    }
    let mut whole = None;
    let mut rest = count;
    for &unit in units {
        if rest == 0 || !rest.is_multiple_of(base) {
            break;
        }
        rest /= base;
        whole = Some((rest, unit));
    }
    whole
}

#[cfg(test)]
mod tests {
    use super::{Bytes, ExactBytes, ExactCount};

    #[test]
    fn amounts_show_in_scale_units() {
        for (count, shown) in [
            (0, "0 B"),
            (1023, "1023 B"),
            (1024, "1.00 KiB"),
            (1536, "1.50 KiB"),
            // 1.125 KiB: half a hundredth rounds up.
            (1152, "1.13 KiB"),
            (1_048_570, "1023.99 KiB"),
            // This count rounds to 1024.00 KiB, so it shows in MiB.
            (1_048_571, "1.00 MiB"),
            (1 << 20, "1.00 MiB"),
            (5 << 30, "5.00 GiB"),
            (3 << 39, "1.50 TiB"),
            (1 << 50, "1.00 PiB"),
            // No unit is larger than PiB.
            (1 << 60, "1024.00 PiB"),
            (u64::MAX, "16384.00 PiB"),
        ] {
            assert_eq!(Bytes(count).to_string(), shown, "{count} bytes");
        }
    }

    #[test]
    fn exact_amounts_show_in_the_largest_unit_that_divides_them() {
        for (count, shown) in [
            (0, "0 B"),
            (512, "512 B"),
            (1024, "1 KiB"),
            // 1.5 KiB is no whole number of KiB.
            (1536, "1536 B"),
            (4100, "4100 B"),
            (16 << 10, "16 KiB"),
            (1 << 20, "1 MiB"),
            (3 << 30, "3 GiB"),
            // No unit is larger than PiB.
            (1 << 60, "1024 PiB"),
            (u64::from(u32::MAX), "4294967295 B"),
        ] {
            assert_eq!(ExactBytes(count).to_string(), shown, "{count} bytes");
        }
        for (count, shown) in [
            (0, "0"),
            (999, "999"),
            (1000, "1K"),
            (1500, "1500"),
            (10_000, "10K"),
            (10_000_000, "10M"),
            (1_000_000_001, "1000000001"),
            (10u64.pow(18), "1000P"),
        ] {
            assert_eq!(ExactCount(count).to_string(), shown, "{count}");
        }
    }
}

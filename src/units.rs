//! Amounts of bytes in summaries and statistics (DESIGN.md §8).
//!
//! Settings, limits, and messages that help to debug, such as panic
//! messages, show exact byte counts without this module: rounding could
//! make them unclear.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

/// Cleared by `--no-human-readable`, before other threads start.
static HUMAN_READABLE: AtomicBool = AtomicBool::new(true);

/// Show every amount as whole bytes, without scale units.
pub fn disable_human_readable() {
    HUMAN_READABLE.store(false, Relaxed);
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

#[cfg(test)]
mod tests {
    use super::Bytes;

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
}

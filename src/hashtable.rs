//! The sender's in-memory membership table (DESIGN.md §5.5): open
//! addressing over 32-bit fingerprints in a flat bucket array. Lookups can
//! return false positives, never false negatives; a false positive only
//! costs a query.

/// 16 slots of one bucket, one cache line.
#[derive(Clone)]
#[repr(align(64))]
struct Bucket([u32; 16]);

const SLOTS: u64 = 16;
const _: () = assert!(std::mem::size_of::<Bucket>() == 64);

pub struct HashTable {
    buckets: Vec<Bucket>,
}

impl HashTable {
    /// Build the table from the list's values, in any order.
    pub fn build(values: &[u64]) -> Self {
        // An empty list allocates no buckets, and every lookup misses
        // without computing a bucket address.
        if values.is_empty() {
            return HashTable {
                buckets: Vec::new(),
            };
        }
        // `buckets = ceil(N / (16 * 0.7))`, so at least 30% of the slots
        // stay empty and insertion always finds one.
        let n = values.len() as u64;
        let buckets = (u128::from(n) * 10).div_ceil(u128::from(SLOTS) * 7) as usize;
        let mut table = HashTable {
            buckets: vec![Bucket([0; 16]); buckets],
        };
        for &value in values {
            table.insert(value);
        }
        table
    }

    /// Insert in the first empty slot, continuing to the next bucket when
    /// full.
    fn insert(&mut self, value: u64) {
        let fingerprint = fingerprint(value);
        let mut bucket = self.bucket_of(value);
        loop {
            for slot in &mut self.buckets[bucket].0 {
                if *slot == 0 {
                    *slot = fingerprint;
                    return;
                }
            }
            bucket = (bucket + 1) % self.buckets.len();
        }
    }

    /// True when `value`'s fingerprint is present: stop at the
    /// fingerprint or an empty slot, continue only through full buckets.
    ///
    /// Insertion fills each bucket's slots in order and never empties one,
    /// so a fingerprint before the first empty slot is a fingerprint
    /// anywhere in the bucket. Comparing all 16 slots together, without a
    /// branch per slot, lets the compiler use vector compares and avoids a
    /// mispredicted branch at the unpredictable end of the filled slots.
    /// The scanner probes once per output byte, so this is inlined.
    #[inline]
    pub fn contains(&self, value: u64) -> bool {
        if self.buckets.is_empty() {
            return false;
        }
        let fingerprint = fingerprint(value);
        let mut bucket = self.bucket_of(value);
        loop {
            let slots = &self.buckets[bucket].0;
            let found = slots
                .iter()
                .fold(false, |found, &slot| found | (slot == fingerprint));
            let empty = slots.iter().fold(false, |empty, &slot| empty | (slot == 0));
            if found {
                return true;
            }
            if empty {
                return false;
            }
            bucket = (bucket + 1) % self.buckets.len();
        }
    }

    /// Map `v` to `high64(v * buckets)` using a 128-bit product.
    fn bucket_of(&self, value: u64) -> usize {
        ((u128::from(value) * self.buckets.len() as u128) >> 64) as usize
    }
}

/// The value's low 32 bits, replacing zero with one; zero slots are empty.
fn fingerprint(value: u64) -> u32 {
    match value as u32 {
        0 => 1,
        fingerprint => fingerprint,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn the_empty_table_has_no_buckets_and_only_misses() {
        let table = HashTable::build(&[]);
        assert!(table.buckets.is_empty());
        assert!(!table.contains(0));
        assert!(!table.contains(u64::MAX));
    }

    #[test]
    fn zero_low_bits_use_fingerprint_one() {
        // Low 32 bits zero: the stored fingerprint must not equal an
        // empty slot.
        let value = 0xABCD_0000_0000_0000;
        let table = HashTable::build(&[value, 0]);
        assert!(table.contains(value));
        assert!(table.contains(0));
    }

    #[test]
    fn clustered_values_overflow_into_following_buckets() {
        // Equal high bits map every value to one bucket; more than 16
        // values must spill over and still be found.
        let values: Vec<u64> = (0..100).collect();
        let table = HashTable::build(&values);
        for &value in &values {
            assert!(table.contains(value), "{value} lost");
        }
    }

    proptest! {
        /// No false negatives, whatever the values (DESIGN.md §11).
        #[test]
        fn no_false_negatives(values in proptest::collection::vec(any::<u64>(), 1..500)) {
            let table = HashTable::build(&values);
            for &value in &values {
                prop_assert!(table.contains(value));
            }
        }

        /// Load stays below 70% of the slots.
        #[test]
        fn bucket_count_leaves_slack(n in 1usize..10_000) {
            let buckets = (n as u128 * 10).div_ceil(112) as usize;
            prop_assert!(buckets as f64 * 16.0 * 0.7 >= n as f64 - 1.0);
        }
    }
}

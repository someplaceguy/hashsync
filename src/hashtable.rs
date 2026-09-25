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

/// A lookup of one value: the bucket where it begins and the fingerprint
/// that it looks for. `HashTable::start_lookup` makes it, and
/// `HashTable::finish_lookup` of the same table finishes it. The default
/// lookup finds nothing, in every table.
#[derive(Clone, Copy, Default)]
pub struct Lookup {
    bucket: usize,
    /// Zero, which no value has, only in a lookup that finds nothing.
    fingerprint: u32,
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

    /// Start the lookup of `value`: compute its bucket and fingerprint
    /// once, and start to load the bucket, so that `finish_lookup` finds it
    /// in the cache later. In the empty table, the lookup computes no bucket
    /// address and finds nothing.
    #[inline(always)]
    pub fn start_lookup(&self, value: u64) -> Lookup {
        if self.buckets.is_empty() {
            return Lookup::default();
        }
        let bucket = self.bucket_of(value);
        crate::sys::prefetch(&self.buckets[bucket]);
        Lookup {
            bucket,
            fingerprint: fingerprint(value),
        }
    }

    /// True when the lookup's fingerprint is present: stop at the
    /// fingerprint or an empty slot, continue only through full buckets.
    ///
    /// `lookup` must come from `start_lookup` of this table, or be the
    /// default lookup. A lookup from a different table can start at an
    /// incorrect bucket and give a false negative, or start after the last
    /// bucket and panic.
    ///
    /// Insertion fills each bucket's slots in order and never empties one.
    /// So the filled slots of a bucket come before its empty slots: the
    /// fingerprint is in the bucket exactly when it is before the first
    /// empty slot, and a bucket has an empty slot exactly when its last
    /// slot is empty. Comparing all 16 slots together, without a branch
    /// per slot, lets the compiler use vector compares and avoids a
    /// mispredicted branch at the unpredictable end of the filled slots.
    /// The scanner probes once per output byte, so this is always inlined.
    #[inline(always)]
    pub fn finish_lookup(&self, lookup: Lookup) -> bool {
        // The default lookup, and every lookup in the empty table.
        if lookup.fingerprint == 0 {
            return false;
        }
        let mut bucket = lookup.bucket;
        loop {
            let slots = &self.buckets[bucket].0;
            if slots
                .iter()
                .fold(false, |found, &slot| found | (slot == lookup.fingerprint))
            {
                return true;
            }
            if slots[slots.len() - 1] == 0 {
                return false;
            }
            // About one lookup in eight passes a full bucket: wrap without
            // a division.
            bucket += 1;
            if bucket == self.buckets.len() {
                bucket = 0;
            }
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

    /// A whole lookup of `value`.
    fn look_up(table: &HashTable, value: u64) -> bool {
        table.finish_lookup(table.start_lookup(value))
    }

    /// The lookup of DESIGN.md §5.5, slot by slot: stop at the fingerprint
    /// or an empty slot, continue only through full buckets.
    fn look_up_by_slots(table: &HashTable, value: u64) -> bool {
        if table.buckets.is_empty() {
            return false;
        }
        let fingerprint = fingerprint(value);
        let mut bucket = table.bucket_of(value);
        loop {
            for &slot in &table.buckets[bucket].0 {
                if slot == fingerprint {
                    return true;
                }
                if slot == 0 {
                    return false;
                }
            }
            bucket = (bucket + 1) % table.buckets.len();
        }
    }

    #[test]
    fn the_empty_table_has_no_buckets_and_only_misses() {
        let table = HashTable::build(&[]);
        assert!(table.buckets.is_empty());
        assert!(!look_up(&table, 0));
        assert!(!look_up(&table, u64::MAX));
    }

    #[test]
    fn the_default_lookup_finds_nothing() {
        // Bucket 0 has empty slots, which the default fingerprint must
        // not match.
        let table = HashTable::build(&[1, 2, 3]);
        assert!(!table.finish_lookup(Lookup::default()));
    }

    #[test]
    fn zero_low_bits_use_fingerprint_one() {
        // Low 32 bits zero: the stored fingerprint must not equal an
        // empty slot.
        let value = 0xABCD_0000_0000_0000;
        let table = HashTable::build(&[value, 0]);
        assert!(look_up(&table, value));
        assert!(look_up(&table, 0));
    }

    #[test]
    fn clustered_values_overflow_into_following_buckets() {
        // Equal high bits map every value to one bucket; more than 16
        // values must spill over and still be found.
        let values: Vec<u64> = (0..100).collect();
        let table = HashTable::build(&values);
        for &value in &values {
            assert!(look_up(&table, value), "{value} lost");
        }
        // An absent fingerprint passes the full buckets and stops at the
        // first bucket with an empty slot.
        assert!(!look_up(&table, 1000));
    }

    proptest! {
        /// No false negatives, whatever the values (DESIGN.md §11).
        #[test]
        fn no_false_negatives(values in proptest::collection::vec(any::<u64>(), 1..500)) {
            let table = HashTable::build(&values);
            for &value in &values {
                prop_assert!(look_up(&table, value));
            }
        }

        /// Lookups give the result of the slot-by-slot rule. Small values
        /// share the first bucket and large values the last bucket, and
        /// overflow them; the last bucket overflows into the first. Queries
        /// near the values share their fingerprints, so the lookups pass
        /// full buckets and wrap.
        #[test]
        fn lookups_follow_the_slot_rule(
            values in proptest::collection::vec(
                prop_oneof![0u64..4096, u64::MAX - 4095..=u64::MAX, any::<u64>()],
                0..1000,
            ),
            queries in proptest::collection::vec(
                prop_oneof![0u64..8192, u64::MAX - 8191..=u64::MAX, any::<u64>()],
                0..200,
            ),
        ) {
            let table = HashTable::build(&values);
            for &query in queries.iter().chain(&values) {
                prop_assert_eq!(look_up(&table, query), look_up_by_slots(&table, query));
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

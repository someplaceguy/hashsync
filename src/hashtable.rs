//! The sender's in-memory membership table (DESIGN.md §5.5): open
//! addressing over 32-bit fingerprints in a flat bucket array. Lookups can
//! return false positives, never false negatives. The high 32 bits of each
//! value wait in a second array, which only `contains_value` reads: the
//! scanner checks each hit with it, so that a false positive costs no
//! strong hash and no query.
//!
//! A table can grow while it is in use: a table sized for the values of a
//! list (`for_values`) takes them one at a time (`insert`), and a lookup
//! between two insertions finds every value inserted before it. The
//! scanner fills its table that way while the list arrives (DESIGN.md
//! §6.3), on its own thread, between its scans, so the table needs no lock.

use crate::hashlist;

/// 16 slots of one bucket, one cache line. The fingerprints and the high
/// bits both use it, so that the slots of a bucket are one cache line in
/// each array, whatever the alignment that the allocator gives.
#[derive(Clone)]
#[repr(align(64))]
struct Bucket([u32; 16]);

const SLOTS: u64 = 16;
const _: () = assert!(std::mem::size_of::<Bucket>() == 64);

#[derive(Clone)]
pub struct HashTable {
    buckets: Vec<Bucket>,
    /// The high 32 bits of the value in each slot of `buckets`. Lookups do
    /// not read them, so that a probe reads one cache line.
    high: Vec<Bucket>,
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
        let mut table = HashTable::for_values(values.len());
        for &value in values {
            table.insert(value);
        }
        table
    }

    /// Build the table straight from the encoded list `encoded`, which
    /// must be canonical (DESIGN.md §5.4), inserting each value as it is
    /// decoded. This function exists so that a list that only feeds the
    /// table needs no vector of all its values in between: at 21 million
    /// values, that vector takes 171 MB.
    pub fn from_list(encoded: &[u8]) -> Result<Self, String> {
        hashlist::decode_with(encoded, HashTable::for_values, HashTable::insert)
    }

    /// The table of no values, in which every lookup misses.
    pub fn empty() -> Self {
        HashTable::for_values(0)
    }

    /// An empty table sized for `n` values, which `insert` adds. A table
    /// takes at most `n` values: with more, an insertion could find no
    /// empty slot.
    pub fn for_values(n: usize) -> Self {
        // An empty list allocates no buckets, and every lookup misses
        // without computing a bucket address.
        if n == 0 {
            return HashTable {
                buckets: Vec::new(),
                high: Vec::new(),
            };
        }
        // `buckets = ceil(N / (16 * 0.7))`, so at least 30% of the slots
        // stay empty and insertion always finds one.
        let buckets = (n as u128 * 10).div_ceil(u128::from(SLOTS) * 7) as usize;
        HashTable {
            buckets: vec![Bucket([0; 16]); buckets],
            high: vec![Bucket([0; 16]); buckets],
        }
    }

    /// Insert in the first empty slot, continuing to the next bucket when
    /// full. An insertion never moves or empties a slot, so a value stays
    /// where earlier lookups found its fingerprint, and no lookup of a value
    /// inserted before it misses. The table must have been sized for this
    /// value too (`for_values`).
    pub fn insert(&mut self, value: u64) {
        let fingerprint = fingerprint(value);
        let mut bucket = self.bucket_of(value);
        loop {
            if let Some(slot) = self.buckets[bucket].0.iter().position(|&slot| slot == 0) {
                self.buckets[bucket].0[slot] = fingerprint;
                self.high[bucket].0[slot] = high_bits(value);
                return;
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
        self.probe(lookup.bucket, |slots, _| {
            slots
                .iter()
                .fold(false, |found, &slot| found | (slot == lookup.fingerprint))
        })
    }

    /// True when `value` is in the list: stop at a slot that has both the
    /// fingerprint and the high bits of `value`, or at an empty slot, and
    /// continue only through full buckets. The check cannot stop where a
    /// lookup stops: a full bucket can have the fingerprint of `value` for
    /// a different value, and `value` can be in a later bucket. Values
    /// whose low 32 bits are 0 and 1, with equal high bits, share a
    /// fingerprint, so they are not told apart (DESIGN.md §5.5). This reads
    /// the second array too, so the scanner calls it only for the hits of
    /// lookups.
    pub fn contains_value(&self, value: u64) -> bool {
        if self.buckets.is_empty() {
            return false;
        }
        let (fingerprint, high) = (fingerprint(value), high_bits(value));
        self.probe(self.bucket_of(value), |slots, bucket| {
            slots
                .iter()
                .zip(&self.high[bucket].0)
                .any(|(&slot, &bits)| slot == fingerprint && bits == high)
        })
    }

    /// True when `found` is true for a bucket on the way of a lookup that
    /// begins at `bucket`: the full buckets from `bucket` on, and the first
    /// bucket with an empty slot. `found` gets the slots of the bucket and
    /// its index. The lookup and the check share this, so that the check
    /// visits the buckets of the lookup.
    #[inline(always)]
    fn probe(&self, mut bucket: usize, found: impl Fn(&[u32; 16], usize) -> bool) -> bool {
        loop {
            let slots = &self.buckets[bucket].0;
            if found(slots, bucket) {
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

/// The value's high 32 bits, which the second array keeps.
fn high_bits(value: u64) -> u32 {
    (value >> 32) as u32
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
        assert!(!table.contains_value(0));
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
        for &value in &values {
            assert!(table.contains_value(value), "{value} lost");
        }
        // The fingerprint of 5 with other high bits: the lookup finds it,
        // the check of the whole value does not.
        let other = 5 | 1 << 32;
        assert!(look_up(&table, other));
        assert!(!table.contains_value(other));
    }

    #[test]
    fn the_check_passes_buckets_full_of_its_fingerprint() {
        // 20 values with one fingerprint and other high bits: the first
        // bucket holds 16 of them, and the rest overflow into the next. A
        // lookup of the last ones stops at the first bucket, which has
        // their fingerprint; the check must go on to the next.
        let values: Vec<u64> = (0..20).map(|high| high << 32 | 5).collect();
        let table = HashTable::build(&values);
        for &value in &values {
            assert!(table.contains_value(value), "{value:#x} lost");
        }
    }

    /// Every slot of both arrays of `table`.
    fn slots(table: &HashTable) -> Vec<[u32; 16]> {
        table
            .buckets
            .iter()
            .chain(&table.high)
            .map(|bucket| bucket.0)
            .collect()
    }

    /// A list that is not canonical gives no table: here a byte follows the
    /// last value (DESIGN.md §5.4).
    #[test]
    fn a_list_that_is_not_canonical_gives_no_table() {
        let mut list = hashlist::encode(&[1, 2, 3]);
        assert!(HashTable::from_list(&list).is_ok());
        list.push(0);
        assert!(HashTable::from_list(&list).is_err());
    }

    proptest! {
        /// A table built straight from an encoded list is the table that
        /// `build` makes of its values, slot for slot.
        #[test]
        fn a_table_from_a_list_is_the_table_of_its_values(
            values in proptest::collection::btree_set(any::<u64>(), 0..2000)
        ) {
            let values: Vec<u64> = values.into_iter().collect();
            let from_list = HashTable::from_list(&hashlist::encode(&values)).unwrap();
            prop_assert_eq!(slots(&from_list), slots(&HashTable::build(&values)));
        }

        /// A table that grows while it is in use finds every value that was
        /// inserted before a lookup, whatever the order of the insertions,
        /// and ends as the table that `build` makes of all the values
        /// (DESIGN.md §6.3). Some values share buckets, so that the
        /// insertions overflow into the buckets of later values.
        #[test]
        fn a_growing_table_finds_every_value_inserted_so_far(
            values in proptest::collection::btree_set(
                prop_oneof![0u64..4096, u64::MAX - 4095..=u64::MAX, any::<u64>()],
                1..1000,
            ),
            sorted in any::<bool>(),
        ) {
            let mut values: Vec<u64> = values.into_iter().collect();
            if !sorted {
                values.reverse();
            }
            let mut table = HashTable::for_values(values.len());
            for (i, &value) in values.iter().enumerate() {
                table.insert(value);
                let step = (i / 8).max(1);
                for &inserted in values[..=i].iter().rev().step_by(step) {
                    prop_assert!(look_up(&table, inserted), "{:#x} lost", inserted);
                    prop_assert!(table.contains_value(inserted), "{:#x} lost", inserted);
                }
            }
            prop_assert_eq!(slots(&table), slots(&HashTable::build(&values)));
        }

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

        /// `contains_value` is true exactly for the values with the
        /// fingerprint and the high bits of a list value. Some queries share
        /// the fingerprint of a list value but differ in their high bits.
        #[test]
        fn contains_value_checks_the_whole_value(
            values in proptest::collection::vec(
                prop_oneof![0u64..4096, u64::MAX - 4095..=u64::MAX, any::<u64>()],
                0..1000,
            ),
            queries in proptest::collection::vec(any::<u64>(), 0..200),
        ) {
            let table = HashTable::build(&values);
            let key = |value: u64| (fingerprint(value), high_bits(value));
            let listed: std::collections::HashSet<_> = values.iter().map(|&v| key(v)).collect();
            let others = values.iter().flat_map(|&v| [v ^ 1 << 40, v ^ 1 << 63]);
            for query in queries.iter().copied().chain(others).chain(values.iter().copied()) {
                prop_assert_eq!(table.contains_value(query), listed.contains(&key(query)), "{:#x}", query);
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

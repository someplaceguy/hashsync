//! The canonical hash-list encoding, the list ID (DESIGN.md §5.4), and
//! direct deltas between lists (§5.6).
//!
//! Sorted distinct 64-bit values become gaps, and each gap is Golomb-Rice
//! coded: `g >> k` one bits, a zero, then the low `k` bits, packed
//! least-significant-bit first and zero-padded to a whole byte. The header
//! is `N` alone; both sides derive `k` from it. Equal sets must produce
//! equal bytes, so the decoder enforces the canonical form: no value
//! overflow, zero padding, and the exact byte length.
//!
//! A delta from a base list to a target list is a pair of lists: the
//! values that only the base has, which the target drops, and the values
//! that only the target has, which it adds. Each goes on the wire as a list
//! of its own.

use std::cmp::Ordering;

use crate::hash::{Strong, StrongHash};

/// The value width `W`: no hash bits are dropped.
const WIDTH: u8 = 64;

/// `N u64`.
const HEADER_LEN: usize = 8;

/// A hash list's identity: its strong-hash algorithm and the full digest
/// of its encoding. Cache file names carry both; the protocol carries only
/// the digest, because every list of a run uses the negotiated algorithm
/// (DESIGN.md §6.2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ListId {
    pub algorithm: StrongHash,
    pub digest: Strong,
}

/// `<algorithm>-<digest>`, the digest in lowercase hex. Messages show this
/// form, and cache file names hold it (DESIGN.md §5.6).
impl std::fmt::Display for ListId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-", self.algorithm.id())?;
        for byte in self.digest {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// The canonical Rice parameter: `k = W - ceil(log2(N))`, and `k = 0` for
/// an empty list.
fn rice_k(n: u64) -> u8 {
    if n == 0 { 0 } else { WIDTH - ceil_log2(n) }
}

/// `ceil(log2(n))` for `n >= 1`.
fn ceil_log2(n: u64) -> u8 {
    if n <= 1 {
        0
    } else {
        (64 - (n - 1).leading_zeros()) as u8
    }
}

/// Encode sorted distinct values canonically.
pub fn encode(values: &[u64]) -> Vec<u8> {
    debug_assert!(values.windows(2).all(|w| w[0] < w[1]));
    let n = values.len() as u64;
    let k = rice_k(n);
    let mut writer = BitWriter::new(values.len(), k);
    writer.out.extend_from_slice(&n.to_le_bytes());
    let mut previous = None;
    for &value in values {
        // Gaps are `g_0 = v_0` and `g_i = v_i - v_(i-1) - 1`.
        let gap = match previous {
            None => value,
            Some(p) => value - p - 1,
        };
        previous = Some(value);
        // `g >> k` one bits, a zero, then the low `k` bits. For `k = 64`
        // the quotient is zero for every value; no 64-bit shift happens.
        let quotient = if k == 64 { 0 } else { gap >> k };
        writer.ones(quotient);
        writer.bits(0, 1);
        writer.bits(gap, u32::from(k));
    }
    writer.finish()
}

/// Decode a canonical list into its sorted distinct values. Any
/// deviation from the canonical form is an error naming the problem.
pub fn decode(bytes: &[u8]) -> Result<Vec<u64>, String> {
    decode_with(bytes, Vec::with_capacity, Vec::push)
}

/// Decode a canonical list as `decode` does, but hand each value to `add`
/// as soon as it is decoded, in order, instead of collecting the values.
/// `start` gets the number of values from the header and makes what
/// `add` adds to. This function exists so that the sender can build its
/// hash table straight from a list (`HashTable::from_list`), without a
/// vector of all its values in between. On an error, what `add` built so
/// far is dropped, and the caller gets only the error.
pub fn decode_with<T>(
    bytes: &[u8],
    start: impl FnOnce(usize) -> T,
    mut add: impl FnMut(&mut T, u64),
) -> Result<T, String> {
    if bytes.len() < HEADER_LEN {
        return Err("truncated header".into());
    }
    let n = u64::from_le_bytes(bytes[..HEADER_LEN].try_into().unwrap());
    let k = rice_k(n);
    let body = &bytes[HEADER_LEN..];
    // Each value takes at least one bit, so this also bounds `n` before
    // any allocation.
    if n > body.len() as u64 * 8 {
        return Err("more values than encoded bits".into());
    }
    let mut reader = BitReader::new(body);
    let mut values = start(n as usize);
    let mut previous: Option<u64> = None;
    for _ in 0..n {
        let quotient = reader.unary().ok_or("truncated value")?;
        let remainder = reader.bits(u32::from(k)).ok_or("truncated value")?;
        let gap = if k == 64 {
            if quotient != 0 {
                return Err("gap overflows the value width".into());
            }
            remainder
        } else {
            if quotient > u64::MAX >> k {
                return Err("gap overflows the value width".into());
            }
            (quotient << k) | remainder
        };
        let value = match previous {
            None => gap,
            Some(p) => p
                .checked_add(gap)
                .and_then(|v| v.checked_add(1))
                .ok_or("values overflow the value width")?,
        };
        previous = Some(value);
        add(&mut values, value);
    }
    // The last byte is padded with zeros and nothing may follow it.
    if !reader.rest_is_zero_padding() {
        return Err("non-zero padding".into());
    }
    if body.len() != reader.bytes_consumed() {
        return Err("excess bytes after the last value".into());
    }
    Ok(values)
}

/// The delta from `base` to `target`, both sorted and distinct: the values
/// to remove (those that only `base` has) and the values to add (those that
/// only `target` has), each sorted and distinct.
pub fn delta(base: &[u64], target: &[u64]) -> (Vec<u64>, Vec<u64>) {
    let (mut removed, mut added) = (Vec::new(), Vec::new());
    let (mut b, mut t) = (0, 0);
    while b < base.len() && t < target.len() {
        match base[b].cmp(&target[t]) {
            Ordering::Less => {
                removed.push(base[b]);
                b += 1;
            }
            Ordering::Greater => {
                added.push(target[t]);
                t += 1;
            }
            Ordering::Equal => (b, t) = (b + 1, t + 1),
        }
    }
    removed.extend_from_slice(&base[b..]);
    added.extend_from_slice(&target[t..]);
    (removed, added)
}

/// Apply a delta to `base`: the values of `base` minus `removed`, plus
/// `added`. All three inputs are sorted and distinct, and so is the result.
///
/// This is plain set arithmetic, so any delta gives a result, even one
/// that was not made from this base: removing a value that `base` lacks
/// does nothing, and adding one that it already has keeps a single copy.
/// The caller needs no separate check for such deltas, because it compares
/// the result with the target's digest anyway, and only the target matches
/// (DESIGN.md §5.6).
pub fn apply(base: &[u64], removed: &[u64], added: &[u64]) -> Vec<u64> {
    let mut values = Vec::with_capacity(base.len() + added.len());
    let mut removed = removed.iter().copied().peekable();
    let mut added = added.iter().copied().peekable();
    for &value in base {
        while let Some(new) = added.next_if(|&new| new < value) {
            values.push(new);
        }
        while removed.next_if(|&old| old < value).is_some() {}
        let gone = removed.next_if_eq(&value).is_some();
        let again = added.next_if_eq(&value).is_some();
        if !gone || again {
            values.push(value);
        }
    }
    values.extend(added);
    values
}

/// Packs bits least-significant-bit first, a 64-bit word at a time. The
/// header bytes are pushed directly to `out` before the first bit.
struct BitWriter {
    out: Vec<u8>,
    /// The bits not written yet, below bit `pending_len`, which is always
    /// less than 64.
    pending: u64,
    pending_len: u32,
}

impl BitWriter {
    fn new(values: usize, k: u8) -> Self {
        // One quotient bit and a zero per value on average, plus the
        // remainders.
        let bits = (values as u64).saturating_mul(u64::from(k) + 2);
        BitWriter {
            out: Vec::with_capacity(HEADER_LEN + (bits / 8) as usize + 16),
            pending: 0,
            pending_len: 0,
        }
    }

    /// Append the low `count` bits of `value`, lowest first. `count` is at
    /// most 64, and the higher bits of `value` are ignored.
    fn bits(&mut self, value: u64, count: u32) {
        let value = match count {
            64 => value,
            _ => value & ((1 << count) - 1),
        };
        self.pending |= value << self.pending_len;
        let total = self.pending_len + count;
        if total < 64 {
            self.pending_len = total;
            return;
        }
        // The word is full: write it out, and keep the bits of `value`
        // that did not fit.
        self.out.extend_from_slice(&self.pending.to_le_bytes());
        self.pending = match self.pending_len {
            0 => 0,
            used => value >> (64 - used),
        };
        self.pending_len = total - 64;
    }

    /// Append `count` one bits.
    fn ones(&mut self, mut count: u64) {
        while count > 0 {
            let take = count.min(63) as u32;
            self.bits((1 << take) - 1, take);
            count -= u64::from(take);
        }
    }

    /// Write out the pending bits, padding the last byte with zeros.
    fn finish(mut self) -> Vec<u8> {
        let bytes = self.pending_len.div_ceil(8) as usize;
        self.out
            .extend_from_slice(&self.pending.to_le_bytes()[..bytes]);
        self.out
    }
}

/// Reads bits least-significant-bit first, 64 at a time, so that a list of
/// millions of values decodes in a fraction of a second. Both sides decode
/// a cached list for every delta (DESIGN.md §5.6).
struct BitReader<'a> {
    body: &'a [u8],
    /// The next unread bit position.
    position: usize,
}

impl BitReader<'_> {
    fn new(body: &[u8]) -> BitReader<'_> {
        BitReader { body, position: 0 }
    }

    /// The number of bits not read yet.
    fn remaining(&self) -> usize {
        self.body.len() * 8 - self.position
    }

    /// The next 64 bits, lowest first, without consuming them. Bits past
    /// the end of the body read as zero.
    fn peek(&self) -> u64 {
        let start = self.position / 8;
        let word = match self.body.get(start..start + 16) {
            Some(bytes) => u128::from_le_bytes(bytes.try_into().unwrap()),
            None => {
                let mut bytes = [0; 16];
                let rest = &self.body[start..];
                bytes[..rest.len()].copy_from_slice(rest);
                u128::from_le_bytes(bytes)
            }
        };
        (word >> (self.position % 8)) as u64
    }

    /// Read the next `count` bits, lowest first. `count` is at most 64.
    fn bits(&mut self, count: u32) -> Option<u64> {
        if count as usize > self.remaining() {
            return None;
        }
        let value = match count {
            64 => self.peek(),
            _ => self.peek() & ((1 << count) - 1),
        };
        self.position += count as usize;
        Some(value)
    }

    /// Count the one bits up to the terminating zero bit, and consume the
    /// zero as well.
    fn unary(&mut self) -> Option<u64> {
        let mut count = 0;
        loop {
            let ones = self.peek().trailing_ones();
            // The zeros that `peek` returns past the end of the body do not
            // terminate a code.
            if ones as usize >= self.remaining() {
                return None;
            }
            if ones < 64 {
                self.position += ones as usize + 1;
                return Some(count + u64::from(ones));
            }
            self.position += 64;
            count += 64;
        }
    }

    /// Bytes the consumed bits occupy.
    fn bytes_consumed(&self) -> usize {
        self.position.div_ceil(8)
    }

    /// True when the bits from the current position to the end of the
    /// current byte are all zero.
    fn rest_is_zero_padding(&self) -> bool {
        if self.position.is_multiple_of(8) {
            return true;
        }
        let byte = self.body[self.position / 8];
        byte >> (self.position % 8) == 0
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use proptest::prelude::*;

    use super::*;

    /// The canonical encoding of `values`, which must be sorted and
    /// distinct, and its BLAKE3 ID. Tests in other modules use this too.
    pub(crate) fn list(values: &[u64]) -> (ListId, Vec<u8>) {
        let encoded = encode(values);
        let id = ListId {
            algorithm: StrongHash::Blake3,
            digest: StrongHash::Blake3.digest(&encoded),
        };
        (id, encoded)
    }

    fn roundtrip(values: &[u64]) -> Vec<u8> {
        let encoded = encode(values);
        assert_eq!(decode(&encoded).unwrap(), values, "values {values:?}");
        encoded
    }

    #[test]
    fn empty_list_is_the_header_alone() {
        let encoded = roundtrip(&[]);
        assert_eq!(encoded, [0; HEADER_LEN]);
    }

    #[test]
    fn single_value_uses_k_64() {
        for value in [0, 1, u64::MAX, 0x0123_4567_89AB_CDEF] {
            let encoded = roundtrip(&[value]);
            // A zero bit, 64 value bits, and 7 padding bits.
            assert_eq!(encoded.len(), HEADER_LEN + 9);
            let mut bits = [0u8; 16];
            bits[..9].copy_from_slice(&encoded[HEADER_LEN..]);
            assert_eq!(u128::from_le_bytes(bits), u128::from(value) << 1);
        }
    }

    #[test]
    fn small_lists_roundtrip() {
        roundtrip(&[0, 1]);
        roundtrip(&[0, u64::MAX]);
        roundtrip(&[1, 2, 3, 4, 5, 6, 7, 8, 9]);
        roundtrip(&[u64::MAX - 1, u64::MAX]);
        let wide: Vec<u64> = (0..100).map(|i| i * 0x0288_DF0C_AC5B_63E6).collect();
        roundtrip(&wide);

        // With 129 values k is 56, so the gap from 127 to the largest value
        // has a quotient of 255, whose unary code spans several of the
        // reader's words. Dropping the last byte truncates it.
        let mut far: Vec<u64> = (0..128).collect();
        far.push(u64::MAX);
        let encoded = roundtrip(&far);
        let truncated = &encoded[..encoded.len() - 1];
        assert!(decode(truncated).unwrap_err().contains("truncated"));
    }

    #[test]
    fn k_follows_the_formula() {
        assert_eq!(rice_k(0), 0);
        assert_eq!(rice_k(1), 64);
        assert_eq!(rice_k(2), 63);
        assert_eq!(rice_k(3), 62);
        assert_eq!(rice_k(4), 62);
        assert_eq!(rice_k(5), 61);
        assert_eq!(rice_k(1 << 20), 44);
        assert_eq!(rice_k((1 << 20) + 1), 43);
        assert_eq!(rice_k(u64::MAX), 0);
    }

    #[test]
    fn deviations_are_rejected() {
        assert!(decode(&[]).unwrap_err().contains("truncated"));
        assert!(
            decode(&[0; HEADER_LEN - 1])
                .unwrap_err()
                .contains("truncated")
        );

        let encoded = encode(&[7, 8, 9]);
        let truncated = &encoded[..encoded.len() - 1];
        assert!(decode(truncated).unwrap_err().contains("truncated"));
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decode(&trailing).unwrap_err().contains("excess"));

        // A unary code that runs to the end of the body has no terminating
        // zero. The reader sees zeros past the end, and they must not
        // terminate the code. Here N = 1, first with one byte of one bits,
        // then with nine, the first eight of which fill a whole word of the
        // reader.
        let mut ones = 1u64.to_le_bytes().to_vec();
        ones.push(0xff);
        assert!(decode(&ones).unwrap_err().contains("truncated"));
        ones.extend([0xff; 8]);
        assert!(decode(&ones).unwrap_err().contains("truncated"));

        // The padding bits of the last byte must be zero.
        let mut padding = encode(&[0]);
        assert_eq!(*padding.last().unwrap() >> 1, 0);
        *padding.last_mut().unwrap() |= 0x80;
        assert!(decode(&padding).unwrap_err().contains("padding"));

        // Two maximal values cannot both be represented: the second gap
        // overflows. N = 2 gives k = 63; the gap `u64::MAX >> 63 = 1`
        // needs quotient 0; force a quotient of 2 with ones before the
        // terminating zero.
        let mut header = 2u64.to_le_bytes().to_vec();
        let mut writer = BitWriter::new(2, 63);
        writer.bits(0, 1);
        writer.bits(u64::MAX, 63);
        writer.ones(2);
        writer.bits(0, 1);
        writer.bits(u64::MAX >> 1, 63);
        header.extend_from_slice(&writer.finish());
        assert!(decode(&header).unwrap_err().contains("overflow"));

        // An announced count larger than the bits can hold must fail
        // before allocating for it.
        assert!(
            decode(&u64::MAX.to_le_bytes())
                .unwrap_err()
                .contains("more values")
        );
    }

    #[test]
    fn a_delta_holds_only_the_changes() {
        assert_eq!(delta(&[1, 2, 3], &[1, 2, 3]), (vec![], vec![]));
        assert_eq!(delta(&[], &[4, 5]), (vec![], vec![4, 5]));
        assert_eq!(delta(&[4, 5], &[]), (vec![4, 5], vec![]));
        assert_eq!(
            delta(&[1, 3, 5, 7], &[2, 3, 6, 7, 9]),
            (vec![1, 5], vec![2, 6, 9])
        );
    }

    /// The sorted distinct values of `values`.
    fn set(values: &[u64]) -> Vec<u64> {
        let mut set = values.to_vec();
        set.sort_unstable();
        set.dedup();
        set
    }

    /// The canonical encoding of `values`, built one bit at a time exactly
    /// as DESIGN.md §5.4 spells it out, without the word-sized steps of
    /// `BitWriter`.
    fn reference_encoding(values: &[u64]) -> Vec<u8> {
        // `k = W - ceil(log2(N))`, and 0 without values.
        let n = values.len() as u64;
        let ceil_log2 = |n: u64| {
            if n <= 1 {
                0
            } else {
                64 - (n - 1).leading_zeros()
            }
        };
        let k = if n == 0 { 0 } else { 64 - ceil_log2(n) };
        let mut bits = Vec::new();
        let mut previous: Option<u64> = None;
        for &value in values {
            let gap = previous.map_or(value, |p| value - p - 1);
            previous = Some(value);
            let quotient = if k == 64 { 0 } else { gap >> k };
            bits.extend(std::iter::repeat_n(true, quotient as usize));
            bits.push(false);
            bits.extend((0..k).map(|i| gap >> i & 1 == 1));
        }
        let mut out = n.to_le_bytes().to_vec();
        for chunk in bits.chunks(8) {
            let byte = chunk
                .iter()
                .enumerate()
                .fold(0u8, |byte, (i, &bit)| byte | u8::from(bit) << i);
            out.push(byte);
        }
        out
    }

    proptest! {
        #[test]
        fn the_encoding_matches_the_bit_by_bit_definition(
            values in proptest::collection::vec(any::<u64>(), 0..300),
            dense in proptest::collection::vec(0u64..2000, 0..300),
        ) {
            // The list ID is the digest of these bytes, so every set must
            // keep encoding to the same bytes: the word-sized writer has to
            // match the definition, including for sets with long unary
            // codes.
            for values in [set(&values), set(&dense), {
                let mut far = set(&dense);
                far.push(u64::MAX);
                far
            }] {
                prop_assert_eq!(encode(&values), reference_encoding(&values));
            }
        }

        #[test]
        fn a_delta_changes_its_base_into_its_target(
            base in proptest::collection::vec(0u64..64, 0..40),
            target in proptest::collection::vec(0u64..64, 0..40),
        ) {
            // Small values, so that the sets share many of them.
            let (base, target) = (set(&base), set(&target));
            let (removed, added) = delta(&base, &target);
            prop_assert_eq!(apply(&base, &removed, &added), target);
        }

        #[test]
        fn any_delta_applies_as_set_operations(
            base in proptest::collection::vec(0u64..64, 0..40),
            removed in proptest::collection::vec(0u64..64, 0..20),
            added in proptest::collection::vec(0u64..64, 0..20),
        ) {
            // Including deltas that were not made from this base: the
            // result is the base without the removed values, plus the added
            // ones.
            let (base, removed, added) = (set(&base), set(&removed), set(&added));
            let mut expected: Vec<u64> =
                base.iter().copied().filter(|v| !removed.contains(v)).collect();
            expected.extend(&added);
            prop_assert_eq!(apply(&base, &removed, &added), set(&expected));
        }

        #[test]
        fn sets_roundtrip(mut values in proptest::collection::vec(any::<u64>(), 0..300)) {
            values.sort_unstable();
            values.dedup();
            roundtrip(&values);
        }

        #[test]
        fn dense_sets_roundtrip(
            start in any::<u64>(),
            gaps in proptest::collection::vec(0u64..3, 1..200),
        ) {
            let mut values = Vec::with_capacity(gaps.len());
            let mut current = start;
            for gap in gaps {
                let Some(next) = current.checked_add(gap) else { break };
                values.push(next);
                let Some(next) = next.checked_add(1) else { break };
                current = next;
            }
            values.sort_unstable();
            values.dedup();
            roundtrip(&values);
        }

        #[test]
        fn equal_sets_produce_equal_bytes(values in proptest::collection::vec(any::<u64>(), 1..100)) {
            let mut sorted = values.clone();
            sorted.sort_unstable();
            sorted.dedup();
            let mut shuffled = sorted.clone();
            shuffled.reverse();
            shuffled.sort_unstable();
            prop_assert_eq!(encode(&sorted), encode(&shuffled));
        }
    }
}

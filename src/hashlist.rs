//! The canonical hash-list encoding (DESIGN.md §5.4).
//!
//! Sorted distinct 64-bit values become gaps, and each gap is Golomb-Rice
//! coded: `g >> k` one bits, a zero, then the low `k` bits, packed
//! least-significant-bit first and zero-padded to a whole byte. Equal sets
//! must produce equal bytes, so the decoder enforces the canonical form:
//! the exact header, the exact `k` for `N`, no value overflow, zero
//! padding, and the exact byte length.

use crate::protocol::WIDTH;

pub const FORMAT_VERSION: u8 = 1;

/// `format_version u8, W u8, k u8, N u64`.
const HEADER_LEN: usize = 11;

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
    writer.out.push(FORMAT_VERSION);
    writer.out.push(WIDTH);
    writer.out.push(k);
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
    if bytes.len() < HEADER_LEN {
        return Err("truncated header".into());
    }
    let (format, width, k) = (bytes[0], bytes[1], bytes[2]);
    let n = u64::from_le_bytes(bytes[3..11].try_into().unwrap());
    if format != FORMAT_VERSION {
        return Err(format!("unsupported format version {format}"));
    }
    if width != WIDTH {
        return Err(format!("unsupported width {width}"));
    }
    if k != rice_k(n) {
        return Err(format!("non-canonical k {k} for {n} values"));
    }
    let body = &bytes[HEADER_LEN..];
    // Each value takes at least one bit, so this also bounds `n` before
    // any allocation.
    if n > body.len() as u64 * 8 {
        return Err("more values than encoded bits".into());
    }
    let mut reader = BitReader::new(body);
    let mut values = Vec::with_capacity(n as usize);
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
        values.push(value);
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

/// Packs bits least-significant-bit first. The header bytes are pushed
/// directly to `out` before the first bit.
struct BitWriter {
    out: Vec<u8>,
    /// Pending bits, below bit `pending_len`.
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

    /// Append the low `count` bits of `value`, lowest first; higher bits
    /// are ignored.
    fn bits(&mut self, value: u64, count: u32) {
        let value = match count {
            64 => value,
            _ => value & ((1u64 << count) - 1),
        };
        let mut acc = u128::from(self.pending) | (u128::from(value) << self.pending_len);
        let mut total = self.pending_len + count;
        while total >= 8 {
            self.out.push(acc as u8);
            acc >>= 8;
            total -= 8;
        }
        self.pending = acc as u64;
        self.pending_len = total;
    }

    /// Append `count` one bits.
    fn ones(&mut self, mut count: u64) {
        while count > 0 {
            let take = count.min(63) as u32;
            self.bits((1 << take) - 1, take);
            count -= u64::from(take);
        }
    }

    /// Pad the last byte with zeros.
    fn finish(mut self) -> Vec<u8> {
        if self.pending_len > 0 {
            self.out.push(self.pending as u8);
        }
        self.out
    }
}

/// Reads bits least-significant-bit first.
struct BitReader<'a> {
    body: &'a [u8],
    /// The next unread bit position.
    position: usize,
}

impl BitReader<'_> {
    fn new(body: &[u8]) -> BitReader<'_> {
        BitReader { body, position: 0 }
    }

    fn bit(&mut self) -> Option<bool> {
        let byte = self.body.get(self.position / 8)?;
        let bit = byte >> (self.position % 8) & 1;
        self.position += 1;
        Some(bit == 1)
    }

    /// The next `count` bits, lowest first.
    fn bits(&mut self, count: u32) -> Option<u64> {
        let mut value = 0u64;
        for i in 0..count {
            value |= u64::from(self.bit()?) << i;
        }
        Some(value)
    }

    /// Count one bits up to the terminating zero bit.
    fn unary(&mut self) -> Option<u64> {
        let mut count = 0u64;
        while self.bit()? {
            count += 1;
        }
        Some(count)
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
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn roundtrip(values: &[u64]) -> Vec<u8> {
        let encoded = encode(values);
        assert_eq!(decode(&encoded).unwrap(), values, "values {values:?}");
        encoded
    }

    #[test]
    fn empty_list_is_the_header_alone() {
        let encoded = roundtrip(&[]);
        assert_eq!(encoded, [FORMAT_VERSION, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn single_value_uses_k_64() {
        for value in [0, 1, u64::MAX, 0x0123_4567_89AB_CDEF] {
            let encoded = roundtrip(&[value]);
            assert_eq!(encoded[2], 64);
            // A zero bit, 64 value bits, and 7 padding bits.
            assert_eq!(encoded.len(), HEADER_LEN + 9);
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
        let mut bad_version = encode(&[]);
        bad_version[0] = 2;
        assert!(decode(&bad_version).unwrap_err().contains("version"));
        let mut bad_width = encode(&[]);
        bad_width[1] = 32;
        assert!(decode(&bad_width).unwrap_err().contains("width"));
        let mut bad_k = encode(&[7, 8, 9]);
        bad_k[2] += 1;
        assert!(decode(&bad_k).unwrap_err().contains("non-canonical k"));

        let encoded = encode(&[7, 8, 9]);
        let truncated = &encoded[..encoded.len() - 1];
        assert!(decode(truncated).unwrap_err().contains("truncated"));
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decode(&trailing).unwrap_err().contains("excess"));

        // The padding bits of the last byte must be zero.
        let mut padding = encode(&[0]);
        assert_eq!(*padding.last().unwrap() >> 1, 0);
        *padding.last_mut().unwrap() |= 0x80;
        assert!(decode(&padding).unwrap_err().contains("padding"));

        // Two maximal values cannot both be represented: the second gap
        // overflows. k = 63; the gap `u64::MAX >> 63 = 1` needs quotient
        // 0; force a quotient of 2 with ones before the terminating zero.
        let mut header = vec![FORMAT_VERSION, 64, 63];
        header.extend_from_slice(&2u64.to_le_bytes());
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
        let mut huge = vec![FORMAT_VERSION, 64, 0];
        huge.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode(&huge).unwrap_err().contains("more values"));
    }

    proptest! {
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

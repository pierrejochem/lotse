//! ZigZag varints as used inside records.
//!
//! Every variable-length integer in a record is a signed value encoded with
//! ZigZag (so small negative numbers like `-1` for "null" stay one byte) and
//! then as an unsigned LEB128 varint.

use crate::Error;

/// Longest encoding of a 64-bit varint.
pub const MAX_LEN_64: usize = 10;

/// Appends `value` as an unsigned LEB128 varint.
pub fn put_u64(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Reads an unsigned LEB128 varint; returns the value and bytes consumed.
pub fn get_u64(buf: &[u8]) -> Result<(u64, usize), Error> {
    let mut value: u64 = 0;
    for (i, &byte) in buf.iter().enumerate().take(MAX_LEN_64) {
        let part = u64::from(byte & 0x7f);
        if i == MAX_LEN_64 - 1 && byte > 0x01 {
            return Err(Error::VarintOverflow);
        }
        value |= part << (7 * i);
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    if buf.len() < MAX_LEN_64 { Err(Error::Truncated) } else { Err(Error::VarintOverflow) }
}

/// ZigZag-encodes `value`.
pub const fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// Reverses [`zigzag`].
pub const fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Appends a signed value as a ZigZag varint.
pub fn put_i64(buf: &mut Vec<u8>, value: i64) {
    put_u64(buf, zigzag(value));
}

/// Reads a ZigZag varint.
pub fn get_i64(buf: &[u8]) -> Result<(i64, usize), Error> {
    let (raw, n) = get_u64(buf)?;
    Ok((unzigzag(raw), n))
}

/// Reads a ZigZag varint that must fit in an `i32`.
pub fn get_i32(buf: &[u8]) -> Result<(i32, usize), Error> {
    let (v, n) = get_i64(buf)?;
    let v = i32::try_from(v).map_err(|_| Error::VarintOverflow)?;
    Ok((v, n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn small_values_are_one_byte() {
        for v in [-1i64, 0, 1, -64, 63] {
            let mut b = Vec::new();
            put_i64(&mut b, v);
            assert_eq!(b.len(), 1, "{v}");
        }
    }

    #[test]
    fn truncated_input_is_reported() {
        assert_eq!(get_u64(&[0x80, 0x80]), Err(Error::Truncated));
        assert_eq!(get_u64(&[]), Err(Error::Truncated));
    }

    #[test]
    fn overlong_input_is_rejected() {
        assert_eq!(get_u64(&[0xff; 11]), Err(Error::VarintOverflow));
        assert_eq!(get_u64(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]), Err(Error::VarintOverflow));
    }

    proptest! {
        #[test]
        fn roundtrip_u64(v: u64) {
            let mut b = Vec::new();
            put_u64(&mut b, v);
            prop_assert_eq!(get_u64(&b), Ok((v, b.len())));
        }

        #[test]
        fn roundtrip_i64(v: i64) {
            let mut b = Vec::new();
            put_i64(&mut b, v);
            prop_assert_eq!(get_i64(&b), Ok((v, b.len())));
        }
    }
}

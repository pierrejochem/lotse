//! The field conventions every API key shares.
//!
//! ```text
//! u8/u16/u32/u64/i64  fixed width, little-endian
//! varint              ZigZag varint (array counts, tags)
//! string              u16 length + UTF-8 (0xFFFF = null)
//! bytes               u32 length + data
//! array<T>            varint count + elements
//! tagged_fields       varint count + {tag varint, bytes}, at the end of every structure
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_record::varint;

use crate::DecodeError;

const NULL_STRING: u16 = 0xFFFF;

/// Writes fields into a frame.
#[derive(Debug, Default)]
pub struct Writer(BytesMut);

impl Writer {
    /// An empty frame.
    pub fn new() -> Self {
        Writer(BytesMut::with_capacity(64))
    }
    /// The frame written so far.
    pub fn finish(self) -> Bytes {
        self.0.freeze()
    }
    /// A `u8`.
    pub fn u8(&mut self, v: u8) {
        self.0.put_u8(v);
    }
    /// A `u16`.
    pub fn u16(&mut self, v: u16) {
        self.0.put_u16_le(v);
    }
    /// A `u32`.
    pub fn u32(&mut self, v: u32) {
        self.0.put_u32_le(v);
    }
    /// A `u64`.
    pub fn u64(&mut self, v: u64) {
        self.0.put_u64_le(v);
    }
    /// An `i64`.
    pub fn i64(&mut self, v: i64) {
        self.0.put_i64_le(v);
    }
    /// A ZigZag varint.
    pub fn varint(&mut self, v: i64) {
        let mut buf = Vec::with_capacity(varint::MAX_LEN_64);
        varint::put_i64(&mut buf, v);
        self.0.put_slice(&buf);
    }
    /// A string; strings are short names, at most 65,534 bytes.
    pub fn string(&mut self, s: &str) {
        let len = u16::try_from(s.len()).ok().filter(|l| *l != NULL_STRING).expect("a string of at most 65,534 bytes");
        self.u16(len);
        self.0.put_slice(s.as_bytes());
    }
    /// A string that may be null.
    pub fn opt_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.string(s),
            None => self.u16(NULL_STRING),
        }
    }
    /// Length-prefixed bytes.
    pub fn bytes(&mut self, b: &[u8]) {
        self.u32(u32::try_from(b.len()).expect("bytes fit a frame"));
        self.0.put_slice(b);
    }
    /// An array, each element written by `f`.
    pub fn array<T>(&mut self, items: &[T], mut f: impl FnMut(&mut Self, &T)) {
        self.varint(items.len() as i64);
        for item in items {
            f(self, item);
        }
    }
    /// Tagged fields: `(tag, value)` in ascending tag order.
    pub fn tags(&mut self, tags: &[(u64, Bytes)]) {
        self.varint(tags.len() as i64);
        for (tag, value) in tags {
            self.varint(*tag as i64);
            self.bytes(value);
        }
    }
    /// No tagged fields.
    pub fn no_tags(&mut self) {
        self.varint(0);
    }
}

/// Reads fields from a frame, refusing anything that runs past its end.
#[derive(Debug)]
pub struct Reader(Bytes);

impl Reader {
    /// Reads `frame`.
    pub fn new(frame: Bytes) -> Self {
        Reader(frame)
    }
    /// Bytes not read yet.
    pub fn remaining(&self) -> usize {
        self.0.remaining()
    }
    /// Fails unless everything was read.
    pub fn finish(self) -> Result<(), DecodeError> {
        if self.0.has_remaining() { Err(DecodeError::Malformed("trailing bytes")) } else { Ok(()) }
    }
    fn need(&self, n: usize) -> Result<(), DecodeError> {
        if self.0.remaining() < n { Err(DecodeError::Truncated) } else { Ok(()) }
    }
    /// A `u8`.
    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        self.need(1)?;
        Ok(self.0.get_u8())
    }
    /// A `u16`.
    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        self.need(2)?;
        Ok(self.0.get_u16_le())
    }
    /// A `u32`.
    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        self.need(4)?;
        Ok(self.0.get_u32_le())
    }
    /// A `u64`.
    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        self.need(8)?;
        Ok(self.0.get_u64_le())
    }
    /// An `i64`.
    pub fn i64(&mut self) -> Result<i64, DecodeError> {
        self.need(8)?;
        Ok(self.0.get_i64_le())
    }
    /// A ZigZag varint.
    pub fn varint(&mut self) -> Result<i64, DecodeError> {
        let (v, n) = varint::get_i64(&self.0).map_err(|_| DecodeError::Truncated)?;
        self.0.advance(n);
        Ok(v)
    }
    /// A count of at least `0`, each element taking at least `min_size`
    /// bytes: counts the frame cannot hold are refused before allocating.
    fn count(&mut self, min_size: usize) -> Result<usize, DecodeError> {
        let n = self.varint()?;
        let n = usize::try_from(n).map_err(|_| DecodeError::Malformed("negative count"))?;
        if n > self.remaining() / min_size.max(1) {
            return Err(DecodeError::Malformed("count exceeds the frame"));
        }
        Ok(n)
    }
    /// A string that must not be null.
    pub fn string(&mut self) -> Result<String, DecodeError> {
        self.opt_string()?.ok_or(DecodeError::Malformed("null string"))
    }
    /// A string that may be null.
    pub fn opt_string(&mut self) -> Result<Option<String>, DecodeError> {
        let len = self.u16()?;
        if len == NULL_STRING {
            return Ok(None);
        }
        let len = usize::from(len);
        self.need(len)?;
        let raw = self.0.split_to(len);
        String::from_utf8(raw.to_vec()).map(Some).map_err(|_| DecodeError::Malformed("string is not UTF-8"))
    }
    /// Length-prefixed bytes, sharing the frame's buffer.
    pub fn bytes(&mut self) -> Result<Bytes, DecodeError> {
        let len = self.u32()? as usize;
        self.need(len)?;
        Ok(self.0.split_to(len))
    }
    /// An array; `min_size` is the smallest encoding of one element.
    pub fn array<T>(
        &mut self,
        min_size: usize,
        mut f: impl FnMut(&mut Self) -> Result<T, DecodeError>,
    ) -> Result<Vec<T>, DecodeError> {
        let n = self.count(min_size)?;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(f(self)?);
        }
        Ok(out)
    }
    /// Tagged fields, in the order sent.
    pub fn tags(&mut self) -> Result<Vec<(u64, Bytes)>, DecodeError> {
        self.array(5, |r| {
            let tag = u64::try_from(r.varint()?).map_err(|_| DecodeError::Malformed("negative tag"))?;
            Ok((tag, r.bytes()?))
        })
    }
}

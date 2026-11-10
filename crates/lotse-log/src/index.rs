//! The index file of a sealed segment (ADR 0094).
//!
//! A sealed segment changes only as a whole, so its sparse index can be
//! written once and read back on the next open instead of scanning every
//! batch. One file carries both lookups: each entry names a batch's first
//! offset, its byte position, and the newest timestamp of the batches before
//! it in the segment, which only grows, so a search by time is a search over
//! the entries too.
//!
//! Layout, little-endian:
//!
//! ```text
//! magic          4   "LTIX"
//! version        1   1
//! base_offset    8   the segment's first offset
//! size           8   the segment's length in bytes
//! next_offset    8   the offset after its last batch
//! max_timestamp  8   its newest record timestamp, ms (i64)
//! last_position  8   byte position of its last batch
//! last_crc       4   that batch's stored CRC
//! count          4   number of entries
//! entries   count*24 (offset u64, position u64, timestamp_before i64)
//! crc32c         4   of everything above
//! ```
//!
//! The file is trusted only if it decodes, its checksum holds, and it
//! describes the segment as it is: the same length, and at `last_position` a
//! valid batch with the recorded CRC that ends the segment at `next_offset`.
//! The batch's offsets are outside its CRC, so they are compared on their
//! own. Anything else and the segment is scanned and the file rewritten.

use bytes::{Buf, BufMut, Bytes, BytesMut};

const MAGIC: &[u8; 4] = b"LTIX";
const VERSION: u8 = 1;
const FIXED: usize = 4 + 1 + 8 + 8 + 8 + 8 + 8 + 4 + 4;
const ENTRY: usize = 24;

/// One sparse index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    /// First offset of the batch.
    pub(crate) offset: u64,
    /// Its byte position in the segment.
    pub(crate) position: u64,
    /// The newest record timestamp of every batch before it in the
    /// segment, ms; `i64::MIN` for the first.
    pub(crate) timestamp_before: i64,
}

/// What an index file says about its segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentIndex {
    pub(crate) base_offset: u64,
    pub(crate) size: u64,
    pub(crate) next_offset: u64,
    pub(crate) max_timestamp: i64,
    pub(crate) last_position: u64,
    pub(crate) last_crc: u32,
    pub(crate) entries: Vec<Entry>,
}

impl SegmentIndex {
    pub(crate) fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(FIXED + self.entries.len() * ENTRY + 4);
        b.put_slice(MAGIC);
        b.put_u8(VERSION);
        b.put_u64_le(self.base_offset);
        b.put_u64_le(self.size);
        b.put_u64_le(self.next_offset);
        b.put_i64_le(self.max_timestamp);
        b.put_u64_le(self.last_position);
        b.put_u32_le(self.last_crc);
        b.put_u32_le(self.entries.len() as u32);
        for e in &self.entries {
            b.put_u64_le(e.offset);
            b.put_u64_le(e.position);
            b.put_i64_le(e.timestamp_before);
        }
        let crc = crc32c::crc32c(&b);
        b.put_u32_le(crc);
        b.freeze()
    }

    /// Decodes a file and checks that it is consistent in itself; whether it
    /// matches the segment is the caller's to check.
    pub(crate) fn decode(bytes: &[u8]) -> Result<SegmentIndex, &'static str> {
        if bytes.len() < FIXED + 4 {
            return Err("shorter than its header");
        }
        let (body, mut tail) = bytes.split_at(bytes.len() - 4);
        if tail.get_u32_le() != crc32c::crc32c(body) {
            return Err("checksum mismatch");
        }
        let mut b = body;
        if &b[..4] != MAGIC {
            return Err("not an index file");
        }
        b.advance(4);
        if b.get_u8() != VERSION {
            return Err("unknown version");
        }
        let mut index = SegmentIndex {
            base_offset: b.get_u64_le(),
            size: b.get_u64_le(),
            next_offset: b.get_u64_le(),
            max_timestamp: b.get_i64_le(),
            last_position: b.get_u64_le(),
            last_crc: b.get_u32_le(),
            entries: Vec::new(),
        };
        let count = b.get_u32_le() as usize;
        if b.len() != count * ENTRY {
            return Err("entry count does not match the length");
        }
        index.entries.reserve(count);
        for _ in 0..count {
            index.entries.push(Entry {
                offset: b.get_u64_le(),
                position: b.get_u64_le(),
                timestamp_before: b.get_i64_le(),
            });
        }
        index.check()?;
        Ok(index)
    }

    /// The rules every index of a non-empty segment keeps.
    fn check(&self) -> Result<(), &'static str> {
        if self.size == 0 {
            return Err("an empty segment has no index");
        }
        let Some(first) = self.entries.first() else { return Err("no entries") };
        if first.offset != self.base_offset || first.position != 0 || first.timestamp_before != i64::MIN {
            return Err("the first entry is not the segment's first batch");
        }
        for w in self.entries.windows(2) {
            if w[1].offset <= w[0].offset
                || w[1].position <= w[0].position
                || w[1].timestamp_before < w[0].timestamp_before
            {
                return Err("entries out of order");
            }
        }
        let last = self.entries.last().expect("checked above");
        if last.position > self.last_position || self.last_position >= self.size || last.offset >= self.next_offset {
            return Err("entries beyond the last batch");
        }
        if self.max_timestamp < last.timestamp_before {
            return Err("newest timestamp older than an entry's");
        }
        Ok(())
    }
}

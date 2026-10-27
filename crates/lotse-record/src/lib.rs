//! The Lotse record batch: one format on disk, on the wire and in Raft.
//!
//! A batch is a fixed 65-byte header followed by a block of records. The
//! header is never compressed, so Raft, the indexes and the transaction logic
//! can read it without touching the records. The leader stamps `base_offset`
//! and `term` when it appends; both sit before the CRC'd range so stamping
//! does not invalidate the checksum.
//!
//! ```text
//! offset  size  field
//!      0     8  base_offset        = Raft index of the first record
//!      8     4  batch_len          bytes after this field
//!     12     8  term               Raft term (leader epoch)
//!     20     1  magic
//!     21     4  crc32c             over bytes 25..end
//!     25     2  attributes         compression(3) | txn(1) | control(1) | kind(3)
//!     27     4  last_offset_delta
//!     31     8  base_ts
//!     39     8  max_ts
//!     47     8  producer_id        0 = not idempotent
//!     55     2  producer_epoch
//!     57     4  base_sequence
//!     61     4  record_count
//!     65     …  records
//! ```
//!
//! All fixed-width fields are little-endian. Records use ZigZag varints, see
//! [`Record`].

#![warn(missing_docs)]

pub mod varint;

use bytes::{Bytes, BytesMut};

/// Size of the fixed batch header.
pub const HEADER_LEN: usize = 65;
/// Bytes before `batch_len` counts: `base_offset` and `batch_len` itself.
pub const LEN_PREFIX: usize = 12;
/// Current format version.
pub const MAGIC: u8 = 1;
/// Largest batch accepted, header included.
pub const MAX_BATCH_LEN: usize = 1024 * 1024;

const CRC_START: usize = 25;

/// Errors from encoding or decoding batches.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The buffer ends before the batch or field does.
    #[error("buffer ends inside a batch")]
    Truncated,
    /// A varint is longer than allowed or its value does not fit.
    #[error("varint overflow")]
    VarintOverflow,
    /// Unknown `magic` byte.
    #[error("unsupported batch format version {0}")]
    UnsupportedMagic(u8),
    /// The CRC does not match the content.
    #[error("checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    Checksum {
        /// CRC stored in the header.
        stored: u32,
        /// CRC computed over the content.
        computed: u32,
    },
    /// A batch is larger than [`MAX_BATCH_LEN`].
    #[error("batch of {0} bytes exceeds the limit")]
    TooLarge(usize),
    /// Header fields contradict each other or the records.
    #[error("malformed batch: {0}")]
    Malformed(&'static str),
    /// The batch uses a compression codec this build does not support.
    #[error("compression codec {0:?} is not supported yet")]
    UnsupportedCompression(Compression),
}

/// Compression of the record block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Records are stored as is.
    None,
    /// zstd (not implemented yet).
    Zstd,
    /// lz4 frame format (not implemented yet).
    Lz4,
}

/// What a batch carries. Queue state lives in the same log as the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchKind {
    /// Messages.
    Data,
    /// Queue acknowledgements as offset ranges.
    Ack,
    /// Delivery counts and other persisted queue state.
    QueueState,
    /// Due times for TTL and delayed delivery.
    Timer,
}

/// The `attributes` field, decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attributes {
    /// Codec of the record block.
    pub compression: Compression,
    /// Part of a transaction; invisible to `read_committed` until committed.
    pub transactional: bool,
    /// A control batch (transaction marker); holds exactly one record.
    pub control: bool,
    /// Payload kind.
    pub kind: BatchKind,
}

impl Attributes {
    /// Plain, uncompressed, non-transactional data.
    pub const DATA: Attributes =
        Attributes { compression: Compression::None, transactional: false, control: false, kind: BatchKind::Data };

    /// Packs into the on-disk representation.
    pub fn to_bits(self) -> u16 {
        let c = match self.compression {
            Compression::None => 0,
            Compression::Zstd => 1,
            Compression::Lz4 => 2,
        };
        let k = match self.kind {
            BatchKind::Data => 0,
            BatchKind::Ack => 1,
            BatchKind::QueueState => 2,
            BatchKind::Timer => 3,
        };
        c | (u16::from(self.transactional) << 3) | (u16::from(self.control) << 4) | (k << 5)
    }

    /// Unpacks the on-disk representation.
    pub fn from_bits(bits: u16) -> Result<Self, Error> {
        let compression = match bits & 0b111 {
            0 => Compression::None,
            1 => Compression::Zstd,
            2 => Compression::Lz4,
            _ => return Err(Error::Malformed("unknown compression")),
        };
        let kind = match (bits >> 5) & 0b111 {
            0 => BatchKind::Data,
            1 => BatchKind::Ack,
            2 => BatchKind::QueueState,
            3 => BatchKind::Timer,
            _ => return Err(Error::Malformed("unknown batch kind")),
        };
        if bits >> 8 != 0 {
            return Err(Error::Malformed("reserved attribute bits set"));
        }
        Ok(Attributes { compression, transactional: bits & (1 << 3) != 0, control: bits & (1 << 4) != 0, kind })
    }
}

/// A record header (key/value metadata, e.g. a schema ID).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Header name.
    pub key: Bytes,
    /// Header value, `None` for null.
    pub value: Option<Bytes>,
}

/// One record, with offset and timestamp resolved against its batch.
///
/// Encoding inside the record block:
/// ```text
/// length        varint   bytes after this field
/// attributes    i8       reserved, 0
/// ts_delta      varint   relative to base_ts
/// offset_delta  varint   relative to base_offset
/// key_len       varint   -1 = null
/// key           bytes
/// value_len     varint   -1 = null (tombstone for compaction)
/// value         bytes
/// header_count  varint
///   [key_len varint, key bytes, value_len varint, value bytes]*
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Absolute offset (`base_offset + offset_delta`).
    pub offset: u64,
    /// Absolute timestamp in milliseconds (`base_ts + ts_delta`).
    pub timestamp: i64,
    /// Key, `None` for null.
    pub key: Option<Bytes>,
    /// Value, `None` for a tombstone.
    pub value: Option<Bytes>,
    /// Headers in insertion order.
    pub headers: Vec<Header>,
}

/// Header fields chosen by the producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchOptions {
    /// Attributes; compression must be [`Compression::None`] for now.
    pub attributes: Attributes,
    /// Producer ID, 0 for non-idempotent producers.
    pub producer_id: u64,
    /// Producer epoch.
    pub producer_epoch: u16,
    /// Sequence number of the first record.
    pub base_sequence: i32,
}

impl Default for BatchOptions {
    fn default() -> Self {
        BatchOptions { attributes: Attributes::DATA, producer_id: 0, producer_epoch: 0, base_sequence: -1 }
    }
}

/// Builds a batch record by record.
#[derive(Debug)]
pub struct BatchBuilder {
    options: BatchOptions,
    base_ts: Option<i64>,
    max_ts: i64,
    count: u32,
    body: Vec<u8>,
}

impl BatchBuilder {
    /// Starts an empty batch.
    pub fn new(options: BatchOptions) -> Self {
        BatchBuilder { options, base_ts: None, max_ts: 0, count: 0, body: Vec::new() }
    }

    /// Number of records so far.
    pub fn len(&self) -> u32 {
        self.count
    }

    /// `true` if no record was added.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Encoded size if the batch were finished now.
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.body.len()
    }

    /// Appends a record. The first record's timestamp becomes `base_ts`.
    pub fn push(
        &mut self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[(&[u8], Option<&[u8]>)],
    ) {
        let base_ts = *self.base_ts.get_or_insert(timestamp);
        self.max_ts = if self.count == 0 { timestamp } else { self.max_ts.max(timestamp) };

        let mut rec = Vec::with_capacity(16 + key.map_or(0, <[u8]>::len) + value.map_or(0, <[u8]>::len));
        rec.push(0); // attributes
        varint::put_i64(&mut rec, timestamp.wrapping_sub(base_ts));
        varint::put_i64(&mut rec, i64::from(self.count));
        put_nullable(&mut rec, key);
        put_nullable(&mut rec, value);
        varint::put_i64(&mut rec, headers.len() as i64);
        for (k, v) in headers {
            put_nullable(&mut rec, Some(k));
            put_nullable(&mut rec, *v);
        }
        varint::put_i64(&mut self.body, rec.len() as i64);
        self.body.extend_from_slice(&rec);
        self.count += 1;
    }

    /// Finishes the batch with `base_offset` and `term` set to zero; the
    /// leader stamps them with [`stamp`] when appending.
    pub fn finish(self) -> Result<Bytes, Error> {
        if self.count == 0 {
            return Err(Error::Malformed("empty batch"));
        }
        if self.options.attributes.compression != Compression::None {
            return Err(Error::UnsupportedCompression(self.options.attributes.compression));
        }
        let total = HEADER_LEN + self.body.len();
        if total > MAX_BATCH_LEN {
            return Err(Error::TooLarge(total));
        }
        let mut buf = BytesMut::zeroed(HEADER_LEN);
        buf.extend_from_slice(&self.body);
        put_u32(&mut buf, 8, (total - LEN_PREFIX) as u32);
        buf[20] = MAGIC;
        put_u16(&mut buf, 25, self.options.attributes.to_bits());
        put_u32(&mut buf, 27, self.count - 1);
        put_i64(&mut buf, 31, self.base_ts.unwrap_or(0));
        put_i64(&mut buf, 39, self.max_ts);
        put_u64(&mut buf, 47, self.options.producer_id);
        put_u16(&mut buf, 55, self.options.producer_epoch);
        put_i32(&mut buf, 57, self.options.base_sequence);
        put_u32(&mut buf, 61, self.count);
        let crc = crc32c::crc32c(&buf[CRC_START..]);
        put_u32(&mut buf, 21, crc);
        Ok(buf.freeze())
    }
}

/// Sets `base_offset` and `term` of an encoded batch in place. Both are
/// outside the checksummed range.
pub fn stamp(batch: &mut [u8], base_offset: u64, term: u64) -> Result<(), Error> {
    if batch.len() < HEADER_LEN {
        return Err(Error::Truncated);
    }
    put_u64(batch, 0, base_offset);
    put_u64(batch, 12, term);
    Ok(())
}

/// Total length of the batch starting at `buf[0]`, if `buf` holds at least
/// its length prefix. Used to frame batches when scanning a segment.
pub fn peek_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < LEN_PREFIX {
        return None;
    }
    Some(LEN_PREFIX + get_u32(buf, 8) as usize)
}

/// A validated batch that borrows nothing: cheap to clone, zero-copy to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    bytes: Bytes,
    attributes: Attributes,
}

impl Batch {
    /// Splits the first batch off `buf`, checking framing, version and CRC.
    pub fn parse(buf: &Bytes) -> Result<(Batch, Bytes), Error> {
        let len = peek_len(buf).ok_or(Error::Truncated)?;
        if len > MAX_BATCH_LEN {
            return Err(Error::TooLarge(len));
        }
        if len < HEADER_LEN {
            return Err(Error::Malformed("batch shorter than its header"));
        }
        if buf.len() < len {
            return Err(Error::Truncated);
        }
        let bytes = buf.slice(..len);
        if bytes[20] != MAGIC {
            return Err(Error::UnsupportedMagic(bytes[20]));
        }
        let stored = get_u32(&bytes, 21);
        let computed = crc32c::crc32c(&bytes[CRC_START..]);
        if stored != computed {
            return Err(Error::Checksum { stored, computed });
        }
        let attributes = Attributes::from_bits(get_u16(&bytes, 25))?;
        let batch = Batch { bytes, attributes };
        let count = batch.record_count();
        if count == 0 {
            return Err(Error::Malformed("empty batch"));
        }
        if batch.last_offset_delta() != count - 1 {
            return Err(Error::Malformed("last_offset_delta does not match record_count"));
        }
        if batch.max_timestamp() < batch.base_timestamp() {
            return Err(Error::Malformed("max_ts before base_ts"));
        }
        if attributes.control && count != 1 {
            return Err(Error::Malformed("control batch must hold one record"));
        }
        Ok((batch, buf.slice(len..)))
    }

    /// The encoded batch, exactly as stored and sent.
    pub fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Encoded length.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Always `false`: a valid batch holds at least one record.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Offset of the first record.
    pub fn base_offset(&self) -> u64 {
        get_u64(&self.bytes, 0)
    }
    /// Offset of the last record.
    pub fn last_offset(&self) -> u64 {
        self.base_offset() + u64::from(self.last_offset_delta())
    }
    /// Offset right after this batch.
    pub fn next_offset(&self) -> u64 {
        self.last_offset() + 1
    }
    /// Raft term the batch was appended in.
    pub fn term(&self) -> u64 {
        get_u64(&self.bytes, 12)
    }
    /// Decoded attributes.
    pub fn attributes(&self) -> Attributes {
        self.attributes
    }
    /// `last_offset_delta` header field.
    pub fn last_offset_delta(&self) -> u32 {
        get_u32(&self.bytes, 27)
    }
    /// Timestamp of the first record.
    pub fn base_timestamp(&self) -> i64 {
        get_i64(&self.bytes, 31)
    }
    /// Largest record timestamp.
    pub fn max_timestamp(&self) -> i64 {
        get_i64(&self.bytes, 39)
    }
    /// Producer ID, 0 if not idempotent.
    pub fn producer_id(&self) -> u64 {
        get_u64(&self.bytes, 47)
    }
    /// Producer epoch.
    pub fn producer_epoch(&self) -> u16 {
        get_u16(&self.bytes, 55)
    }
    /// Sequence number of the first record.
    pub fn base_sequence(&self) -> i32 {
        get_i32(&self.bytes, 57)
    }
    /// Number of records.
    pub fn record_count(&self) -> u32 {
        get_u32(&self.bytes, 61)
    }

    /// Decodes the records. Fails on compressed batches until codecs land.
    pub fn records(&self) -> Result<Vec<Record>, Error> {
        if self.attributes.compression != Compression::None {
            return Err(Error::UnsupportedCompression(self.attributes.compression));
        }
        let base_offset = self.base_offset();
        let base_ts = self.base_timestamp();
        let mut rest = &self.bytes[HEADER_LEN..];
        let mut out = Vec::with_capacity(self.record_count() as usize);
        for expected_delta in 0..self.record_count() {
            let (len, n) = varint::get_i64(rest)?;
            rest = &rest[n..];
            let len = usize::try_from(len).map_err(|_| Error::Malformed("negative record length"))?;
            if rest.len() < len {
                return Err(Error::Truncated);
            }
            let (rec, tail) = rest.split_at(len);
            rest = tail;
            let mut r = Cursor { buf: rec, base: &self.bytes, start: self.bytes.len() - tail.len() - len };
            let _attributes = r.byte()?;
            let ts_delta = r.i64()?;
            let offset_delta = r.i32()?;
            if offset_delta != expected_delta as i32 {
                return Err(Error::Malformed("record offset deltas are not consecutive"));
            }
            let key = r.nullable()?;
            let value = r.nullable()?;
            let header_count = usize::try_from(r.i64()?).map_err(|_| Error::Malformed("negative header count"))?;
            let mut headers = Vec::with_capacity(header_count.min(64));
            for _ in 0..header_count {
                let key = r.nullable()?.ok_or(Error::Malformed("null header key"))?;
                let value = r.nullable()?;
                headers.push(Header { key, value });
            }
            if !r.buf.is_empty() {
                return Err(Error::Malformed("trailing bytes in record"));
            }
            out.push(Record {
                offset: base_offset + offset_delta as u64,
                timestamp: base_ts.wrapping_add(ts_delta),
                key,
                value,
                headers,
            });
        }
        if !rest.is_empty() {
            return Err(Error::Malformed("trailing bytes after records"));
        }
        Ok(out)
    }
}

/// Transaction marker type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlType {
    /// The producer's open transaction in this partition is aborted.
    Abort,
    /// The producer's open transaction in this partition is committed.
    Commit,
}

/// Builds a control batch (transaction marker) for `producer_id`.
pub fn control_batch(
    producer_id: u64,
    producer_epoch: u16,
    marker: ControlType,
    coordinator_epoch: u32,
    timestamp: i64,
) -> Result<Bytes, Error> {
    let mut b = BatchBuilder::new(BatchOptions {
        attributes: Attributes {
            compression: Compression::None,
            transactional: true,
            control: true,
            kind: BatchKind::Data,
        },
        producer_id,
        producer_epoch,
        base_sequence: -1,
    });
    let ty: u16 = match marker {
        ControlType::Abort => 0,
        ControlType::Commit => 1,
    };
    let mut key = [0u8; 4]; // version u16 | type u16
    key[2..].copy_from_slice(&ty.to_le_bytes());
    b.push(timestamp, Some(&key), Some(&coordinator_epoch.to_le_bytes()), &[]);
    b.finish()
}

/// Raft's own entries, stored as control batches that readers skip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftControl {
    /// The empty entry a new leader appends.
    Noop,
    /// A membership change; the record value is the encoded membership.
    Config,
}

/// Builds a Raft control batch carrying `payload` (empty for a no-op).
pub fn raft_control_batch(kind: RaftControl, payload: &[u8]) -> Result<Bytes, Error> {
    let mut b = BatchBuilder::new(BatchOptions {
        attributes: Attributes {
            compression: Compression::None,
            transactional: false,
            control: true,
            kind: BatchKind::Data,
        },
        producer_id: 0,
        producer_epoch: 0,
        base_sequence: -1,
    });
    let ty: u16 = match kind {
        RaftControl::Noop => 2,
        RaftControl::Config => 3,
    };
    let mut key = [0u8; 4]; // version u16 | type u16
    key[2..].copy_from_slice(&ty.to_le_bytes());
    b.push(0, Some(&key), Some(payload), &[]);
    b.finish()
}

/// If `batch` is a Raft control batch, its kind and payload.
pub fn raft_control(batch: &Batch) -> Result<Option<(RaftControl, Bytes)>, Error> {
    if !batch.attributes().control {
        return Ok(None);
    }
    let rec = batch.records()?.into_iter().next().ok_or(Error::Malformed("empty control batch"))?;
    let key = rec.key.ok_or(Error::Malformed("control record without key"))?;
    if key.len() != 4 {
        return Err(Error::Malformed("control record has the wrong size"));
    }
    let kind = match u16::from_le_bytes([key[2], key[3]]) {
        2 => RaftControl::Noop,
        3 => RaftControl::Config,
        _ => return Ok(None), // a transaction marker
    };
    Ok(Some((kind, rec.value.unwrap_or_default())))
}

/// Reads the marker of a control batch.
pub fn control_marker(batch: &Batch) -> Result<(ControlType, u32), Error> {
    if !batch.attributes().control {
        return Err(Error::Malformed("not a control batch"));
    }
    let rec = batch.records()?.into_iter().next().ok_or(Error::Malformed("empty control batch"))?;
    let key = rec.key.ok_or(Error::Malformed("control record without key"))?;
    let value = rec.value.ok_or(Error::Malformed("control record without value"))?;
    if key.len() != 4 || value.len() != 4 {
        return Err(Error::Malformed("control record has the wrong size"));
    }
    let marker = match u16::from_le_bytes([key[2], key[3]]) {
        0 => ControlType::Abort,
        1 => ControlType::Commit,
        _ => return Err(Error::Malformed("unknown control type")),
    };
    Ok((marker, u32::from_le_bytes([value[0], value[1], value[2], value[3]])))
}

struct Cursor<'a> {
    buf: &'a [u8],
    base: &'a Bytes,
    start: usize,
}

impl Cursor<'_> {
    fn advance(&mut self, n: usize) {
        self.buf = &self.buf[n..];
        self.start += n;
    }
    fn byte(&mut self) -> Result<u8, Error> {
        let b = *self.buf.first().ok_or(Error::Truncated)?;
        self.advance(1);
        Ok(b)
    }
    fn i64(&mut self) -> Result<i64, Error> {
        let (v, n) = varint::get_i64(self.buf)?;
        self.advance(n);
        Ok(v)
    }
    fn i32(&mut self) -> Result<i32, Error> {
        let (v, n) = varint::get_i32(self.buf)?;
        self.advance(n);
        Ok(v)
    }
    /// Length-prefixed bytes, sliced out of the batch without copying.
    fn nullable(&mut self) -> Result<Option<Bytes>, Error> {
        let len = self.i64()?;
        if len == -1 {
            return Ok(None);
        }
        let len = usize::try_from(len).map_err(|_| Error::Malformed("negative length"))?;
        if self.buf.len() < len {
            return Err(Error::Truncated);
        }
        let out = self.base.slice(self.start..self.start + len);
        self.advance(len);
        Ok(Some(out))
    }
}

fn put_nullable(buf: &mut Vec<u8>, v: Option<&[u8]>) {
    match v {
        None => varint::put_i64(buf, -1),
        Some(b) => {
            varint::put_i64(buf, b.len() as i64);
            buf.extend_from_slice(b);
        }
    }
}

fn get_u16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}
fn get_u32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}
fn get_i32(b: &[u8], at: usize) -> i32 {
    get_u32(b, at) as i32
}
fn get_u64(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}
fn get_i64(b: &[u8], at: usize) -> i64 {
    get_u64(b, at) as i64
}
fn put_u16(b: &mut [u8], at: usize, v: u16) {
    b[at..at + 2].copy_from_slice(&v.to_le_bytes());
}
fn put_u32(b: &mut [u8], at: usize, v: u32) {
    b[at..at + 4].copy_from_slice(&v.to_le_bytes());
}
fn put_i32(b: &mut [u8], at: usize, v: i32) {
    put_u32(b, at, v as u32);
}
fn put_u64(b: &mut [u8], at: usize, v: u64) {
    b[at..at + 8].copy_from_slice(&v.to_le_bytes());
}
fn put_i64(b: &mut [u8], at: usize, v: i64) {
    put_u64(b, at, v as u64);
}

#[cfg(test)]
mod tests;

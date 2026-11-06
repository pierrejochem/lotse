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
/// Largest record block of a compressed batch once decompressed: a batch
/// that would decompress to more is refused, so a small batch cannot make
/// a reader allocate without bound (ADR 0058).
pub const MAX_RECORDS_LEN: usize = 16 * 1024 * 1024;
/// zstd level the [`BatchBuilder`] compresses at: zstd's default.
pub const ZSTD_LEVEL: i32 = 3;

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
    /// A codec failed: a compressed record block does not decode.
    #[error("compression codec {0:?} failed: {1}")]
    Codec(Compression, String),
    /// A compressed record block decodes to more than [`MAX_RECORDS_LEN`].
    #[error("compressed record block decodes to more than {MAX_RECORDS_LEN} bytes")]
    DecompressedTooLarge,
}

/// Compression of the record block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Records are stored as is.
    None,
    /// The record block is one zstd frame.
    Zstd,
    /// The record block is one lz4 frame (the LZ4 frame format, not the
    /// bare block format).
    Lz4,
}

impl Compression {
    /// Compresses a record block.
    fn compress(self, block: &[u8]) -> Result<Vec<u8>, Error> {
        use std::io::Write as _;
        match self {
            Compression::None => Ok(block.to_vec()),
            Compression::Zstd => zstd::bulk::compress(block, ZSTD_LEVEL).map_err(|e| Error::Codec(self, e.to_string())),
            Compression::Lz4 => {
                let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::with_capacity(block.len() / 2 + 64));
                enc.write_all(block).map_err(|e| Error::Codec(self, e.to_string()))?;
                enc.finish().map_err(|e| Error::Codec(self, e.to_string()))
            }
        }
    }

    /// Decompresses a record block, refusing one that decodes to more than
    /// [`MAX_RECORDS_LEN`] bytes before it gets there.
    fn decompress(self, block: &[u8]) -> Result<Bytes, Error> {
        use std::io::Read as _;
        let limit = MAX_RECORDS_LEN as u64 + 1;
        let mut out = Vec::new();
        let read = match self {
            Compression::None => return Ok(Bytes::copy_from_slice(block)),
            Compression::Zstd => {
                zstd::stream::read::Decoder::new(block).and_then(|d| d.take(limit).read_to_end(&mut out))
            }
            Compression::Lz4 => lz4_flex::frame::FrameDecoder::new(block).take(limit).read_to_end(&mut out),
        };
        read.map_err(|e| Error::Codec(self, e.to_string()))?;
        if out.len() > MAX_RECORDS_LEN {
            return Err(Error::DecompressedTooLarge);
        }
        Ok(Bytes::from(out))
    }
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
    /// Messages moved here from another queue's dead-lettering (ADR 0055):
    /// deliverable like data, but each record names where it came from, so
    /// that a repeated move is recognised when the batch is applied.
    DeadLetter,
    /// Messages leaving a queue because of its limits: the claim that
    /// settles them and obliges the leader to forward them, and the note
    /// that they have been forwarded (ADR 0055).
    Retire,
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
            BatchKind::DeadLetter => 4,
            BatchKind::Retire => 5,
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
            4 => BatchKind::DeadLetter,
            5 => BatchKind::Retire,
            _ => return Err(Error::Malformed("unknown batch kind")),
        };
        // Bit 8 marks a compacted batch (ADR 0067); [`Batch::is_compacted`]
        // reads it.
        if bits >> 9 != 0 {
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
    /// Attributes; with a codec, [`BatchBuilder::finish`] compresses the
    /// record block.
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

    /// Encoded size if the batch were finished now without compression: an
    /// upper bound for a compressed one, give or take the codec's frame.
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
        let codec = self.options.attributes.compression;
        if codec != Compression::None && self.body.len() > MAX_RECORDS_LEN {
            return Err(Error::DecompressedTooLarge);
        }
        let body = if codec == Compression::None { self.body } else { codec.compress(&self.body)? };
        let total = HEADER_LEN + body.len();
        if total > MAX_BATCH_LEN {
            return Err(Error::TooLarge(total));
        }
        let mut buf = BytesMut::zeroed(HEADER_LEN);
        buf.extend_from_slice(&body);
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

/// Compaction of one data batch (ADR 0067): the records `keep` accepts, at
/// their own offsets and timestamps, in a batch that spans the same offsets
/// and keeps the header (term, producer, sequence, timestamps, codec), with
/// the compacted bit set. `None` if every record is kept: the batch stays as
/// it was. A control batch is never compacted.
pub fn compact(batch: &Batch, mut keep: impl FnMut(&Record) -> bool) -> Result<Option<Bytes>, Error> {
    if batch.attributes().control {
        return Ok(None);
    }
    let records = batch.records()?;
    let kept: Vec<&Record> = records.iter().filter(|r| keep(r)).collect();
    if kept.len() == records.len() {
        return Ok(None);
    }
    let base_offset = batch.base_offset();
    let base_ts = batch.base_timestamp();
    let mut body = Vec::new();
    for r in &kept {
        let mut rec =
            Vec::with_capacity(16 + r.key.as_ref().map_or(0, |k| k.len()) + r.value.as_ref().map_or(0, |v| v.len()));
        rec.push(0);
        varint::put_i64(&mut rec, r.timestamp.wrapping_sub(base_ts));
        varint::put_i64(&mut rec, (r.offset - base_offset) as i64);
        put_nullable(&mut rec, r.key.as_deref());
        put_nullable(&mut rec, r.value.as_deref());
        varint::put_i64(&mut rec, r.headers.len() as i64);
        for h in &r.headers {
            put_nullable(&mut rec, Some(&h.key));
            put_nullable(&mut rec, h.value.as_deref());
        }
        varint::put_i64(&mut body, rec.len() as i64);
        body.extend_from_slice(&rec);
    }
    let codec = batch.attributes().compression;
    let body = if codec == Compression::None || body.is_empty() { body } else { codec.compress(&body)? };
    let total = HEADER_LEN + body.len();
    let mut buf = BytesMut::from(&batch.as_bytes()[..HEADER_LEN]);
    buf.extend_from_slice(&body);
    put_u32(&mut buf, 8, (total - LEN_PREFIX) as u32);
    put_u16(&mut buf, 25, batch.attributes().to_bits() | (1 << 8));
    put_u32(&mut buf, 61, kept.len() as u32);
    let crc = crc32c::crc32c(&buf[CRC_START..]);
    put_u32(&mut buf, 21, crc);
    Ok(Some(buf.freeze()))
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

/// Moves an encoded batch's timestamps so that its first record has `base_ts`,
/// keeping the distances between records, and recomputes the checksum. The
/// leader of a queue with a message TTL stamps the batches it appends with
/// its own clock, so that a message's age does not depend on its producer's.
pub fn set_base_timestamp(batch: &mut [u8], base_ts: i64) -> Result<(), Error> {
    let len = peek_len(batch).ok_or(Error::Truncated)?;
    if len < HEADER_LEN || batch.len() < len {
        return Err(Error::Truncated);
    }
    let (old_base, old_max) = (get_i64(batch, 31), get_i64(batch, 39));
    put_i64(batch, 31, base_ts);
    put_i64(batch, 39, base_ts.wrapping_add(old_max.wrapping_sub(old_base)));
    let crc = crc32c::crc32c(&batch[CRC_START..len]);
    put_u32(batch, 21, crc);
    Ok(())
}

/// Sets the producer fields of an encoded batch in place and recomputes
/// its checksum, which covers them. An idempotent client stamps the batch it
/// was given with its producer id, epoch and the sequence of the first record.
pub fn set_producer(batch: &mut [u8], producer_id: u64, producer_epoch: u16, base_sequence: i32) -> Result<(), Error> {
    let len = peek_len(batch).ok_or(Error::Truncated)?;
    if len < HEADER_LEN || batch.len() < len {
        return Err(Error::Truncated);
    }
    put_u64(batch, 47, producer_id);
    put_u16(batch, 55, producer_epoch);
    put_i32(batch, 57, base_sequence);
    let crc = crc32c::crc32c(&batch[CRC_START..len]);
    put_u32(batch, 21, crc);
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
        if batch.is_compacted() {
            // Compaction keeps a batch's offsets and drops records from it,
            // down to none (ADR 0067).
            if attributes.control {
                return Err(Error::Malformed("a control batch is never compacted"));
            }
            if count > batch.span() {
                return Err(Error::Malformed("more records than offsets"));
            }
        } else {
            if count == 0 {
                return Err(Error::Malformed("empty batch"));
            }
            if batch.last_offset_delta() != count - 1 {
                return Err(Error::Malformed("last_offset_delta does not match record_count"));
            }
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

    /// Whether no record is left: only a compacted batch can be empty.
    pub fn is_empty(&self) -> bool {
        self.record_count() == 0
    }

    /// Whether compaction rewrote this batch (attribute bit 8, ADR 0067):
    /// its records are a subset of the offsets it spans, in order, and it
    /// may hold none.
    pub fn is_compacted(&self) -> bool {
        get_u16(&self.bytes, 25) & (1 << 8) != 0
    }

    /// How many offsets the batch spans: `last_offset_delta + 1`. Equal to
    /// the record count unless the batch is compacted.
    pub fn span(&self) -> u32 {
        self.last_offset_delta() + 1
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

    /// The record block, decompressed if the batch is compressed. Keys,
    /// values and headers of [`Batch::records`] point into it.
    pub fn record_block(&self) -> Result<Bytes, Error> {
        if self.is_compacted() && self.record_count() == 0 {
            // Compaction writes an emptied batch's block plain, as nothing.
            return Ok(Bytes::new());
        }
        match self.attributes.compression {
            Compression::None => Ok(self.bytes.slice(HEADER_LEN..)),
            codec => codec.decompress(&self.bytes[HEADER_LEN..]),
        }
    }

    /// Decodes the records, decompressing the block first if need be.
    pub fn records(&self) -> Result<Vec<Record>, Error> {
        let block = self.record_block()?;
        let base_offset = self.base_offset();
        let base_ts = self.base_timestamp();
        let mut rest = &block[..];
        let mut out = Vec::with_capacity(self.record_count() as usize);
        let compacted = self.is_compacted();
        let mut last_delta: i32 = -1;
        for expected_delta in 0..self.record_count() {
            let (len, n) = varint::get_i64(rest)?;
            rest = &rest[n..];
            let len = usize::try_from(len).map_err(|_| Error::Malformed("negative record length"))?;
            if rest.len() < len {
                return Err(Error::Truncated);
            }
            let (rec, tail) = rest.split_at(len);
            rest = tail;
            let mut r = Cursor { buf: rec, base: &block, start: block.len() - tail.len() - len };
            let _attributes = r.byte()?;
            let ts_delta = r.i64()?;
            let offset_delta = r.i32()?;
            if compacted {
                // Gaps where records went, but in order and inside the batch.
                if offset_delta <= last_delta || offset_delta as i64 > i64::from(self.last_offset_delta()) {
                    return Err(Error::Malformed("record offset deltas out of order"));
                }
                last_delta = offset_delta;
            } else if offset_delta != expected_delta as i32 {
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

/// The broker's own records in a coordinator group, stored as control
/// batches so that no user state machine or consumer takes them for data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InternalControl {
    /// A producer id allocation; the entry's Raft index is the id.
    ProducerId,
    /// The state of one transactional id; the value is its encoding.
    TxnState,
    /// The membership and assignment of one consumer group.
    GroupState,
    /// Offsets committed for a consumer group.
    GroupOffsets,
    /// A change to the cluster's resources, in the metadata group.
    Metadata,
    /// Coordinator keys to forget: of a tenant's incarnation that is gone.
    Forget,
}

/// Builds an internal control batch carrying `payload`.
pub fn internal_control_batch(kind: InternalControl, payload: &[u8]) -> Result<Bytes, Error> {
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
        InternalControl::ProducerId => 4,
        InternalControl::TxnState => 5,
        InternalControl::GroupState => 6,
        InternalControl::GroupOffsets => 7,
        InternalControl::Metadata => 8,
        InternalControl::Forget => 9,
    };
    let mut key = [0u8; 4]; // version u16 | type u16
    key[2..].copy_from_slice(&ty.to_le_bytes());
    b.push(0, Some(&key), Some(payload), &[]);
    b.finish()
}

/// Builds an ACK batch: the offset ranges `[from, to)` a queue's consumers
/// acknowledged, as ZigZag varints (`from`, then the length) in the value of
/// one record with an empty key.
pub fn ack_batch(ranges: &[(u64, u64)]) -> Result<Bytes, Error> {
    let mut value = Vec::new();
    varint::put_i64(&mut value, ranges.len() as i64);
    for (from, to) in ranges {
        if to <= from {
            return Err(Error::Malformed("empty ack range"));
        }
        varint::put_i64(&mut value, *from as i64);
        varint::put_i64(&mut value, (to - from) as i64);
    }
    queue_batch(BatchKind::Ack, &value)
}

/// The ranges of an ACK batch.
pub fn ack_ranges(batch: &Batch) -> Result<Vec<(u64, u64)>, Error> {
    let value = queue_value(batch, BatchKind::Ack)?;
    let mut rest = &value[..];
    let mut next = || -> Result<u64, Error> {
        let (v, n) = varint::get_i64(rest)?;
        rest = &rest[n..];
        u64::try_from(v).map_err(|_| Error::Malformed("negative ack offset"))
    };
    let count = next()?;
    let mut out = Vec::with_capacity(count.min(1024) as usize);
    for _ in 0..count {
        let from = next()?;
        let len = next()?;
        if len == 0 {
            return Err(Error::Malformed("empty ack range"));
        }
        out.push((from, from.checked_add(len).ok_or(Error::Malformed("ack range overflows"))?));
    }
    if !rest.is_empty() {
        return Err(Error::Malformed("trailing bytes in ack batch"));
    }
    Ok(out)
}

/// What a RETIRE batch says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retire {
    /// The leader claims these messages `(offset, reason, deliveries)`: they
    /// are settled in this queue and must be forwarded to queue `target`.
    Out {
        /// Resource id of the dead-letter queue.
        target: u64,
        /// The messages.
        entries: Vec<(u64, dead_letter::Reason, u16)>,
    },
    /// These claimed messages have been forwarded; nothing is owed for them.
    Done(Vec<u64>),
}

/// Builds a RETIRE batch claiming `entries` for queue `target`.
pub fn retire_out_batch(target: u64, entries: &[(u64, dead_letter::Reason, u16)]) -> Result<Bytes, Error> {
    let mut value = Vec::new();
    varint::put_i64(&mut value, 0);
    varint::put_i64(&mut value, target as i64);
    varint::put_i64(&mut value, entries.len() as i64);
    for (offset, reason, deliveries) in entries {
        varint::put_i64(&mut value, *offset as i64);
        varint::put_i64(&mut value, i64::from(reason.code()));
        varint::put_i64(&mut value, i64::from(*deliveries));
    }
    queue_batch(BatchKind::Retire, &value)
}

/// Builds a RETIRE batch noting that `offsets` have been forwarded.
pub fn retire_done_batch(offsets: &[u64]) -> Result<Bytes, Error> {
    let mut value = Vec::new();
    varint::put_i64(&mut value, 1);
    varint::put_i64(&mut value, offsets.len() as i64);
    for offset in offsets {
        varint::put_i64(&mut value, *offset as i64);
    }
    queue_batch(BatchKind::Retire, &value)
}

/// What a RETIRE batch says.
pub fn retire(batch: &Batch) -> Result<Retire, Error> {
    let value = queue_value(batch, BatchKind::Retire)?;
    let mut rest = &value[..];
    let mut next = || -> Result<u64, Error> {
        let (v, n) = varint::get_i64(rest)?;
        rest = &rest[n..];
        u64::try_from(v).map_err(|_| Error::Malformed("negative number in retire batch"))
    };
    let out = match next()? {
        0 => {
            let target = next()?;
            let count = next()?;
            let mut entries = Vec::new();
            for _ in 0..count {
                let offset = next()?;
                let reason = u8::try_from(next()?)
                    .ok()
                    .and_then(dead_letter::Reason::from_code)
                    .ok_or(Error::Malformed("unknown dead-letter reason"))?;
                let deliveries = u16::try_from(next()?).map_err(|_| Error::Malformed("delivery count out of range"))?;
                entries.push((offset, reason, deliveries));
            }
            Retire::Out { target, entries }
        }
        1 => {
            let count = next()?;
            let mut offsets = Vec::new();
            for _ in 0..count {
                offsets.push(next()?);
            }
            Retire::Done(offsets)
        }
        _ => return Err(Error::Malformed("unknown retire batch")),
    };
    if !rest.is_empty() {
        return Err(Error::Malformed("trailing bytes in retire batch"));
    }
    Ok(out)
}

/// Builds a QUEUE_STATE batch: delivery counts `(offset, count)` of a
/// queue's messages, written on nack or ack timeout.
pub fn queue_state_batch(counts: &[(u64, u16)]) -> Result<Bytes, Error> {
    let mut value = Vec::new();
    varint::put_i64(&mut value, counts.len() as i64);
    for (offset, count) in counts {
        varint::put_i64(&mut value, *offset as i64);
        varint::put_i64(&mut value, i64::from(*count));
    }
    queue_batch(BatchKind::QueueState, &value)
}

/// The delivery counts of a QUEUE_STATE batch.
pub fn queue_state_counts(batch: &Batch) -> Result<Vec<(u64, u16)>, Error> {
    let value = queue_value(batch, BatchKind::QueueState)?;
    let mut rest = &value[..];
    let mut next = || -> Result<i64, Error> {
        let (v, n) = varint::get_i64(rest)?;
        rest = &rest[n..];
        Ok(v)
    };
    let count = next()?;
    let mut out = Vec::new();
    for _ in 0..count {
        let offset = u64::try_from(next()?).map_err(|_| Error::Malformed("negative offset"))?;
        let n = u16::try_from(next()?).map_err(|_| Error::Malformed("delivery count out of range"))?;
        out.push((offset, n));
    }
    if !rest.is_empty() {
        return Err(Error::Malformed("trailing bytes in queue state batch"));
    }
    Ok(out)
}

fn queue_batch(kind: BatchKind, value: &[u8]) -> Result<Bytes, Error> {
    let mut b = BatchBuilder::new(BatchOptions {
        attributes: Attributes { compression: Compression::None, transactional: false, control: false, kind },
        producer_id: 0,
        producer_epoch: 0,
        base_sequence: -1,
    });
    b.push(0, None, Some(value), &[]);
    b.finish()
}

fn queue_value(batch: &Batch, kind: BatchKind) -> Result<Bytes, Error> {
    if batch.attributes().kind != kind {
        return Err(Error::Malformed("not a batch of this kind"));
    }
    let rec = batch.records()?.into_iter().next().ok_or(Error::Malformed("empty queue batch"))?;
    rec.value.ok_or(Error::Malformed("queue batch without value"))
}

/// Header names of a dead-lettered message (ADR 0055). Values are decimal
/// text or the reason's name, so that a consumer can read them without a
/// codec. They describe the latest move: the ones a message arrived with are
/// replaced when it is dead-lettered again.
pub mod dead_letter {
    /// Why: [`Reason::name`].
    pub const REASON: &[u8] = b"x-lotse-dead-letter-reason";
    /// Name of the queue the message came from.
    pub const QUEUE: &[u8] = b"x-lotse-dead-letter-queue";
    /// Resource id of that queue; with the offset, the message's identity.
    pub const QUEUE_ID: &[u8] = b"x-lotse-dead-letter-queue-id";
    /// The message's offset in that queue.
    pub const OFFSET: &[u8] = b"x-lotse-dead-letter-offset";
    /// How often it had been delivered.
    pub const DELIVERIES: &[u8] = b"x-lotse-dead-letter-deliveries";
    /// The ack floor of that queue when the message was sent: every offset
    /// below it is settled there, so it will not be sent again.
    pub const FLOOR: &[u8] = b"x-lotse-dead-letter-floor";

    /// Why a message was dead-lettered.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Reason {
        /// It was delivered as often as the queue allows.
        MaxDeliveries,
        /// It waited longer than the queue's message TTL.
        Expired,
        /// An operator moved it from a dead-letter queue back to the queue
        /// it came from (ADR 0064).
        Requeued,
    }

    impl Reason {
        /// The code in a retire batch.
        pub fn code(self) -> u8 {
            match self {
                Reason::MaxDeliveries => 0,
                Reason::Expired => 1,
                Reason::Requeued => 2,
            }
        }

        /// The reason a code stands for.
        pub fn from_code(code: u8) -> Option<Reason> {
            match code {
                0 => Some(Reason::MaxDeliveries),
                1 => Some(Reason::Expired),
                2 => Some(Reason::Requeued),
                _ => None,
            }
        }

        /// The name in the reason header.
        pub fn name(self) -> &'static str {
            match self {
                Reason::MaxDeliveries => "max-deliveries",
                Reason::Expired => "expired",
                Reason::Requeued => "requeued",
            }
        }
    }
}

/// Where a dead-lettered message came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Origin {
    /// Resource id of the queue it was dead-lettered from.
    pub queue_id: u64,
    /// Its offset there.
    pub offset: u64,
    /// That queue's ack floor when it was sent.
    pub floor: u64,
}

/// What the leader of a queue knows when it dead-letters a message.
#[derive(Debug, Clone, Copy)]
pub struct DeadLetterInfo<'a> {
    /// Name of the source queue.
    pub queue: &'a str,
    /// Its resource id.
    pub queue_id: u64,
    /// The message's offset there.
    pub offset: u64,
    /// The source's ack floor.
    pub floor: u64,
    /// Why.
    pub reason: dead_letter::Reason,
    /// How often it was delivered.
    pub deliveries: u16,
}

fn is_dead_letter_header(key: &[u8]) -> bool {
    key.starts_with(b"x-lotse-dead-letter-")
}

/// A one-record data batch holding `record` as it was (key, value, headers,
/// timestamp) plus the headers of [`dead_letter`] describing its move.
pub fn dead_letter_item(record: &Record, info: DeadLetterInfo<'_>) -> Result<Bytes, Error> {
    let mut b = BatchBuilder::new(BatchOptions::default());
    let own: [(&[u8], String); 6] = [
        (dead_letter::REASON, info.reason.name().to_string()),
        (dead_letter::QUEUE, info.queue.to_string()),
        (dead_letter::QUEUE_ID, info.queue_id.to_string()),
        (dead_letter::OFFSET, info.offset.to_string()),
        (dead_letter::DELIVERIES, info.deliveries.to_string()),
        (dead_letter::FLOOR, info.floor.to_string()),
    ];
    let mut headers: Vec<(&[u8], Option<&[u8]>)> = record
        .headers
        .iter()
        .filter(|h| !is_dead_letter_header(&h.key))
        .map(|h| (&h.key[..], h.value.as_deref()))
        .collect();
    headers.extend(own.iter().map(|(k, v)| (*k, Some(v.as_bytes()))));
    b.push(record.timestamp, record.key.as_deref(), record.value.as_deref(), &headers);
    b.finish()
}

/// The origin a dead-lettered record names; an error if it names none or
/// names it badly.
pub fn dead_letter_origin(record: &Record) -> Result<Origin, Error> {
    let number = |name: &[u8]| -> Result<u64, Error> {
        let value = record
            .headers
            .iter()
            .rev()
            .find(|h| h.key[..] == *name)
            .and_then(|h| h.value.as_ref())
            .ok_or(Error::Malformed("dead-letter record without its origin"))?;
        std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(Error::Malformed("dead-letter origin is not a number"))
    };
    Ok(Origin {
        queue_id: number(dead_letter::QUEUE_ID)?,
        offset: number(dead_letter::OFFSET)?,
        floor: number(dead_letter::FLOOR)?,
    })
}

/// Joins the records of `items` (one-record batches from
/// [`dead_letter_item`]) into one dead-letter batch stamped with `timestamp`,
/// the time the leader of the receiving queue appends it.
pub fn dead_letter_batch(items: &[Batch], timestamp: i64) -> Result<Bytes, Error> {
    let mut b = BatchBuilder::new(BatchOptions {
        attributes: Attributes {
            compression: Compression::None,
            transactional: false,
            control: false,
            kind: BatchKind::DeadLetter,
        },
        ..BatchOptions::default()
    });
    for item in items {
        for r in item.records()? {
            dead_letter_origin(&r)?;
            let headers: Vec<(&[u8], Option<&[u8]>)> =
                r.headers.iter().map(|h| (&h.key[..], h.value.as_deref())).collect();
            b.push(timestamp, r.key.as_deref(), r.value.as_deref(), &headers);
        }
    }
    b.finish()
}

/// What a control batch is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    /// A transaction marker and its coordinator epoch.
    Marker(ControlType, u32),
    /// Raft's own entry and its payload.
    Raft(RaftControl, Bytes),
    /// A coordinator record and its payload.
    Internal(InternalControl, Bytes),
}

/// Classifies a control batch; `None` for data batches.
pub fn control(batch: &Batch) -> Result<Option<Control>, Error> {
    if !batch.attributes().control {
        return Ok(None);
    }
    let rec = batch.records()?.into_iter().next().ok_or(Error::Malformed("empty control batch"))?;
    let key = rec.key.ok_or(Error::Malformed("control record without key"))?;
    if key.len() != 4 {
        return Err(Error::Malformed("control record has the wrong size"));
    }
    let value = rec.value.unwrap_or_default();
    Ok(Some(match u16::from_le_bytes([key[2], key[3]]) {
        ty @ (0 | 1) => {
            let epoch: [u8; 4] =
                value[..].try_into().map_err(|_| Error::Malformed("marker value has the wrong size"))?;
            let marker = if ty == 0 { ControlType::Abort } else { ControlType::Commit };
            Control::Marker(marker, u32::from_le_bytes(epoch))
        }
        2 => Control::Raft(RaftControl::Noop, value),
        3 => Control::Raft(RaftControl::Config, value),
        4 => Control::Internal(InternalControl::ProducerId, value),
        5 => Control::Internal(InternalControl::TxnState, value),
        6 => Control::Internal(InternalControl::GroupState, value),
        7 => Control::Internal(InternalControl::GroupOffsets, value),
        8 => Control::Internal(InternalControl::Metadata, value),
        9 => Control::Internal(InternalControl::Forget, value),
        _ => return Err(Error::Malformed("unknown control type")),
    }))
}

/// Marks an encoded batch as transactional (or not) and recomputes its
/// checksum. A transactional client sets this on the batches it is given.
pub fn set_transactional(batch: &mut [u8], transactional: bool) -> Result<(), Error> {
    let len = peek_len(batch).ok_or(Error::Truncated)?;
    if len < HEADER_LEN || batch.len() < len {
        return Err(Error::Truncated);
    }
    let bits = u16::from_le_bytes([batch[25], batch[26]]);
    let bits = if transactional { bits | 1 << 3 } else { bits & !(1 << 3) };
    put_u16(batch, 25, bits);
    let crc = crc32c::crc32c(&batch[CRC_START..len]);
    put_u32(batch, 21, crc);
    Ok(())
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

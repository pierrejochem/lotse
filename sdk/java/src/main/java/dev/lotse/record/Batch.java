package dev.lotse.record;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * A validated record batch.
 *
 * <p>{@link #parse} checks framing, format version and checksum and then the header's own consistency;
 * {@link #records()} decodes and checks the records. Layout of the 65-byte header, all fixed-width fields
 * little-endian:
 *
 * <pre>
 * offset  size  field
 *      0     8  base_offset        Raft index of the first record
 *      8     4  batch_len          bytes after this field
 *     12     8  term               Raft term (leader epoch)
 *     20     1  magic
 *     21     4  crc32c             over bytes 25..end
 *     25     2  attributes         compression(3) | txn(1) | control(1) | kind(3)
 *     27     4  last_offset_delta
 *     31     8  base_ts
 *     39     8  max_ts
 *     47     8  producer_id        0 = not idempotent
 *     55     2  producer_epoch
 *     57     4  base_sequence
 *     61     4  record_count
 *     65     …  records
 * </pre>
 *
 * <p>The broker stamps {@code base_offset} and {@code term} when it appends; both sit before the checksummed range.
 * The static methods edit an encoded batch in place the way a client does before sending it.
 */
public final class Batch {
    /** The size of the fixed batch header. */
    public static final int HEADER_LEN = 65;
    /** The bytes before {@code batch_len} counts: {@code base_offset} and {@code batch_len} itself. */
    public static final int LEN_PREFIX = 12;
    /** The current format version. */
    public static final int MAGIC = 1;
    /** The largest batch accepted, header included. */
    public static final int MAX_BATCH_LEN = 1024 * 1024;
    /** The most a compressed record block may decompress to. */
    public static final int MAX_RECORDS_LEN = 16 * 1024 * 1024;

    static final int CRC_START = 25;

    private final ByteString bytes;
    private final Attributes attributes;

    private Batch(ByteString bytes, Attributes attributes) {
        this.bytes = bytes;
        this.attributes = attributes;
    }

    /**
     * The first batch of a buffer and what follows it.
     *
     * @param batch the first batch
     * @param rest the bytes after it; empty when the buffer held exactly one batch
     */
    public record Split(Batch batch, ByteString rest) {}

    /**
     * Splits the first batch off {@code buf}, checking in this order: that the length prefix is there, the size
     * limit, that the batch is at least a header and fully present, the magic byte, the checksum, the attributes,
     * a non-empty record count, {@code last_offset_delta == record_count - 1}, {@code max_ts >= base_ts} and that
     * a control batch holds one record.
     *
     * @throws RecordFormatException if any check fails
     */
    public static Split parse(ByteString buf) {
        Objects.requireNonNull(buf);
        long len = peekLen(buf.array(), buf.offset(), buf.size());
        if (len < 0) {
            throw RecordFormatException.truncated();
        }
        if (len > MAX_BATCH_LEN) {
            throw new RecordFormatException(
                    RecordFormatException.Kind.TOO_LARGE, "batch of " + len + " bytes exceeds the limit");
        }
        if (len < HEADER_LEN) {
            throw RecordFormatException.malformed("batch shorter than its header");
        }
        if (buf.size() < len) {
            throw RecordFormatException.truncated();
        }
        int n = (int) len;
        ByteString bytes = buf.slice(0, n);
        byte[] a = bytes.array();
        int o = bytes.offset();
        int magic = a[o + 20] & 0xFF;
        if (magic != MAGIC) {
            throw new RecordFormatException(
                    RecordFormatException.Kind.UNSUPPORTED_MAGIC, "unsupported batch format version " + magic);
        }
        long stored = getU32(a, o + 21);
        long computed = crc(a, o + CRC_START, o + n);
        if (stored != computed) {
            throw new RecordFormatException(
                    RecordFormatException.Kind.CHECKSUM,
                    String.format("checksum mismatch: stored 0x%08x, computed 0x%08x", stored, computed));
        }
        Attributes attributes = Attributes.fromBits(getU16(a, o + 25));
        Batch batch = new Batch(bytes, attributes);
        long count = batch.recordCount();
        if (count == 0) {
            throw RecordFormatException.malformed("empty batch");
        }
        if (batch.lastOffsetDelta() != count - 1) {
            throw RecordFormatException.malformed("last_offset_delta does not match record_count");
        }
        if (batch.maxTimestamp() < batch.baseTimestamp()) {
            throw RecordFormatException.malformed("max_ts before base_ts");
        }
        if (attributes.control() && count != 1) {
            throw RecordFormatException.malformed("control batch must hold one record");
        }
        return new Split(batch, buf.slice(n, buf.size()));
    }

    /** Parses every batch of {@code buf}, as in the records of a fetch response; fails on the first bad one. */
    public static List<Batch> parseAll(ByteString buf) {
        List<Batch> out = new ArrayList<>();
        ByteString rest = buf;
        while (!rest.isEmpty()) {
            Split s = parse(rest);
            out.add(s.batch());
            rest = s.rest();
        }
        return out;
    }

    /** The encoded batch, exactly as stored and sent. */
    public ByteString asBytes() {
        return bytes;
    }

    /** The encoded length. */
    public int length() {
        return bytes.size();
    }

    /** The offset of the first record, a {@code u64}. */
    public long baseOffset() {
        return getU64(0);
    }

    /** The offset of the last record, a {@code u64}. */
    public long lastOffset() {
        return baseOffset() + lastOffsetDelta();
    }

    /** The offset right after this batch, a {@code u64}. */
    public long nextOffset() {
        return lastOffset() + 1;
    }

    /** The Raft term the batch was appended in, a {@code u64}. */
    public long term() {
        return getU64(12);
    }

    /** The decoded attributes. */
    public Attributes attributes() {
        return attributes;
    }

    /** The {@code last_offset_delta} header field, a {@code u32}. */
    public long lastOffsetDelta() {
        return getU32(bytes.array(), bytes.offset() + 27);
    }

    /** The timestamp of the first record. */
    public long baseTimestamp() {
        return getU64(31);
    }

    /** The largest record timestamp. */
    public long maxTimestamp() {
        return getU64(39);
    }

    /** The producer id, a {@code u64}; {@code 0} if the batch is not idempotent. */
    public long producerId() {
        return getU64(47);
    }

    /** The producer epoch, a {@code u16}. */
    public int producerEpoch() {
        return getU16(bytes.array(), bytes.offset() + 55);
    }

    /** The sequence number of the first record. */
    public int baseSequence() {
        return (int) getU32(bytes.array(), bytes.offset() + 57);
    }

    /** The number of records, a {@code u32}. */
    public long recordCount() {
        return getU32(bytes.array(), bytes.offset() + 61);
    }

    /**
     * Decodes the records, checking that offset deltas run 0, 1, 2 and so on, that nothing is left over in a
     * record or after the last one, and that lengths and counts are not negative.
     *
     * <p>A compressed block is decompressed first, each time this is called.
     *
     * @throws RecordFormatException if the records are damaged, or compressed with a codec whose library is not on
     *     the class path ({@link RecordFormatException.Kind#UNSUPPORTED_COMPRESSION})
     */
    public List<BatchRecord> records() {
        byte[] a;
        int pos;
        int end;
        if (attributes.compression() == Compression.NONE) {
            a = bytes.array();
            pos = bytes.offset() + HEADER_LEN;
            end = bytes.offset() + bytes.size();
        } else {
            a = Codecs.decompress(
                    attributes.compression(), bytes.array(), bytes.offset() + HEADER_LEN, bytes.size() - HEADER_LEN);
            pos = 0;
            end = a.length;
        }
        long baseOffset = baseOffset();
        long baseTs = baseTimestamp();
        long count = recordCount();
        List<BatchRecord> out = new ArrayList<>((int) Math.min(count, 1024));
        for (long expectedDelta = 0; expectedDelta < count; expectedDelta++) {
            Varint.Decoded len = Varint.getI64(a, pos, end);
            pos += len.length();
            if (len.value() < 0) {
                throw RecordFormatException.malformed("negative record length");
            }
            if (end - pos < len.value()) {
                throw RecordFormatException.truncated();
            }
            int recEnd = pos + (int) len.value();
            Cursor c = new Cursor(a, pos, recEnd);
            c.byteValue(); // attributes, reserved
            long tsDelta = c.i64();
            int offsetDelta = c.i32();
            if (offsetDelta != (int) expectedDelta) {
                throw RecordFormatException.malformed("record offset deltas are not consecutive");
            }
            ByteString key = c.nullable();
            ByteString value = c.nullable();
            long headerCount = c.i64();
            if (headerCount < 0) {
                throw RecordFormatException.malformed("negative header count");
            }
            List<Header> headers = new ArrayList<>((int) Math.min(headerCount, 64));
            for (long i = 0; i < headerCount; i++) {
                ByteString hk = c.nullable();
                if (hk == null) {
                    throw RecordFormatException.malformed("null header key");
                }
                headers.add(new Header(hk, c.nullable()));
            }
            if (c.pos != recEnd) {
                throw RecordFormatException.malformed("trailing bytes in record");
            }
            out.add(new BatchRecord(baseOffset + offsetDelta, baseTs + tsDelta, key, value, headers));
            pos = recEnd;
        }
        if (pos != end) {
            throw RecordFormatException.malformed("trailing bytes after records");
        }
        return out;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Batch other && bytes.equals(other.bytes);
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    @Override
    public String toString() {
        return "Batch[baseOffset=" + Unsigned.toString(baseOffset()) + ", records=" + recordCount() + ", " + attributes + "]";
    }

    // ---- editing encoded batches in place ----

    /**
     * The total length of the batch starting at {@code buf[0]}, if {@code buf} holds at least its length prefix,
     * otherwise {@code -1}. Used to frame batches when scanning concatenated ones. The result is up to
     * {@code 2^32 + 11}, hence a {@code long}.
     */
    public static long peekLen(byte[] buf) {
        return peekLen(buf, 0, buf.length);
    }

    private static long peekLen(byte[] buf, int off, int available) {
        if (available < LEN_PREFIX) {
            return -1;
        }
        return LEN_PREFIX + getU32(buf, off + 8);
    }

    /** Sets {@code base_offset} (a {@code u64}) and {@code term} of an encoded batch in place. Both are outside the checksum. */
    public static void stamp(byte[] batch, long baseOffset, long term) {
        if (batch.length < HEADER_LEN) {
            throw RecordFormatException.truncated();
        }
        putI64(batch, 0, baseOffset);
        putI64(batch, 12, term);
    }

    /**
     * Moves an encoded batch's timestamps so that its first record has {@code baseTs}, keeping the distances
     * between records, and recomputes the checksum. A queue's leader does this for queues with a message TTL.
     */
    public static void setBaseTimestamp(byte[] batch, long baseTs) {
        int len = wholeLength(batch);
        long oldBase = getI64(batch, 31);
        long oldMax = getI64(batch, 39);
        putI64(batch, 31, baseTs);
        putI64(batch, 39, baseTs + (oldMax - oldBase));
        resum(batch, len);
    }

    /**
     * Sets the producer fields of an encoded batch in place and recomputes its checksum, which covers them. An
     * idempotent client stamps the batch it was given with its producer id (a {@code u64}), epoch (a
     * {@code u16}) and the sequence of the first record.
     */
    public static void setProducer(byte[] batch, long producerId, int producerEpoch, int baseSequence) {
        if (producerEpoch < 0 || producerEpoch > Unsigned.U16_MAX) {
            throw new IllegalArgumentException("producer epoch is not a u16: " + producerEpoch);
        }
        int len = wholeLength(batch);
        putI64(batch, 47, producerId);
        putU16(batch, 55, producerEpoch);
        putU32(batch, 57, baseSequence & Unsigned.U32_MAX);
        resum(batch, len);
    }

    /** Marks an encoded batch as transactional (or not) and recomputes its checksum. */
    public static void setTransactional(byte[] batch, boolean transactional) {
        int len = wholeLength(batch);
        int bits = getU16(batch, 25);
        bits = transactional ? bits | 1 << 3 : bits & ~(1 << 3);
        putU16(batch, 25, bits);
        resum(batch, len);
    }

    private static int wholeLength(byte[] batch) {
        long len = peekLen(batch);
        if (len < 0 || len < HEADER_LEN || batch.length < len) {
            throw RecordFormatException.truncated();
        }
        return (int) len;
    }

    private static void resum(byte[] batch, int len) {
        putU32(batch, 21, crc(batch, CRC_START, len));
    }

    // ---- fixed-width access ----

    static long crc(byte[] a, int from, int to) {
        CRC32C c = new CRC32C();
        c.update(a, from, to - from);
        return c.getValue();
    }

    private long getU64(int at) {
        return getI64(bytes.array(), bytes.offset() + at);
    }

    static int getU16(byte[] b, int at) {
        return (b[at] & 0xFF) | (b[at + 1] & 0xFF) << 8;
    }

    static long getU32(byte[] b, int at) {
        return (b[at] & 0xFFL) | (b[at + 1] & 0xFFL) << 8 | (b[at + 2] & 0xFFL) << 16 | (b[at + 3] & 0xFFL) << 24;
    }

    static long getI64(byte[] b, int at) {
        return getU32(b, at) | getU32(b, at + 4) << 32;
    }

    static void putU16(byte[] b, int at, int v) {
        b[at] = (byte) v;
        b[at + 1] = (byte) (v >>> 8);
    }

    static void putU32(byte[] b, int at, long v) {
        b[at] = (byte) v;
        b[at + 1] = (byte) (v >>> 8);
        b[at + 2] = (byte) (v >>> 16);
        b[at + 3] = (byte) (v >>> 24);
    }

    static void putI64(byte[] b, int at, long v) {
        putU32(b, at, v);
        putU32(b, at + 4, v >>> 32);
    }

    /** Reads the fields of one record, bounded by the record's own length. */
    private static final class Cursor {
        private final byte[] a;
        private final int end;
        private int pos;

        Cursor(byte[] a, int pos, int end) {
            this.a = a;
            this.pos = pos;
            this.end = end;
        }

        int byteValue() {
            if (pos >= end) {
                throw RecordFormatException.truncated();
            }
            return a[pos++] & 0xFF;
        }

        long i64() {
            Varint.Decoded d = Varint.getI64(a, pos, end);
            pos += d.length();
            return d.value();
        }

        int i32() {
            Varint.Decoded d = Varint.getI32(a, pos, end);
            pos += d.length();
            return (int) d.value();
        }

        ByteString nullable() {
            long len = i64();
            if (len == -1) {
                return null;
            }
            if (len < 0) {
                throw RecordFormatException.malformed("negative length");
            }
            if (end - pos < len) {
                throw RecordFormatException.truncated();
            }
            ByteString out = ByteString.view(a, pos, (int) len);
            pos += (int) len;
            return out;
        }
    }
}

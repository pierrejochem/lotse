package dev.lotse.record;

import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Builds a batch record by record.
 *
 * <p>The first record's timestamp becomes the batch's base timestamp. {@link #finish()} writes the 65-byte
 * header with {@code base_offset} and {@code term} set to zero; the broker's leader stamps both when it appends,
 * and a client never needs to. Not thread-safe.
 */
public final class BatchBuilder {
    private final BatchOptions options;
    private boolean hasBase;
    private long baseTs;
    private long maxTs;
    private long count;
    private final GrowableBytes body = new GrowableBytes(256);

    /** Starts an empty batch. */
    public BatchBuilder(BatchOptions options) {
        this.options = Objects.requireNonNull(options);
    }

    /** Starts an empty batch of plain data. */
    public BatchBuilder() {
        this(BatchOptions.DEFAULT);
    }

    /** The number of records so far. */
    public long size() {
        return count;
    }

    /** Whether no record was added. */
    public boolean isEmpty() {
        return count == 0;
    }

    /** The encoded size if the batch were finished now. */
    public int encodedLength() {
        return Batch.HEADER_LEN + body.size();
    }

    /** Appends a record. {@code key} and {@code value} may be {@code null}; a null value is a tombstone. */
    public BatchBuilder push(long timestamp, byte[] key, byte[] value, List<Header> headers) {
        return push(
                timestamp,
                key == null ? null : ByteString.view(key, 0, key.length),
                value == null ? null : ByteString.view(value, 0, value.length),
                headers);
    }

    /** Appends a record without headers. */
    public BatchBuilder push(long timestamp, byte[] key, byte[] value) {
        return push(timestamp, key, value, List.of());
    }

    /** Appends a record. {@code key} and {@code value} may be {@code null}; a null value is a tombstone. */
    public BatchBuilder push(long timestamp, ByteString key, ByteString value, List<Header> headers) {
        if (!hasBase) {
            hasBase = true;
            baseTs = timestamp;
        }
        maxTs = count == 0 ? timestamp : Math.max(maxTs, timestamp);

        GrowableBytes rec = new GrowableBytes(16 + (key == null ? 0 : key.size()) + (value == null ? 0 : value.size()));
        rec.put(0); // attributes
        rec.putVarint(timestamp - baseTs); // wraps like the broker's
        rec.putVarint(count);
        putNullable(rec, key);
        putNullable(rec, value);
        rec.putVarint(headers.size());
        for (Header h : headers) {
            putNullable(rec, h.key());
            putNullable(rec, h.value());
        }
        body.putVarint(rec.size());
        body.put(rec.array(), 0, rec.size());
        count++;
        return this;
    }

    private static void putNullable(GrowableBytes buf, ByteString v) {
        if (v == null) {
            buf.putVarint(-1);
        } else {
            buf.putVarint(v.size());
            buf.put(v);
        }
    }

    /**
     * Finishes the batch.
     *
     * <p>With a codec in the options, the record block is compressed; its library must be on the class path.
     *
     * @throws RecordFormatException if the batch is empty, larger than {@link Batch#MAX_BATCH_LEN}, compressed with
     *     records of more than {@link Batch#MAX_RECORDS_LEN} bytes, or compressed with a codec whose library is
     *     missing
     */
    public ByteString finish() {
        if (count == 0) {
            throw RecordFormatException.malformed("empty batch");
        }
        Compression codec = options.attributes().compression();
        byte[] block = body.array();
        int blockLen = body.size();
        if (codec != Compression.NONE) {
            if (blockLen > Batch.MAX_RECORDS_LEN) {
                throw new RecordFormatException(
                        RecordFormatException.Kind.TOO_LARGE,
                        "records of " + blockLen + " bytes exceed what a compressed batch may hold");
            }
            block = Codecs.compress(codec, block, 0, blockLen);
            blockLen = block.length;
        }
        long total = (long) Batch.HEADER_LEN + blockLen;
        if (total > Batch.MAX_BATCH_LEN) {
            throw new RecordFormatException(
                    RecordFormatException.Kind.TOO_LARGE, "batch of " + total + " bytes exceeds the limit");
        }
        byte[] buf = new byte[(int) total];
        System.arraycopy(block, 0, buf, Batch.HEADER_LEN, blockLen);
        Batch.putU32(buf, 8, total - Batch.LEN_PREFIX);
        buf[20] = Batch.MAGIC;
        Batch.putU16(buf, 25, options.attributes().toBits());
        Batch.putU32(buf, 27, count - 1);
        Batch.putI64(buf, 31, hasBase ? baseTs : 0);
        Batch.putI64(buf, 39, maxTs);
        Batch.putI64(buf, 47, options.producerId());
        Batch.putU16(buf, 55, options.producerEpoch());
        Batch.putU32(buf, 57, options.baseSequence() & Unsigned.U32_MAX);
        Batch.putU32(buf, 61, count);
        CRC32C crc = new CRC32C();
        crc.update(buf, Batch.CRC_START, buf.length - Batch.CRC_START);
        Batch.putU32(buf, 21, crc.getValue());
        return ByteString.view(buf, 0, buf.length);
    }
}

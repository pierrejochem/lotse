package dev.lotse.record;

import java.util.List;
import java.util.Objects;

/**
 * One record, with offset and timestamp resolved against its batch.
 *
 * <p>Encoding inside the record block, every {@code varint} a ZigZag varint:
 *
 * <pre>
 * length        varint   bytes after this field
 * attributes    i8       reserved, 0
 * ts_delta      varint   relative to base_ts
 * offset_delta  varint   relative to base_offset
 * key_len       varint   -1 = null
 * key           bytes
 * value_len     varint   -1 = null (a tombstone)
 * value         bytes
 * header_count  varint
 *   [key_len varint, key bytes, value_len varint, value bytes]*
 * </pre>
 *
 * @param offset absolute offset ({@code base_offset + offset_delta}), a {@code u64}
 * @param timestamp absolute timestamp in milliseconds ({@code base_ts + ts_delta}, wrapping)
 * @param key the key, or {@code null}
 * @param value the value, or {@code null} for a tombstone
 * @param headers headers in insertion order
 */
public record BatchRecord(long offset, long timestamp, ByteString key, ByteString value, List<Header> headers) {
    /** Creates a record. */
    public BatchRecord {
        headers = List.copyOf(Objects.requireNonNull(headers));
    }
}

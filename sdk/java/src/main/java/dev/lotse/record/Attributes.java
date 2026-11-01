package dev.lotse.record;

import java.util.Objects;

/**
 * The {@code attributes} field of a batch header, decoded: {@code compression(3) | transactional(1) | control(1)
 * | kind(3)}, the low bit first.
 *
 * @param compression codec of the record block
 * @param transactional part of a transaction; invisible to read-committed consumers until committed
 * @param control a control batch (transaction marker); holds exactly one record
 * @param kind payload kind
 */
public record Attributes(Compression compression, boolean transactional, boolean control, BatchKind kind) {
    /** Plain, uncompressed, non-transactional data. */
    public static final Attributes DATA = new Attributes(Compression.NONE, false, false, BatchKind.DATA);

    /** Creates attributes. */
    public Attributes {
        Objects.requireNonNull(compression);
        Objects.requireNonNull(kind);
    }

    /** The packed on-disk value. */
    public int toBits() {
        return compression.code() | (transactional ? 1 << 3 : 0) | (control ? 1 << 4 : 0) | (kind.code() << 5);
    }

    /** Unpacks the on-disk value, refusing unknown codecs, kinds and reserved bits. */
    public static Attributes fromBits(int bits) {
        Compression compression = Compression.fromCode(bits & 0b111);
        BatchKind kind = BatchKind.fromCode((bits >> 5) & 0b111);
        if (bits >> 8 != 0) {
            throw RecordFormatException.malformed("reserved attribute bits set");
        }
        return new Attributes(compression, (bits & (1 << 3)) != 0, (bits & (1 << 4)) != 0, kind);
    }
}

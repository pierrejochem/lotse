package dev.lotse.record;

/**
 * ZigZag varints as used inside records and in the protocol's counts and tags.
 *
 * <p>A signed value is ZigZag-encoded (so {@code -1}, used for "null", stays one byte) and then written as an
 * unsigned LEB128 varint of at most ten bytes. The reader is lenient about overlong encodings such as
 * {@code 80 00} for zero, as the broker's is; writers never produce them.
 */
public final class Varint {
    /** The longest encoding of a 64-bit varint. */
    public static final int MAX_LEN_64 = 10;

    private Varint() {}

    /** A decoded varint and the number of bytes it took. */
    public record Decoded(long value, int length) {}

    /** ZigZag-encodes {@code value}. */
    public static long zigzag(long value) {
        return (value << 1) ^ (value >> 63);
    }

    /** Reverses {@link #zigzag}. */
    public static long unzigzag(long value) {
        return (value >>> 1) ^ -(value & 1);
    }

    /** Writes {@code value} as an unsigned LEB128 varint at {@code dst[off]}; returns the bytes written (at most 10). */
    public static int putU64(byte[] dst, int off, long value) {
        int n = 0;
        while (Long.compareUnsigned(value, 0x80) >= 0) {
            dst[off + n++] = (byte) (value | 0x80);
            value >>>= 7;
        }
        dst[off + n++] = (byte) value;
        return n;
    }

    /** Writes a signed value as a ZigZag varint; returns the bytes written. */
    public static int putI64(byte[] dst, int off, long value) {
        return putU64(dst, off, zigzag(value));
    }

    /** Reads an unsigned LEB128 varint from {@code buf[off, end)}. */
    public static Decoded getU64(byte[] buf, int off, int end) {
        long value = 0;
        int available = Math.min(MAX_LEN_64, end - off);
        for (int i = 0; i < available; i++) {
            int b = buf[off + i] & 0xFF;
            if (i == MAX_LEN_64 - 1 && b > 0x01) {
                throw new RecordFormatException(RecordFormatException.Kind.VARINT_OVERFLOW, "varint overflow");
            }
            value |= (long) (b & 0x7f) << (7 * i);
            if ((b & 0x80) == 0) {
                return new Decoded(value, i + 1);
            }
        }
        if (end - off < MAX_LEN_64) {
            throw RecordFormatException.truncated();
        }
        throw new RecordFormatException(RecordFormatException.Kind.VARINT_OVERFLOW, "varint overflow");
    }

    /** Reads a ZigZag varint from {@code buf[off, end)}. */
    public static Decoded getI64(byte[] buf, int off, int end) {
        Decoded raw = getU64(buf, off, end);
        return new Decoded(unzigzag(raw.value()), raw.length());
    }

    /** Reads a ZigZag varint that must fit in an {@code int}. */
    public static Decoded getI32(byte[] buf, int off, int end) {
        Decoded v = getI64(buf, off, end);
        if (v.value() != (int) v.value()) {
            throw new RecordFormatException(RecordFormatException.Kind.VARINT_OVERFLOW, "varint overflow");
        }
        return v;
    }
}

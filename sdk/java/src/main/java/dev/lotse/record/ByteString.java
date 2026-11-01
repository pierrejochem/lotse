package dev.lotse.record;

import java.util.Arrays;

/**
 * An immutable sequence of bytes. Keys, values, header fields and whole batches are byte strings; the protocol
 * does not interpret them.
 *
 * <p>{@link #copyOf} copies. {@link #view} does not: it shares the array and is for code that owns the array and
 * will never change it again, such as a decoder slicing a received frame.
 */
public final class ByteString {
    /** The empty byte string. */
    public static final ByteString EMPTY = new ByteString(new byte[0], 0, 0);

    private final byte[] array;
    private final int offset;
    private final int length;

    private ByteString(byte[] array, int offset, int length) {
        this.array = array;
        this.offset = offset;
        this.length = length;
    }

    /** A copy of {@code bytes}. */
    public static ByteString copyOf(byte[] bytes) {
        return copyOf(bytes, 0, bytes.length);
    }

    /** A copy of {@code length} bytes of {@code bytes} from {@code offset}. */
    public static ByteString copyOf(byte[] bytes, int offset, int length) {
        java.util.Objects.checkFromIndexSize(offset, length, bytes.length);
        return length == 0 ? EMPTY : new ByteString(Arrays.copyOfRange(bytes, offset, offset + length), 0, length);
    }

    /**
     * A view of {@code length} bytes of {@code bytes} from {@code offset}, without copying. The caller must not
     * change those bytes afterwards.
     */
    public static ByteString view(byte[] bytes, int offset, int length) {
        java.util.Objects.checkFromIndexSize(offset, length, bytes.length);
        return length == 0 ? EMPTY : new ByteString(bytes, offset, length);
    }

    /** The UTF-8 encoding of {@code s}. */
    public static ByteString utf8(String s) {
        return copyOf(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /** The number of bytes. */
    public int size() {
        return length;
    }

    /** Whether there are no bytes. */
    public boolean isEmpty() {
        return length == 0;
    }

    /** The byte at {@code index}, as a value in {@code [0, 256)}. */
    public int at(int index) {
        java.util.Objects.checkIndex(index, length);
        return array[offset + index] & 0xFF;
    }

    /** A copy of the bytes. */
    public byte[] toByteArray() {
        return Arrays.copyOfRange(array, offset, offset + length);
    }

    /** The bytes {@code [from, to)}, sharing this string's storage. */
    public ByteString slice(int from, int to) {
        java.util.Objects.checkFromToIndex(from, to, length);
        return to == from ? EMPTY : new ByteString(array, offset + from, to - from);
    }

    /** Copies the bytes into {@code dst} at {@code dstOffset}. */
    public void copyTo(byte[] dst, int dstOffset) {
        System.arraycopy(array, offset, dst, dstOffset, length);
    }

    /** The bytes as a UTF-8 string; invalid sequences become U+FFFD. */
    public String toUtf8() {
        return new String(array, offset, length, java.nio.charset.StandardCharsets.UTF_8);
    }

    // The storage, for decoders in this package that read without copying.
    byte[] array() {
        return array;
    }

    int offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ByteString other
                && Arrays.equals(array, offset, offset + length, other.array, other.offset, other.offset + other.length);
    }

    @Override
    public int hashCode() {
        int h = 1;
        for (int i = 0; i < length; i++) {
            h = 31 * h + array[offset + i];
        }
        return h;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ByteString[").append(length).append(" bytes");
        int shown = Math.min(length, 16);
        if (shown > 0) {
            sb.append(": ");
        }
        for (int i = 0; i < shown; i++) {
            sb.append(String.format("%02x", array[offset + i]));
        }
        return sb.append(shown < length ? "..." : "").append(']').toString();
    }
}

package dev.lotse.record;

import java.util.Arrays;

/** A byte buffer that grows as needed, for building batches. */
final class GrowableBytes {
    private byte[] data;
    private int size;

    GrowableBytes(int capacity) {
        data = new byte[Math.max(capacity, 16)];
    }

    int size() {
        return size;
    }

    byte[] array() {
        return data;
    }

    private void reserve(int extra) {
        long needed = (long) size + extra;
        if (needed > Integer.MAX_VALUE - 8) {
            throw new RecordFormatException(RecordFormatException.Kind.TOO_LARGE, "buffer too large");
        }
        if (needed > data.length) {
            data = Arrays.copyOf(data, (int) Math.max(needed, Math.min((long) data.length * 2, Integer.MAX_VALUE - 8)));
        }
    }

    void put(int b) {
        reserve(1);
        data[size++] = (byte) b;
    }

    void put(byte[] src, int off, int len) {
        reserve(len);
        System.arraycopy(src, off, data, size, len);
        size += len;
    }

    void put(ByteString s) {
        reserve(s.size());
        s.copyTo(data, size);
        size += s.size();
    }

    void putVarint(long signed) {
        reserve(Varint.MAX_LEN_64);
        size += Varint.putI64(data, size, signed);
    }

    byte[] toByteArray() {
        return Arrays.copyOf(data, size);
    }
}

package dev.lotse.proto;

import dev.lotse.record.ByteString;
import dev.lotse.record.Unsigned;
import dev.lotse.record.Varint;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Writes fields into a frame. Conventions, shared by every message:
 *
 * <pre>
 * u8/u16/u32/u64/i32/i64  fixed width, little-endian
 * varint                  ZigZag varint (array counts, tags)
 * string                  u16 length + UTF-8 (0xFFFF = null), at most 65,534 bytes
 * bytes                   u32 length + data
 * array&lt;T&gt;              varint count + elements
 * tagged fields           varint count + {tag varint, bytes}, at the end of most structures
 * optional                one byte 0 or 1, then the value if 1
 * </pre>
 *
 * Not thread-safe.
 */
public final class Writer {
    private static final int NULL_STRING = 0xFFFF;

    private byte[] buf = new byte[64];
    private int size;

    /** Creates an empty writer. */
    public Writer() {}

    /** The bytes written so far. */
    public byte[] toByteArray() {
        return Arrays.copyOf(buf, size);
    }

    private void reserve(int extra) {
        long needed = (long) size + extra;
        if (needed > Integer.MAX_VALUE - 8) {
            throw new IllegalStateException("frame too large");
        }
        if (needed > buf.length) {
            buf = Arrays.copyOf(buf, (int) Math.max(needed, Math.min((long) buf.length * 2, Integer.MAX_VALUE - 8)));
        }
    }

    /** A {@code u8}. */
    public Writer u8(int v) {
        if (v < 0 || v > Unsigned.U8_MAX) {
            throw new IllegalArgumentException("not a u8: " + v);
        }
        reserve(1);
        buf[size++] = (byte) v;
        return this;
    }

    /** A boolean as one byte, 0 or 1. */
    public Writer bool(boolean v) {
        return u8(v ? 1 : 0);
    }

    /** A {@code u16}. */
    public Writer u16(int v) {
        if (v < 0 || v > Unsigned.U16_MAX) {
            throw new IllegalArgumentException("not a u16: " + v);
        }
        reserve(2);
        buf[size++] = (byte) v;
        buf[size++] = (byte) (v >>> 8);
        return this;
    }

    /** A {@code u32}. */
    public Writer u32(long v) {
        if (!Unsigned.fitsU32(v)) {
            throw new IllegalArgumentException("not a u32: " + v);
        }
        return fixed32((int) v);
    }

    /** An {@code i32}. */
    public Writer i32(int v) {
        return fixed32(v);
    }

    private Writer fixed32(int v) {
        reserve(4);
        for (int i = 0; i < 4; i++) {
            buf[size++] = (byte) (v >>> (8 * i));
        }
        return this;
    }

    /** A {@code u64}, given as its unsigned bit pattern. */
    public Writer u64(long v) {
        return i64(v);
    }

    /** An {@code i64}. */
    public Writer i64(long v) {
        reserve(8);
        for (int i = 0; i < 8; i++) {
            buf[size++] = (byte) (v >>> (8 * i));
        }
        return this;
    }

    /** A ZigZag varint. */
    public Writer varint(long v) {
        reserve(Varint.MAX_LEN_64);
        size += Varint.putI64(buf, size, v);
        return this;
    }

    /** A string; strings are short names, at most 65,534 UTF-8 bytes. */
    public Writer string(String s) {
        if (s == null) {
            throw new IllegalArgumentException("null string where the protocol needs one; use optString");
        }
        ByteBuffer utf8;
        try {
            utf8 = StandardCharsets.UTF_8
                    .newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(s));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("string is not valid Unicode", e);
        }
        int len = utf8.remaining();
        if (len >= NULL_STRING) {
            throw new IllegalArgumentException("a string of at most 65,534 bytes, got " + len);
        }
        u16(len);
        reserve(len);
        utf8.get(buf, size, len);
        size += len;
        return this;
    }

    /** A string that may be {@code null}. */
    public Writer optString(String s) {
        if (s == null) {
            return u16(NULL_STRING);
        }
        return string(s);
    }

    /** Length-prefixed bytes. */
    public Writer bytes(ByteString b) {
        u32(b.size());
        reserve(b.size());
        b.copyTo(buf, size);
        size += b.size();
        return this;
    }

    /** An array, each element written by {@code f}. */
    public <T> Writer array(List<T> items, BiConsumer<Writer, T> f) {
        varint(items.size());
        for (T item : items) {
            f.accept(this, item);
        }
        return this;
    }

    /** Tagged fields, in ascending tag order. */
    public Writer tags(List<Tag> tags) {
        varint(tags.size());
        for (Tag t : tags) {
            varint(t.tag());
            bytes(t.value());
        }
        return this;
    }

    /** An empty block of tagged fields. */
    public Writer noTags() {
        return varint(0);
    }
}

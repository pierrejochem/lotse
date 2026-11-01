package dev.lotse.proto;

import dev.lotse.record.ByteString;
import dev.lotse.record.RecordFormatException;
import dev.lotse.record.Varint;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Reads fields from a frame, refusing anything that runs past its end. The conventions are those of
 * {@link Writer}.
 *
 * <p>The reader takes the array without copying it, and the {@link ByteString}s it returns point into it: the
 * caller must not change the array afterwards. Not thread-safe.
 */
public final class Reader {
    private static final int NULL_STRING = 0xFFFF;

    private final byte[] buf;
    private final int end;
    private int pos;

    /** Reads {@code frame}, which the caller hands over. */
    public Reader(byte[] frame) {
        this(frame, 0, frame.length);
    }

    /** Reads {@code frame[offset, offset + length)}. */
    public Reader(byte[] frame, int offset, int length) {
        java.util.Objects.checkFromIndexSize(offset, length, frame.length);
        this.buf = frame;
        this.pos = offset;
        this.end = offset + length;
    }

    /** The number of bytes not read yet. */
    public int remaining() {
        return end - pos;
    }

    /** Fails unless everything was read. */
    public void finish() {
        if (pos != end) {
            throw DecodeException.malformed("trailing bytes");
        }
    }

    private void need(long n) {
        if (end - pos < n) {
            throw DecodeException.truncated();
        }
    }

    /** A {@code u8}. */
    public int u8() {
        need(1);
        return buf[pos++] & 0xFF;
    }

    /** A boolean: any non-zero byte is true, as the broker reads it. */
    public boolean bool() {
        return u8() != 0;
    }

    /** A {@code u16}. */
    public int u16() {
        need(2);
        int v = (buf[pos] & 0xFF) | (buf[pos + 1] & 0xFF) << 8;
        pos += 2;
        return v;
    }

    /** A {@code u32}, in {@code [0, 2^32)}. */
    public long u32() {
        return i32() & 0xFFFF_FFFFL;
    }

    /** An {@code i32}. */
    public int i32() {
        need(4);
        int v = 0;
        for (int i = 0; i < 4; i++) {
            v |= (buf[pos + i] & 0xFF) << (8 * i);
        }
        pos += 4;
        return v;
    }

    /** A {@code u64}, as its unsigned bit pattern. */
    public long u64() {
        return i64();
    }

    /** An {@code i64}. */
    public long i64() {
        need(8);
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (buf[pos + i] & 0xFFL) << (8 * i);
        }
        pos += 8;
        return v;
    }

    /** A ZigZag varint. */
    public long varint() {
        try {
            Varint.Decoded d = Varint.getI64(buf, pos, end);
            pos += d.length();
            return d.value();
        } catch (RecordFormatException e) {
            // The broker does not tell a short varint from an overlong one either.
            throw DecodeException.truncated();
        }
    }

    /**
     * A count of at least 0, each element taking at least {@code minSize} bytes: counts the frame cannot hold are
     * refused before anything is allocated.
     */
    private int count(int minSize) {
        long n = varint();
        if (n < 0) {
            throw DecodeException.malformed("negative count");
        }
        if (n > remaining() / Math.max(minSize, 1)) {
            throw DecodeException.malformed("count exceeds the frame");
        }
        return (int) n;
    }

    /** A string that must not be null. */
    public String string() {
        String s = optString();
        if (s == null) {
            throw DecodeException.malformed("null string");
        }
        return s;
    }

    /** A string that may be null. */
    public String optString() {
        int len = u16();
        if (len == NULL_STRING) {
            return null;
        }
        need(len);
        try {
            String s = StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(buf, pos, len))
                    .toString();
            pos += len;
            return s;
        } catch (CharacterCodingException e) {
            throw DecodeException.malformed("string is not UTF-8");
        }
    }

    /** Length-prefixed bytes, sharing the frame's array. */
    public ByteString bytes() {
        long len = u32();
        need(len);
        ByteString out = ByteString.view(buf, pos, (int) len);
        pos += (int) len;
        return out;
    }

    /** An array; {@code minSize} is the smallest encoding of one element. */
    public <T> List<T> array(int minSize, Function<Reader, T> f) {
        int n = count(minSize);
        List<T> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(f.apply(this));
        }
        return out;
    }

    /** Tagged fields, in the order sent. */
    public List<Tag> tags() {
        return array(5, r -> {
            long tag = r.varint();
            if (tag < 0) {
                throw DecodeException.malformed("negative tag");
            }
            return new Tag(tag, r.bytes());
        });
    }
}

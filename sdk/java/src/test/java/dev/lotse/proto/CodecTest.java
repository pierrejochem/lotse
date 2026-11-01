package dev.lotse.proto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.lotse.record.ByteString;
import java.util.List;
import org.junit.jupiter.api.Test;

class CodecTest {
    private static DecodeException.Kind kind(Runnable r) {
        return assertThrows(DecodeException.class, r::run).kind();
    }

    @Test
    void fixedWidthIntegersAreLittleEndian() {
        byte[] b = new Writer().u16(0x0102).u32(0x03040506L).u64(0x0708090a0b0c0d0eL).i32(-2).toByteArray();
        assertArrayEquals(
                new byte[] {2, 1, 6, 5, 4, 3, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 9, 8, 7, -2, -1, -1, -1}, b);
        Reader r = new Reader(b);
        assertEquals(0x0102, r.u16());
        assertEquals(0x03040506L, r.u32());
        assertEquals(0x0708090a0b0c0d0eL, r.u64());
        assertEquals(-2, r.i32());
        r.finish();
    }

    @Test
    void unsignedRangesAreChecked() {
        assertThrows(IllegalArgumentException.class, () -> new Writer().u8(256));
        assertThrows(IllegalArgumentException.class, () -> new Writer().u8(-1));
        assertThrows(IllegalArgumentException.class, () -> new Writer().u16(65536));
        assertThrows(IllegalArgumentException.class, () -> new Writer().u32(1L << 32));
        assertThrows(IllegalArgumentException.class, () -> new Writer().u32(-1));
        assertEquals(0xFFFF_FFFFL, new Reader(new Writer().u32(0xFFFF_FFFFL).toByteArray()).u32());
    }

    @Test
    void stringsAreLengthPrefixedUtf8WithANullMarker() {
        byte[] b = new Writer().string("päckchen").optString(null).optString("").toByteArray();
        assertArrayEquals(new byte[] {9, 0, 'p', (byte) 0xc3, (byte) 0xa4, 'c', 'k', 'c', 'h', 'e', 'n', -1, -1, 0, 0}, b);
        Reader r = new Reader(b);
        assertEquals("päckchen", r.string());
        assertNull(r.optString());
        assertEquals("", r.string());
    }

    @Test
    void aNullStringWhereOneIsRequiredIsMalformed() {
        Reader r = new Reader(new byte[] {-1, -1});
        assertEquals(DecodeException.Kind.MALFORMED, kind(r::string));
    }

    @Test
    void invalidUtf8IsMalformed() {
        Reader r = new Reader(new byte[] {2, 0, (byte) 0xc3, 0x28});
        assertEquals(DecodeException.Kind.MALFORMED, kind(r::string));
        // A lone surrogate half, encoded as UTF-8, is not UTF-8 either.
        Reader s = new Reader(new byte[] {3, 0, (byte) 0xed, (byte) 0xa0, (byte) 0x80});
        assertEquals(DecodeException.Kind.MALFORMED, kind(s::string));
    }

    @Test
    void theLongestStringIs65534Bytes() {
        String ok = "a".repeat(65534);
        assertEquals(ok, new Reader(new Writer().string(ok).toByteArray()).string());
        assertThrows(IllegalArgumentException.class, () -> new Writer().string("a".repeat(65535)));
        assertThrows(IllegalArgumentException.class, () -> new Writer().string("\ud800"));
    }

    @Test
    void bytesAreLengthPrefixedByAU32() {
        byte[] b = new Writer().bytes(ByteString.copyOf(new byte[] {1, 2, 3})).toByteArray();
        assertArrayEquals(new byte[] {3, 0, 0, 0, 1, 2, 3}, b);
        assertEquals(ByteString.copyOf(new byte[] {1, 2, 3}), new Reader(b).bytes());
        // A length of 2^32 - 1 is a truncation, not a negative number.
        assertEquals(DecodeException.Kind.TRUNCATED, kind(() -> new Reader(new byte[] {-1, -1, -1, -1}).bytes()));
    }

    @Test
    void countsAreZigZagVarints() {
        byte[] b = new Writer().array(List.of("a", "b"), Writer::string).toByteArray();
        assertEquals(4, b[0], "count 2 is ZigZag 4");
        assertEquals(List.of("a", "b"), new Reader(b).array(2, Reader::string));
    }

    @Test
    void aNegativeCountIsMalformed() {
        byte[] b = new Writer().varint(-1).toByteArray();
        assertEquals(DecodeException.Kind.MALFORMED, kind(() -> new Reader(b).array(1, Reader::u8)));
    }

    @Test
    void aCountTheFrameCannotHoldIsRefusedBeforeAllocating() {
        byte[] b = new Writer().varint(Long.MAX_VALUE / 2).toByteArray();
        assertEquals(DecodeException.Kind.MALFORMED, kind(() -> new Reader(b).array(1, Reader::u8)));
        byte[] two = new Writer().varint(3).u8(1).u8(2).toByteArray();
        assertEquals(DecodeException.Kind.MALFORMED, kind(() -> new Reader(two).array(1, Reader::u8)));
        assertEquals(List.of(1, 2), new Reader(new Writer().varint(2).u8(1).u8(2).toByteArray()).array(1, Reader::u8));
    }

    @Test
    void anOverlongVarintIsRefused() {
        byte[] b = new byte[11];
        java.util.Arrays.fill(b, (byte) 0xff);
        assertEquals(DecodeException.Kind.TRUNCATED, kind(() -> new Reader(b).varint()));
    }

    @Test
    void tagsAreACountAndTagValuePairs() {
        byte[] b = new Writer().tags(List.of(new Tag(0, ByteString.copyOf(new byte[] {7})))).toByteArray();
        assertArrayEquals(new byte[] {2, 0, 1, 0, 0, 0, 7}, b);
        List<Tag> tags = new Reader(b).tags();
        assertEquals(1, tags.size());
        assertEquals(0, tags.get(0).tag());
        byte[] negative = new Writer().varint(1).varint(-1).u32(0).toByteArray();
        assertEquals(DecodeException.Kind.MALFORMED, kind(() -> new Reader(negative).tags()));
    }

    @Test
    void finishRefusesTrailingBytes() {
        Reader r = new Reader(new byte[] {1, 2});
        r.u8();
        assertEquals(DecodeException.Kind.MALFORMED, kind(r::finish));
    }

    @Test
    void errorCodes() {
        assertEquals("NOT_LEADER", ErrorCode.NOT_LEADER.codeName());
        assertEquals("AUTHENTICATION_FAILED", ErrorCode.AUTHENTICATION_FAILED.codeName());
        assertEquals("UNSUPPORTED_SASL_MECHANISM", ErrorCode.UNSUPPORTED_SASL_MECHANISM.codeName());
        assertEquals("UNKNOWN", new ErrorCode(999).codeName());
        assertEquals("NOT_LEADER (1)", ErrorCode.NOT_LEADER.toString());
        int retriable = 0;
        for (int c = 0; c <= 28; c++) {
            if (new ErrorCode(c).retriable()) {
                retriable++;
            }
        }
        assertEquals(8, retriable);
    }
}

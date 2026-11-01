package dev.lotse.record;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class VarintTest {
    private static byte[] encodeI64(long v) {
        byte[] buf = new byte[Varint.MAX_LEN_64];
        int n = Varint.putI64(buf, 0, v);
        return Arrays.copyOf(buf, n);
    }

    private static byte[] encodeU64(long v) {
        byte[] buf = new byte[Varint.MAX_LEN_64];
        int n = Varint.putU64(buf, 0, v);
        return Arrays.copyOf(buf, n);
    }

    private static RecordFormatException.Kind failure(byte[] in) {
        return assertThrows(RecordFormatException.class, () -> Varint.getU64(in, 0, in.length)).kind();
    }

    @Test
    void vectorsFromTheBroker() {
        for (String[] v : Vectors.lines("varints.txt")) {
            byte[] bytes = Vectors.hex(v[v.length - 1]);
            switch (v[0]) {
                case "zigzag" -> {
                    long value = Long.parseLong(v[1]);
                    assertArrayEquals(bytes, encodeI64(value), line(v));
                    Varint.Decoded d = Varint.getI64(bytes, 0, bytes.length);
                    assertEquals(value, d.value(), line(v));
                    assertEquals(bytes.length, d.length(), line(v));
                }
                case "unsigned" -> {
                    long value = Long.parseUnsignedLong(v[1]);
                    assertArrayEquals(bytes, encodeU64(value), line(v));
                    Varint.Decoded d = Varint.getU64(bytes, 0, bytes.length);
                    assertEquals(value, d.value(), line(v));
                }
                case "lenient" -> {
                    Varint.Decoded d = Varint.getU64(bytes, 0, bytes.length);
                    assertEquals(Long.parseUnsignedLong(v[1]), d.value(), line(v));
                    assertEquals(bytes.length, d.length(), line(v));
                }
                case "bad" -> {
                    RecordFormatException.Kind expected = v[1].equals("truncated")
                            ? RecordFormatException.Kind.TRUNCATED
                            : RecordFormatException.Kind.VARINT_OVERFLOW;
                    assertEquals(expected, failure(bytes), line(v));
                }
                default -> throw new AssertionError("unknown vector kind " + v[0]);
            }
        }
    }

    private static String line(String[] v) {
        return String.join(" ", v);
    }

    @Test
    void theTenthByteMayOnlyBeOne() {
        byte[] ok = {-1, -1, -1, -1, -1, -1, -1, -1, -1, 0x01};
        assertEquals(-1L, Varint.getU64(ok, 0, ok.length).value());
        byte[] tooBig = {-1, -1, -1, -1, -1, -1, -1, -1, -1, 0x02};
        assertEquals(RecordFormatException.Kind.VARINT_OVERFLOW, failure(tooBig));
    }

    @Test
    void anElevenByteVarintIsAnOverflowNotATruncation() {
        byte[] b = new byte[11];
        Arrays.fill(b, (byte) 0xff);
        assertEquals(RecordFormatException.Kind.VARINT_OVERFLOW, failure(b));
    }

    @Test
    void aShortOpenVarintIsTruncated() {
        assertEquals(RecordFormatException.Kind.TRUNCATED, failure(new byte[] {(byte) 0x80, (byte) 0x80}));
        assertEquals(RecordFormatException.Kind.TRUNCATED, failure(new byte[0]));
    }

    @Test
    void anI32MustFit() {
        byte[] big = encodeI64(1L << 31);
        assertEquals(RecordFormatException.Kind.VARINT_OVERFLOW,
                assertThrows(RecordFormatException.class, () -> Varint.getI32(big, 0, big.length)).kind());
        byte[] min = encodeI64(Integer.MIN_VALUE);
        assertEquals(Integer.MIN_VALUE, Varint.getI32(min, 0, min.length).value());
    }

    @Test
    void zigzagIsItsOwnInverse() {
        for (long v : new long[] {0, -1, 1, Long.MAX_VALUE, Long.MIN_VALUE, 123456789L, -987654321L}) {
            assertEquals(v, Varint.unzigzag(Varint.zigzag(v)));
        }
    }
}

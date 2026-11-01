package dev.lotse.record;

/**
 * Helpers for the unsigned integers of the protocol. {@code u64} travels in a {@code long} as its unsigned bit
 * pattern, so comparing, dividing and printing it needs the unsigned methods; {@code u32} fits a {@code long}
 * and {@code u16} an {@code int}.
 */
public final class Unsigned {
    /** The largest {@code u32}. */
    public static final long U32_MAX = 0xFFFF_FFFFL;
    /** The largest {@code u16}. */
    public static final int U16_MAX = 0xFFFF;
    /** The largest {@code u8}. */
    public static final int U8_MAX = 0xFF;

    private Unsigned() {}

    /** The {@code u32} held by an {@code int}'s bits. */
    public static long u32(int bits) {
        return bits & U32_MAX;
    }

    /** The {@code u16} held by a {@code short}'s bits. */
    public static int u16(short bits) {
        return bits & U16_MAX;
    }

    /** The {@code u8} held by a {@code byte}'s bits. */
    public static int u8(byte bits) {
        return bits & U8_MAX;
    }

    /** Whether {@code v} is a {@code u32}. */
    public static boolean fitsU32(long v) {
        return v >= 0 && v <= U32_MAX;
    }

    /** Compares two {@code u64}. */
    public static int compare(long a, long b) {
        return Long.compareUnsigned(a, b);
    }

    /** A {@code u64} in decimal. */
    public static String toString(long u64) {
        return Long.toUnsignedString(u64);
    }

    /** The sum of two {@code u64}, failing when it does not fit. */
    public static long addExact(long a, long b) {
        long sum = a + b;
        if (Long.compareUnsigned(sum, a) < 0) {
            throw new ArithmeticException("u64 overflow");
        }
        return sum;
    }
}

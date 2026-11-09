namespace Lotse.Record;

/// <summary>ZigZag and plain LEB128 varints, at most 10 bytes, as the record format and the protocol use them.</summary>
public static class Varint
{
    /// <summary>The longest varint of a 64-bit value.</summary>
    public const int MaxLen64 = 10;

    /// <summary>Maps a signed value to an unsigned one so small magnitudes stay short.</summary>
    public static ulong Zigzag(long value) => (ulong)((value << 1) ^ (value >> 63));

    /// <summary>The inverse of <see cref="Zigzag"/>.</summary>
    public static long Unzigzag(ulong value) => (long)(value >> 1) ^ -(long)(value & 1);

    /// <summary>Writes <paramref name="value"/> as plain LEB128 at <c>dst[off]</c>; returns the bytes written.</summary>
    public static int PutU64(byte[] dst, int off, ulong value)
    {
        int n = 0;
        while (value >= 0x80)
        {
            dst[off + n++] = (byte)(value | 0x80);
            value >>= 7;
        }
        dst[off + n++] = (byte)value;
        return n;
    }

    /// <summary>Writes <paramref name="value"/> as a ZigZag varint; returns the bytes written.</summary>
    public static int PutI64(byte[] dst, int off, long value) => PutU64(dst, off, Zigzag(value));

    /// <summary>
    /// Reads a plain LEB128 varint from <c>buf[off, end)</c>. Overlong forms such as <c>80 00</c> are accepted, as
    /// the broker accepts them; the tenth byte may only be 0 or 1.
    /// </summary>
    /// <exception cref="RecordFormatException">
    /// <see cref="RecordFormatErrorKind.Truncated"/> if the buffer ends first,
    /// <see cref="RecordFormatErrorKind.VarintOverflow"/> if the value does not fit 64 bits.
    /// </exception>
    public static (ulong Value, int Length) GetU64(byte[] buf, int off, int end)
    {
        ulong value = 0;
        int available = System.Math.Min(MaxLen64, end - off);
        for (int i = 0; i < available; i++)
        {
            int b = buf[off + i];
            if (i == MaxLen64 - 1 && b > 0x01)
            {
                throw new RecordFormatException(RecordFormatErrorKind.VarintOverflow, "varint overflow");
            }
            value |= (ulong)(b & 0x7f) << (7 * i);
            if ((b & 0x80) == 0)
            {
                return (value, i + 1);
            }
        }
        if (end - off < MaxLen64)
        {
            throw RecordFormatException.TruncatedBuffer();
        }
        throw new RecordFormatException(RecordFormatErrorKind.VarintOverflow, "varint overflow");
    }

    /// <summary>Reads a ZigZag varint.</summary>
    public static (long Value, int Length) GetI64(byte[] buf, int off, int end)
    {
        var raw = GetU64(buf, off, end);
        return (Unzigzag(raw.Value), raw.Length);
    }

    /// <summary>Reads a ZigZag varint that must fit 32 bits.</summary>
    public static (int Value, int Length) GetI32(byte[] buf, int off, int end)
    {
        var v = GetI64(buf, off, end);
        if (v.Value != (int)v.Value)
        {
            throw new RecordFormatException(RecordFormatErrorKind.VarintOverflow, "varint overflow");
        }
        return ((int)v.Value, v.Length);
    }
}

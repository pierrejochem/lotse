using System;

namespace Lotse.Record;

/// <summary>CRC32C (Castagnoli), the checksum of a record batch. Written here so the SDK needs no package.</summary>
public static class Crc32c
{
    // Reflected polynomial 0x1EDC6F41.
    private const uint Polynomial = 0x82F63B78;

    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++)
            {
                c = (c & 1) != 0 ? (c >> 1) ^ Polynomial : c >> 1;
            }
            table[i] = c;
        }
        return table;
    }

    /// <summary>The checksum of <paramref name="data"/>.</summary>
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        uint c = 0xFFFFFFFF;
        foreach (byte b in data)
        {
            c = Table[(c ^ b) & 0xFF] ^ (c >> 8);
        }
        return ~c;
    }

    /// <summary>The checksum of <c>a[from, to)</c>.</summary>
    public static uint Compute(byte[] a, int from, int to) => Compute(new ReadOnlySpan<byte>(a, from, to - from));
}

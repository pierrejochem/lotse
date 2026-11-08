using System;
using System.IO;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;
using Lotse.Record;
using ZstdSharp;

namespace Lotse.Codecs;

/// <summary>
/// The zstd and LZ4 codecs of <c>Lotse.Client</c>, built on ZstdSharp.Port and K4os.Compression.LZ4.Streams (ADR 0084).
/// <para>
/// <c>Lotse.Client</c> finds this type by name the first time a compressed batch is read or built, so adding the
/// package is enough. <see cref="LotseCodecs.Register"/> does the same by hand, for programs that trim assemblies.
/// </para>
/// </summary>
public sealed class DefaultCodecProvider : IBlockCodecProvider
{
    /// <inheritdoc/>
    public IBlockCodec? Create(Compression codec) => codec switch
    {
        Compression.Zstd => new ZstdBlockCodec(),
        Compression.Lz4 => new Lz4BlockCodec(),
        _ => null,
    };
}

/// <summary>Registers the codecs by hand.</summary>
public static class LotseCodecs
{
    /// <summary>Makes zstd and LZ4 available to <c>Lotse.Client</c>.</summary>
    public static void Register()
    {
        CompressionCodecs.Register(Compression.Zstd, new ZstdBlockCodec());
        CompressionCodecs.Register(Compression.Lz4, new Lz4BlockCodec());
    }
}

internal static class Bounded
{
    /// <summary>Reads until the end or one byte past <paramref name="limit"/>.</summary>
    internal static byte[] Read(Stream source, long limit)
    {
        var output = new MemoryStream();
        byte[] buffer = new byte[64 * 1024];
        long total = 0;
        long stop = limit + 1;
        while (total < stop)
        {
            int n = source.Read(buffer, 0, (int)Math.Min(buffer.Length, stop - total));
            if (n <= 0)
            {
                break;
            }
            output.Write(buffer, 0, n);
            total += n;
        }
        return output.ToArray();
    }
}

internal sealed class ZstdBlockCodec : IBlockCodec
{
    /// <summary>The level the broker's own builder uses.</summary>
    private const int Level = 3;

    public byte[] Decompress(ReadOnlySpan<byte> block, long limit)
    {
        using var input = new MemoryStream(block.ToArray(), writable: false);
        using var decoder = new DecompressionStream(input);
        return Bounded.Read(decoder, limit);
    }

    public byte[] Compress(ReadOnlySpan<byte> records)
    {
        using var compressor = new Compressor(Level);
        return compressor.Wrap(records).ToArray();
    }
}

internal sealed class Lz4BlockCodec : IBlockCodec
{
    public byte[] Decompress(ReadOnlySpan<byte> block, long limit)
    {
        using var input = new MemoryStream(block.ToArray(), writable: false);
        using var decoder = LZ4Stream.Decode(input);
        return Bounded.Read(decoder, limit);
    }

    public byte[] Compress(ReadOnlySpan<byte> records)
    {
        var output = new MemoryStream(records.Length / 2 + 64);
        // Independent 64 KiB blocks, as the broker writes them.
        var settings = new LZ4EncoderSettings
        {
            BlockSize = 64 * 1024,
            ChainBlocks = false,
            ContentChecksum = false,
            BlockChecksum = false,
            CompressionLevel = LZ4Level.L00_FAST,
        };
        using (var encoder = LZ4Stream.Encode(output, settings, leaveOpen: true))
        {
            encoder.Write(records);
        }
        return output.ToArray();
    }
}

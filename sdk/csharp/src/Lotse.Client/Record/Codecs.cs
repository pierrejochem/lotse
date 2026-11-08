namespace Lotse.Record;

/// <summary>
/// The record block codecs. In this phase the SDK reads and writes uncompressed batches only: a batch that needs
/// zstd or LZ4 parses (its header is checked without a codec) and fails with
/// <see cref="RecordFormatErrorKind.UnsupportedCompression"/> when its records are read or built. Which library
/// will do the work is settled with the stream client (ADR 0083: optional, never a hard dependency).
/// </summary>
internal static class Codecs
{
    internal static RecordFormatException Unsupported(Compression codec, string verb) =>
        new(
            RecordFormatErrorKind.UnsupportedCompression,
            $"compression codec {codec}: this version of the SDK cannot {verb} {Name(codec)} batches; "
                + "it bundles no compression library yet, and the broker serves a batch as its producer sent it");

    private static string Name(Compression codec) => codec == Compression.Lz4 ? "lz4" : "zstd";

    internal static byte[] Decompress(Compression codec) => throw Unsupported(codec, "read");

    internal static byte[] Compress(Compression codec) => throw Unsupported(codec, "write");
}

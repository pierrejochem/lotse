using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace Lotse.Record;

/// <summary>
/// The zstd or LZ4 implementation behind one <see cref="Compression"/> value. The SDK has none of its own (ADR 0084):
/// the package <c>Lotse.Client.Codecs</c> provides them, and a project that wants other libraries registers its own
/// with <see cref="CompressionCodecs.Register"/>.
/// <para>The record block is one zstd frame or one LZ4 frame (the frame format, not the bare block format).</para>
/// </summary>
public interface IBlockCodec
{
    /// <summary>
    /// Decompresses one frame. Decompression must stop once the output is one byte longer than
    /// <paramref name="limit"/>: the caller then refuses the batch, and a small batch cannot make the client allocate
    /// without bound. A block that is not one complete frame throws.
    /// </summary>
    /// <param name="block">The frame.</param>
    /// <param name="limit">The most a block may decompress to.</param>
    byte[] Decompress(ReadOnlySpan<byte> block, long limit);

    /// <summary>Compresses the records into one frame: zstd at level 3, LZ4 as independent 64 KiB blocks, as the broker writes them.</summary>
    byte[] Compress(ReadOnlySpan<byte> records);
}

/// <summary>
/// Finds an <see cref="IBlockCodec"/>. A package that provides codecs implements this, with a public parameterless
/// constructor, as <c>Lotse.Codecs.DefaultCodecProvider</c> in the assembly <c>Lotse.Client.Codecs</c>: the SDK looks
/// for exactly that type, once, the first time a compressed batch is read or built.
/// </summary>
public interface IBlockCodecProvider
{
    /// <summary>The codec for <paramref name="codec"/>, or <c>null</c> if this provider has none.</summary>
    IBlockCodec? Create(Compression codec);
}

/// <summary>Where the SDK finds the zstd and LZ4 codecs (ADR 0084).</summary>
public static class CompressionCodecs
{
    internal const string ProviderType = "Lotse.Codecs.DefaultCodecProvider, Lotse.Client.Codecs";
    internal const string Package = "Lotse.Client.Codecs";

    private static readonly object Gate = new();
    private static readonly Dictionary<Compression, IBlockCodec> Registered = new();
    private static readonly Dictionary<Compression, string> Failures = new();
    private static IBlockCodecProvider? _provider;
    private static bool _providerLooked;

    /// <summary>
    /// Uses <paramref name="implementation"/> for <paramref name="codec"/> from now on, in place of whatever the
    /// SDK found. For programs that trim or publish without loading assemblies by name, and for other libraries.
    /// </summary>
    public static void Register(Compression codec, IBlockCodec implementation)
    {
        ArgumentNullException.ThrowIfNull(implementation);
        if (codec == Compression.None)
        {
            throw new ArgumentException("a plain record block needs no codec", nameof(codec));
        }
        lock (Gate)
        {
            Registered[codec] = implementation;
            Failures.Remove(codec);
        }
    }

    /// <summary>Whether <paramref name="codec"/> can be read and written now. <see cref="Compression.None"/> always can.</summary>
    public static bool IsAvailable(Compression codec) => codec == Compression.None || Find(codec, out _) is not null;

    /// <summary>The codec, or <c>null</c>, with the reason a library that was found could not be used.</summary>
    private static IBlockCodec? Find(Compression codec, out string? failure)
    {
        lock (Gate)
        {
            failure = null;
            if (Registered.TryGetValue(codec, out IBlockCodec? known))
            {
                return known;
            }
            if (Failures.TryGetValue(codec, out string? failed))
            {
                failure = failed;
                return null;
            }
            if (!_providerLooked)
            {
                _providerLooked = true;
                _provider = LoadProvider(out string? why);
                if (why is not null)
                {
                    Failures[Compression.Zstd] = why;
                    Failures[Compression.Lz4] = why;
                    failure = why;
                    return null;
                }
            }
            if (_provider is null)
            {
                return null;
            }
            try
            {
                IBlockCodec? created = _provider.Create(codec);
                if (created is not null)
                {
                    Registered[codec] = created;
                }
                return created;
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                // A library the codecs package needs is missing, or has no native part for this platform.
                failure = Package + " cannot load its " + Name(codec) + " library here: " + e.Message;
                Failures[codec] = failure;
                return null;
            }
        }
    }

    private static IBlockCodecProvider? LoadProvider(out string? failure)
    {
        failure = null;
        try
        {
            Type? type = Type.GetType(ProviderType, throwOnError: false);
            if (type is null)
            {
                return null;
            }
            return (IBlockCodecProvider?)Activator.CreateInstance(type);
        }
        catch (Exception e) when (e is TypeLoadException or FileNotFoundException or FileLoadException or BadImageFormatException
                                      or TargetInvocationException or InvalidCastException or MissingMethodException)
        {
            failure = Package + " cannot be loaded here: " + e.Message;
            return null;
        }
    }

    /// <summary>For tests: forgets every registration and failure, and uses <paramref name="provider"/> (none: nothing is found).</summary>
    internal static void ResetForTests(IBlockCodecProvider? provider)
    {
        lock (Gate)
        {
            Registered.Clear();
            Failures.Clear();
            _provider = provider;
            _providerLooked = true;
        }
    }

    internal static string Name(Compression codec) => codec == Compression.Lz4 ? "lz4" : "zstd";

    internal static RecordFormatException Unsupported(Compression codec, string verb, string? why = null) =>
        new(
            RecordFormatErrorKind.UnsupportedCompression,
            $"compression codec {codec}: "
                + (why ?? $"add the package {Package} to your project, or call CompressionCodecs.Register, to {verb} {Name(codec)} batches"));

    /// <summary>The codec for a compressed block, or the named error.</summary>
    internal static IBlockCodec Require(Compression codec, string verb) =>
        Find(codec, out string? failure) ?? throw Unsupported(codec, verb, failure);
}

/// <summary>The record block codecs (ADR 0058, ADR 0084), with the limits the broker's own have.</summary>
internal static class BlockCodecs
{
    /// <summary>Decompresses a record block; at most <see cref="Batch.MaxRecordsLen"/> bytes come out.</summary>
    internal static byte[] Decompress(Compression codec, byte[] a, int offset, int length)
    {
        IBlockCodec impl = CompressionCodecs.Require(codec, "read");
        byte[] output;
        try
        {
            output = impl.Decompress(new ReadOnlySpan<byte>(a, offset, length), Batch.MaxRecordsLen);
        }
        catch (Exception e) when (e is not (OutOfMemoryException or RecordFormatException))
        {
            throw RecordFormatException.MalformedBatch($"{CompressionCodecs.Name(codec)} block does not decode: {e.Message}");
        }
        if (output.Length > Batch.MaxRecordsLen)
        {
            throw new RecordFormatException(
                RecordFormatErrorKind.TooLarge,
                $"{CompressionCodecs.Name(codec)} block decompresses to more than {Batch.MaxRecordsLen} bytes");
        }
        return output;
    }

    /// <summary>Compresses a record block.</summary>
    internal static byte[] Compress(Compression codec, byte[] a, int offset, int length)
    {
        IBlockCodec impl = CompressionCodecs.Require(codec, "write");
        try
        {
            return impl.Compress(new ReadOnlySpan<byte>(a, offset, length));
        }
        catch (Exception e) when (e is not (OutOfMemoryException or RecordFormatException))
        {
            throw RecordFormatException.MalformedBatch($"{CompressionCodecs.Name(codec)} compression failed: {e.Message}");
        }
    }
}

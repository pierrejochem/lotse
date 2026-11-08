using System;
using System.Collections.Generic;
using System.IO;
using Lotse.Client;
using Lotse.Client.Tests;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Lotse.Client.NoCodecs.Tests;

/// <summary>
/// Runs without <c>Lotse.Client.Codecs</c> next to the SDK (ADR 0084): compressed batches still parse, and reading or
/// building one fails with <see cref="RecordFormatErrorKind.UnsupportedCompression"/> naming the package to add, never
/// with a <see cref="TypeLoadException"/> or as damage. Plain batches are unaffected. These tests change the codec
/// registry of the process, so none runs beside another.
/// </summary>
public sealed class WithoutCodecsTests : IDisposable
{
    public WithoutCodecsTests() => CompressionCodecs.ResetForTests(null);

    public void Dispose() => CompressionCodecs.ResetForTests(null);

    private static string Name(Compression c) => c == Compression.Lz4 ? "lz4" : "zstd";

    [Fact]
    public void NothingIsFoundBesidePlain()
    {
        // The real discovery, not the test seam: the package is not here to be found.
        CompressionCodecs.ResetForTests(null);
        Assert.True(CompressionCodecs.IsAvailable(Compression.None));
        Assert.False(CompressionCodecs.IsAvailable(Compression.Zstd));
        Assert.False(CompressionCodecs.IsAvailable(Compression.Lz4));
    }

    [Fact]
    public void CompressedBatchesAreRefusedWithThePackageToAdd()
    {
        foreach (string[] v in Vectors.Lines("compressed.hex"))
        {
            Batch b = Batch.Parse(ByteString.CopyOf(Vectors.Hex(v[1]))).Batch; // parses: header and checksum need no codec
            var e = Assert.Throws<RecordFormatException>(() => b.Records());
            Assert.Equal(RecordFormatErrorKind.UnsupportedCompression, e.Kind);
            Assert.Contains("add the package Lotse.Client.Codecs", e.Message);
            Assert.Contains(Name(b.Attributes.Compression), e.Message);
            Assert.StartsWith("compression codec " + b.Attributes.Compression, e.Message);
        }
        foreach (Compression codec in new[] { Compression.Zstd, Compression.Lz4 })
        {
            var b = new BatchBuilder(new BatchOptions(new Attributes(codec, false, false, BatchKind.Data), 0, 0, -1));
            b.Push(1, (byte[]?)null, new byte[] { 1 });
            var e = Assert.Throws<RecordFormatException>(() => b.Finish());
            Assert.Equal(RecordFormatErrorKind.UnsupportedCompression, e.Kind);
            Assert.Contains("add the package Lotse.Client.Codecs", e.Message);
        }
    }

    [Fact]
    public void ACompactedBatchNeedsTheCodecOnlyIfItStillHasRecords()
    {
        foreach (string[] v in Vectors.Lines("compacted.hex"))
        {
            Batch b = Batch.Parse(ByteString.CopyOf(Vectors.Hex(v[3]))).Batch;
            if (b.Attributes.Compression == Compression.None || b.RecordCount == 0)
            {
                b.Records();
            }
            else
            {
                Assert.Equal(RecordFormatErrorKind.UnsupportedCompression, Assert.Throws<RecordFormatException>(() => b.Records()).Kind);
            }
        }
    }

    [Fact]
    public void PlainBatchesNeedNoCodec()
    {
        foreach (string[] v in Vectors.Lines("batches.hex"))
        {
            Batch.Parse(ByteString.CopyOf(Vectors.Hex(v[1]))).Batch.Records();
        }
    }

    [Fact]
    public void AFetchedBatchWithAMissingCodecIsNotMistakenForDamage()
    {
        string[] v = Vectors.Lines("compressed.hex")[0];
        var fetched = new Fetched(ByteString.CopyOf(Vectors.Hex(v[1])), 1000, 1000, 0, Array.Empty<Messages.AbortedTxn>());
        Assert.Single(fetched.CommittedBatches());
        var e = Assert.Throws<RecordFormatException>(() => fetched.CommittedMessages());
        Assert.Equal(RecordFormatErrorKind.UnsupportedCompression, e.Kind);
        Assert.Equal(103UL, fetched.NextOffset(0)); // the position is known without the codec
    }

    private sealed class Reversing : IBlockCodec
    {
        public int Compressed;

        public int Decompressed;

        public byte[] Compress(ReadOnlySpan<byte> records)
        {
            Compressed++;
            return records.ToArray();
        }

        public byte[] Decompress(ReadOnlySpan<byte> block, long limit)
        {
            Decompressed++;
            return block.ToArray();
        }
    }

    [Fact]
    public void ACodecRegisteredByHandIsUsedInBothDirectionsForItsCodecOnly()
    {
        var fake = new Reversing(); // "compresses" by copying: enough to see that the registry is asked
        CompressionCodecs.Register(Compression.Zstd, fake);
        Assert.True(CompressionCodecs.IsAvailable(Compression.Zstd));
        Assert.False(CompressionCodecs.IsAvailable(Compression.Lz4));

        var b = new BatchBuilder(new BatchOptions(new Attributes(Compression.Zstd, false, false, BatchKind.Data), 0, 0, -1));
        b.Push(1, new byte[] { 7 }, new byte[] { 8, 9 });
        Batch read = Batch.Parse(b.Finish()).Batch;
        Assert.Equal(1, fake.Compressed);
        BatchRecord r = Assert.Single(read.Records());
        Assert.Equal(1, fake.Decompressed);
        Assert.Equal(new byte[] { 8, 9 }, r.Value!.ToByteArray());
        Assert.Equal(RecordFormatErrorKind.UnsupportedCompression, Assert.Throws<RecordFormatException>(() =>
            Batch.Parse(ByteString.CopyOf(Vectors.Hex(Vectors.Lines("compressed.hex").Find(l => l[0] == "lz4-stamped")![1]))).Batch.Records()).Kind);
        Assert.Throws<ArgumentException>(() => CompressionCodecs.Register(Compression.None, fake));
    }

    private sealed class Overflowing : IBlockCodec
    {
        public byte[] Compress(ReadOnlySpan<byte> records) => records.ToArray();

        public byte[] Decompress(ReadOnlySpan<byte> block, long limit) => new byte[limit + 1];
    }

    private sealed class Broken : IBlockCodec
    {
        public byte[] Compress(ReadOnlySpan<byte> records) => throw new InvalidOperationException("no");

        public byte[] Decompress(ReadOnlySpan<byte> block, long limit) => throw new InvalidDataException("not a frame");
    }

    [Fact]
    public void TheLimitIsTheSdksNotTheCodecsAndACodecThatThrowsMeansDamage()
    {
        CompressionCodecs.Register(Compression.Zstd, new Overflowing());
        byte[] head = new BatchBuilder().Push(1, (byte[]?)null, null).Finish().ToByteArray();
        Batch.PutU16(head, 25, (ushort)new Attributes(Compression.Zstd, false, false, BatchKind.Data).ToBits());
        Batch.PutU32(head, 21, Batch.Crc(head, 25, head.Length));
        Batch b = Batch.Parse(ByteString.CopyOf(head)).Batch;
        Assert.Equal(RecordFormatErrorKind.TooLarge, Assert.Throws<RecordFormatException>(() => b.Records()).Kind);

        CompressionCodecs.Register(Compression.Zstd, new Broken());
        var e = Assert.Throws<RecordFormatException>(() => b.Records());
        Assert.Equal(RecordFormatErrorKind.Malformed, e.Kind);
        Assert.Contains("not a frame", e.Message);
    }

    private sealed class Failing : IBlockCodecProvider
    {
        public IBlockCodec? Create(Compression codec) => throw new DllNotFoundException("libzstd.so");
    }

    [Fact]
    public void ALibraryThatIsFoundButCannotBeLoadedIsNamedToo()
    {
        CompressionCodecs.ResetForTests(new Failing());
        string[] v = Vectors.Lines("compressed.hex")[0];
        Batch b = Batch.Parse(ByteString.CopyOf(Vectors.Hex(v[1]))).Batch;
        var e = Assert.Throws<RecordFormatException>(() => b.Records());
        Assert.Equal(RecordFormatErrorKind.UnsupportedCompression, e.Kind);
        Assert.Contains("cannot load", e.Message);
        Assert.Contains("libzstd.so", e.Message);
    }
}

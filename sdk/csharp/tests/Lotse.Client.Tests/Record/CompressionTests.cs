using System;
using System.Collections.Generic;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Record;

/// <summary>
/// zstd and LZ4 record blocks (ADR 0058, ADR 0084), with the package <c>Lotse.Client.Codecs</c> loaded as this test
/// project references it: the broker's compressed batches read like their plain twins, batches compressed here read
/// back, and a block that does not decode or decodes to too much is refused. The missing-codec cases are in
/// <c>Lotse.Client.NoCodecs.Tests</c>, which does not reference the package.
/// </summary>
public class CompressionTests
{
    private const long Ts = 1_700_000_000_000L;
    private static readonly byte[] ZstdMagic = { 0x28, 0xb5, 0x2f, 0xfd };
    private static readonly byte[] Lz4Magic = { 0x04, 0x22, 0x4d, 0x18 };

    private static Dictionary<string, List<string>> ExpectedRecords()
    {
        var expected = new Dictionary<string, List<string>>();
        foreach (string[] v in Vectors.Lines("compressed-records.txt"))
        {
            if (!expected.TryGetValue(v[0], out var list))
            {
                expected[v[0]] = list = new List<string>();
            }
            list.Add(string.Join(' ', v));
        }
        return expected;
    }

    [Fact]
    public void TheCodecsArriveWithThePackageWithoutAnyRegistration()
    {
        Assert.True(CompressionCodecs.IsAvailable(Compression.None));
        Assert.True(CompressionCodecs.IsAvailable(Compression.Zstd));
        Assert.True(CompressionCodecs.IsAvailable(Compression.Lz4));
    }

    [Fact]
    public void TheBrokersCompressedBatchesReadAsWritten()
    {
        var expected = ExpectedRecords();
        int seen = 0;
        foreach (string[] v in Vectors.Lines("compressed.hex"))
        {
            Batch b = Batch.Parse(ByteString.CopyOf(Vectors.Hex(v[1]))).Batch;
            Assert.Equal(v[0].StartsWith("zstd") ? Compression.Zstd : Compression.Lz4, b.Attributes.Compression);
            Assert.Equal(expected[v[0]], BatchConformanceTests.Lines(v[0], b.Records()));
            seen++;
        }
        Assert.Equal(expected.Count, seen); // every compressed vector has its records
    }

    [Fact]
    public void TheHeaderAgreesWithTheRecordsAndTheBlockIsOneFrameRightAfterIt()
    {
        var expected = ExpectedRecords();
        foreach (string[] v in Vectors.Lines("compressed.hex"))
        {
            byte[] bytes = Vectors.Hex(v[1]);
            Batch b = Batch.Parse(ByteString.CopyOf(bytes)).Batch; // the checksum covers the compressed bytes
            bool zstd = v[0].StartsWith("zstd");
            List<string> lines = expected[v[0]];
            string[] first = lines[0].Split(' ');
            Assert.Equal(ulong.Parse(first[2]), b.BaseOffset);
            Assert.Equal(long.Parse(first[3]), b.BaseTimestamp);
            Assert.Equal((uint)lines.Count, b.RecordCount);
            Assert.Equal((uint)lines.Count - 1, b.LastOffsetDelta);
            Assert.Equal(zstd ? ZstdMagic : Lz4Magic, bytes.AsSpan(Batch.HeaderLen, 4).ToArray());

            // The batch is written again to the same bytes by the in-place edits.
            byte[] again = (byte[])bytes.Clone();
            Batch.SetProducer(again, b.ProducerId, b.ProducerEpoch, b.BaseSequence);
            Batch.Stamp(again, b.BaseOffset, b.Term);
            Assert.Equal(v[1], Vectors.Hex(again));
        }
    }

    private static BatchBuilder Large(Compression codec)
    {
        byte[] key = new byte[200];
        byte[] value = new byte[20_000];
        Array.Fill(key, (byte)0xab);
        Array.Fill(value, (byte)0xcd);
        var b = new BatchBuilder(new BatchOptions(new Attributes(codec, false, false, BatchKind.Data), 9, 1, 4));
        b.Push(Ts, key, value, new[] { new Header(ByteString.Utf8("h"), ByteString.Utf8("x")) });
        b.Push(Ts + 3, (byte[]?)null, null);
        b.Push(-Ts, System.Text.Encoding.UTF8.GetBytes("negative"), Array.Empty<byte>());
        return b;
    }

    [Theory]
    [InlineData(Compression.Zstd)]
    [InlineData(Compression.Lz4)]
    public void BatchesCompressedHereReadLikeTheirPlainTwin(Compression codec)
    {
        Batch plain = Batch.Parse(Large(Compression.None).Finish()).Batch;
        Batch c = Batch.Parse(Large(codec).Finish()).Batch;
        Assert.Equal(codec, c.Attributes.Compression);
        Assert.True(c.Length < plain.Length / 10, $"{codec}: {c.Length} of {plain.Length}");
        Assert.Equal(BatchConformanceTests.Lines("x", plain.Records()), BatchConformanceTests.Lines("x", c.Records()));
        Assert.Equal(plain.RecordCount, c.RecordCount);
        Assert.Equal(plain.MaxTimestamp, c.MaxTimestamp);
        Assert.Equal(plain.ProducerId, c.ProducerId);
        Assert.Equal(plain.BaseSequence, c.BaseSequence);
    }

    [Theory]
    [InlineData(Compression.Zstd)]
    [InlineData(Compression.Lz4)]
    public void TheBlockWeWriteIsOneFrameOfItsKind(Compression codec)
    {
        byte[] bytes = Large(codec).Finish().ToByteArray();
        Assert.Equal(codec == Compression.Zstd ? ZstdMagic : Lz4Magic, bytes.AsSpan(Batch.HeaderLen, 4).ToArray());
    }

    private static byte[] Sum(byte[] b)
    {
        Batch.PutU32(b, 21, Batch.Crc(b, 25, b.Length));
        return b;
    }

    [Theory]
    [InlineData(Compression.Zstd)]
    [InlineData(Compression.Lz4)]
    public void ABlockThatDoesNotDecodeIsRefused(Compression codec)
    {
        byte[] good = Large(codec).Finish().ToByteArray();
        // A frame cut short, and a frame with its body overwritten.
        byte[] cut = good[..^4];
        Batch.PutU32(cut, 8, (uint)(cut.Length - Batch.LenPrefix));
        byte[] garbled = (byte[])good.Clone();
        Array.Fill(garbled, (byte)0x5a, Batch.HeaderLen + 6, garbled.Length - Batch.HeaderLen - 6);
        foreach (byte[] bad in new[] { cut, garbled })
        {
            Batch b = Batch.Parse(ByteString.CopyOf(Sum(bad))).Batch;
            Assert.Equal(RecordFormatErrorKind.Malformed, Assert.Throws<RecordFormatException>(() => b.Records()).Kind);
        }
        // A plain block marked compressed is no frame at all.
        byte[] plain = Large(Compression.None).Finish().ToByteArray();
        Batch.PutU16(plain, 25, (ushort)new Attributes(codec, false, false, BatchKind.Data).ToBits());
        Batch marked = Batch.Parse(ByteString.CopyOf(Sum(plain))).Batch;
        Assert.Equal(RecordFormatErrorKind.Malformed, Assert.Throws<RecordFormatException>(() => marked.Records()).Kind);
    }

    /// <summary>A batch whose block is <paramref name="block"/>, compressed with <paramref name="codec"/>, claiming one record.</summary>
    private static byte[] WithBlock(Compression codec, byte[] block)
    {
        byte[] head = new BatchBuilder().Push(1, (byte[]?)null, null).Finish().ToByteArray();
        byte[] output = new byte[Batch.HeaderLen + block.Length];
        Array.Copy(head, output, Batch.HeaderLen);
        Array.Copy(block, 0, output, Batch.HeaderLen, block.Length);
        Batch.PutU32(output, 8, (uint)(output.Length - Batch.LenPrefix));
        Batch.PutU16(output, 25, (ushort)new Attributes(codec, false, false, BatchKind.Data).ToBits());
        return Sum(output);
    }

    [Theory]
    [InlineData(Compression.Zstd)]
    [InlineData(Compression.Lz4)]
    public void ABlockThatDecompressesPastTheLimitIsRefused(Compression codec)
    {
        byte[] atLimit = new byte[Batch.MaxRecordsLen];
        byte[] pastIt = new byte[Batch.MaxRecordsLen + 1];
        byte[] big = WithBlock(codec, BlockCodecs.Compress(codec, pastIt, 0, pastIt.Length));
        Assert.True(big.Length < Batch.MaxBatchLen, "a small batch: " + big.Length);
        Batch b = Batch.Parse(ByteString.CopyOf(big)).Batch;
        Assert.Equal(RecordFormatErrorKind.TooLarge, Assert.Throws<RecordFormatException>(() => b.Records()).Kind);
        // Exactly at the limit is decompressed; zeros are no valid records, which is refused otherwise.
        Batch ok = Batch.Parse(ByteString.CopyOf(WithBlock(codec, BlockCodecs.Compress(codec, atLimit, 0, atLimit.Length)))).Batch;
        Assert.NotEqual(RecordFormatErrorKind.TooLarge, Assert.Throws<RecordFormatException>(() => ok.Records()).Kind);
    }

    [Fact]
    public void TheBuilderRefusesToCompressMoreThanTheLimit()
    {
        byte[] mib = new byte[1024 * 1024];
        var b = new BatchBuilder(new BatchOptions(new Attributes(Compression.Zstd, false, false, BatchKind.Data), 0, 0, -1));
        for (int i = 0; i < 16; i++)
        {
            b.Push(Ts, (byte[]?)null, mib);
        }
        Assert.Equal(RecordFormatErrorKind.TooLarge, Assert.Throws<RecordFormatException>(() => b.Finish()).Kind);
        // Plain, the same records are simply too large a batch.
        var p = new BatchBuilder();
        p.Push(Ts, (byte[]?)null, mib);
        Assert.Equal(RecordFormatErrorKind.TooLarge, Assert.Throws<RecordFormatException>(() => p.Finish()).Kind);
    }

    [Fact]
    public void ACompressedBatchOfManyBlocksAndRepeatsRoundTrips()
    {
        // More than one 64 KiB LZ4 block, and data that is not all one byte.
        var random = new Random(7);
        byte[] value = new byte[300_000];
        random.NextBytes(value);
        for (int i = 0; i < value.Length; i += 3)
        {
            value[i] = 0x41;
        }
        foreach (Compression codec in new[] { Compression.Zstd, Compression.Lz4 })
        {
            var b = new BatchBuilder(new BatchOptions(new Attributes(codec, false, false, BatchKind.Data), 0, 0, -1));
            b.Push(Ts, (byte[]?)null, value);
            Batch read = Batch.Parse(b.Finish()).Batch;
            Assert.Equal(value, read.Records()[0].Value!.ToByteArray());
        }
    }

    [Fact]
    public void PlainBatchesAreUnaffected()
    {
        var b = new BatchBuilder();
        b.Push(1, (byte[]?)null, new byte[] { 1 });
        Assert.Single(Batch.Parse(b.Finish()).Batch.Records());
    }
}

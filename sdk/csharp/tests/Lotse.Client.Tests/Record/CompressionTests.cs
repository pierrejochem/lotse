using System;
using System.Collections.Generic;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Record;

/// <summary>
/// Phase 1 has no zstd or LZ4 library (ADR 0083: optional). The broker's compressed batches still parse and their
/// header and record block are checked; reading their records, or building one, fails with a named error.
/// </summary>
public class CompressionTests
{
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
    public void TheBrokersCompressedBatchesParseAtTheHeaderAndRecordBlockLevel()
    {
        var expected = ExpectedRecords();
        int seen = 0;
        foreach (string[] v in Vectors.Lines("compressed.hex"))
        {
            byte[] bytes = Vectors.Hex(v[1]);
            Batch b = Batch.Parse(ByteString.CopyOf(bytes)).Batch; // the checksum covers the compressed bytes
            bool zstd = v[0].StartsWith("zstd");
            Assert.Equal(zstd ? Compression.Zstd : Compression.Lz4, b.Attributes.Compression);

            // The header agrees with the records the broker listed for it.
            List<string> lines = expected[v[0]];
            string[] first = lines[0].Split(' ');
            Assert.Equal(ulong.Parse(first[2]), b.BaseOffset);
            Assert.Equal(long.Parse(first[3]), b.BaseTimestamp);
            Assert.Equal((uint)lines.Count, b.RecordCount);
            Assert.Equal((uint)lines.Count - 1, b.LastOffsetDelta);

            // The record block is one zstd frame or one LZ4 frame, right after the header.
            Assert.Equal(zstd ? ZstdMagic : Lz4Magic, bytes.AsSpan(Batch.HeaderLen, 4).ToArray());

            // The batch is written again to the same bytes by the in-place edits.
            byte[] again = (byte[])bytes.Clone();
            Batch.SetProducer(again, b.ProducerId, b.ProducerEpoch, b.BaseSequence);
            Batch.Stamp(again, b.BaseOffset, b.Term);
            Assert.Equal(v[1], Vectors.Hex(again));
            seen++;
        }
        Assert.Equal(expected.Count, seen);
    }

    [Fact]
    public void ReadingACompressedBatchNamesTheMissingCodec()
    {
        foreach (string[] v in Vectors.Lines("compressed.hex"))
        {
            Batch b = Batch.Parse(ByteString.CopyOf(Vectors.Hex(v[1]))).Batch;
            var e = Assert.Throws<RecordFormatException>(() => b.Records());
            Assert.Equal(RecordFormatErrorKind.UnsupportedCompression, e.Kind);
            string name = b.Attributes.Compression == Compression.Lz4 ? "lz4" : "zstd";
            Assert.Contains(name, e.Message);
            Assert.Contains("compression codec", e.Message);
        }
    }

    [Fact]
    public void BuildingACompressedBatchNamesTheMissingCodec()
    {
        foreach (Compression codec in new[] { Compression.Zstd, Compression.Lz4 })
        {
            var b = new BatchBuilder(new BatchOptions(new Attributes(codec, false, false, BatchKind.Data), 0, 0, -1));
            b.Push(1, (byte[]?)null, new byte[] { 1 });
            var e = Assert.Throws<RecordFormatException>(() => b.Finish());
            Assert.Equal(RecordFormatErrorKind.UnsupportedCompression, e.Kind);
            Assert.Contains(codec == Compression.Lz4 ? "lz4" : "zstd", e.Message);
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

using System.Collections.Generic;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Record;

/// <summary>
/// Batches compaction rewrote (ADR 0067): what a fetch of a compacted stream returns. They read with gaps between
/// their records' offsets, may hold no record at all, and keep every header field but the length, the checksum, the
/// record count and attribute bit 8.
/// </summary>
public class CompactedConformanceTests
{
    private sealed record Vector(uint Count, long Span, byte[] Bytes);

    private static Dictionary<string, Vector> Load()
    {
        var output = new Dictionary<string, Vector>();
        foreach (string[] v in Vectors.Lines("compacted.hex"))
        {
            output[v[0]] = new Vector(uint.Parse(v[1]), long.Parse(v[2]), Vectors.Hex(v[3]));
        }
        return output;
    }

    [Fact]
    public void CompactedBatchesReadWithTheirGaps()
    {
        var expected = new Dictionary<string, List<string>>();
        foreach (string[] v in Vectors.Lines("compacted-records.txt"))
        {
            if (!expected.TryGetValue(v[0], out var list))
            {
                expected[v[0]] = list = new List<string>();
            }
            list.Add(string.Join(' ', v));
        }
        var all = Load();
        Assert.Equal(5, all.Count);
        foreach (var e in all)
        {
            string label = e.Key;
            Batch b = Batch.Parse(ByteString.CopyOf(e.Value.Bytes)).Batch;
            Assert.True(b.IsCompacted, label);
            Assert.Equal(e.Value.Count, b.RecordCount);
            Assert.Equal(e.Value.Span, b.Span);
            Assert.Equal(unchecked(b.BaseOffset + (ulong)b.Span), b.NextOffset);
            if (b.Attributes.Compression != Compression.None && b.RecordCount > 0)
            {
                // Without a codec the records of a compressed block cannot be read; the error names the codec.
                Assert.Equal(
                    RecordFormatErrorKind.UnsupportedCompression,
                    Assert.Throws<RecordFormatException>(() => b.Records()).Kind);
                continue;
            }
            Assert.Equal(
                expected.GetValueOrDefault(label, new List<string>()),
                BatchConformanceTests.Lines(label, b.Records()));
        }
    }

    [Fact]
    public void AnEmptiedBatchReadsAsNothingWhateverItsCodec()
    {
        var all = Load();
        Batch emptied = Batch.Parse(ByteString.CopyOf(all["lz4-emptied"].Bytes)).Batch;
        Assert.Equal(Compression.Lz4, emptied.Attributes.Compression);
        Assert.Equal(0u, emptied.RecordCount);
        Assert.Empty(emptied.Records());
    }

    [Fact]
    public void TheHeaderOfTheOriginalStays()
    {
        var originals = new Dictionary<string, byte[]>();
        foreach (string file in new[] { "batches.hex", "compressed.hex" })
        {
            foreach (string[] v in Vectors.Lines(file))
            {
                originals[v[0]] = Vectors.Hex(v[1]);
            }
        }
        var from = new Dictionary<string, string>
        {
            ["gap"] = "data-stamped",
            ["last-only"] = "idempotent",
            ["emptied"] = "transactional",
            ["zstd-gap"] = "zstd-stamped",
            ["lz4-emptied"] = "lz4-large",
        };
        foreach (var e in Load())
        {
            Batch c = Batch.Parse(ByteString.CopyOf(e.Value.Bytes)).Batch;
            Batch o = Batch.Parse(ByteString.CopyOf(originals[from[e.Key]])).Batch;
            Assert.False(o.IsCompacted);
            string label = e.Key;
            Assert.True(o.BaseOffset == c.BaseOffset, label);
            Assert.True(o.LastOffsetDelta == c.LastOffsetDelta, label);
            Assert.True(o.Term == c.Term, label);
            Assert.True(o.Attributes == c.Attributes, label);
            Assert.True(o.ProducerId == c.ProducerId, label);
            Assert.True(o.ProducerEpoch == c.ProducerEpoch, label);
            Assert.True(o.BaseSequence == c.BaseSequence, label);
            Assert.True(o.BaseTimestamp == c.BaseTimestamp, label);
            Assert.True(o.MaxTimestamp == c.MaxTimestamp, label);
        }
    }

    // b with attribute bit 8 set and the checksum computed again.
    private static byte[] MarkedCompacted(byte[] b)
    {
        byte[] output = (byte[])b.Clone();
        output[26] |= 1;
        Batch.PutU32(output, 21, Crc32c.Compute(output, 25, output.Length));
        return output;
    }

    [Fact]
    public void OnlyDataBatchesAreCompactedAndNeverHoldMoreRecordsThanOffsets()
    {
        var batches = new Dictionary<string, byte[]>();
        foreach (string[] v in Vectors.Lines("batches.hex"))
        {
            batches[v[0]] = Vectors.Hex(v[1]);
        }
        // A whole batch marked compacted still reads: nothing was left out.
        Batch whole = Batch.Parse(ByteString.CopyOf(MarkedCompacted(batches["data-stamped"]))).Batch;
        Assert.Equal(3, whole.Records().Count);
        Assert.Throws<RecordFormatException>(() => Batch.Parse(ByteString.CopyOf(MarkedCompacted(batches["control-commit"]))));
        // More records than the batch spans.
        byte[] over = MarkedCompacted(batches["data-stamped"]);
        over[27] = 1; // last_offset_delta 1: two offsets for three records
        Batch.PutU32(over, 21, Crc32c.Compute(over, 25, over.Length));
        Assert.Throws<RecordFormatException>(() => Batch.Parse(ByteString.CopyOf(over)));
        // Not compacted, the strict rules stay: an empty batch is refused.
        byte[] plain = (byte[])Load()["emptied"].Bytes.Clone();
        plain[26] &= 0xFE;
        Batch.PutU32(plain, 21, Crc32c.Compute(plain, 25, plain.Length));
        Assert.Throws<RecordFormatException>(() => Batch.Parse(ByteString.CopyOf(plain)));
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Record;

/// <summary>Every batch the broker's code wrote is read here, checked field by field and written again byte for byte.</summary>
public class BatchConformanceTests
{
    private const long Ts = 1_700_000_000_000L;

    private static Dictionary<string, byte[]> Batches()
    {
        var output = new Dictionary<string, byte[]>();
        foreach (string[] v in Vectors.Lines("batches.hex"))
        {
            output[v[0]] = Vectors.Hex(v[1]);
        }
        return output;
    }

    private static Dictionary<string, Dictionary<string, string>> Fields()
    {
        var output = new Dictionary<string, Dictionary<string, string>>();
        foreach (string[] v in Vectors.Lines("batch-fields.txt"))
        {
            var m = new Dictionary<string, string>();
            for (int i = 1; i < v.Length; i++)
            {
                string[] kv = v[i].Split('=', 2);
                m[kv[0]] = kv[1];
            }
            output[v[0]] = m;
        }
        return output;
    }

    private static Batch ParseOne(byte[] bytes) => Batch.Parse(ByteString.CopyOf(bytes)).Batch;

    [Fact]
    public void HeaderFieldsMatch()
    {
        var expected = Fields();
        foreach (var e in Batches())
        {
            Batch.Split s = Batch.Parse(ByteString.CopyOf(e.Value));
            Assert.True(s.Rest.IsEmpty, e.Key);
            Batch b = s.Batch;
            var f = expected[e.Key];
            string label = e.Key;
            Assert.True(ulong.Parse(f["base_offset"]) == b.BaseOffset, label);
            Assert.True(ulong.Parse(f["term"]) == b.Term, label);
            Assert.True(int.Parse(f["attributes"]) == b.Attributes.ToBits(), label);
            Assert.True(int.Parse(f["compression"]) == (int)b.Attributes.Compression, label);
            Assert.True(bool.Parse(f["transactional"]) == b.Attributes.Transactional, label);
            Assert.True(bool.Parse(f["control"]) == b.Attributes.Control, label);
            Assert.True(int.Parse(f["kind"]) == (int)b.Attributes.Kind, label);
            Assert.True(uint.Parse(f["last_offset_delta"]) == b.LastOffsetDelta, label);
            Assert.True(long.Parse(f["base_ts"]) == b.BaseTimestamp, label);
            Assert.True(long.Parse(f["max_ts"]) == b.MaxTimestamp, label);
            Assert.True(ulong.Parse(f["producer_id"]) == b.ProducerId, label);
            Assert.True(ushort.Parse(f["producer_epoch"]) == b.ProducerEpoch, label);
            Assert.True(int.Parse(f["base_sequence"]) == b.BaseSequence, label);
            Assert.True(uint.Parse(f["record_count"]) == b.RecordCount, label);
            Assert.True(int.Parse(f["len"]) == b.Length, label);
        }
    }

    internal static string Nullable(ByteString? b)
    {
        if (b is null)
        {
            return "-";
        }
        if (b.Size > 64)
        {
            return "#" + b.Size + ":" + Crc32c.Compute(b.Span).ToString("x8");
        }
        return "=" + Vectors.Hex(b.ToByteArray());
    }

    internal static List<string> Lines(string label, IReadOnlyList<BatchRecord> records)
    {
        var output = new List<string>();
        for (int i = 0; i < records.Count; i++)
        {
            BatchRecord r = records[i];
            string headers = r.Headers.Count == 0
                ? "-"
                : string.Join(",", r.Headers.Select(h => Vectors.Hex(h.Key.ToByteArray()) + ":" + Nullable(h.Value)));
            output.Add(string.Join(' ', label, i.ToString(), r.Offset.ToString(), r.Timestamp.ToString(), Nullable(r.Key), Nullable(r.Value), headers));
        }
        return output;
    }

    [Fact]
    public void RecordsMatch()
    {
        var expected = new Dictionary<string, List<string>>();
        foreach (string[] v in Vectors.Lines("batch-records.txt"))
        {
            if (!expected.TryGetValue(v[0], out var list))
            {
                expected[v[0]] = list = new List<string>();
            }
            list.Add(string.Join(' ', v));
        }
        foreach (var e in Batches())
        {
            Assert.Equal(expected[e.Key], Lines(e.Key, ParseOne(e.Value).Records()));
        }
    }

    [Fact]
    public void ParsedBatchesAreWrittenAgainByteForByte()
    {
        foreach (var e in Batches())
        {
            Batch b = ParseOne(e.Value);
            var builder = new BatchBuilder(new BatchOptions(b.Attributes, b.ProducerId, b.ProducerEpoch, b.BaseSequence));
            foreach (BatchRecord r in b.Records())
            {
                builder.Push(r.Timestamp, r.Key, r.Value, r.Headers);
            }
            byte[] again = builder.Finish().ToByteArray();
            Batch.Stamp(again, b.BaseOffset, b.Term);
            Assert.Equal(Vectors.Hex(e.Value), Vectors.Hex(again));
        }
    }

    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    // The broker's own construction, repeated here without the reader: the oracle is written in the test.
    [Fact]
    public void BatchesBuiltHereMatchTheBrokersBytes()
    {
        var expected = Batches();

        var b = new BatchBuilder();
        b.Push(Ts, Bytes("k1"), Bytes("v1"), new[]
        {
            new Header(ByteString.Utf8("h"), ByteString.Utf8("x")), new Header(ByteString.Utf8("n"), null),
        });
        b.Push(Ts + 5, (byte[]?)null, (byte[]?)null);
        b.Push(Ts - 1, Array.Empty<byte>(), Array.Empty<byte>(), new[] { new Header(ByteString.Empty, ByteString.Empty) });
        byte[] built = b.Finish().ToByteArray();
        Batch.Stamp(built, 100, 7);
        Assert.Equal(Vectors.Hex(expected["data-stamped"]), Vectors.Hex(built));

        b = new BatchBuilder(new BatchOptions(Attributes.Data, 17, 2, 5));
        for (byte i = 0; i < 3; i++)
        {
            b.Push(Ts + i, new[] { i }, new[] { i, i });
        }
        built = b.Finish().ToByteArray();
        Batch.Stamp(built, 3, 1);
        Assert.Equal(Vectors.Hex(expected["idempotent"]), Vectors.Hex(built));

        b = new BatchBuilder(new BatchOptions(new Attributes(Compression.None, true, false, BatchKind.Data), 17, 2, 8));
        b.Push(Ts, Bytes("t"), Bytes("transactional"));
        built = b.Finish().ToByteArray();
        Batch.Stamp(built, 200, 2);
        Assert.Equal(Vectors.Hex(expected["transactional"]), Vectors.Hex(built));

        built = ControlBatches.Marker(17, 3, ControlType.Commit, 9, Ts).ToByteArray();
        Batch.Stamp(built, 204, 2);
        Assert.Equal(Vectors.Hex(expected["control-commit"]), Vectors.Hex(built));
        built = ControlBatches.Marker(18, 0, ControlType.Abort, 0, Ts).ToByteArray();
        Batch.Stamp(built, 205, 2);
        Assert.Equal(Vectors.Hex(expected["control-abort"]), Vectors.Hex(built));

        byte[] key = new byte[200];
        byte[] value = new byte[20_000];
        Array.Fill(key, (byte)0xab);
        Array.Fill(value, (byte)0xcd);
        b = new BatchBuilder();
        b.Push(Ts, key, value);
        b.Push(-Ts, Bytes("negative"), (byte[]?)null);
        Assert.Equal(Vectors.Hex(expected["data-large"]), Vectors.Hex(b.Finish().ToByteArray()));
    }

    [Fact]
    public void InPlaceEditsMatch()
    {
        foreach (string[] v in Vectors.Lines("batch-ops.txt"))
        {
            byte[] batch = Vectors.Hex(v[^2]);
            string[] args = v[1].Split(',');
            switch (v[0])
            {
                case "set_producer":
                    Batch.SetProducer(batch, ulong.Parse(args[0]), ushort.Parse(args[1]), int.Parse(args[2]));
                    break;
                case "set_transactional":
                    Batch.SetTransactional(batch, bool.Parse(args[0]));
                    break;
                case "set_base_timestamp":
                    Batch.SetBaseTimestamp(batch, long.Parse(args[0]));
                    break;
                default:
                    throw new InvalidOperationException(v[0]);
            }
            Assert.Equal(v[^1], Vectors.Hex(batch));
            // The edited batch is still valid: the checksum was recomputed.
            Batch.Parse(ByteString.CopyOf(batch));
        }
    }

    [Fact]
    public void ConcatenatedBatchesAreSplitByTheirLength()
    {
        foreach (string[] v in Vectors.Lines("fetch-records.hex"))
        {
            byte[] all = Vectors.Hex(v[1]);
            List<Batch> parsed = Batch.ParseAll(ByteString.CopyOf(all));
            Assert.True(parsed.Count >= 4, v[0]);
            int total = 0;
            foreach (Batch b in parsed)
            {
                Assert.Equal(b.Length, Batch.PeekLen(all.AsSpan(total).ToArray()));
                total += b.Length;
            }
            Assert.Equal(all.Length, total);
            // Cutting the last batch short is a truncation, not a shorter list.
            byte[] cut = Vectors.Cut(all, all.Length - 1);
            Assert.Equal(
                RecordFormatErrorKind.Truncated,
                Assert.Throws<RecordFormatException>(() => Batch.ParseAll(ByteString.CopyOf(cut))).Kind);
        }
    }

    [Fact]
    public void EachBatchCutAtEveryLengthIsTruncated()
    {
        foreach (var e in Batches())
        {
            for (int cut = 0; cut < e.Value.Length; cut++)
            {
                byte[] part = Vectors.Cut(e.Value, cut);
                var ex = Assert.Throws<RecordFormatException>(() => Batch.Parse(ByteString.CopyOf(part)));
                Assert.Equal(RecordFormatErrorKind.Truncated, ex.Kind);
            }
        }
    }

    [Fact]
    public void AByteAddedAfterABatchIsLeftOverAndNeverPartOfIt()
    {
        foreach (var e in Batches())
        {
            byte[] longer = Vectors.Longer(e.Value);
            Batch.Split s = Batch.Parse(ByteString.CopyOf(longer));
            Assert.Equal(e.Value.Length, s.Batch.Length);
            Assert.Equal(1, s.Rest.Size);
            Assert.Equal(
                RecordFormatErrorKind.Truncated,
                Assert.Throws<RecordFormatException>(() => Batch.ParseAll(ByteString.CopyOf(longer))).Kind);
        }
    }

    [Fact]
    public void ChecksumsAreVerifiedOnTheWholeBatchFromByte25()
    {
        foreach (var e in Batches())
        {
            byte[] b = e.Value;
            Assert.Equal(Crc32c.Compute(b, 25, b.Length), Batch.GetU32(b, 21));
            // Stamping changes bytes 0..8 and 12..20 only, so the checksum stays valid.
            byte[] stamped = (byte[])b.Clone();
            Batch.Stamp(stamped, 123456789UL, 42);
            Batch.Parse(ByteString.CopyOf(stamped));
            Assert.Equal(b.AsSpan(21).ToArray(), stamped.AsSpan(21).ToArray());
        }
    }

    [Fact]
    public void ControlMarkersAreRead()
    {
        var batches = Batches();
        Batch commit = ParseOne(batches["control-commit"]);
        Assert.Equal(new ControlMarker(ControlType.Commit, 9), ControlBatches.ReadMarker(commit));
        Assert.Equal(ControlKind.CommitMarker, ControlBatches.Kind(commit));
        Batch abort = ParseOne(batches["control-abort"]);
        Assert.Equal(new ControlMarker(ControlType.Abort, 0), ControlBatches.ReadMarker(abort));
        Assert.Equal(17UL, commit.ProducerId);
        Assert.Equal(ControlKind.RaftNoop, ControlBatches.Kind(ParseOne(batches["raft-noop"])));
        Assert.Equal(ControlKind.RaftConfig, ControlBatches.Kind(ParseOne(batches["raft-config"])));
        Assert.Equal(ControlKind.Metadata, ControlBatches.Kind(ParseOne(batches["internal-metadata"])));
        Batch data = ParseOne(batches["data-stamped"]);
        Assert.Null(ControlBatches.Kind(data));
        Assert.False(data.Attributes.Control);
    }

    [Fact]
    public void TheBatchKindsOfTheQueueLog()
    {
        var batches = Batches();
        Assert.Equal(BatchKind.Ack, ParseOne(batches["ack"]).Attributes.Kind);
        Assert.Equal(BatchKind.QueueState, ParseOne(batches["queue-state"]).Attributes.Kind);
        Assert.Equal(BatchKind.DeadLetter, ParseOne(batches["dead-letter"]).Attributes.Kind);
        Assert.Equal(BatchKind.Retire, ParseOne(batches["retire-out"]).Attributes.Kind);
        Assert.Equal(BatchKind.Retire, ParseOne(batches["retire-done"]).Attributes.Kind);
    }
}

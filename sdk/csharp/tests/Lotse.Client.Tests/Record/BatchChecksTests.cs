using System;
using System.Collections.Generic;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Record;

/// <summary>Each check of <c>Batch.Parse</c> and <c>Batch.Records</c> has a test that fails without it.</summary>
public class BatchChecksTests
{
    private static byte[] Sample()
    {
        var b = new BatchBuilder();
        b.Push(1000, new byte[] { 1 }, new byte[] { 2, 3 });
        b.Push(1001, (byte[]?)null, new byte[] { 4 });
        return b.Finish().ToByteArray();
    }

    // Recomputes the checksum, so a test changes one thing and the checksum is not what is refused.
    private static byte[] Sum(byte[] b)
    {
        Batch.PutU32(b, 21, Crc32c.Compute(b, 25, b.Length));
        return b;
    }

    private static RecordFormatErrorKind ParseFails(byte[] b) =>
        Assert.Throws<RecordFormatException>(() => Batch.Parse(ByteString.CopyOf(b))).Kind;

    private static RecordFormatErrorKind RecordsFail(byte[] b)
    {
        Batch batch = Batch.Parse(ByteString.CopyOf(b)).Batch;
        return Assert.Throws<RecordFormatException>(() => batch.Records()).Kind;
    }

    [Fact]
    public void AValidBatchParses()
    {
        Batch b = Batch.Parse(ByteString.CopyOf(Sample())).Batch;
        Assert.Equal(2u, b.RecordCount);
        Assert.Equal(2, b.Records().Count);
    }

    [Fact]
    public void AFlippedByteAnywhereAfterTheChecksumFieldIsRefused()
    {
        byte[] good = Sample();
        for (int i = 25; i < good.Length; i++)
        {
            byte[] bad = (byte[])good.Clone();
            bad[i] ^= 0x01;
            Assert.Equal(RecordFormatErrorKind.Checksum, ParseFails(bad));
        }
    }

    [Fact]
    public void AWrongStoredChecksumIsRefused()
    {
        byte[] bad = Sample();
        bad[21] ^= 0x01;
        Assert.Equal(RecordFormatErrorKind.Checksum, ParseFails(bad));
    }

    [Fact]
    public void TheChecksumIsNotComputedOverTheBytesBefore25()
    {
        byte[] b = Sample();
        b[3] ^= 0x55; // base_offset
        b[14] ^= 0x55; // term
        Batch.Parse(ByteString.CopyOf(b));
    }

    [Fact]
    public void AWrongMagicIsRefusedBeforeTheChecksum()
    {
        byte[] b = Sample();
        b[20] = 2;
        Assert.Equal(RecordFormatErrorKind.UnsupportedMagic, ParseFails(b));
    }

    [Fact]
    public void ABufferShorterThanTheLengthPrefixOrTheBatchIsTruncated()
    {
        Assert.Equal(RecordFormatErrorKind.Truncated, ParseFails(new byte[11]));
        byte[] b = Sample();
        Assert.Equal(RecordFormatErrorKind.Truncated, ParseFails(Vectors.Cut(b, b.Length - 1)));
    }

    [Fact]
    public void ALengthShorterThanTheHeaderIsMalformed()
    {
        byte[] b = Sample();
        Batch.PutU32(b, 8, 10);
        Assert.Equal(RecordFormatErrorKind.Malformed, ParseFails(b));
    }

    [Fact]
    public void ALengthOverTheLimitIsRefusedWithoutOverflowing()
    {
        byte[] b = Sample();
        Batch.PutU32(b, 8, (uint)(Batch.MaxBatchLen - Batch.LenPrefix + 1));
        Assert.Equal(RecordFormatErrorKind.TooLarge, ParseFails(b));
        Batch.PutU32(b, 8, 0xFFFF_FFFFu);
        Assert.Equal(RecordFormatErrorKind.TooLarge, ParseFails(b));
        Assert.Equal(0xFFFF_FFFFL + 12, Batch.PeekLen(b));
    }

    [Fact]
    public void AttributesAreValidated()
    {
        foreach (ushort bits in new ushort[] { 3, 6 << 5, 7 << 5, 1 << 9, 1 << 15 })
        {
            byte[] b = Sample();
            Batch.PutU16(b, 25, bits); // compression 3, kind 6, kind 7, reserved bits
            Assert.Equal(RecordFormatErrorKind.Malformed, ParseFails(Sum(b)));
        }
    }

    [Fact]
    public void EveryKnownAttributeCombinationRoundTrips()
    {
        foreach (Compression c in Enum.GetValues<Compression>())
        {
            foreach (BatchKind k in Enum.GetValues<BatchKind>())
            {
                for (int flags = 0; flags < 4; flags++)
                {
                    var a = new Attributes(c, (flags & 1) != 0, (flags & 2) != 0, k);
                    Assert.Equal(a, Attributes.FromBits(a.ToBits()));
                }
            }
        }
    }

    [Fact]
    public void AnEmptyRecordCountIsRefused()
    {
        byte[] b = Sample();
        Batch.PutU32(b, 61, 0);
        Batch.PutU32(b, 27, 0xFFFF_FFFFu);
        Assert.Equal(RecordFormatErrorKind.Malformed, ParseFails(Sum(b)));
    }

    [Fact]
    public void LastOffsetDeltaMustMatchTheRecordCount()
    {
        byte[] b = Sample();
        Batch.PutU32(b, 27, 5);
        Assert.Equal(RecordFormatErrorKind.Malformed, ParseFails(Sum(b)));
    }

    [Fact]
    public void MaxTimestampBeforeBaseTimestampIsRefused()
    {
        byte[] b = Sample();
        Batch.PutU64(b, 39, 999);
        Assert.Equal(RecordFormatErrorKind.Malformed, ParseFails(Sum(b)));
    }

    [Fact]
    public void AControlBatchHoldsOneRecord()
    {
        var b = new BatchBuilder(new BatchOptions(new Attributes(Compression.None, true, true, BatchKind.Data), 1, 0, -1));
        b.Push(1, new byte[4], new byte[4]);
        b.Push(2, new byte[4], new byte[4]);
        Assert.Equal(RecordFormatErrorKind.Malformed, ParseFails(b.Finish().ToByteArray()));
    }

    [Fact]
    public void RecordsMustHaveConsecutiveOffsetDeltas()
    {
        byte[] b = Sample();
        // First record: length, attributes, ts_delta, offset_delta... offset_delta is the 3rd byte after the length.
        b[Batch.HeaderLen + 3] = (byte)Varint.Zigzag(1); // offset_delta 1 instead of 0
        Assert.Equal(RecordFormatErrorKind.Malformed, RecordsFail(Sum(b)));
    }

    [Fact]
    public void BytesAfterTheLastRecordAreRefused()
    {
        byte[] sample = Sample();
        byte[] b = Vectors.Longer(sample);
        Batch.PutU32(b, 8, (uint)(b.Length - Batch.LenPrefix));
        Assert.Equal(RecordFormatErrorKind.Malformed, RecordsFail(Sum(b)));
    }

    [Fact]
    public void BytesInsideARecordAreRefused()
    {
        byte[] good = Sample();
        // Grow the first record by one byte: its length varint says 1 more, and one more byte follows it.
        byte[] b = new byte[good.Length + 1];
        Buffer.BlockCopy(good, 0, b, 0, Batch.HeaderLen + 1 + 7);
        int lenAt = Batch.HeaderLen;
        int recordLen = (int)Varint.Unzigzag(good[lenAt]);
        b[lenAt] = (byte)Varint.Zigzag(recordLen + 1);
        b[Batch.HeaderLen + 1 + recordLen] = 0x7f;
        Buffer.BlockCopy(good, Batch.HeaderLen + 1 + recordLen, b, Batch.HeaderLen + 2 + recordLen, good.Length - Batch.HeaderLen - 1 - recordLen);
        Batch.PutU32(b, 8, (uint)(b.Length - Batch.LenPrefix));
        Assert.Equal(RecordFormatErrorKind.Malformed, RecordsFail(Sum(b)));
    }

    [Fact]
    public void ARecordLongerThanTheBatchIsTruncated()
    {
        byte[] b = Sample();
        b[Batch.HeaderLen] = (byte)Varint.Zigzag(100);
        Assert.Equal(RecordFormatErrorKind.Truncated, RecordsFail(Sum(b)));
    }

    [Fact]
    public void ANegativeRecordLengthIsRefused()
    {
        byte[] b = Sample();
        b[Batch.HeaderLen] = (byte)Varint.Zigzag(-5);
        Assert.Equal(RecordFormatErrorKind.Malformed, RecordsFail(Sum(b)));
    }

    [Fact]
    public void AnEmptyBuilderAndAnOversizedBatchAreRefused()
    {
        Assert.Equal(RecordFormatErrorKind.Malformed, Assert.Throws<RecordFormatException>(() => new BatchBuilder().Finish()).Kind);
        var b = new BatchBuilder();
        b.Push(1, (byte[]?)null, new byte[Batch.MaxBatchLen]);
        Assert.Equal(RecordFormatErrorKind.TooLarge, Assert.Throws<RecordFormatException>(() => b.Finish()).Kind);
    }

    [Fact]
    public void EditsRefuseBuffersThatAreNotWholeBatches()
    {
        byte[] b = Sample();
        byte[] cut = Vectors.Cut(b, b.Length - 1);
        Assert.Equal(RecordFormatErrorKind.Truncated, Assert.Throws<RecordFormatException>(() => Batch.SetProducer(cut, 1, 1, 1)).Kind);
        Assert.Equal(RecordFormatErrorKind.Truncated, Assert.Throws<RecordFormatException>(() => Batch.SetTransactional(cut, true)).Kind);
        Assert.Equal(RecordFormatErrorKind.Truncated, Assert.Throws<RecordFormatException>(() => Batch.SetBaseTimestamp(cut, 1)).Kind);
        Assert.Equal(RecordFormatErrorKind.Truncated, Assert.Throws<RecordFormatException>(() => Batch.Stamp(new byte[10], 1, 1)).Kind);
        Assert.Equal(-1, Batch.PeekLen(new byte[11]));
    }

    [Fact]
    public void NullAndEmptyAreDifferentInKeysValuesAndHeaders()
    {
        var b = new BatchBuilder();
        b.Push(5, (byte[]?)null, Array.Empty<byte>(), new[] { new Header(ByteString.Empty, null) });
        BatchRecord r = Batch.Parse(b.Finish()).Batch.Records()[0];
        Assert.Null(r.Key);
        Assert.True(r.Value!.IsEmpty);
        Assert.Null(r.Headers[0].Value);
    }

    [Fact]
    public void U64OffsetsAreUnsigned()
    {
        byte[] b = Sample();
        Batch.Stamp(b, ulong.MaxValue - 1, 1); // 2^64 - 2
        Batch batch = Batch.Parse(ByteString.CopyOf(b)).Batch;
        Assert.Equal("18446744073709551614", batch.BaseOffset.ToString());
        Assert.Equal(ulong.MaxValue, batch.LastOffset);
        Assert.Equal(0UL, batch.NextOffset);
    }

    [Fact]
    public void TheRecordsCarryTheirAbsoluteOffsetsAndTimestamps()
    {
        byte[] b = Sample();
        Batch.Stamp(b, 40, 3);
        List<BatchRecord> records = Batch.Parse(ByteString.CopyOf(b)).Batch.Records();
        Assert.Equal(40UL, records[0].Offset);
        Assert.Equal(41UL, records[1].Offset);
        Assert.Equal(1000, records[0].Timestamp);
        Assert.Equal(1001, records[1].Timestamp);
    }
}

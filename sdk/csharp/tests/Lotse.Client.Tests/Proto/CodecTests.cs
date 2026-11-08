using System;
using System.Collections.Generic;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Proto;

public class CodecTests
{
    private static DecodeErrorKind Kind(Action a) => Assert.Throws<DecodeException>(a).Kind;

    [Fact]
    public void FixedWidthIntegersAreLittleEndian()
    {
        byte[] b = new Writer().U16(0x0102).U32(0x03040506u).U64(0x0708090a0b0c0d0eUL).I32(-2).ToByteArray();
        Assert.Equal(
            new byte[] { 2, 1, 6, 5, 4, 3, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 9, 8, 7, 0xfe, 0xff, 0xff, 0xff }, b);
        var r = new Reader(b);
        Assert.Equal(0x0102, r.U16());
        Assert.Equal(0x03040506u, r.U32());
        Assert.Equal(0x0708090a0b0c0d0eUL, r.U64());
        Assert.Equal(-2, r.I32());
        r.Finish();
    }

    [Fact]
    public void UnsignedValuesKeepTheirFullRange()
    {
        Assert.Equal(0xFFFF_FFFFu, new Reader(new Writer().U32(0xFFFF_FFFFu).ToByteArray()).U32());
        Assert.Equal(ulong.MaxValue, new Reader(new Writer().U64(ulong.MaxValue).ToByteArray()).U64());
        Assert.Equal(long.MinValue, new Reader(new Writer().I64(long.MinValue).ToByteArray()).I64());
        Assert.Equal(new byte[] { 0xff, 0xff }, new Writer().U16(ushort.MaxValue).ToByteArray());
        Assert.Equal(new byte[] { 0xff }, new Writer().U8(byte.MaxValue).ToByteArray());
    }

    [Fact]
    public void StringsAreLengthPrefixedUtf8WithANullMarker()
    {
        byte[] b = new Writer().String("päckchen").OptString(null).OptString("").ToByteArray();
        Assert.Equal(
            new byte[] { 9, 0, (byte)'p', 0xc3, 0xa4, (byte)'c', (byte)'k', (byte)'c', (byte)'h', (byte)'e', (byte)'n', 0xff, 0xff, 0, 0 },
            b);
        var r = new Reader(b);
        Assert.Equal("päckchen", r.String());
        Assert.Null(r.OptString());
        Assert.Equal("", r.String());
    }

    [Fact]
    public void ANullStringWhereOneIsRequiredIsMalformed()
    {
        var r = new Reader(new byte[] { 0xff, 0xff });
        Assert.Equal(DecodeErrorKind.Malformed, Kind(() => r.String()));
    }

    [Fact]
    public void InvalidUtf8IsMalformed()
    {
        var r = new Reader(new byte[] { 2, 0, 0xc3, 0x28 });
        Assert.Equal(DecodeErrorKind.Malformed, Kind(() => r.String()));
        // A lone surrogate half, encoded as UTF-8, is not UTF-8 either.
        var s = new Reader(new byte[] { 3, 0, 0xed, 0xa0, 0x80 });
        Assert.Equal(DecodeErrorKind.Malformed, Kind(() => s.String()));
    }

    [Fact]
    public void TheLongestStringIs65534Bytes()
    {
        string ok = new('a', 65534);
        Assert.Equal(ok, new Reader(new Writer().String(ok).ToByteArray()).String());
        Assert.Throws<ArgumentException>(() => new Writer().String(new string('a', 65535)));
        Assert.Throws<ArgumentException>(() => new Writer().String("\ud800"));
    }

    [Fact]
    public void BytesAreLengthPrefixedByAU32()
    {
        byte[] b = new Writer().Bytes(ByteString.CopyOf(new byte[] { 1, 2, 3 })).ToByteArray();
        Assert.Equal(new byte[] { 3, 0, 0, 0, 1, 2, 3 }, b);
        Assert.Equal(ByteString.CopyOf(new byte[] { 1, 2, 3 }), new Reader(b).Bytes());
        // A length of 2^32 - 1 is a truncation, not a negative number.
        Assert.Equal(DecodeErrorKind.Truncated, Kind(() => new Reader(new byte[] { 0xff, 0xff, 0xff, 0xff }).Bytes()));
    }

    [Fact]
    public void CountsAreZigZagVarints()
    {
        byte[] b = new Writer().Array(new List<string> { "a", "b" }, (w, s) => w.String(s)).ToByteArray();
        Assert.Equal(4, b[0]); // count 2 is ZigZag 4
        Assert.Equal(new List<string> { "a", "b" }, new Reader(b).Array(2, r => r.String()));
    }

    [Fact]
    public void ANegativeCountIsMalformed()
    {
        byte[] b = new Writer().Varint(-1).ToByteArray();
        Assert.Equal(DecodeErrorKind.Malformed, Kind(() => new Reader(b).Array(1, r => r.U8())));
    }

    [Fact]
    public void ACountTheFrameCannotHoldIsRefusedBeforeAllocating()
    {
        byte[] b = new Writer().Varint(long.MaxValue / 2).ToByteArray();
        Assert.Equal(DecodeErrorKind.Malformed, Kind(() => new Reader(b).Array(1, r => r.U8())));
        byte[] two = new Writer().Varint(3).U8(1).U8(2).ToByteArray();
        Assert.Equal(DecodeErrorKind.Malformed, Kind(() => new Reader(two).Array(1, r => r.U8())));
        Assert.Equal(
            new List<byte> { 1, 2 }, new Reader(new Writer().Varint(2).U8(1).U8(2).ToByteArray()).Array(1, r => r.U8()));
    }

    [Fact]
    public void AnOverlongVarintIsRefused()
    {
        byte[] b = new byte[11];
        Array.Fill(b, (byte)0xff);
        Assert.Equal(DecodeErrorKind.Truncated, Kind(() => new Reader(b).Varint()));
    }

    [Fact]
    public void TagsAreACountAndTagValuePairs()
    {
        byte[] b = new Writer().Tags(new[] { new Tag(0, ByteString.CopyOf(new byte[] { 7 })) }).ToByteArray();
        Assert.Equal(new byte[] { 2, 0, 1, 0, 0, 0, 7 }, b);
        List<Tag> tags = new Reader(b).Tags();
        Assert.Single(tags);
        Assert.Equal(0, tags[0].Number);
        byte[] negative = new Writer().Varint(1).Varint(-1).U32(0).ToByteArray();
        Assert.Equal(DecodeErrorKind.Malformed, Kind(() => new Reader(negative).Tags()));
        Assert.Throws<ArgumentException>(() => new Tag(-1, ByteString.Empty));
    }

    [Fact]
    public void FinishRefusesTrailingBytes()
    {
        var r = new Reader(new byte[] { 1, 2 });
        r.U8();
        Assert.Equal(DecodeErrorKind.Malformed, Kind(r.Finish));
    }

    [Fact]
    public void ABoolIsZeroOrOneAndAnyNonZeroByteReadsAsTrue()
    {
        Assert.Equal(new byte[] { 1, 0 }, new Writer().Bool(true).Bool(false).ToByteArray());
        Assert.True(new Reader(new byte[] { 7 }).Bool());
    }

    [Fact]
    public void ErrorCodes()
    {
        Assert.Equal("NOT_LEADER", ErrorCode.NotLeader.CodeName);
        Assert.Equal("AUTHENTICATION_FAILED", ErrorCode.AuthenticationFailed.CodeName);
        Assert.Equal("UNSUPPORTED_SASL_MECHANISM", ErrorCode.UnsupportedSaslMechanism.CodeName);
        Assert.Equal("UNKNOWN", new ErrorCode(999).CodeName);
        Assert.Equal("NOT_LEADER (1)", ErrorCode.NotLeader.ToString());
        int retriable = 0;
        for (ushort c = 0; c <= 28; c++)
        {
            if (new ErrorCode(c).Retriable)
            {
                retriable++;
            }
        }
        Assert.Equal(8, retriable);
    }
}

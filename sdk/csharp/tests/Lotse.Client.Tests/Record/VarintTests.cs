using System;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Record;

public class VarintTests
{
    private static byte[] EncodeI64(long v)
    {
        byte[] buf = new byte[Varint.MaxLen64];
        int n = Varint.PutI64(buf, 0, v);
        return buf.AsSpan(0, n).ToArray();
    }

    private static byte[] EncodeU64(ulong v)
    {
        byte[] buf = new byte[Varint.MaxLen64];
        int n = Varint.PutU64(buf, 0, v);
        return buf.AsSpan(0, n).ToArray();
    }

    private static RecordFormatErrorKind Failure(byte[] input) =>
        Assert.Throws<RecordFormatException>(() => Varint.GetU64(input, 0, input.Length)).Kind;

    [Fact]
    public void VectorsFromTheBroker()
    {
        foreach (string[] v in Vectors.Lines("varints.txt"))
        {
            byte[] bytes = Vectors.Hex(v[^1]);
            string line = string.Join(' ', v);
            switch (v[0])
            {
                case "zigzag":
                {
                    long value = long.Parse(v[1]);
                    Assert.Equal(bytes, EncodeI64(value));
                    var d = Varint.GetI64(bytes, 0, bytes.Length);
                    Assert.Equal(value, d.Value);
                    Assert.Equal(bytes.Length, d.Length);
                    break;
                }
                case "unsigned":
                {
                    ulong value = ulong.Parse(v[1]);
                    Assert.Equal(bytes, EncodeU64(value));
                    var d = Varint.GetU64(bytes, 0, bytes.Length);
                    Assert.Equal(value, d.Value);
                    break;
                }
                case "lenient":
                {
                    var d = Varint.GetU64(bytes, 0, bytes.Length);
                    Assert.Equal(ulong.Parse(v[1]), d.Value);
                    Assert.Equal(bytes.Length, d.Length);
                    break;
                }
                case "bad":
                {
                    RecordFormatErrorKind expected = v[1] == "truncated"
                        ? RecordFormatErrorKind.Truncated
                        : RecordFormatErrorKind.VarintOverflow;
                    Assert.Equal(expected, Failure(bytes));
                    break;
                }
                default:
                    throw new InvalidOperationException("unknown vector kind " + line);
            }
        }
    }

    [Fact]
    public void TheTenthByteMayOnlyBeOne()
    {
        byte[] ok = { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 };
        Assert.Equal(ulong.MaxValue, Varint.GetU64(ok, 0, ok.Length).Value);
        byte[] tooBig = { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02 };
        Assert.Equal(RecordFormatErrorKind.VarintOverflow, Failure(tooBig));
    }

    [Fact]
    public void AnElevenByteVarintIsAnOverflowNotATruncation()
    {
        byte[] b = new byte[11];
        Array.Fill(b, (byte)0xff);
        Assert.Equal(RecordFormatErrorKind.VarintOverflow, Failure(b));
    }

    [Fact]
    public void AShortOpenVarintIsTruncated()
    {
        Assert.Equal(RecordFormatErrorKind.Truncated, Failure(new byte[] { 0x80, 0x80 }));
        Assert.Equal(RecordFormatErrorKind.Truncated, Failure(Array.Empty<byte>()));
    }

    [Fact]
    public void AnI32MustFit()
    {
        byte[] big = EncodeI64(1L << 31);
        Assert.Equal(
            RecordFormatErrorKind.VarintOverflow,
            Assert.Throws<RecordFormatException>(() => Varint.GetI32(big, 0, big.Length)).Kind);
        byte[] min = EncodeI64(int.MinValue);
        Assert.Equal(int.MinValue, Varint.GetI32(min, 0, min.Length).Value);
    }

    [Fact]
    public void ZigzagIsItsOwnInverse()
    {
        foreach (long v in new[] { 0L, -1, 1, long.MaxValue, long.MinValue, 123456789L, -987654321L })
        {
            Assert.Equal(v, Varint.Unzigzag(Varint.Zigzag(v)));
        }
    }
}

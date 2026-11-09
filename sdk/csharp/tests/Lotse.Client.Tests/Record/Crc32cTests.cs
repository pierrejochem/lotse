using System;
using System.Text;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Record;

public class Crc32cTests
{
    [Fact]
    public void VectorsFromTheBroker()
    {
        foreach (string[] v in Vectors.Lines("crc32c.txt"))
        {
            byte[] input = Vectors.Hex(v[1]);
            Assert.Equal(Convert.ToUInt32(v[2], 16), Crc32c.Compute(input, 0, input.Length));
        }
    }

    [Fact]
    public void TheStandardCheckValue()
    {
        byte[] input = Encoding.ASCII.GetBytes("123456789");
        Assert.Equal(0xE3069283u, Crc32c.Compute(input, 0, input.Length));
        Assert.Equal(0xE3069283u, Crc32c.Compute(input));
    }

    [Fact]
    public void ARangeIsChecksummedWithoutItsNeighbours()
    {
        byte[] input = Encoding.ASCII.GetBytes("xx123456789yy");
        Assert.Equal(0xE3069283u, Crc32c.Compute(input, 2, 11));
    }
}

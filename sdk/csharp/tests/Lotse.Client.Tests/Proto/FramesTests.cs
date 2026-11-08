using System;
using System.Collections.Generic;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Proto;

public class FramesTests
{
    [Fact]
    public void TheLengthPrefixIsBigEndianAndTheRestLittleEndian()
    {
        byte[] frame = Frames.EncodeRequest(0x01020304u, new Request.ApiVersions());
        // api key 0, version 0, correlation 04 03 02 01 (little-endian), flags 0, empty tags, empty body tags
        Assert.Equal(new byte[] { 0, 0, 0, 0, 4, 3, 2, 1, 0, 0, 0, 0 }, frame);
        byte[] wire = Frames.WithLengthPrefix(frame);
        Assert.Equal(new byte[] { 0, 0, 0, 12 }, Vectors.Cut(wire, 4));
        Assert.Equal(12, Frames.ReadLength(wire));
        Assert.Equal(0x010203, Frames.ReadLength(new byte[] { 0, 1, 2, 3 })); // big-endian: the first byte is the most significant
        Assert.Equal(256, Frames.ReadLength(new byte[] { 0, 0, 1, 0 }));
    }

    [Fact]
    public void ALengthOverTheLimitEndsTheConnection()
    {
        Assert.Equal(Frames.MaxFrame, Frames.ReadLength(new byte[] { 1, 0, 0, 0 }));
        Assert.Throws<DecodeException>(() => Frames.ReadLength(new byte[] { 1, 0, 0, 1 }));
        Assert.Throws<DecodeException>(() => Frames.ReadLength(new byte[] { 0xff, 0xff, 0xff, 0xff }));
        Assert.Throws<ArgumentException>(() => Frames.WithLengthPrefix(new byte[Frames.MaxFrame + 1]));
        Assert.Equal(Frames.MaxFrame + 4, Frames.WithLengthPrefix(new byte[Frames.MaxFrame]).Length);
    }

    [Fact]
    public void AnUnknownKeyOrVersionIsReportedWithItsHeader()
    {
        byte[] frame = Frames.EncodeRequest(77, new Request.ApiVersions());
        frame[2] = 9; // version 9
        var e = Assert.Throws<DecodeException>(() => Frames.DecodeRequest(frame));
        Assert.Equal(DecodeErrorKind.Unsupported, e.Kind);
        Assert.Equal(77u, e.Header!.CorrelationId);
        Assert.Equal(9, e.Header.ApiVersion);
        byte[] other = Frames.EncodeRequest(5, new Request.ApiVersions());
        other[0] = 18; // a key the design leaves free
        Assert.Equal(DecodeErrorKind.Unsupported, Assert.Throws<DecodeException>(() => Frames.DecodeRequest(other)).Kind);
    }

    [Fact]
    public void TheNodeInternalKeysAreOnlyConstants()
    {
        foreach (ApiKey key in new[]
                 {
                     ApiKey.SettleVoters, ApiKey.DeadLetter, ApiKey.VerifyTxn, ApiKey.CheckEmpty, ApiKey.WriteTxnMarkers,
                 })
        {
            Assert.False(key.IsImplemented(0));
            byte[] frame = Frames.EncodeRequest(1, new Request.ApiVersions());
            frame[0] = (byte)key.Id;
            frame[1] = (byte)(key.Id >> 8);
            Assert.Equal(DecodeErrorKind.Unsupported, Assert.Throws<DecodeException>(() => Frames.DecodeRequest(frame)).Kind);
        }
        Assert.Equal(43, ApiKey.SettleVoters.Id);
        Assert.Equal(44, ApiKey.DeadLetter.Id);
        Assert.Equal(45, ApiKey.VerifyTxn.Id);
        Assert.Equal(46, ApiKey.CheckEmpty.Id);
        Assert.Equal(1000, ApiKey.WriteTxnMarkers.Id);
    }

    [Fact]
    public void UnknownTagsAreSkipped()
    {
        var w = new Writer();
        w.U32(1).U16(0);
        w.Tags(new[] { new Tag(42, ByteString.Utf8("from the future")) });
        new Response.Metadata(new List<Messages.Broker>(), new List<Messages.ResourceMetadata>()).Write(w);
        Frames.DecodedResponse r = Frames.DecodeResponse(w.ToByteArray(), ApiKey.Metadata);
        Assert.Equal(1u, r.CorrelationId);
        var metadata = Assert.IsType<Response.Metadata>(r.Response);
        Assert.Empty(metadata.Brokers);
        Assert.Empty(metadata.Resources);
    }

    [Fact]
    public void APushIsToldFromAResponse()
    {
        byte[] response = Frames.EncodeResponse(5, new Response.Outcome(ErrorCode.None, null));
        Assert.False(Frames.IsPush(response));
        Assert.False(Frames.IsPush(new byte[] { 0, 0, 0, 0, 2 }));
        Assert.False(Frames.IsPush(new byte[] { 0, 0, 0, 0, 2, 0 })); // ERROR is not PUSH
        Assert.True(Frames.IsPush(new byte[] { 0, 0, 0, 0, 1, 0 }));
        Assert.True(Frames.IsPush(new byte[] { 0, 0, 0, 0, 3, 0 }));
        for (int i = 0; i < 4; i++)
        {
            byte[] frame = { 0, 0, 0, 0, 1, 0 };
            frame[i] = 1;
            Assert.False(Frames.IsPush(frame)); // a nonzero correlation id is a response
        }
        Assert.Throws<DecodeException>(() => Frames.DecodePush(response));
    }

    [Fact]
    public void APushNeedsItsFlagAndAZeroCorrelationId()
    {
        byte[] push = Frames.EncodePush(new Frames.DeliverPush(1, ErrorCode.None, ByteString.Empty, new List<Messages.DeliveredOffset>(), null));
        Assert.Equal(1u, Frames.DecodePush(push).SubscriptionId);
        Assert.True(Frames.DecodePush(push).IsActivation);
        foreach (byte flags in new byte[] { 0, 2 })
        {
            byte[] bad = (byte[])push.Clone();
            bad[4] = flags;
            Assert.Throws<DecodeException>(() => Frames.DecodePush(bad));
        }
        byte[] zero = (byte[])push.Clone();
        zero[0] = 1;
        Assert.Throws<DecodeException>(() => Frames.DecodePush(zero));
    }

    [Fact]
    public void CorrelationIdsUseTheWholeU32Range()
    {
        byte[] frame = Frames.EncodeRequest(0xFFFF_FFFFu, new Request.WhoAmI());
        Assert.Equal(0xFFFF_FFFFu, Frames.DecodeRequest(frame).Header.CorrelationId);
        Assert.Equal(
            0xFFFF_FFFFu,
            Frames.ResponseCorrelationId(Frames.EncodeResponse(0xFFFF_FFFFu, new Response.Failed(ErrorCode.Unauthorized))));
    }

    [Fact]
    public void AnErrorResponseIsOnlyACodeAndATagBlock()
    {
        byte[] frame = Frames.EncodeResponse(9, new Response.Failed(ErrorCode.UnsupportedVersion));
        // correlation 9, flags ERROR, no tags, code 14, no tags
        Assert.Equal(new byte[] { 9, 0, 0, 0, 2, 0, 0, 14, 0, 0 }, frame);
        Assert.Equal(new Response.Failed(ErrorCode.UnsupportedVersion), Frames.DecodeResponse(frame, ApiKey.Fetch).Response);
    }

    [Fact]
    public void U64ValuesKeepTheirBitsAndNoOffsetIsAllOnes()
    {
        var p = new Messages.OffsetFetchPartition(0, ErrorCode.None, Messages.NoOffset, 0, null);
        byte[] frame = Frames.EncodeResponse(
            1, new Response.OffsetFetch(new[] { new Messages.OffsetFetchResource(7, new[] { p }) }));
        var back = Assert.IsType<Response.OffsetFetch>(Frames.DecodeResponse(frame, ApiKey.OffsetFetch).Response);
        ulong offset = back.Resources[0].Partitions[0].Offset;
        Assert.Equal(ulong.MaxValue, offset);
        Assert.Equal(-1L, unchecked((long)offset));
        Assert.Equal(Messages.NoOffsetSigned, unchecked((long)Messages.NoOffset));
        Assert.Equal("18446744073709551615", offset.ToString());
        Assert.Null(back.Resources[0].Partitions[0].Metadata);
    }
}

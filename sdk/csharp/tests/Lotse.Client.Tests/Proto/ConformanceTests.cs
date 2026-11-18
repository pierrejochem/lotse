using System;
using System.Collections.Generic;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Proto;

/// <summary>Every frame the broker's codec wrote is decoded here and written again byte for byte.</summary>
public class ConformanceTests
{
    [Fact]
    public void EveryRequestRoundTrips()
    {
        List<string[]> vectors = Vectors.Lines("requests.hex");
        Assert.True(vectors.Count >= 40, "the vectors cover every client request");
        foreach (string[] v in vectors)
        {
            byte[] frame = Vectors.Hex(v[1]);
            Frames.DecodedRequest decoded = Frames.DecodeRequest(frame);
            uint correlationId = uint.Parse(v[0].Split('-')[1]) + 1;
            Assert.Equal(correlationId, decoded.Header.CorrelationId);
            Assert.Equal(0, decoded.Header.ApiVersion);
            Assert.Equal(0, decoded.Header.Flags);
            Assert.Equal(decoded.Request.ApiKey, decoded.Header.ApiKey);
            Assert.Equal(v[1], Vectors.Hex(Frames.EncodeRequest(correlationId, decoded.Request)));
        }
    }

    [Fact]
    public void EveryRequestKeyIsCovered()
    {
        var seen = new SortedSet<int>();
        foreach (string[] v in Vectors.Lines("requests.hex"))
        {
            seen.Add(Frames.DecodeRequest(Vectors.Hex(v[1])).Header.ApiKey.Id);
        }
        foreach (ApiVersionRange r in ApiKey.Implemented)
        {
            Assert.True(seen.Contains(r.ApiKey.Id), "no vector for API key " + r.ApiKey.Id);
        }
    }

    [Fact]
    public void EveryResponseRoundTrips()
    {
        List<string[]> vectors = Vectors.Lines("responses.hex");
        Assert.True(vectors.Count >= 40);
        foreach (string[] v in vectors)
        {
            byte[] frame = Vectors.Hex(v[2]);
            var key = new ApiKey(ushort.Parse(v[1]));
            Frames.DecodedResponse decoded = Frames.DecodeResponse(frame, key);
            uint correlationId = uint.Parse(v[0].Split('-')[1]);
            Assert.Equal(correlationId, decoded.CorrelationId);
            Assert.Equal(correlationId, Frames.ResponseCorrelationId(frame));
            Assert.Equal(v[2], Vectors.Hex(Frames.EncodeResponse(correlationId, decoded.Response)));
        }
    }

    [Fact]
    public void EveryPushRoundTrips()
    {
        foreach (string[] v in Vectors.Lines("pushes.hex"))
        {
            byte[] frame = Vectors.Hex(v[1]);
            Assert.True(Frames.IsPush(frame), v[0]);
            Frames.DeliverPush push = Frames.DecodePush(frame);
            Assert.Equal(v[1], Vectors.Hex(Frames.EncodePush(push)));
        }
    }

    [Fact]
    public void ThePushLayoutPutsTheSubscriptionIdFirstInTheBody()
    {
        // correlation 0 | flags PUSH | no tags | api key 11 | version 0 | subscription id | error | ...
        byte[] frame = Vectors.Hex(Vectors.Lines("pushes.hex")[0][1]);
        Assert.Equal(new byte[] { 0, 0, 0, 0, 1, 0, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0 }, Vectors.Cut(frame, 17));
        Frames.DeliverPush push = Frames.DecodePush(frame);
        Assert.Equal(3u, push.SubscriptionId);
        Assert.Equal(ErrorCode.None, push.Error);
        Assert.Equal(2, push.Deliveries.Count);
        Assert.Equal(42UL, push.Deliveries[1].Offset);
        Assert.Equal(3, push.Deliveries[1].DeliveryCount);
        Frames.DeliverPush ended = Frames.DecodePush(Vectors.Hex(Vectors.Lines("pushes.hex")[1][1]));
        Assert.Equal(ErrorCode.NotLeader, ended.Error);
        Assert.Equal(new LeaderHint(1, 7), ended.LeaderHint);
    }

    [Fact]
    public void ATruncatedFrameIsRefusedAtEveryCut()
    {
        foreach (string[] v in Vectors.Lines("requests.hex"))
        {
            byte[] frame = Vectors.Hex(v[1]);
            for (int cut = 0; cut < frame.Length; cut++)
            {
                byte[] part = Vectors.Cut(frame, cut);
                Assert.Throws<DecodeException>(() => Frames.DecodeRequest(part));
            }
        }
        foreach (string[] v in Vectors.Lines("responses.hex"))
        {
            byte[] frame = Vectors.Hex(v[2]);
            var key = new ApiKey(ushort.Parse(v[1]));
            for (int cut = 0; cut < frame.Length; cut++)
            {
                byte[] part = Vectors.Cut(frame, cut);
                Assert.Throws<DecodeException>(() => Frames.DecodeResponse(part, key));
            }
        }
        foreach (string[] v in Vectors.Lines("pushes.hex"))
        {
            byte[] frame = Vectors.Hex(v[1]);
            for (int cut = 0; cut < frame.Length; cut++)
            {
                byte[] part = Vectors.Cut(frame, cut);
                Assert.Throws<DecodeException>(() => Frames.DecodePush(part));
            }
        }
    }

    [Fact]
    public void AFrameWithTrailingBytesIsRefused()
    {
        foreach (string[] v in Vectors.Lines("requests.hex"))
        {
            byte[] longer = Vectors.Longer(Vectors.Hex(v[1]));
            var e = Assert.Throws<DecodeException>(() => Frames.DecodeRequest(longer));
            Assert.Equal(DecodeErrorKind.Malformed, e.Kind);
        }
        foreach (string[] v in Vectors.Lines("responses.hex"))
        {
            byte[] longer = Vectors.Longer(Vectors.Hex(v[2]));
            var key = new ApiKey(ushort.Parse(v[1]));
            var e = Assert.Throws<DecodeException>(() => Frames.DecodeResponse(longer, key));
            Assert.Equal(DecodeErrorKind.Malformed, e.Kind);
        }
        foreach (string[] v in Vectors.Lines("pushes.hex"))
        {
            byte[] longer = Vectors.Longer(Vectors.Hex(v[1]));
            Assert.Throws<DecodeException>(() => Frames.DecodePush(longer));
        }
    }

    [Fact]
    public void ErrorCodesHaveTheBrokersNamesAndRetryClasses()
    {
        foreach (string[] v in Vectors.Lines("error-codes.txt"))
        {
            var code = new ErrorCode(ushort.Parse(v[0]));
            Assert.Equal(v[1], code.CodeName);
            Assert.Equal(bool.Parse(v[2]), code.Retriable);
        }
    }

    [Fact]
    public void TheErrorFlagIsCheckedBeforeTheTypedBody()
    {
        // A response with the ERROR flag whose body would be nonsense as a Produce response.
        foreach (string[] v in Vectors.Lines("responses.hex"))
        {
            byte[] frame = Vectors.Hex(v[2]);
            if ((frame[4] & Flags.Error) == 0)
            {
                continue;
            }
            foreach (ApiKey key in new[] { ApiKey.Produce, ApiKey.Fetch, ApiKey.Metadata, ApiKey.DescribeMetrics })
            {
                Response r = Frames.DecodeResponse(frame, key).Response;
                Assert.True(r is Response.Failed or Response.Refused, v[0]);
            }
        }
        // And without the flag the same bytes are read as the typed body, which fails for the wrong key.
        byte[] failed = Vectors.Hex(Vectors.Lines("responses.hex").Find(v => v[0].EndsWith("Error"))![2]);
        failed[4] = 0;
        Assert.Throws<DecodeException>(() => Frames.DecodeResponse(failed, ApiKey.Produce));
    }

    [Fact]
    public void ARefusalCarriesItsReason()
    {
        bool found = false;
        foreach (string[] v in Vectors.Lines("responses.hex"))
        {
            Response r = Frames.DecodeResponse(Vectors.Hex(v[2]), new ApiKey(ushort.Parse(v[1]))).Response;
            if (r is Response.Refused refused)
            {
                found = true;
                Assert.Equal(ErrorCode.Unauthorized, refused.Code);
                Assert.Equal("no producer role for orders in shop", refused.Message);
            }
        }
        Assert.True(found);
    }

    /// <summary>Tags 0 and 1 of a partition in a metadata answer: its pinned leader and placement (ADR 0121).</summary>
    [Fact]
    public void PinsAndPlacementsAreReadFromTheirPartitions()
    {
        bool found = false;
        foreach (string[] v in Vectors.Lines("responses.hex"))
        {
            Response r = Frames.DecodeResponse(Vectors.Hex(v[2]), new ApiKey(ushort.Parse(v[1]))).Response;
            if (r is Response.Metadata m)
            {
                foreach (Messages.ResourceMetadata res in m.Resources)
                {
                    foreach (Messages.PartitionMetadata p in res.Partitions)
                    {
                        // Only partition 0 of "orders" has a pin (node 2) and a placement.
                        bool set = res.Name == "orders" && p.Partition == 0;
                        found |= set;
                        Assert.Equal(set ? 2u : 0u, p.PinnedLeader);
                        Assert.Equal(set, p.Placed);
                    }
                }
            }
        }
        Assert.True(found);
    }

    /// <summary>Tag 3 of a metrics answer: ready messages per priority level and the subscriptions' states (ADR 0120).</summary>
    [Fact]
    public void QueueDetailsAreReadIntoTheirQueues()
    {
        bool found = false;
        foreach (string[] v in Vectors.Lines("responses.hex"))
        {
            Response r = Frames.DecodeResponse(Vectors.Hex(v[2]), new ApiKey(ushort.Parse(v[1]))).Response;
            if (r is Response.DescribeMetrics m && m.Queues.Count > 0)
            {
                found = true;
                Messages.QueueMetrics q = m.Queues[0];
                Assert.Equal(new ulong[] { 2, 1, 1 }, q.ReadyByLevel);
                Assert.Equal(1UL, q.ReadyLevelUnread);
                Assert.Equal(Messages.ConsumerState.Active, q.Consumers[0].State);
                Assert.Equal(Messages.ConsumerState.Standby, q.Consumers[1].State);
                Assert.Equal("10.0.0.9:40100", q.Consumers[1].Peer);
                // Tags 4 to 6 (ADR 0122).
                Messages.PartitionMetrics p = m.Partitions[0];
                Assert.Equal(1_763_000_000_000L, p.OldestTimestampMs);
                Assert.Equal(1U, p.OpenTransactions);
                Assert.Equal(2048UL, q.Consumers[0].InFlightBytes);
                Assert.Equal(0UL, q.Consumers[1].InFlightBytes);
                Assert.Equal(new Messages.DiskMetrics(1UL << 30, true), m.Disk);
            }
        }
        Assert.True(found);
    }

    [Fact]
    public void LeaderHintsAreDecodedWhereTheyAreTagBlocks()
    {
        bool produce = false;
        bool describe = false;
        foreach (string[] v in Vectors.Lines("responses.hex"))
        {
            Response r = Frames.DecodeResponse(Vectors.Hex(v[2]), new ApiKey(ushort.Parse(v[1]))).Response;
            if (r is Response.Produce p)
            {
                foreach (var res in p.Resources)
                {
                    foreach (var part in res.Partitions)
                    {
                        if (part.Error == ErrorCode.NotLeader)
                        {
                            Assert.Equal(new LeaderHint(2, 9), part.LeaderHint);
                            produce = true;
                        }
                        else
                        {
                            Assert.Null(part.LeaderHint);
                        }
                    }
                }
            }
            if (r is Response.DescribeRoleBindings { LeaderHint: not null } d)
            {
                Assert.Equal(new LeaderHint(3, 9), d.LeaderHint);
                describe = true;
            }
        }
        Assert.True(produce && describe);
        Assert.False(Frames.IsPush(Vectors.Hex(Vectors.Lines("responses.hex")[0][2])));
    }
}

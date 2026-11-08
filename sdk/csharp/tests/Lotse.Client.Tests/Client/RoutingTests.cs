using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>Routes: how a client learns where a partition's leader is, and when it stops believing it.</summary>
public sealed class RoutingTests : IDisposable
{
    private const ulong StreamId = 7;

    private readonly FakeNode _a = new(1);
    private readonly FakeNode _b = new(2);
    private LotseClient? _client;

    public void Dispose()
    {
        _client?.Dispose();
        _a.Dispose();
        _b.Dispose();
    }

    private LotseClient Client(bool idempotent = false)
    {
        _client = new LotseClient(Fx.Config(_a).WithIdempotence(idempotent).WithRetryFor(TimeSpan.FromMilliseconds(500)).Build());
        return _client;
    }

    [Fact]
    public async Task ARouteIsUsedUntilARequestFailsNotUntilItIsOld()
    {
        _a.On((n, r) => r is Request.Metadata
            ? Fx.Metadata(new[] { n }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1)))
            : Fx.Produced(StreamId, 0, 5));
        LotseClient lotse = Client();
        for (int i = 0; i < 3; i++)
        {
            await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        }
        Assert.Equal(1, _a.Count<Request.Metadata>());
        Assert.Equal(new LotseClient.Route(StreamId, 1, 1), lotse.RouteOf("s", 0));
    }

    [Fact]
    public async Task AnUnknownStreamOrPartitionIsReportedAtOnce()
    {
        _a.On((n, r) => r is Request.Metadata m
            ? Fx.Metadata(new[] { n }, m.Names.Any(x => x == "s") ? Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1)) : Fx.Unknown(m.Names[0]))
            : throw new InvalidOperationException("nothing may be produced"));
        LotseClient lotse = Client();
        var e = await Assert.ThrowsAsync<UnknownStreamException>(() => lotse.ProduceAsync("nope", 0, Fx.Msgs(1)));
        Assert.Equal("nope", e.Name);
        var p = await Assert.ThrowsAsync<UnknownStreamException>(() => lotse.ProduceAsync("s", 3, Fx.Msgs(1)));
        Assert.Equal(3, p.Partition);
        Assert.Equal(0, _a.Count<Request.Produce>());
    }

    [Fact]
    public async Task AStreamOnlySomeBrokersKnowIsFoundByAskingThemAll()
    {
        // Node 1 has not heard of the stream yet; node 2 has.
        FakeNode[] both = { _a, _b };
        _a.On((n, r) => r is Request.Metadata m
            ? Fx.Metadata(both, Fx.Unknown("s"))
            : Fx.Produced(StreamId, 0, 1));
        _b.On((n, r) => r is Request.Metadata
            ? Fx.Metadata(both, Fx.Stream("s", StreamId, Fx.Partition(0, 2, 4, 2)))
            : Fx.Produced(StreamId, 0, 99));
        // The first metadata call learns both brokers from node 1, which then answers the second one.
        LotseClient lotse = Client();
        await lotse.MetadataAsync();
        Assert.Equal(99UL, (await lotse.ProduceAsync("s", 0, Fx.Msgs(1))).BaseOffset);
        Assert.Equal(1, _b.Count<Request.Produce>());
    }

    [Fact]
    public async Task OfTwoResourcesWithOneNameTheNewestIdIsUsed()
    {
        // A name deleted and created again has a higher id; a node that lags still reports the old one.
        using var c = new FakeNode(3);
        FakeNode[] all = { _a, _b, c };
        _a.On((n, r) => Fx.Metadata(all, Fx.Unknown("s")));
        _b.On((n, r) => r is Request.Metadata ? Fx.Metadata(all, Fx.Stream("s", 5, Fx.Partition(0, 1, 1, 1))) : null);
        c.On((n, r) => r is Request.Metadata
            ? Fx.Metadata(all, Fx.Stream("s", 9, Fx.Partition(0, 3, 1, 3)))
            : Fx.Produced(9, 0, 1));
        LotseClient lotse = Client();
        await lotse.MetadataAsync(); // learn the brokers from node 1
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        Assert.Equal(9UL, c.Requests<Request.Produce>()[0].Resources[0].ResourceId);
    }

    [Fact]
    public void TheMergeKeepsTheNewestResourcePerNameAndIgnoresErrors()
    {
        var merged = new List<Messages.ResourceMetadata> { Fx.Stream("s", 5) };
        LotseClient.MergeNewest(merged, new[] { Fx.Stream("s", 9), Fx.Unknown("t"), Fx.Stream("u", 1) });
        Assert.Equal(new ulong[] { 9, 1 }, merged.Select(r => r.ResourceId).ToArray());
        LotseClient.MergeNewest(merged, new[] { Fx.Stream("s", 3) });
        Assert.Equal(9UL, merged[0].ResourceId);
        Assert.Equal(2, merged.Count);
        // Ids compare as unsigned 64-bit numbers.
        LotseClient.MergeNewest(merged, new[] { Fx.Stream("u", ulong.MaxValue) });
        Assert.Equal(ulong.MaxValue, merged[1].ResourceId);
    }

    [Fact]
    public async Task AnOlderHintIsNotFollowedAndTheRouteIsLearntAgain()
    {
        FakeNode[] both = { _a, _b };
        int produces = 0;
        _a.On((n, r) => r switch
        {
            Request.Metadata => Fx.Metadata(both, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 5, 1, 2))),
            // Node 1 is not the leader any more; a node that lags points back at an older leader and term.
            Request.Produce => Interlocked.Increment(ref produces) == 1
                ? Fx.Produced(StreamId, 0, ErrorCode.NotLeader, 0, new LeaderHint(2, 3))
                : Fx.Produced(StreamId, 0, 42),
            _ => null,
        });
        _b.On((n, r) => throw new InvalidOperationException("an older hint must not send the client to node 2"));
        LotseClient lotse = Client();
        Assert.Equal(42UL, (await lotse.ProduceAsync("s", 0, Fx.Msgs(1))).BaseOffset);
        Assert.Equal(2, _a.Count<Request.Metadata>()); // dropped, asked again
        Assert.Empty(_b.Requests());
    }

    [Fact]
    public async Task AHintNamingTheSameLeaderDropsTheRoute()
    {
        FakeNode[] both = { _a, _b };
        int produces = 0;
        _a.On((n, r) => r switch
        {
            Request.Metadata => Fx.Metadata(both, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 5, 1))),
            Request.Produce => Interlocked.Increment(ref produces) == 1
                ? Fx.Produced(StreamId, 0, ErrorCode.FencedLeaderTerm, 0, new LeaderHint(1, 9))
                : Fx.Produced(StreamId, 0, 1),
            _ => null,
        });
        LotseClient lotse = Client();
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        Assert.Equal(2, _a.Count<Request.Metadata>());
    }

    [Fact]
    public async Task ANewerHintMovesTheRouteWithoutAskingMetadataAgain()
    {
        FakeNode[] both = { _a, _b };
        _a.On((n, r) => r is Request.Metadata
            ? Fx.Metadata(both, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 5, 1, 2)))
            : Fx.Produced(StreamId, 0, ErrorCode.NotLeader, 0, new LeaderHint(2, 6)));
        _b.On((n, r) => Fx.Produced(StreamId, 0, 17));
        LotseClient lotse = Client();
        Assert.Equal(17UL, (await lotse.ProduceAsync("s", 0, Fx.Msgs(1))).BaseOffset);
        Assert.Equal(1, _a.Count<Request.Metadata>());
        Assert.Equal(new LotseClient.Route(StreamId, 2, 6), lotse.RouteOf("s", 0));
    }

    [Fact]
    public async Task APartitionWithoutAKnownLeaderIsAskedOfItsVoters()
    {
        // Created at runtime: node 1 does not host it, node 2 does and knows its leader.
        FakeNode[] both = { _a, _b };
        _a.On((n, r) => Fx.Metadata(both, Fx.Stream("s", StreamId, Fx.Partition(0, 0, 0, 2))));
        _b.On((n, r) => r is Request.Metadata
            ? Fx.Metadata(both, Fx.Stream("s", StreamId, Fx.Partition(0, 2, 3, 2)))
            : Fx.Produced(StreamId, 0, 8));
        LotseClient lotse = Client();
        Assert.Equal(8UL, (await lotse.ProduceAsync("s", 0, Fx.Msgs(1))).BaseOffset);
        Assert.Equal(new LotseClient.Route(StreamId, 2, 3), lotse.RouteOf("s", 0));
    }

    [Fact]
    public async Task ALeaderWithoutAnAddressIsRetriedAfterAskingAgain()
    {
        int asked = 0;
        _a.On((n, r) =>
        {
            if (r is Request.Metadata)
            {
                // First the leader (node 9) is listed in no broker entry; then it is node 1.
                bool first = Interlocked.Increment(ref asked) == 1;
                return Fx.Metadata(new[] { n }, Fx.Stream("s", StreamId, Fx.Partition(0, first ? 9u : 1u, 1, 1)));
            }
            return Fx.Produced(StreamId, 0, 3);
        });
        Assert.Equal(3UL, (await Client().ProduceAsync("s", 0, Fx.Msgs(1))).BaseOffset);
        Assert.Equal(2, _a.Count<Request.Metadata>());
    }

    [Fact]
    public async Task ADeletedStreamDropsTheRouteSoTheNameCanComeBackAsANewResource()
    {
        int produces = 0;
        _a.On((n, r) => r switch
        {
            Request.Metadata => Fx.Metadata(new[] { n }, Fx.Stream("s", Volatile.Read(ref produces) == 0 ? 7ul : 8ul, Fx.Partition(0, 1, 1, 1))),
            Request.Produce p => Interlocked.Increment(ref produces) == 1
                ? Fx.Produced(7, 0, ErrorCode.UnknownResource, 0)
                : Fx.Produced(p.Resources[0].ResourceId, 0, 1),
            _ => null,
        });
        LotseClient lotse = Client();
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
        Assert.Equal(ErrorCode.UnknownResource, e.Code);
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        Assert.Equal(8UL, _a.Requests<Request.Produce>()[1].Resources[0].ResourceId);
    }

    [Fact]
    public async Task TheTenantGoesWithTheRequestsThatNameOne()
    {
        _a.On((n, r) => r switch
        {
            Request.Metadata => Fx.Metadata(new[] { n }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1))),
            Request.FindCoordinator => Fx.Coordinator(n),
            Request.InitProducer => Fx.Producer(5, 1),
            _ => Fx.Produced(StreamId, 0, 1),
        });
        _client = new LotseClient(Fx.Config(_a).WithTenant("acme").Build());
        await _client.ProduceAsync("s", 0, Fx.Msgs(1));
        Assert.Equal("acme", _a.Requests<Request.InitProducer>()[0].Tenant);
        Assert.Equal("acme", _a.Requests<Request.Produce>()[0].Tenant);
    }
}

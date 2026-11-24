using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>Fetch requests, retries, and what a read_committed reader sees.</summary>
public sealed class FetchTests : IDisposable
{
    private const ulong StreamId = 7;

    private readonly FakeNode _a = new(1);
    private readonly FakeNode _b = new(2);
    private readonly FakeNode _c = new(3);
    private LotseClient? _client;

    public void Dispose()
    {
        _client?.Dispose();
        _a.Dispose();
        _b.Dispose();
        _c.Dispose();
    }

    // ---- batches as the broker stores them ----

    /// <summary>A batch of data records at <paramref name="baseOffset"/>; producer 0 is not idempotent.</summary>
    internal static ByteString Data(ulong baseOffset, ulong producer, bool transactional, params string[] values)
    {
        var builder = new BatchBuilder();
        foreach (string v in values)
        {
            builder.Push(1_000, (byte[]?)null, Encoding.UTF8.GetBytes(v));
        }
        byte[] bytes = builder.Finish().ToByteArray();
        if (producer != 0)
        {
            Batch.SetProducer(bytes, producer, 1, 0);
        }
        if (transactional)
        {
            Batch.SetTransactional(bytes, true);
        }
        Batch.Stamp(bytes, baseOffset, 1);
        return ByteString.View(bytes, 0, bytes.Length);
    }

    internal static ByteString Marker(ulong baseOffset, ulong producer, ControlType type)
    {
        byte[] bytes = ControlBatches.Marker(producer, 1, type, 1, 1_000).ToByteArray();
        Batch.Stamp(bytes, baseOffset, 1);
        return ByteString.View(bytes, 0, bytes.Length);
    }

    internal static ByteString Concat(params ByteString[] parts)
    {
        byte[] all = new byte[parts.Sum(p => p.Size)];
        int at = 0;
        foreach (ByteString p in parts)
        {
            p.CopyTo(all, at);
            at += p.Size;
        }
        return ByteString.View(all, 0, all.Length);
    }

    private static Fetched Fetched(ByteString records, params Messages.AbortedTxn[] aborted) => new(records, 100, 100, 0, aborted);

    private static List<string> Values(Fetched f) => f.CommittedMessages().Select(r => r.Value!.ToUtf8()).ToList();

    // ---- committed batches ----

    [Fact]
    public void AReadCommittedReaderSkipsAbortedTransactionsAndMarkers()
    {
        ByteString records = Concat(
            Data(0, 0, false, "plain"),
            Data(1, 7, true, "aborted-1"),
            Data(2, 8, true, "committed"),
            Data(3, 7, true, "aborted-2"),
            Marker(4, 8, ControlType.Commit),
            Marker(5, 7, ControlType.Abort),
            Data(6, 7, true, "later transaction of the same producer"),
            Marker(7, 7, ControlType.Commit));
        Fetched f = Fetched(records, new Messages.AbortedTxn(7, 1));
        Assert.Equal(new[] { "plain", "committed", "later transaction of the same producer" }, Values(f));
    }

    [Fact]
    public void TheAbortedListNeedNotArriveInOrder()
    {
        // The broker lists aborted transactions by producer; the reader must take them by first offset.
        ByteString records = Concat(
            Data(1, 7, true, "aborted-7"),
            Marker(2, 7, ControlType.Abort),
            Data(10, 9, true, "aborted-9"),
            Marker(11, 9, ControlType.Abort),
            Data(12, 9, true, "kept"));
        Fetched f = Fetched(records, new Messages.AbortedTxn(9, 10), new Messages.AbortedTxn(7, 1));
        Assert.Equal(new[] { "kept" }, Values(f));
    }

    [Fact]
    public void AnAbortedProducerIsSkippedOnlyFromItsFirstOffset()
    {
        // The same producer wrote a committed transaction before the aborted one began.
        ByteString records = Concat(
            Data(0, 7, true, "earlier, committed"),
            Marker(1, 7, ControlType.Commit),
            Data(2, 7, true, "aborted"),
            Marker(3, 7, ControlType.Abort));
        Assert.Equal(new[] { "earlier, committed" }, Values(Fetched(records, new Messages.AbortedTxn(7, 2))));
    }

    [Fact]
    public void OnlyTransactionalBatchesOfAnAbortedProducerAreSkipped()
    {
        ByteString records = Concat(
            Data(1, 7, true, "aborted"),
            Data(2, 7, false, "not transactional"),
            Marker(3, 7, ControlType.Abort));
        Assert.Equal(new[] { "not transactional" }, Values(Fetched(records, new Messages.AbortedTxn(7, 1))));
    }

    [Fact]
    public void AnAbortOfOneProducerDoesNotEndTheSkippingOfAnother()
    {
        ByteString records = Concat(
            Data(1, 7, true, "a7"),
            Data(2, 8, true, "a8"),
            Marker(3, 7, ControlType.Abort),
            Data(4, 8, true, "a8 again"),
            Marker(5, 8, ControlType.Abort));
        Assert.Empty(Values(Fetched(records, new Messages.AbortedTxn(7, 1), new Messages.AbortedTxn(8, 2))));
    }

    [Fact]
    public void ACommitMarkerDoesNotEndTheSkipping()
    {
        // Cannot happen for one producer in one transaction, but the rule is "up to its ABORT marker".
        ByteString records = Concat(
            Data(1, 7, true, "a"),
            Marker(2, 7, ControlType.Commit),
            Data(3, 7, true, "b"),
            Marker(4, 7, ControlType.Abort));
        Assert.Empty(Values(Fetched(records, new Messages.AbortedTxn(7, 1))));
    }

    [Fact]
    public void WithoutAbortedTransactionsEveryDataBatchIsSeen()
    {
        ByteString records = Concat(Data(0, 8, true, "x", "y"), Marker(2, 8, ControlType.Commit), Data(3, 0, false, "z"));
        Assert.Equal(new[] { "x", "y", "z" }, Values(Fetched(records)));
        Assert.Equal(2, Fetched(records).CommittedBatches().Count); // the marker is not a batch of data
    }

    [Fact]
    public void ABatchOfARaftControlKindIsSkipped()
    {
        // A control batch that is not a transaction marker (Raft's own entry) is skipped, not an error.
        var builder = new BatchBuilder(new BatchOptions(new Attributes(Compression.None, false, true, BatchKind.Data), 0, 0, -1));
        builder.Push(1, new byte[] { 0, 0, 2, 0 }, new byte[] { 1, 2, 3 });
        byte[] raft = builder.Finish().ToByteArray();
        Batch.Stamp(raft, 0, 1);
        ByteString records = Concat(ByteString.View(raft, 0, raft.Length), Data(1, 0, false, "v"));
        Assert.Equal(new[] { "v" }, Values(Fetched(records)));
    }

    [Fact]
    public void RecordsThatDoNotParseAreACorruptBatch()
    {
        byte[] bytes = Data(0, 0, false, "x").ToByteArray();
        bytes[^1] ^= 1;
        var e = Assert.Throws<BrokerErrorException>(() => Fetched(ByteString.View(bytes, 0, bytes.Length)).CommittedBatches());
        Assert.Equal(ErrorCode.CorruptBatch, e.Code);
    }

    [Fact]
    public void TheNextOffsetFollowsTheLastBatch()
    {
        ByteString records = Concat(Data(0, 0, false, "a", "b", "c"), Data(3, 0, false, "d"));
        Assert.Equal(4UL, Fetched(records).NextOffset(0));
        Assert.Equal(9UL, Fetched(ByteString.Empty).NextOffset(9)); // nothing read: where it was
    }

    [Fact]
    public void TheFetchedRecordsAreAvailableWithoutFiltering()
    {
        ByteString records = Concat(Data(0, 7, true, "t"), Marker(1, 7, ControlType.Abort));
        Fetched f = Fetched(records, new Messages.AbortedTxn(7, 0));
        Assert.Equal(records, f.Records);
        Assert.Empty(f.CommittedBatches());
        Assert.Equal(2, Batch.ParseAll(f.Records).Count);
    }

    // ---- the request and the retries ----

    [Fact]
    public async Task TheFetchRequestAsksForCommittedDataFromTheRoutesLeaderTerm()
    {
        _a.On((node, request) => request is Request.Metadata
            ? Fx.Metadata(new[] { node }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 5, 1)))
            : Fx.Fetched(StreamId, 0, ErrorCode.None, Data(10, 0, false, "v"), 12, 11, 3, new[] { new Messages.AbortedTxn(4, 5) }));
        _client = new LotseClient(Fx.Config(_a).Build());
        Fetched f = await _client.FetchAsync("s", 0, 10, TimeSpan.FromMilliseconds(250));

        Request.Fetch sent = _a.Requests<Request.Fetch>()[0];
        Assert.Equal(250u, sent.MaxWaitMs);
        Assert.Equal(1u, sent.MinBytes);
        Assert.Equal(4u << 20, sent.MaxBytes);
        Assert.Equal(Messages.Isolation.Committed, sent.Isolation);
        Assert.Equal(StreamId, sent.Resources[0].ResourceId);
        Messages.FetchPartition p = sent.Resources[0].Partitions[0];
        Assert.Equal(0u, p.Partition);
        Assert.Equal(10UL, p.FetchOffset);
        Assert.Equal(5UL, p.LeaderTerm); // the term of the route
        Assert.Equal(1u << 20, p.MaxBytes);

        Assert.Equal(12UL, f.HighWatermark);
        Assert.Equal(11UL, f.LastStableOffset);
        Assert.Equal(3UL, f.LogStartOffset);
        Assert.Equal(new[] { new Messages.AbortedTxn(4, 5) }, f.Aborted);
        Assert.Equal(new[] { "v" }, Values(f));
    }

    [Fact]
    public async Task FetchUncommittedAsksForTheHighWatermark()
    {
        _a.On((node, request) => request is Request.Metadata
            ? Fx.Metadata(new[] { node }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 5, 1)))
            : Fx.Fetched(StreamId, 0, ErrorCode.None, ByteString.Empty, 0, 0, 0));
        _client = new LotseClient(Fx.Config(_a).Build());
        await _client.FetchUncommittedAsync("s", 0, 0, TimeSpan.FromMilliseconds(1));
        Assert.Equal(Messages.Isolation.Uncommitted, _a.Requests<Request.Fetch>()[0].Isolation);
    }

    [Fact]
    public async Task AnOffsetOutOfRangeSaysWhereThePartitionStarts()
    {
        _a.On((node, request) => request is Request.Metadata
            ? Fx.Metadata(new[] { node }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 5, 1)))
            : Fx.Fetched(StreamId, 0, ErrorCode.OffsetOutOfRange, ByteString.Empty, 50, 50, 40));
        _client = new LotseClient(Fx.Config(_a).Build());
        var e = await Assert.ThrowsAsync<OffsetOutOfRangeException>(() => _client.FetchAsync("s", 0, 3, TimeSpan.FromMilliseconds(1)));
        Assert.Equal(40UL, e.LogStart);
        Assert.Equal(3UL, e.Offset);
        Assert.Equal("s", e.Stream);
        Assert.Equal(1, _a.Count<Request.Fetch>()); // not retried
    }

    [Fact]
    public async Task ARetriableFetchErrorFollowsTheLeaderHint()
    {
        FakeNode[] both = { _a, _b };
        _a.On((node, request) => request is Request.Metadata
            ? Fx.Metadata(both, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 5, 1, 2)))
            : Fx.Fetched(StreamId, 0, ErrorCode.NotLeader, ByteString.Empty, 0, 0, 0, null, new LeaderHint(2, 6)));
        _b.On((node, request) => Fx.Fetched(StreamId, 0, ErrorCode.None, Data(0, 0, false, "from b"), 1, 1, 0));
        _client = new LotseClient(Fx.Config(_a).Build());
        Assert.Equal(new[] { "from b" }, Values(await _client.FetchAsync("s", 0, 0, TimeSpan.FromMilliseconds(1))));
        Assert.Equal(1, _a.Count<Request.Metadata>()); // the hint was enough
        Assert.Equal(6UL, _b.Requests<Request.Fetch>()[0].Resources[0].Partitions[0].LeaderTerm);
    }

    /// <summary>Node 1 leads in zone a, node 2 follows in zone b; node 2 answers with <paramref name="followerCode"/>.</summary>
    private void Zones(ErrorCode followerCode)
    {
        FakeNode[] both = { _a, _b };
        string[] zones = { "a", "b" };
        var meta = new Response.Metadata(
            both.Select(n => new Messages.Broker(n.Id, "127.0.0.1", (ushort)n.Address.Port, zones[n.Id - 1])).ToList(),
            new[] { Fx.Stream("s", StreamId, Fx.Partition(0, 1, 5, 1, 2)) });
        Func<FakeNode, Request, Response?> handler = (node, request) =>
        {
            if (request is Request.Metadata)
            {
                return meta;
            }
            ErrorCode code = node.Id == 2 ? followerCode : ErrorCode.None;
            if (request is Request.ListOffsets)
            {
                return new Response.ListOffsets(new[]
                {
                    new Messages.ListOffsetsResourceResponse(
                        StreamId, new[] { new Messages.ListOffsetsPartitionResponse(0, code, (ulong)node.Id * 10, -1, null) }),
                });
            }
            ByteString records = code.IsOk ? Data(0, 0, false, "from " + node.Id) : ByteString.Empty;
            return Fx.Fetched(StreamId, 0, code, records, 1, 1, 0);
        };
        _a.On(handler);
        _b.On(handler);
    }

    [Fact]
    public async Task AClientInAZoneListsOffsetsAtTheReplicaThereAndLeavesOneThatLags()
    {
        Zones(ErrorCode.None);
        _client = new LotseClient(Fx.Config(_a).WithZone("b").Build());
        Assert.Equal(20UL, await _client.ListOffsetAsync("s", 0, OffsetAt.Latest)); // the follower in zone b
        Assert.Equal(0, _a.Count<Request.ListOffsets>());
        Assert.True(_b.Requests<Request.ListOffsets>()[0].AllowFollower);
        await _client.DisposeAsync();
        // A follower that lags too far answers NOT_LEADER (ADR 0116): asked once, then left for the leader.
        Zones(ErrorCode.NotLeader);
        int before = _b.Count<Request.ListOffsets>();
        _client = new LotseClient(Fx.Config(_a).WithZone("b").Build());
        Assert.Equal(10UL, await _client.ListOffsetAsync("s", 0, OffsetAt.Latest));
        Assert.Equal(10UL, await _client.ListOffsetAsync("s", 0, OffsetAt.Latest));
        Assert.Equal(before + 1, _b.Count<Request.ListOffsets>());
        Assert.All(_a.Requests<Request.ListOffsets>(), r => Assert.False(r.AllowFollower));
        await _client.DisposeAsync();
        _client = null;
    }

    [Fact]
    public async Task AClientInAZoneFetchesFromTheReplicaThere()
    {
        Zones(ErrorCode.None);
        _client = new LotseClient(Fx.Config(_a).WithZone("b").Build());
        Assert.Equal(new[] { "from 2" }, Values(await _client.FetchAsync("s", 0, 0, TimeSpan.FromMilliseconds(1))));
        Assert.Equal(new[] { "from 2" }, Values(await _client.FetchAsync("s", 0, 0, TimeSpan.FromMilliseconds(1))));
        Assert.Equal(0, _a.Count<Request.Fetch>()); // the leader is in another zone
        Assert.True(_b.Requests<Request.Fetch>()[0].AllowFollower);
        _client.Dispose();
        // In the leader's zone, or in one without a replica, the leader serves, as without a zone.
        foreach (string? zone in new[] { "a", "c", null })
        {
            _client = new LotseClient(Fx.Config(_a).WithZone(zone).Build());
            Assert.Equal(new[] { "from 1" }, Values(await _client.FetchAsync("s", 0, 0, TimeSpan.FromMilliseconds(1))));
            _client.Dispose();
        }
        Assert.All(_a.Requests<Request.Fetch>(), r => Assert.False(r.AllowFollower));
        _client = null;
    }

    [Fact]
    public async Task AReplicaThatFailsIsLeftForTheLeader()
    {
        // An older node answers NOT_LEADER; one whose log starts later, OFFSET_OUT_OF_RANGE.
        foreach (ErrorCode code in new[] { ErrorCode.NotLeader, ErrorCode.OffsetOutOfRange })
        {
            Zones(code);
            int before = _b.Count<Request.Fetch>();
            _client = new LotseClient(Fx.Config(_a).WithZone("b").Build());
            Assert.Equal(new[] { "from 1" }, Values(await _client.FetchAsync("s", 0, 0, TimeSpan.FromMilliseconds(1))));
            Assert.Equal(new[] { "from 1" }, Values(await _client.FetchAsync("s", 0, 0, TimeSpan.FromMilliseconds(1))));
            Assert.Equal(before + 1, _b.Count<Request.Fetch>()); // asked once, then left
            _client.Dispose();
        }
        _client = null;
    }

    [Fact]
    public async Task AFetchThatNeverGetsALeaderGivesUp()
    {
        _a.On((node, request) => request is Request.Metadata
            ? Fx.Metadata(new[] { node }, Fx.Stream("s", StreamId, Fx.Partition(0, 0, 0)))
            : null);
        _client = new LotseClient(Fx.Config(_a).WithRetryFor(TimeSpan.FromMilliseconds(150)).Build());
        await Assert.ThrowsAsync<GaveUpException>(() => _client.FetchAsync("s", 0, 0, TimeSpan.FromMilliseconds(1)));
    }

    [Fact]
    public async Task ANegativeWaitIsRefused()
    {
        _client = new LotseClient(Fx.Config(_a).Build());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _client.FetchAsync("s", 0, 0, TimeSpan.FromMilliseconds(-1)));
    }

    // ---- zone reads: offsets and admin state in the client's zone, and back to the zone ----
    //
    // Node 1 (a) is in zone x and leads everything; nodes 2 (b) and 3 (c) are in zone y. Stream s (id 7) lives on node 1
    // alone, stream t (id 8) on nodes 1 and 2. Node 1 coordinates, with all three voting.

    private static readonly LeaderHint Hint = new(1, 1);

    /// <summary>What every node heard, in order across the nodes.</summary>
    private readonly ConcurrentQueue<FakeNode.Heard> _heard = new();

    /// <summary>Indexes from pairs of group and index.</summary>
    private static GroupIndex[] At(params ulong[] pairs) =>
        Enumerable.Range(0, pairs.Length / 2).Select(i => new GroupIndex(pairs[2 * i], pairs[(2 * i) + 1])).ToArray();

    private static readonly GroupIndex[] None = Array.Empty<GroupIndex>();

    /// <summary>The metadata of the zone tests, listing the nodes <paramref name="listed"/>.</summary>
    private static Response.Metadata ZoneMetadata(params FakeNode[] listed)
    {
        string[] zones = { "x", "y", "y" };
        return new Response.Metadata(
            listed.Select(n => new Messages.Broker(n.Id, "127.0.0.1", (ushort)n.Address.Port, zones[n.Id - 1])).ToList(),
            new[] { Fx.Stream("s", 7, Fx.Partition(0, 1, 1, 1)), Fx.Stream("t", 8, Fx.Partition(0, 1, 1, 1, 2)) });
    }

    private static Response.Fetch ZoneFetched(ErrorCode error, uint? preferredReplica) =>
        new(
            0,
            new[]
            {
                new Messages.FetchResourceResponse(
                    0,
                    new[]
                    {
                        new Messages.FetchPartitionResponse(
                            0, error, 0, 0, 0, Array.Empty<Messages.AbortedTxn>(), ByteString.Empty, null, preferredReplica),
                    }),
            });

    /// <summary>The answer to a zone read, with <paramref name="error"/> (and then the leader's hint) or without.</summary>
    private static Response Read(Request req, ErrorCode error)
    {
        LeaderHint? hint = error.IsOk ? null : Hint;
        return req switch
        {
            Request.DescribeConfigs d => new Response.DescribeConfigs(
                error, hint, d.ResourceIds.Select(id => new Messages.ResourceConfigs(id, ErrorCode.None, Array.Empty<Messages.ConfigEntry>())).ToList()),
            Request.DescribeNodes => new Response.DescribeNodes(error, hint, Array.Empty<Messages.NodeEntry>()),
            Request.DescribeUsers => new Response.DescribeUsers(error, hint, Array.Empty<Messages.UserEntry>()),
            Request.DescribeTokens => new Response.DescribeTokens(error, hint, Array.Empty<Messages.TokenEntry>()),
            Request.DescribeCertificates => new Response.DescribeCertificates(error, hint, 0, Array.Empty<Messages.CertificateEntry>()),
            Request.DescribeCertificateRequests =>
                new Response.DescribeCertificateRequests(error, hint, 0, Array.Empty<Messages.CertificateRequestEntry>()),
            Request.DescribeUpgrade => new Response.DescribeUpgrade(error, hint, null, false, null, Array.Empty<Response.UpgradeNode>()),
            Request.DescribeTenants => new Response.DescribeTenants(error, hint, Array.Empty<Messages.TenantEntry>()),
            Request.DescribeRoleBindings => new Response.DescribeRoleBindings(error, hint, Array.Empty<Messages.SourcedBinding>()),
            Request.OffsetFetch o => new Response.OffsetFetch(o.Resources
                .Select(r => new Messages.OffsetFetchResource(
                    r.ResourceId, r.Partitions.Select(p => new Messages.OffsetFetchPartition(p, error, 0, 1, null)).ToList()))
                .ToList()),
            _ => throw new InvalidOperationException("not a zone read: " + req),
        };
    }

    /// <summary>What every node answers, unless a test says otherwise.</summary>
    private Response Common(Request req) => req switch
    {
        Request.Metadata => ZoneMetadata(_a, _b, _c),
        Request.FindCoordinator => new Response.FindCoordinator(
            ErrorCode.None, 1, "127.0.0.1", (ushort)_a.Address.Port, null, new uint[] { 1, 2, 3 }),
        Request.GroupHeartbeat => new Response.GroupHeartbeat(
            0, ErrorCode.None, "m", 1, 1_000, new[] { new Messages.TxnResource(7, new uint[] { 0 }) }),
        Request.Fetch => ZoneFetched(ErrorCode.None, null),
        Request.ListOffsets => new Response.ListOffsets(new[]
        {
            new Messages.ListOffsetsResourceResponse(0, new[] { new Messages.ListOffsetsPartitionResponse(0, ErrorCode.None, 0, -1, null) }),
        }),
        Request.CommitOffsets co => new Response.CommitOffsets(co.Resources
            .Select(r => new Messages.TxnResourceResult(
                r.ResourceId, r.Partitions.Select(p => new Messages.TxnPartitionResult(p.Partition, ErrorCode.None)).ToList()))
            .ToList()),
        _ => Read(req, ErrorCode.None),
    };

    /// <summary>Every node answers <see cref="Common"/> and records what it hears in <see cref="_heard"/>.</summary>
    private void ZoneNodes()
    {
        foreach (FakeNode n in new[] { _a, _b, _c })
        {
            n.HeardIn(_heard).On((_, req) => Common(req));
        }
    }

    private LotseClient ZoneClient(string? zone)
    {
        _client = new LotseClient(Fx.Config(_a).WithZone(zone).Build());
        return _client;
    }

    /// <summary>A node and the <c>read_at_least</c> it heard, as "node [group:index ...]".</summary>
    private static string Tagged(uint node, IEnumerable<GroupIndex> readAtLeast) =>
        $"{node} [{string.Join(" ", readAtLeast.Select(g => $"{g.Group}:{g.Index}"))}]";

    /// <summary>The requests of type <typeparamref name="T"/> that were heard, as <see cref="Tagged"/>.</summary>
    private List<string> HeardOf<T>()
        where T : Request =>
        _heard.Where(h => h.Request is T).Select(h => Tagged(h.Node, h.ReadAtLeast)).ToList();

    /// <summary>The nodes that heard the requests of type <typeparamref name="T"/>, in order.</summary>
    private List<uint> NodesOf<T>()
        where T : Request => _heard.Where(h => h.Request is T).Select(h => h.Node).ToList();

    /// <summary>Indexes only rise, and every connection's answers raise the one set.</summary>
    [Fact]
    public void AnIndexOnlyRises()
    {
        var i = new Zone.Indexes();
        Assert.Equal(At(1, 0, 2, 0), i.Snapshot());
        i.Raise(At(1, 5, 2, 3));
        i.Raise(At(1, 2));
        Assert.Equal(At(1, 5, 2, 3), i.Snapshot());
        i.Raise(At(2, ulong.MaxValue));
        Assert.Equal(At(1, 5, 2, ulong.MaxValue), i.Snapshot());
    }

    [Fact]
    public void TheZoneNodeIsTheLowestIdInTheZone()
    {
        var zones = new Dictionary<uint, string> { [1] = "x", [2] = "y", [3] = "y" };
        Assert.Equal(2u, Zone.ZoneNode("y", new uint[] { 3, 2, 1 }, zones));
        Assert.Null(Zone.ZoneNode("x", new uint[] { 2, 3 }, zones));
        Assert.Null(Zone.ZoneNode("z", zones.Keys, zones));
    }

    /// <summary>
    /// Every <c>applied</c> the client hears raises its index of that group; a lower one (from a follower further
    /// behind) leaves it as it is.
    /// </summary>
    [Fact]
    public async Task AppliedRaisesTheIndexAndNeverLowersIt()
    {
        ZoneNodes();
        _a.Applied(r => r is Request.Metadata ? At(1, 5, 2, 3) : None);
        var answers = new ConcurrentQueue<GroupIndex[]>(new[] { At(1, 2), At(1, 9) });
        _b.Applied(_ => answers.TryDequeue(out GroupIndex[]? next) ? next : None);
        await ZoneClient("y").MetadataAsync();
        for (int i = 0; i < 3; i++)
        {
            await _client!.DescribeUsersAsync();
        }
        // 2 is below 5 and leaves it; 9 raises it.
        Assert.Equal(new[] { "2 [1:5 2:3]", "2 [1:5 2:3]", "2 [1:9 2:3]" }, HeardOf<Request.DescribeUsers>());
    }

    /// <summary>Each of the nine admin reads goes to the lowest-id node in the client's zone with both groups' indexes.</summary>
    [Fact]
    public async Task AnAdminReadGoesToTheZoneWithReadAtLeast()
    {
        ZoneNodes();
        _a.Applied(r => r is Request.Metadata ? At(1, 4) : None);
        LotseClient client = ZoneClient("y");
        await client.MetadataAsync();
        await client.DescribeConfigsAsync("s");
        await client.DescribeNodesAsync();
        await client.DescribeUsersAsync();
        await client.DescribeTokensAsync();
        await client.DescribeCertificatesAsync();
        await client.DescribeCertificateRequestsAsync();
        await client.DescribeUpgradeAsync();
        await client.DescribeTenantsAsync();
        await client.DescribeRoleBindingsAsync();
        List<FakeNode.Heard> reads = _heard.Where(h => h.Request is not Request.Metadata).ToList();
        Assert.Equal(9, reads.Count);
        Assert.All(reads, h => Assert.Equal("2 [1:4 2:0]", Tagged(h.Node, h.ReadAtLeast)));
    }

    /// <summary>
    /// A zone node that has not applied what the client saw refuses with NOT_LEADER: that read goes to the leader,
    /// without the tag, and the next read tries the zone again.
    /// </summary>
    [Fact]
    public async Task ARefusedZoneReadGoesToTheLeaderAndTheNextTriesTheZoneAgain()
    {
        ZoneNodes();
        _a.Applied(_ => At(1, 6));
        int refused = 0;
        _b.On((_, r) => r is Request.DescribeTenants && Interlocked.Exchange(ref refused, 1) == 0
            ? Read(r, ErrorCode.NotLeader)
            : Common(r));
        await ZoneClient("y").MetadataAsync();
        await _client!.DescribeTenantsAsync();
        await _client.DescribeTenantsAsync();
        Assert.Equal(new[] { "2 [1:6 2:0]", "1 []", "2 [1:6 2:0]" }, HeardOf<Request.DescribeTenants>());
    }

    /// <summary>
    /// A node of an older release ignores the tag and refuses the leader-only reads with NOT_LEADER: every such read
    /// ends at the leader.
    /// </summary>
    [Fact]
    public async Task AnOlderZoneNodeSendsTheReadToTheLeader()
    {
        ZoneNodes();
        _b.On((_, r) => r is Request.DescribeUpgrade ? Read(r, ErrorCode.NotLeader) : Common(r));
        await ZoneClient("y").MetadataAsync();
        await _client!.DescribeUpgradeAsync();
        await _client.DescribeUpgradeAsync();
        Assert.Equal(new uint[] { 2, 1, 2, 1 }, NodesOf<Request.DescribeUpgrade>());
    }

    /// <summary>A group member reads its committed offsets from the lowest-id coordinator voter in its zone.</summary>
    [Fact]
    public async Task OffsetFetchGoesToACoordinatorVoterInTheZone()
    {
        ZoneNodes();
        _a.Applied(r => r switch
        {
            Request.Metadata => At(1, 4),
            Request.FindCoordinator or Request.GroupHeartbeat => At(2, 6),
            _ => None,
        });
        await using GroupConsumer group = await ZoneClient("y").JoinGroupAsync("g", new[] { "s" });
        await group.PollAsync(TimeSpan.FromMilliseconds(10));
        Assert.Equal(0UL, group.Positions["s"][0]); // the committed offset
        Assert.Equal(new[] { "2 [1:4 2:6]" }, HeardOf<Request.OffsetFetch>());
    }

    /// <summary>A fetch that goes to the leader names the client's zone, so the leader can point back to it; one to the zone does not.</summary>
    [Fact]
    public async Task AFetchToTheLeaderNamesTheZone()
    {
        ZoneNodes();
        await ZoneClient("y").FetchAsync("s", 0, 0, TimeSpan.Zero);
        await _client!.FetchAsync("t", 0, 0, TimeSpan.Zero);
        List<string> fetches = _heard
            .Select(h => h.Request is Request.Fetch f ? $"{h.Node} {f.Zone ?? "null"} {f.AllowFollower}" : null)
            .OfType<string>()
            .ToList();
        Assert.Equal(new[] { "1 y False", "2 null True" }, fetches);
    }

    /// <summary>The reads of t, as "node kind allow_follower".</summary>
    private List<string> StreamReads() => _heard
        .Select(h => h.Request switch
        {
            Request.Fetch f => $"{h.Node} fetch {f.AllowFollower}",
            Request.ListOffsets l => $"{h.Node} list {l.AllowFollower}",
            _ => null,
        })
        .OfType<string>()
        .ToList();

    /// <summary>
    /// After a replica in the zone failed, the leader serves; once it names that replica as current again, the next
    /// fetch and offset lookup go back there, without asking for metadata.
    /// </summary>
    [Fact]
    public async Task PreferredReplicaSendsTheNextFetchBackToTheZone()
    {
        ZoneNodes();
        _a.On((_, r) => r is Request.Fetch ? ZoneFetched(ErrorCode.None, 2) : Common(r));
        int refused = 0;
        _b.On((_, r) => r is Request.Fetch && Interlocked.Exchange(ref refused, 1) == 0
            ? ZoneFetched(ErrorCode.NotLeader, null)
            : Common(r));
        await ZoneClient("y").FetchAsync("t", 0, 0, TimeSpan.Zero);
        await _client!.FetchAsync("t", 0, 0, TimeSpan.Zero);
        await _client.ListOffsetAsync("t", 0, OffsetAt.Latest);
        Assert.Equal(new[] { "2 fetch True", "1 fetch False", "2 fetch True", "2 list True" }, StreamReads());
        Assert.Single(NodesOf<Request.Metadata>()); // metadata only for the first route
    }

    /// <summary>A client without a zone sends neither tag, and its reads go to the leaders.</summary>
    [Fact]
    public async Task NoZoneNoTags()
    {
        ZoneNodes();
        _a.Applied(_ => At(1, 4, 2, 6));
        foreach (FakeNode n in new[] { _b, _c })
        {
            n.On((node, r) => r is Request.Metadata or Request.FindCoordinator
                ? Common(r)
                : throw new InvalidOperationException($"only the leader is asked, not node {node.Id} for {r}"));
        }
        LotseClient client = ZoneClient(null);
        await client.DescribeUpgradeAsync();
        await client.DescribeUsersAsync();
        await client.FetchAsync("t", 0, 0, TimeSpan.Zero);
        await client.ListOffsetAsync("t", 0, OffsetAt.Latest);
        await using (GroupConsumer group = await client.JoinGroupAsync("g", new[] { "s" }))
        {
            await group.PollAsync(TimeSpan.FromMilliseconds(10));
        }
        Assert.Contains(_heard, h => h.Request is Request.OffsetFetch);
        foreach (FakeNode.Heard h in _heard)
        {
            Assert.Empty(h.ReadAtLeast);
            if (h.Request is Request.Fetch f)
            {
                Assert.Null(f.Zone);
                Assert.False(f.AllowFollower);
            }
            if (h.Request is not (Request.Metadata or Request.FindCoordinator))
            {
                Assert.Equal(1u, h.Node);
            }
        }
    }

    /// <summary>
    /// A node the latest metadata no longer lists is no zone node, for admin reads and offset reads alike: here node 2
    /// left, and the reads go to node 3 without trying node 2 first.
    /// </summary>
    [Fact]
    public async Task ZoneReadsSkipANodeTheLatestMetadataLeftOut()
    {
        ZoneNodes();
        int answers = 0;
        _a.On((_, r) => r is Request.Metadata
            ? (Interlocked.Increment(ref answers) == 1 ? ZoneMetadata(_a, _b, _c) : ZoneMetadata(_a, _c))
            : Common(r));
        LotseClient client = ZoneClient("y");
        await client.MetadataAsync();
        await client.MetadataAsync();
        await client.DescribeUsersAsync();
        await using (GroupConsumer group = await client.JoinGroupAsync("g", new[] { "s" }))
        {
            await group.PollAsync(TimeSpan.FromMilliseconds(10));
        }
        Assert.Equal(new uint[] { 3 }, NodesOf<Request.DescribeUsers>());
        Assert.Equal(new uint[] { 3 }, NodesOf<Request.OffsetFetch>());
    }

    /// <summary>Node 2 refuses the first fetch of t, then serves; node 1 answers its n-th fetch with what <paramref name="leader"/> gives.</summary>
    private async Task<List<string>> AfterAFallback(Func<int, Response> leader)
    {
        ZoneNodes();
        int count = 0;
        _a.On((_, r) => r is Request.Fetch ? leader(Interlocked.Increment(ref count)) : Common(r));
        int refused = 0;
        _b.On((_, r) => r is Request.Fetch && Interlocked.Exchange(ref refused, 1) == 0
            ? ZoneFetched(ErrorCode.NotLeader, null)
            : Common(r));
        LotseClient client = ZoneClient("y");
        try
        {
            await client.FetchAsync("t", 0, 0, TimeSpan.Zero);
        }
        catch (ClientException)
        {
            // the leader's answer to it is the test's to choose
        }
        await client.FetchAsync("t", 0, 0, TimeSpan.Zero);
        return StreamReads();
    }

    /// <summary>A leader answer with an error says nothing about the replicas: its preferred replica leaves the next fetch with the leader.</summary>
    [Fact]
    public async Task PreferredReplicaOnAnErrorIsIgnored()
    {
        List<string> seen = await AfterAFallback(n => n == 1
            ? ZoneFetched(ErrorCode.OffsetOutOfRange, 2)
            : ZoneFetched(ErrorCode.None, null));
        Assert.Equal(new[] { "2 fetch True", "1 fetch False", "1 fetch False" }, seen);
    }

    /// <summary>
    /// A replica the client has no address for cannot be asked: the fallback stays, rather than sending the next fetch to
    /// the replica that failed.
    /// </summary>
    [Fact]
    public async Task PreferredReplicaWithoutAnAddressLeavesTheFetchOnTheLeader()
    {
        List<string> seen = await AfterAFallback(n => ZoneFetched(ErrorCode.None, n == 1 ? 4u : null));
        Assert.Equal(new[] { "2 fetch True", "1 fetch False", "1 fetch False" }, seen);
    }

    /// <summary>
    /// The zone voter refuses an <c>OffsetFetch</c>, or the call to it fails: that read goes to the coordinator, and the
    /// next tries the zone again.
    /// </summary>
    [Fact]
    public async Task ARefusedOffsetFetchGoesToTheCoordinatorAndTheNextTriesTheZoneAgain()
    {
        foreach (bool failedCall in new[] { false, true })
        {
            _heard.Clear();
            ZoneNodes();
            int refused = 0;
            _b.On((_, r) => r is Request.OffsetFetch && Interlocked.Exchange(ref refused, 1) == 0
                ? (failedCall ? new Response.Failed(ErrorCode.InvalidRequest) : Read(r, ErrorCode.NotLeader))
                : Common(r));
            LotseClient client = ZoneClient("y");
            await client.MetadataAsync();
            var req = new Request.OffsetFetch("g", true, new[] { new Messages.TxnResource(7, new uint[] { 0 }) });
            for (int i = 0; i < 2; i++)
            {
                Response r = await client.CallCoordinatorReadAsync("g", req, CancellationToken.None);
                Assert.True(r is Response.OffsetFetch && !Zone.Refused(r), r.ToString());
            }
            Assert.Equal(new uint[] { 2, 1, 2 }, NodesOf<Request.OffsetFetch>());
            client.Dispose();
        }
        _client = null;
    }

    // ---- batches the Rust side wrote ----

    private static ByteString Vector(string label)
    {
        foreach (string[] v in Vectors.Lines("batches.hex"))
        {
            if (v[0] == label)
            {
                return ByteString.CopyOf(Vectors.Hex(v[1]));
            }
        }
        throw new InvalidOperationException("no vector " + label);
    }

    /// <summary>
    /// The transaction batches and markers in <c>batches.hex</c> were written by the Rust code. This is the closest the
    /// tests come to a read_committed fetch of a Rust-written transaction without a node: a real transaction needs a
    /// transactional client, which comes with a later phase.
    /// </summary>
    [Fact]
    public void ControlBatchesWrittenByRustAreSkippedAndItsTransactionalDataIsKept()
    {
        ByteString records = Concat(
            Vector("transactional"), // producer 17, offset 200
            Vector("raft-noop"),
            Vector("raft-config"),
            Vector("control-commit")); // producer 17
        Fetched committed = Fetched(records);
        Assert.Single(committed.CommittedBatches());
        Assert.Equal(200UL, committed.CommittedBatches()[0].BaseOffset);
    }

    [Fact]
    public void ATransactionOfRustsProducerStaysHiddenUntilItsOwnAbortMarker()
    {
        // The abort marker in the vectors belongs to producer 18, so producer 17's transaction is still open.
        ByteString records = Concat(Vector("transactional"), Vector("control-abort"));
        Assert.Empty(Fetched(records, new Messages.AbortedTxn(17, 200)).CommittedBatches());
        Assert.Single(Fetched(records).CommittedBatches());
    }
}

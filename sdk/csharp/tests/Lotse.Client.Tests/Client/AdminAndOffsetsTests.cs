using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Auth;
using Lotse.Net;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>Creating streams and looking up offsets against scripted brokers: node 2 leads the metadata group, node 1 points to it.</summary>
public sealed class AdminAndOffsetsTests : IDisposable
{
    private const ulong StreamId = 7;

    private readonly FakeNode _a = new(1);
    private readonly FakeNode _b = new(2);
    private LotseClient? _client;
    private int _calls;

    public void Dispose()
    {
        _client?.Dispose();
        _a.Dispose();
        _b.Dispose();
    }

    private Response Metadata() =>
        Fx.Metadata(new[] { _a, _b }, Fx.Stream("s", StreamId, Fx.Partition(0, 2, 1, 2)));

    private void Serve(Func<FakeNode, Request, Response?> admin)
    {
        _a.On((n, r) => r is Request.Metadata ? Metadata() : new Response.Create(ErrorCode.NotLeader, 0, null, new LeaderHint(2, 1)));
        _b.On((n, r) => r is Request.Metadata ? Metadata() : admin(n, r));
        _client = new LotseClient(Fx.Config(_a).WithRetryFor(TimeSpan.FromSeconds(1)).Build());
    }

    [Fact]
    public async Task ACreateFollowsTheHintToTheMetadataLeader()
    {
        Serve((n, r) => new Response.Create(ErrorCode.None, 31, null, null));
        Assert.Equal(31UL, await _client!.CreateStreamAsync("t", 2, 3));
        Request.CreateStream sent = _b.Requests<Request.CreateStream>()[0];
        Assert.Equal("t", sent.Name);
        Assert.Equal("default", sent.Tenant);
        Assert.Equal(2u, sent.Partitions);
        Assert.Equal((byte)3, sent.Replication);
        Assert.Empty(sent.Configs);
        await _client.CreateStreamAsync("u", 1, 1);
        Assert.Single(_a.Requests<Request.CreateStream>()); // the leader, once known, is asked directly
    }

    [Fact]
    public async Task TheSettingsOfAStreamTravelWithTheCreate()
    {
        Serve((n, r) => new Response.Create(ErrorCode.None, 31, null, null));
        _client = new LotseClient(Fx.Config(_a).WithTenant("acme").Build());
        await _client.CreateStreamAsync(
            "t", 1, 3, new Dictionary<string, string> { ["retention.ms"] = "60000", ["retention.bytes"] = "1048576" });
        Request.CreateStream sent = _b.Requests<Request.CreateStream>().Count > 0
            ? _b.Requests<Request.CreateStream>()[0]
            : _a.Requests<Request.CreateStream>()[0];
        Assert.Equal("acme", sent.Tenant);
        Assert.Equal(
            new[] { ("retention.ms", "60000"), ("retention.bytes", "1048576") },
            sent.Configs.Select(c => (c.Key, c.Value)).ToArray());
    }

    [Fact]
    public async Task ACreateWhoseAnswerWasLostFindsItAndSaysSo()
    {
        Serve((n, r) =>
            Interlocked.Increment(ref _calls) == 1 ? throw new FakeNode.Drop() : new Response.Create(ErrorCode.ResourceExists, 9, null, null));
        var e = await Assert.ThrowsAsync<ResourceExistsException>(() => _client!.CreateStreamAsync("t", 1, 3));
        Assert.Equal(9UL, e.ResourceId);
        Assert.Equal("t", e.Name);
    }

    [Fact]
    public async Task ACreateRefusedForGoodIsABrokerErrorWithItsMessage()
    {
        Serve((n, r) => new Response.Create(ErrorCode.InvalidRequest, 0, "replication 5 exceeds the 3 nodes", null));
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.CreateStreamAsync("t", 1, 5));
        Assert.Equal(ErrorCode.InvalidRequest, e.Code);
        Assert.Equal("replication 5 exceeds the 3 nodes", e.BrokerMessage);
        Assert.Equal(1, _b.Count<Request.CreateStream>()); // not retried
    }

    [Fact]
    public async Task ACreateWithoutAnyLeaderKeepsTryingAndThenSaysItMayHaveHappened()
    {
        Serve((n, r) => new Response.Create(ErrorCode.NotLeader, 0, null, null));
        await Assert.ThrowsAsync<OutcomeUnknownException>(() => _client!.CreateStreamAsync("t", 1, 3));
    }

    [Fact]
    public async Task NonsenseArgumentsAreRefusedBeforeAnythingIsSent()
    {
        Serve((n, r) => throw new InvalidOperationException("nothing may be sent"));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _client!.CreateStreamAsync("t", 0, 3));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _client!.CreateStreamAsync("t", 1, 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _client!.CreateStreamAsync("t", 1, 256));
        Assert.Empty(_a.Requests());
    }

    // ---- list offsets ----

    private void ServeOffsets(Func<Request.ListOffsets, Response> answer)
    {
        _a.On((n, r) => r is Request.Metadata ? Metadata() : throw new InvalidOperationException("node 1 leads nothing"));
        _b.On((n, r) => r is Request.Metadata ? Metadata() : answer((Request.ListOffsets)r));
        _client = new LotseClient(Fx.Config(_a).WithRetryFor(TimeSpan.FromMilliseconds(500)).Build());
    }

    private static Response Offset(ErrorCode code, ulong offset, LeaderHint? hint = null) =>
        new Response.ListOffsets(new[]
        {
            new Messages.ListOffsetsResourceResponse(
                StreamId, new[] { new Messages.ListOffsetsPartitionResponse(0, code, offset, -1, hint) }),
        });

    [Fact]
    public async Task ListOffsetsAsksTheLeaderForEarliestLatestOrATime()
    {
        ServeOffsets(r => Offset(ErrorCode.None, (ulong)r.Resources[0].Partitions[0].Timestamp + 1000));
        Assert.Equal(998UL, await _client!.ListOffsetAsync("s", 0, OffsetAt.Earliest));
        Assert.Equal(999UL, await _client.ListOffsetAsync("s", 0, OffsetAt.Latest));
        Assert.Equal(2_700UL, await _client.ListOffsetAsync("s", 0, OffsetAt.Time(1_700)));
        Assert.Equal(1_000UL, await _client.ListOffsetAsync("s", 0, OffsetAt.Time(-5))); // a negative time counts as 0

        List<Request.ListOffsets> sent = _b.Requests<Request.ListOffsets>();
        Assert.Equal(new long[] { -2, -1, 1700, 0 }, sent.Select(s => s.Resources[0].Partitions[0].Timestamp).ToArray());
        foreach (Request.ListOffsets s in sent)
        {
            Assert.Equal(Messages.Isolation.Committed, s.Isolation);
            Assert.Equal(StreamId, s.Resources[0].ResourceId);
            Assert.Equal(0u, s.Resources[0].Partitions[0].Partition);
        }
        Assert.Equal(1, _a.Count<Request.Metadata>());
    }

    [Fact]
    public async Task AListOffsetsErrorThatCanBeRetriedFollowsTheHint()
    {
        int n = 0;
        ServeOffsets(_ => Interlocked.Increment(ref n) == 1 ? Offset(ErrorCode.NotLeader, 0, new LeaderHint(2, 9)) : Offset(ErrorCode.None, 12));
        Assert.Equal(12UL, await _client!.ListOffsetAsync("s", 0, OffsetAt.Latest));
        Assert.Equal(2, _b.Count<Request.ListOffsets>());
    }

    [Fact]
    public async Task AListOffsetsErrorThatCannotBeRetriedIsReported()
    {
        ServeOffsets(_ => Offset(ErrorCode.InvalidRequest, 0));
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.ListOffsetAsync("s", 0, OffsetAt.Latest));
        Assert.Equal(ErrorCode.InvalidRequest, e.Code);
        Assert.Equal(1, _b.Count<Request.ListOffsets>());
    }

    [Fact]
    public void OffsetAtSaysWhatItAsksFor()
    {
        Assert.Equal("Earliest", OffsetAt.Earliest.ToString());
        Assert.Equal("Latest", OffsetAt.Latest.ToString());
        Assert.Equal("Time(5)", OffsetAt.Time(5).ToString());
        Assert.Equal(OffsetAt.Time(0), OffsetAt.Time(-9));
        Assert.NotEqual(OffsetAt.Earliest, OffsetAt.Latest);
    }

    // ---- login failures ----

    [Fact]
    public async Task ANodeThatRequiresLoginIsReportedAsAuthenticationNotRetried()
    {
        _a.On((n, r) => new Response.Failed(ErrorCode.Unauthorized));
        _client = new LotseClient(Fx.Config(_a).WithRetryFor(TimeSpan.FromSeconds(5)).Build());
        var e = await Assert.ThrowsAsync<ClientAuthenticationException>(() => _client.MetadataAsync());
        Assert.Contains("requires authentication", e.Message);
        Assert.Single(_a.Requests());
    }
}

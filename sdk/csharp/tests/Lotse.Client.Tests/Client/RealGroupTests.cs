using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// Consumer groups against three real <c>lotse</c> processes on loopback (plain TCP, no authentication): two members
/// that share a stream and never own a partition together while it moves, a static member taken over by its next
/// instance, a pattern that picks up streams created later, and a group that starts where it is told to. Skipped,
/// with a message, when <c>LOTSE_BIN</c> is not set.
/// </summary>
[Collection("real cluster")]
public sealed class RealGroupTests : IClassFixture<RealCluster>, IAsyncLifetime
{
    private readonly RealCluster _cluster;
    private readonly List<LotseClient> _clients = new();

    public RealGroupTests(RealCluster cluster) => _cluster = cluster;

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync()
    {
        foreach (LotseClient c in _clients)
        {
            c.Dispose();
        }
        return Task.CompletedTask;
    }

    private LotseClient NewClient()
    {
        var client = new LotseClient(ClientConfig.CreateBuilder(_cluster.Bootstrap)
            .PlaintextOnLoopback()
            .WithRequestTimeout(TimeSpan.FromSeconds(5))
            .WithRetryFor(TimeSpan.FromSeconds(60))
            .Build());
        _clients.Add(client);
        return client;
    }


    private Task CreateStreamEverywhereAsync(LotseClient client, string name, int partitions) =>
        RealWaits.CreateStreamEverywhereAsync(_cluster, client, name, partitions);

    private static HashSet<string> Owned(GroupConsumer g) =>
        g.Assignment.SelectMany(s => s.Value.Select(p => $"{s.Key}/{p}")).ToHashSet();

    [LotseFact]
    public async Task TwoMembersShareAStreamAndNoPartitionHasTwoOwners()
    {
        LotseClient admin = NewClient();
        await CreateStreamEverywhereAsync(admin, "share", 4);
        for (int i = 0; i < 40; i++)
        {
            await admin.ProduceAsync("share", i % 4, OutMessage.FromValue($"{i % 4}:{i}"));
        }

        var config = GroupConfig.CreateBuilder().WithSessionTimeout(TimeSpan.FromSeconds(30)).Build();
        await using GroupConsumer a = await NewClient().JoinGroupAsync("share-group", new[] { "share" }, config);
        var seen = new List<string>();
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (Owned(a).Count < 4)
        {
            Assert.True(DateTime.UtcNow < deadline, "member a did not get the partitions: " + string.Join(",", Owned(a)));
            seen.AddRange((await a.PollAsync(TimeSpan.FromMilliseconds(100))).Select(r => r.Record.Value!.ToUtf8()));
        }

        await using GroupConsumer b = await NewClient().JoinGroupAsync("share-group", new[] { "share" }, config);
        var shared = false;
        int checks = 0;
        deadline = DateTime.UtcNow.AddSeconds(90);
        while (!shared)
        {
            Assert.True(DateTime.UtcNow < deadline, $"the partitions were not shared: a={string.Join(",", Owned(a))} b={string.Join(",", Owned(b))}");
            seen.AddRange((await a.PollAsync(TimeSpan.FromMilliseconds(100))).Select(r => r.Record.Value!.ToUtf8()));
            Assert.Empty(Owned(a).Intersect(Owned(b))); // never two owners, not even for a moment
            seen.AddRange((await b.PollAsync(TimeSpan.FromMilliseconds(100))).Select(r => r.Record.Value!.ToUtf8()));
            Assert.Empty(Owned(a).Intersect(Owned(b)));
            checks++;
            shared = Owned(a).Count == 2 && Owned(b).Count == 2;
        }
        Assert.True(checks > 0);
        Assert.Equal(4, Owned(a).Union(Owned(b)).Count());

        // Everything was read, and a revoked partition went on where its position was committed: nothing twice.
        deadline = DateTime.UtcNow.AddSeconds(30);
        while (seen.Distinct().Count() < 40 && DateTime.UtcNow < deadline)
        {
            seen.AddRange((await a.PollAsync(TimeSpan.FromMilliseconds(100))).Select(r => r.Record.Value!.ToUtf8()));
            seen.AddRange((await b.PollAsync(TimeSpan.FromMilliseconds(100))).Select(r => r.Record.Value!.ToUtf8()));
        }
        Assert.Equal(40, seen.Distinct().Count());
        Assert.Equal(seen.Count, seen.Distinct().Count());
    }

    [LotseFact]
    public async Task AStaticMemberIsTakenOverByItsNextInstanceAndTheEarlierOneIsFenced()
    {
        LotseClient admin = NewClient();
        await CreateStreamEverywhereAsync(admin, "static", 2);
        var config = GroupConfig.CreateBuilder().WithInstanceId("worker-1").Build();

        GroupConsumer first = await NewClient().JoinGroupAsync("static-group", new[] { "static" }, config);
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        while (Owned(first).Count < 2)
        {
            Assert.True(DateTime.UtcNow < deadline, $"the first instance got no partitions: member '{first.MemberId}' epoch {first.MemberEpoch}, owns {string.Join(",", Owned(first))}\n{_cluster.Log(1)}\n{_cluster.Log(2)}\n{_cluster.Log(3)}");
            await first.PollAsync(TimeSpan.FromMilliseconds(100));
        }
        string memberId = first.MemberId;
        HashSet<string> partitions = Owned(first);

        // The next instance with the same instance id takes the place, and the partitions with it.
        await using GroupConsumer second = await NewClient().JoinGroupAsync("static-group", new[] { "static" }, config);
        deadline = DateTime.UtcNow.AddSeconds(60);
        while (Owned(second).Count < 2)
        {
            Assert.True(DateTime.UtcNow < deadline, "the second instance did not take over");
            await second.PollAsync(TimeSpan.FromMilliseconds(100));
        }
        Assert.Equal(partitions, Owned(second));
        Assert.NotEqual(memberId, second.MemberId); // a new member id fences the old one (ADR 0072)

        // The first instance is told, at its next heartbeat, that it was replaced; and it stays out.
        deadline = DateTime.UtcNow.AddSeconds(60);
        BrokerErrorException? fenced = null;
        while (fenced is null)
        {
            Assert.True(DateTime.UtcNow < deadline, "the first instance was not fenced");
            try
            {
                await first.PollAsync(TimeSpan.FromMilliseconds(100));
            }
            catch (BrokerErrorException e)
            {
                fenced = e;
            }
        }
        Assert.Equal(ErrorCode.FencedInstanceId, fenced.Code);
        await Assert.ThrowsAsync<BrokerErrorException>(() => first.PollAsync(TimeSpan.FromMilliseconds(10)));
        Assert.Equal(partitions, Owned(second)); // and the second one is unharmed
        await second.PollAsync(TimeSpan.FromMilliseconds(100));
        Assert.Equal(partitions, Owned(second));
    }

    [LotseFact]
    public async Task APatternPicksUpStreamsCreatedLater()
    {
        LotseClient admin = NewClient();
        await CreateStreamEverywhereAsync(admin, "pat-a", 1);
        await CreateStreamEverywhereAsync(admin, "pat-b", 2);
        await CreateStreamEverywhereAsync(admin, "not-pat", 1);
        var config = GroupConfig.CreateBuilder().WithSessionTimeout(TimeSpan.FromSeconds(30)).Build();
        await using GroupConsumer g = await NewClient().JoinGroupMatchingAsync("pat-group", "pat-.*", config);
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (Owned(g).Count < 3)
        {
            Assert.True(DateTime.UtcNow < deadline, "got " + string.Join(",", Owned(g)));
            await g.PollAsync(TimeSpan.FromMilliseconds(100));
        }
        Assert.Equal(new[] { "pat-a/0", "pat-b/0", "pat-b/1" }, Owned(g).OrderBy(x => x, StringComparer.Ordinal).ToArray());

        await CreateStreamEverywhereAsync(admin, "pat-c", 1);
        deadline = DateTime.UtcNow.AddSeconds(60);
        while (!Owned(g).Contains("pat-c/0"))
        {
            Assert.True(DateTime.UtcNow < deadline, "the new stream did not join: " + string.Join(",", Owned(g)));
            await g.PollAsync(TimeSpan.FromMilliseconds(200));
        }
        Assert.DoesNotContain("not-pat/0", Owned(g));
    }

    [LotseFact]
    public async Task AGroupStartsWhereItIsToldAndContinuesFromItsCommit()
    {
        LotseClient admin = NewClient();
        await CreateStreamEverywhereAsync(admin, "start", 1);
        for (int i = 0; i < 5; i++)
        {
            await admin.ProduceAsync("start", 0, OutMessage.FromValue("old-" + i));
        }

        // From the end: only what comes later.
        var latest = GroupConfig.CreateBuilder().WithStartFrom(OffsetAt.Latest).Build();
        await using (GroupConsumer g = await NewClient().JoinGroupAsync("start-latest", new[] { "start" }, latest))
        {
            DateTime deadline = DateTime.UtcNow.AddSeconds(30);
            while (!g.Positions.ContainsKey("start"))
            {
                Assert.True(DateTime.UtcNow < deadline, "no position");
                Assert.Empty(await g.PollAsync(TimeSpan.FromMilliseconds(100)));
            }
            await admin.ProduceAsync("start", 0, OutMessage.FromValue("new-0"));
            var got = new List<string>();
            deadline = DateTime.UtcNow.AddSeconds(30);
            while (got.Count < 1 && DateTime.UtcNow < deadline)
            {
                got.AddRange((await g.PollAsync(TimeSpan.FromMilliseconds(200))).Select(r => r.Record.Value!.ToUtf8()));
            }
            Assert.Equal(new[] { "new-0" }, got.ToArray());
        }

        // From the start, then on from the commit.
        await using (GroupConsumer g = await NewClient().JoinGroupAsync("start-earliest", new[] { "start" }))
        {
            var got = new List<string>();
            DateTime deadline = DateTime.UtcNow.AddSeconds(30);
            while (got.Count < 6 && DateTime.UtcNow < deadline)
            {
                got.AddRange((await g.PollAsync(TimeSpan.FromMilliseconds(200))).Select(r => r.Record.Value!.ToUtf8()));
            }
            Assert.Equal(6, got.Count);
            Assert.Equal("old-0", got[0]);
        } // leaving commits the positions
        await admin.ProduceAsync("start", 0, OutMessage.FromValue("new-1"));
        await using (GroupConsumer g = await NewClient().JoinGroupAsync("start-earliest", new[] { "start" }))
        {
            var got = new List<string>();
            DateTime deadline = DateTime.UtcNow.AddSeconds(30);
            while (got.Count < 1 && DateTime.UtcNow < deadline)
            {
                got.AddRange((await g.PollAsync(TimeSpan.FromMilliseconds(200))).Select(r => r.Record.Value!.ToUtf8()));
            }
            Assert.Equal(new[] { "new-1" }, got.ToArray());
        }
    }
}

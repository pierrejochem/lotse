using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// The admin calls against three real <c>lotse</c> processes on loopback (plain TCP, no authentication, so the client
/// may do everything): creating and deleting, the conditional delete, purge, requeue, settings, partitions, nodes,
/// metrics, pins and who the client is. Users, tenants, roles and sessions need a login and are in
/// <see cref="RealAdminLoginTests"/>. After a change the tests wait for every node to have applied it before they
/// look at a node other than the metadata leader. Skipped, with a message, when <c>LOTSE_BIN</c> is not set.
/// </summary>
[Collection("real cluster")]
public sealed class RealAdminTests : IClassFixture<RealCluster>, IAsyncLifetime
{
    private readonly RealCluster _cluster;
    private LotseClient? _client;

    public RealAdminTests(RealCluster cluster) => _cluster = cluster;

    public Task InitializeAsync()
    {
        if (_cluster.Available)
        {
            _client = new LotseClient(ClientConfig.CreateBuilder(_cluster.Bootstrap)
                .PlaintextOnLoopback()
                .WithRequestTimeout(TimeSpan.FromSeconds(5))
                .WithRetryFor(TimeSpan.FromSeconds(60))
                .Build());
        }
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        _client?.Dispose();
        return Task.CompletedTask;
    }

    private LotseClient Client => _client!;

    private async Task<uint> LeaderOfAsync(string name, int partition = 0)
    {
        uint leader = 0;
        await RealWaits.UntilAsync($"a leader for {name}/{partition}", async () =>
        {
            Response.Metadata m = await Client.MetadataAsync(new[] { name });
            leader = m.Resources.Where(r => r.Name == name).SelectMany(r => r.Partitions).FirstOrDefault(p => p.Partition == partition)?.Leader ?? 0;
            return leader != 0;
        });
        return leader;
    }

    /// <summary>
    /// Pins the queue's leader where it is and waits until it leads there steadily (<see cref="RealWaits.PinLeaderAsync"/>):
    /// these tests rely on what the leader keeps in memory, and a new queue's leader otherwise moves to its preferred node
    /// at a time no test can know.
    /// </summary>
    private async Task SettledAsync(string queue)
    {
        await RealWaits.PinLeaderAsync(Client, queue);
    }

    private static async Task<Delivery> Take(QueueConsumer c, int seconds = 30)
    {
        Delivery? d = await c.PollAsync(TimeSpan.FromSeconds(seconds));
        Assert.NotNull(d);
        return d!;
    }

    private async Task ProduceAsync(string queue, int count, string prefix)
    {
        for (int i = 0; i < count; i++)
        {
            await Client.ProduceAsync(queue, 0, OutMessage.FromValue($"{prefix}-{i}"));
        }
    }

    private static Dictionary<string, Messages.ConfigEntry> ByKey(IEnumerable<Messages.ConfigEntry> entries) =>
        entries.ToDictionary(e => e.Key);

    // ---- create and delete ----

    [LotseFact]
    public async Task AStreamAndAQueueAreCreatedAndDeletedAndTheNodesForgetThem()
    {
        ulong stream = await Client.CreateStreamAsync("adm-stream", 2, 3);
        ulong queue = await Client.CreateQueueAsync("adm-queue");
        Assert.NotEqual(stream, queue);
        var again = await Assert.ThrowsAsync<ResourceExistsException>(() => Client.CreateStreamAsync("adm-stream", 2, 3));
        Assert.Equal(stream, again.ResourceId);
        await RealWaits.OnEveryNodeAsync(_cluster, "knows adm-stream", "adm-stream", r => r is { } x && x.Partitions.Count == 2);
        await RealWaits.OnEveryNodeAsync(_cluster, "knows adm-queue", "adm-queue", r => r is not null);
        await Client.ProduceAsync("adm-stream", 1, OutMessage.FromValue("x"));

        Assert.Equal(stream, await Client.DeleteAsync("adm-stream"));
        Assert.Equal(queue, await Client.DeleteAsync("adm-queue"));
        await RealWaits.OnEveryNodeAsync(_cluster, "forgot adm-stream", "adm-stream", r => r is null);
        await RealWaits.OnEveryNodeAsync(_cluster, "forgot adm-queue", "adm-queue", r => r is null);
        // This client still holds a route: the leader's UNKNOWN_RESOURCE drops it, and the next call finds no such stream.
        var stale = await Assert.ThrowsAsync<BrokerErrorException>(() => Client.ProduceAsync("adm-stream", 1, OutMessage.FromValue("y")));
        Assert.Equal(ErrorCode.UnknownResource, stale.Code);
        await Assert.ThrowsAsync<UnknownStreamException>(() => Client.ProduceAsync("adm-stream", 1, OutMessage.FromValue("y")));
        await Assert.ThrowsAsync<UnknownStreamException>(() => Client.DeleteAsync("adm-stream"));

        // A name made again is a new resource.
        ulong newer = await Client.CreateStreamAsync("adm-stream", 1, 3);
        Assert.NotEqual(stream, newer);
        await Client.DeleteAsync("adm-stream");
    }

    [LotseFact]
    public async Task AStreamIsCreatedWithItsSettings()
    {
        await Client.CreateStreamAsync(
            "adm-compact",
            1,
            3,
            new Dictionary<string, string> { ["cleanup.policy"] = "compact", ["retention.ms"] = "3600000", ["delete.retention.ms"] = "1000" });
        Dictionary<string, Messages.ConfigEntry> got = ByKey(await Client.DescribeConfigsAsync("adm-compact"));
        Assert.Equal(("compact", Messages.ConfigSource.Dynamic), (got["cleanup.policy"].Value, got["cleanup.policy"].Source));
        Assert.Equal(("3600000", Messages.ConfigSource.Dynamic), (got["retention.ms"].Value, got["retention.ms"].Source));
        Assert.Equal(("1000", Messages.ConfigSource.Dynamic), (got["delete.retention.ms"].Value, got["delete.retention.ms"].Source));
        Assert.Equal(Messages.ConfigSource.Default, got["retention.bytes"].Source);

        var refused = await Assert.ThrowsAsync<BrokerErrorException>(
            () => Client.CreateStreamAsync("adm-bad", 1, 3, new Dictionary<string, string> { ["cleanup.policy"] = "shred" }));
        Assert.Equal(ErrorCode.InvalidRequest, refused.Code);
        Assert.Contains("compact", refused.BrokerMessage);
        await Client.DeleteAsync("adm-compact");
    }

    [LotseFact]
    public async Task AnExchangeIsDeletedWithItsBindings()
    {
        await Client.CreateQueueAsync("adm-bound");
        ulong x = await Client.DeclareExchangeAsync("adm-x", Messages.ExchangeType.Direct);
        await Client.BindAsync("adm-x", "adm-bound", "k");
        await Client.PublishAsync("adm-x", "k", OutMessage.FromValue("before"));
        Assert.Equal(x, await Client.DeleteIfEmptyAsync("adm-x")); // an exchange holds no messages
        await RealWaits.OnEveryNodeAsync(_cluster, "forgot adm-x", "adm-x", r => r is null);
        await Assert.ThrowsAsync<UnknownStreamException>(() => Client.PublishAsync("adm-x", "k", OutMessage.FromValue("after")));
        await Client.DeleteAsync("adm-bound");
    }

    [LotseFact]
    public async Task AConditionalDeleteIsRefusedWhileTheQueueHoldsAnUnsettledMessageAndDoneOnceItIsSettled()
    {
        await Client.CreateQueueAsync("adm-busy");
        await ProduceAsync("adm-busy", 1, "busy");
        await SettledAsync("adm-busy");
        var refused = await Assert.ThrowsAsync<BrokerErrorException>(() => Client.DeleteIfEmptyAsync("adm-busy"));
        Assert.Equal(ErrorCode.ResourceNotEmpty, refused.Code);

        await using (QueueConsumer c = await Client.SubscribeAsync("adm-busy"))
        {
            Delivery d = await Take(c);
            // In flight is not settled: still refused, and nothing was deleted.
            var again = await Assert.ThrowsAsync<BrokerErrorException>(() => Client.DeleteIfEmptyAsync("adm-busy"));
            Assert.Equal(ErrorCode.ResourceNotEmpty, again.Code);
            await c.AckAsync(d);
        }
        // Still there: it can be read and written.
        await ProduceAsync("adm-busy", 1, "later");
        await using (QueueConsumer c = await Client.SubscribeAsync("adm-busy"))
        {
            Delivery d = await Take(c);
            Assert.Equal("later-0", d.Value!.ToUtf8());
            await c.AckAsync(d);
        }
        Assert.NotEqual(0UL, await Client.DeleteIfEmptyAsync("adm-busy"));
        await RealWaits.OnEveryNodeAsync(_cluster, "forgot adm-busy", "adm-busy", r => r is null);
    }

    [LotseFact]
    public async Task AConditionalDeleteIsRefusedForAStreamWithRecordsAndDoneForAnEmptyOne()
    {
        await Client.CreateStreamAsync("adm-full", 2, 3);
        await Client.CreateStreamAsync("adm-void", 2, 3);
        await Client.ProduceAsync("adm-full", 1, OutMessage.FromValue("one record, in the second partition"));
        var refused = await Assert.ThrowsAsync<BrokerErrorException>(() => Client.DeleteIfEmptyAsync("adm-full"));
        Assert.Equal(ErrorCode.ResourceNotEmpty, refused.Code);
        Fetched f = await Client.FetchAsync("adm-full", 1, 0, TimeSpan.FromMilliseconds(200));
        Assert.True(f.CommittedMessages().Any() || await Client.ListOffsetAsync("adm-full", 1, OffsetAt.Latest) > 0, "the stream is untouched");

        await Client.DeleteIfEmptyAsync("adm-void");
        await RealWaits.OnEveryNodeAsync(_cluster, "forgot adm-void", "adm-void", r => r is null);
        await Client.DeleteAsync("adm-full"); // an unconditional delete takes the data with it
    }

    // ---- purge and requeue ----

    [LotseFact]
    public async Task APurgeSettlesTheReadyMessagesAndLeavesWhatAConsumerHolds()
    {
        await Client.CreateQueueAsync("adm-purge");
        await ProduceAsync("adm-purge", 6, "p");
        await SettledAsync("adm-purge");
        Assert.Equal(6UL, await Client.PurgeAsync("adm-purge"));
        Assert.Equal(0UL, await Client.PurgeAsync("adm-purge")); // nothing is ready any more
        await using (QueueConsumer c = await Client.SubscribeAsync("adm-purge"))
        {
            Assert.Null(await c.PollAsync(TimeSpan.FromSeconds(1))); // purged messages are not delivered
        }

        await ProduceAsync("adm-purge", 3, "q");
        await using QueueConsumer holder = await Client.SubscribeAsync("adm-purge", SubscribeOptions.WithCredit(1));
        Delivery held = await Take(holder);
        // One is out with the consumer, one is buffered behind the credit; a purge settles what is ready and not held.
        ulong purged = await Client.PurgeAsync("adm-purge");
        Assert.InRange(purged, 1UL, 2UL);
        Assert.Equal("q-0", held.Value!.ToUtf8());
        await holder.AckAsync(held); // what a consumer holds is still its own
    }

    [LotseFact]
    public async Task ARequeueMovesDeadLettersBackToTheirQueue()
    {
        await Client.CreateQueueAsync("adm-dead");
        await Client.CreateQueueAsync("adm-work", QueueOptions.CreateBuilder().WithMaxDeliveries(1).WithDeadLetterQueue("adm-dead").Build());
        await SettledAsync("adm-work");
        await SettledAsync("adm-dead");
        await ProduceAsync("adm-work", 3, "w");
        await using (QueueConsumer c = await Client.SubscribeAsync("adm-work", SubscribeOptions.WithCredit(8)))
        {
            for (int i = 0; i < 3; i++)
            {
                await c.NackAsync(await Take(c), true); // one delivery is all a message gets
            }
        }
        // They arrive in the dead-letter queue (the leader moves them), and the work queue is empty.
        await RealWaits.UntilAsync("three dead letters", async () =>
        {
            Response.Metadata _ = await Client.MetadataAsync(new[] { "adm-dead" });
            return (await Client.NodeMetricsAsync())
                .Where(n => n.Metrics is not null)
                .SelectMany(n => n.Metrics!.Queues)
                .Any(q => q.Ready == 3);
        });

        Assert.Equal(3UL, await Client.RequeueAsync("adm-dead"));
        Assert.Equal(0UL, await Client.RequeueAsync("adm-dead")); // each moves once
        await using (QueueConsumer c = await Client.SubscribeAsync("adm-work", SubscribeOptions.WithCredit(8)))
        {
            var seen = new List<string>();
            for (int i = 0; i < 3; i++)
            {
                Delivery d = await Take(c);
                seen.Add(d.Value!.ToUtf8());
                await c.AckAsync(d);
            }
            Assert.Equal(new[] { "w-0", "w-1", "w-2" }, seen.OrderBy(x => x, StringComparer.Ordinal).ToArray());
        }
        await using QueueConsumer dead = await Client.SubscribeAsync("adm-dead");
        Assert.Null(await dead.PollAsync(TimeSpan.FromSeconds(1)));

        // A queue that is not a dead-letter queue has nothing to requeue.
        Assert.Equal(0UL, await Client.RequeueAsync("adm-work"));
    }

    // ---- settings ----

    [LotseFact]
    public async Task StreamSettingsAreChangedResetAndReadWithTheirSource()
    {
        await Client.CreateStreamAsync("adm-cfg-s", 1, 3);
        Dictionary<string, Messages.ConfigEntry> start = ByKey(await Client.DescribeConfigsAsync("adm-cfg-s"));
        Assert.Equal(new[] { "cleanup.policy", "delete.retention.ms", "retention.bytes", "retention.ms" }, start.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
        Assert.All(start.Values, e => Assert.Equal(Messages.ConfigSource.Default, e.Source));
        Assert.Equal("delete", start["cleanup.policy"].Value);
        Assert.Equal("-1", start["retention.ms"].Value); // the node's default in this cluster file

        await Client.AlterConfigsAsync(
            "adm-cfg-s",
            new Dictionary<string, string> { ["retention.ms"] = "7200000", ["retention.bytes"] = "1048576", ["cleanup.policy"] = "compact,delete" });
        Dictionary<string, Messages.ConfigEntry> changed = ByKey(await Client.DescribeConfigsAsync("adm-cfg-s"));
        Assert.Equal(("7200000", Messages.ConfigSource.Dynamic), (changed["retention.ms"].Value, changed["retention.ms"].Source));
        Assert.Equal(("1048576", Messages.ConfigSource.Dynamic), (changed["retention.bytes"].Value, changed["retention.bytes"].Source));
        Assert.Equal(("compact,delete", Messages.ConfigSource.Dynamic), (changed["cleanup.policy"].Value, changed["cleanup.policy"].Source));
        Assert.Equal(Messages.ConfigSource.Default, changed["delete.retention.ms"].Source);

        await Client.AlterConfigsAsync("adm-cfg-s", null, new[] { "retention.ms", "cleanup.policy" });
        Dictionary<string, Messages.ConfigEntry> reset = ByKey(await Client.DescribeConfigsAsync("adm-cfg-s"));
        Assert.Equal(("-1", Messages.ConfigSource.Default), (reset["retention.ms"].Value, reset["retention.ms"].Source));
        Assert.Equal(("delete", Messages.ConfigSource.Default), (reset["cleanup.policy"].Value, reset["cleanup.policy"].Source));
        Assert.Equal(("1048576", Messages.ConfigSource.Dynamic), (reset["retention.bytes"].Value, reset["retention.bytes"].Source));

        await Client.DeleteAsync("adm-cfg-s");
    }

    [LotseFact]
    public async Task QueueSettingsAreChangedResetAndReadWithTheirSource()
    {
        await Client.CreateQueueAsync("adm-cfg-dead");
        await Client.CreateQueueAsync("adm-cfg-q", QueueOptions.CreateBuilder().WithAckTimeout(TimeSpan.FromSeconds(5)).Build());
        Dictionary<string, Messages.ConfigEntry> start = ByKey(await Client.DescribeConfigsAsync("adm-cfg-q"));
        Assert.Equal(("5000", Messages.ConfigSource.Dynamic), (start["ack.timeout.ms"].Value, start["ack.timeout.ms"].Source));
        Assert.Equal(("0", Messages.ConfigSource.Default), (start["max.deliveries"].Value, start["max.deliveries"].Source));

        await Client.AlterConfigsAsync(
            "adm-cfg-q",
            new Dictionary<string, string>
            {
                ["ack.timeout.ms"] = "7000",
                ["max.deliveries"] = "4",
                ["message.ttl.ms"] = "60000",
                ["dead.letter.queue"] = "adm-cfg-dead",
            });
        Dictionary<string, Messages.ConfigEntry> changed = ByKey(await Client.DescribeConfigsAsync("adm-cfg-q"));
        Assert.Equal("7000", changed["ack.timeout.ms"].Value);
        Assert.Equal(("4", Messages.ConfigSource.Dynamic), (changed["max.deliveries"].Value, changed["max.deliveries"].Source));
        Assert.Equal(("60000", Messages.ConfigSource.Dynamic), (changed["message.ttl.ms"].Value, changed["message.ttl.ms"].Source));
        Assert.Equal(("adm-cfg-dead", Messages.ConfigSource.Dynamic), (changed["dead.letter.queue"].Value, changed["dead.letter.queue"].Source));

        await Client.AlterConfigsAsync("adm-cfg-q", null, new[] { "max.deliveries", "dead.letter.queue" });
        Dictionary<string, Messages.ConfigEntry> reset = ByKey(await Client.DescribeConfigsAsync("adm-cfg-q"));
        Assert.Equal(("0", Messages.ConfigSource.Default), (reset["max.deliveries"].Value, reset["max.deliveries"].Source));
        Assert.Equal(("", Messages.ConfigSource.Default), (reset["dead.letter.queue"].Value, reset["dead.letter.queue"].Source));
        Assert.Equal("60000", reset["message.ttl.ms"].Value);
        Assert.Equal("7000", reset["ack.timeout.ms"].Value);

        await Client.DeleteAsync("adm-cfg-q");
        await Client.DeleteAsync("adm-cfg-dead");
    }

    [LotseFact]
    public async Task SettingsTheNodeDoesNotAcceptAreRefusedWithTheirReason()
    {
        await Client.CreateStreamAsync("adm-refuse-s", 1, 3);
        await Client.CreateQueueAsync("adm-refuse-q");

        var wrongKey = await Assert.ThrowsAsync<BrokerErrorException>(
            () => Client.AlterConfigsAsync("adm-refuse-s", new Dictionary<string, string> { ["ack.timeout.ms"] = "1000" }));
        Assert.Equal(ErrorCode.InvalidRequest, wrongKey.Code);
        Assert.Contains("streams have no setting ack.timeout.ms", wrongKey.BrokerMessage);

        var wrongValue = await Assert.ThrowsAsync<BrokerErrorException>(
            () => Client.AlterConfigsAsync("adm-refuse-s", new Dictionary<string, string> { ["retention.ms"] = "soon" }));
        Assert.Equal(ErrorCode.InvalidRequest, wrongValue.Code);
        Assert.Contains("retention.ms = soon", wrongValue.BrokerMessage);

        var twice = await Assert.ThrowsAsync<BrokerErrorException>(
            () => Client.AlterConfigsAsync("adm-refuse-q", new Dictionary<string, string> { ["ack.timeout.ms"] = "1000" }, new[] { "ack.timeout.ms" }));
        Assert.Equal(ErrorCode.InvalidRequest, twice.Code);
        Assert.Contains("given twice", twice.BrokerMessage);

        var notFound = await Assert.ThrowsAsync<BrokerErrorException>(
            () => Client.AlterConfigsAsync("adm-refuse-q", new Dictionary<string, string> { ["dead.letter.queue"] = "no-such-queue" }));
        Assert.Equal(ErrorCode.InvalidRequest, notFound.Code);

        await Assert.ThrowsAsync<UnknownStreamException>(() => Client.AlterConfigsAsync("no-such-resource", new Dictionary<string, string>()));
        Dictionary<string, Messages.ConfigEntry> untouched = ByKey(await Client.DescribeConfigsAsync("adm-refuse-s"));
        Assert.Equal(Messages.ConfigSource.Default, untouched["retention.ms"].Source);
        await Client.DeleteAsync("adm-refuse-s");
        await Client.DeleteAsync("adm-refuse-q");
    }

    // ---- partitions ----

    [LotseFact]
    public async Task APartitionCountIsRaisedOnEveryNodeAndNeverLowered()
    {
        await Client.CreateStreamAsync("adm-grow", 1, 3);
        await Client.ProduceAsync("adm-grow", 0, OutMessage.FromValue("first"));
        await Client.CreatePartitionsAsync("adm-grow", 3);
        await RealWaits.OnEveryNodeAsync(
            _cluster, "three partitions of adm-grow", "adm-grow", r => r is { } x && x.Partitions.Count == 3 && x.Partitions.All(p => p.Leader != 0));
        ProduceResult r = await Client.ProduceAsync("adm-grow", 2, OutMessage.FromValue("in the new one"));
        Fetched f = await Client.FetchAsync("adm-grow", 2, r.BaseOffset, TimeSpan.FromSeconds(5));
        Assert.Equal("in the new one", f.CommittedMessages().First().Value!.ToUtf8());
        Fetched old = await Client.FetchAsync("adm-grow", 0, 0, TimeSpan.FromSeconds(5));
        Assert.Contains(old.CommittedMessages(), m => m.Value!.ToUtf8() == "first"); // the old partition keeps its records

        var shrink = await Assert.ThrowsAsync<BrokerErrorException>(() => Client.CreatePartitionsAsync("adm-grow", 2));
        Assert.Equal(ErrorCode.InvalidRequest, shrink.Code);
        var same = await Assert.ThrowsAsync<BrokerErrorException>(() => Client.CreatePartitionsAsync("adm-grow", 3));
        Assert.Equal(ErrorCode.InvalidRequest, same.Code);
        await Assert.ThrowsAsync<UnknownStreamException>(() => Client.CreatePartitionsAsync("adm-no-such", 3));
        await Client.DeleteAsync("adm-grow");
    }

    // ---- nodes and metrics ----

    [LotseFact]
    public async Task TheNodesAreListedAsTheMetadataGroupKnowsThem()
    {
        IReadOnlyList<Messages.NodeEntry> nodes = await Client.DescribeNodesAsync();
        Assert.Equal(new ulong[] { 1, 2, 3 }, nodes.Select(n => n.NodeId).OrderBy(i => i).ToArray());
        foreach (Messages.NodeEntry n in nodes)
        {
            Assert.Equal(Messages.NodeState.Active, n.State);
            Assert.Equal(_cluster.AddressOf((int)n.NodeId).ToString(), n.Client);
            Assert.StartsWith("127.0.0.1:", n.Peer);
            Assert.Equal(0u, n.Moving);
        }
        Assert.All(nodes, n => Assert.True(n.Replicas >= 2, $"node {n.NodeId} is a voter of the metadata and coordinator groups"));
    }

    [LotseFact]
    public async Task EveryNodeAnswersForItsOwnMetrics()
    {
        // The id from the answer: a node asked for metadata right after may not know the queue yet.
        ulong queueId = await Client.CreateQueueAsync("adm-metrics");
        // Produce counters are the leader's, in memory: the leader stays where it took the produce.
        await SettledAsync("adm-metrics");
        await ProduceAsync("adm-metrics", 4, "m");
        IReadOnlyList<NodeMetrics> all = await Client.NodeMetricsAsync();
        Assert.Equal(new uint[] { 1, 2, 3 }, all.Select(n => n.NodeId).OrderBy(i => i).ToArray());
        foreach (NodeMetrics n in all)
        {
            Assert.Null(n.Error);
            Response.DescribeMetrics m = n.Metrics!;
            Assert.Equal(n.NodeId, m.NodeId);
            Assert.False(string.IsNullOrEmpty(m.Version));
            Assert.NotEqual(0UL, m.MetadataGroup);
            Assert.NotEqual(0UL, m.CoordinatorGroup);
            Assert.NotEqual(0UL, m.ClientInBytes);
        }
        // Exactly one node leads the queue, and it counts the four messages.
        List<Messages.PartitionMetrics> led = all.SelectMany(n => n.Metrics!.Partitions).Where(p => p.ResourceId == queueId && p.Leader).ToList();
        Assert.Single(led);
        Assert.True(led[0].ProduceRecords >= 4, $"{led[0].ProduceRecords} records produced");
        Assert.Equal(3, all.SelectMany(n => n.Metrics!.Partitions).Count(p => p.ResourceId == queueId)); // one replica per node
        await Client.DeleteAsync("adm-metrics");
    }

    // ---- pins ----

    [LotseFact]
    public async Task ALeaderIsPinnedToANodeAndUnpinned()
    {
        await Client.CreateQueueAsync("adm-pin");
        await SettledAsync("adm-pin");
        uint current = await LeaderOfAsync("adm-pin");
        uint target = current % 3 + 1;
        await Client.MoveLeaderAsync("adm-pin", 0, target);
        await RealWaits.UntilAsync($"node {target} to lead adm-pin", async () => await LeaderOfAsync("adm-pin") == target);
        await SettledAsync("adm-pin");
        Assert.Equal(target, await LeaderOfAsync("adm-pin"));

        // The pin holds: it survives a moment and the queue still works.
        await Task.Delay(1500);
        Assert.Equal(target, await LeaderOfAsync("adm-pin"));
        await Client.ProduceAsync("adm-pin", 0, OutMessage.FromValue("pinned"));

        uint other = target % 3 + 1;
        await Client.MoveLeaderAsync("adm-pin", 0, other);
        await RealWaits.UntilAsync($"node {other} to lead adm-pin", async () => await LeaderOfAsync("adm-pin") == other);
        await Client.MoveLeaderAsync("adm-pin", 0, null); // unpinned: leaders spread by themselves again
        await Client.ProduceAsync("adm-pin", 0, OutMessage.FromValue("unpinned"));

        var notAVoter = await Assert.ThrowsAsync<BrokerErrorException>(() => Client.MoveLeaderAsync("adm-pin", 0, 99));
        Assert.Equal(ErrorCode.InvalidRequest, notAVoter.Code);
        await Assert.ThrowsAsync<UnknownStreamException>(() => Client.MoveLeaderAsync("adm-no-such", 0, 1));
        await Client.DeleteAsync("adm-pin");
    }

    // ---- who am I ----

    [LotseFact]
    public async Task OnANodeWithoutAuthenticationTheClientIsAnonymous()
    {
        Response.WhoAmI who = await Client.WhoAmIAsync();
        Assert.Equal(Messages.PrincipalKind.Anonymous, who.Kind);
        Assert.Empty(who.Groups);
    }
}

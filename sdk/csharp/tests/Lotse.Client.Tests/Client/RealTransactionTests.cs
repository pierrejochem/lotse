using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// Transactions and publishing against three real <c>lotse</c> processes on loopback (plain TCP, no authentication):
/// commit and abort seen through read_committed, a producer fenced by a newer epoch, exactly-once
/// consume-transform-produce from a stream and from a queue, a message two transactions cannot both hold, and
/// publishing through a topic exchange, outside a transaction (a node routes) and inside one (this client routes).
/// Skipped, with a message, when <c>LOTSE_BIN</c> is not set.
/// </summary>
[Collection("real cluster")]
public sealed class RealTransactionTests : IClassFixture<RealCluster>, IAsyncLifetime
{
    private readonly RealCluster _cluster;
    private readonly List<LotseClient> _clients = new();

    public RealTransactionTests(RealCluster cluster) => _cluster = cluster;

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync()
    {
        foreach (LotseClient c in _clients)
        {
            c.Dispose();
        }
        return Task.CompletedTask;
    }

    private LotseClient NewClient(string? txnId = null)
    {
        ClientConfig.Builder b = ClientConfig.CreateBuilder(_cluster.Bootstrap)
            .PlaintextOnLoopback()
            .WithRequestTimeout(TimeSpan.FromSeconds(5))
            .WithRetryFor(TimeSpan.FromSeconds(60));
        if (txnId is not null)
        {
            b = b.WithTransactionalId(txnId);
        }
        var client = new LotseClient(b.Build());
        _clients.Add(client);
        return client;
    }


    private async Task<int> LeaderOfAsync(LotseClient client, string queue)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            Response.Metadata m = await client.MetadataAsync(new[] { queue });
            foreach (Messages.ResourceMetadata r in m.Resources)
            {
                foreach (Messages.PartitionMetadata p in r.Partitions)
                {
                    if (r.Name == queue && p.Partition == 0 && p.Leader != 0)
                    {
                        return (int)p.Leader;
                    }
                }
            }
            await Task.Delay(100);
        }
        throw new Xunit.Sdk.XunitException($"no leader for {queue}\n{_cluster.Log(1)}");
    }

    /// <summary>
    /// Creates a queue and waits until its leader has stayed the same for half a second. A queue's group elects its leader
    /// in its first moments; a leader that changes while a test holds a message ends the subscription, which is the
    /// failover behaviour these tests do not want to measure.
    /// </summary>
    private async Task CreateQueueAsync(LotseClient client, string name)
    {
        await client.CreateQueueAsync(name);
        int leader = await LeaderOfAsync(client, name);
        int same = 0;
        while (same < 5)
        {
            await Task.Delay(100);
            int now = await LeaderOfAsync(client, name);
            same = now == leader ? same + 1 : 0;
            leader = now;
        }
    }

    private static List<string> Texts(IEnumerable<BatchRecord> records) => records.Select(r => r.Value!.ToUtf8()).ToList();

    /// <summary>What a read_committed reader sees of a partition from its start, in one pass over what is readable now.</summary>
    private static async Task<(List<string> Values, ulong LastStable, ulong End)> ReadNowAsync(LotseClient reader, string stream, int partition)
    {
        var values = new List<string>();
        ulong offset = 0;
        ulong stable;
        while (true)
        {
            Fetched f = await reader.FetchAsync(stream, partition, offset, TimeSpan.FromMilliseconds(200));
            values.AddRange(Texts(f.CommittedMessages()));
            stable = f.LastStableOffset;
            ulong next = f.NextOffset(offset);
            if (next == offset || next >= stable)
            {
                return (values, stable, next);
            }
            offset = next;
        }
    }

    private async Task<List<string>> ReadUntilAsync(LotseClient reader, string stream, int partition, int count)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            (List<string> values, _, _) = await ReadNowAsync(reader, stream, partition);
            if (values.Count >= count)
            {
                return values;
            }
            await Task.Delay(100);
        }
        throw new Xunit.Sdk.XunitException($"fewer than {count} records in {stream}/{partition}\n{_cluster.Log(1)}");
    }

    private static async Task<Delivery?> TakeAsync(QueueConsumer c, double seconds = 30) =>
        await c.PollAsync(TimeSpan.FromSeconds(seconds));

    [LotseFact]
    public async Task CommittedWritesBecomeVisibleTogetherAndAbortedOnesNever()
    {
        LotseClient admin = NewClient();
        await admin.CreateStreamAsync("vis", 2, 3);
        LotseClient lotse = NewClient("vis-txn");

        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("vis", 0, new[] { OutMessage.FromValue("c0-a"), OutMessage.FromValue("c0-b") });
            await txn.ProduceAsync("vis", 1, OutMessage.FromValue("c1-a"));

            // Open: the last stable offset holds readers back from both partitions.
            Assert.Empty((await ReadNowAsync(admin, "vis", 0)).Values);
            Assert.Empty((await ReadNowAsync(admin, "vis", 1)).Values);
            Fetched peek = await admin.FetchUncommittedAsync("vis", 0, 0, TimeSpan.FromMilliseconds(200));
            Assert.False(peek.Records.IsEmpty); // a log inspector sees the open batch
            await txn.CommitAsync();
        }
        Assert.Equal(new[] { "c0-a", "c0-b" }, (await ReadUntilAsync(admin, "vis", 0, 2)).ToArray());
        Assert.Equal(new[] { "c1-a" }, (await ReadUntilAsync(admin, "vis", 1, 1)).ToArray());

        // An aborted transaction leaves nothing, and does not hold the readers back afterwards.
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("vis", 0, OutMessage.FromValue("gone-0"));
            await txn.ProduceAsync("vis", 1, OutMessage.FromValue("gone-1"));
            await txn.AbortAsync();
        }
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("vis", 0, OutMessage.FromValue("c0-c"));
            await txn.CommitAsync();
        }
        List<string> zero = await ReadUntilAsync(admin, "vis", 0, 3);
        Assert.Equal(new[] { "c0-a", "c0-b", "c0-c" }, zero.ToArray());
        Assert.Equal(new[] { "c1-a" }, (await ReadNowAsync(admin, "vis", 1)).Values.ToArray());
    }

    [LotseFact]
    public async Task ATransactionAbandonedByAnExceptionIsAbortedAndLetsReadersPass()
    {
        LotseClient admin = NewClient();
        await admin.CreateStreamAsync("abandon", 1, 3);
        LotseClient lotse = NewClient("abandon-txn");
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using Transaction txn = await lotse.BeginTransactionAsync();
            await txn.ProduceAsync("abandon", 0, OutMessage.FromValue("never"));
            throw new InvalidOperationException("the work failed");
        });
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("abandon", 0, OutMessage.FromValue("after"));
            await txn.CommitAsync();
        }
        Assert.Equal(new[] { "after" }, (await ReadUntilAsync(admin, "abandon", 0, 1)).ToArray());
        (List<string> values, ulong stable, ulong end) = await ReadNowAsync(admin, "abandon", 0);
        Assert.Equal(new[] { "after" }, values.ToArray());
        Assert.Equal(end, stable); // no open transaction holds the readers
    }

    [LotseFact]
    public async Task ACancelledProduceLeavesOnlyAnAbortAndTheAbortWorks()
    {
        LotseClient admin = NewClient();
        await admin.CreateStreamAsync("cancel", 1, 3);
        LotseClient lotse = NewClient("cancel-txn");
        await using Transaction txn = await lotse.BeginTransactionAsync();
        await txn.ProduceAsync("cancel", 0, OutMessage.FromValue("first"));
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => txn.ProduceAsync("cancel", 0, OutMessage.FromValue("second"), cancelled.Token));
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
        await txn.AbortAsync();
        Assert.Empty((await ReadNowAsync(admin, "cancel", 0)).Values);
    }

    [LotseFact]
    public async Task AProducerFencedByANewerEpochCannotCommit()
    {
        LotseClient admin = NewClient();
        await admin.CreateStreamAsync("fence", 1, 3);
        LotseClient older = NewClient("fence-txn");
        LotseClient newer = NewClient("fence-txn");

        await using Transaction stale = await older.BeginTransactionAsync();
        await stale.ProduceAsync("fence", 0, OutMessage.FromValue("from-older"));

        // The same transactional id: the coordinator raises the epoch, fences the older client and aborts what it left open.
        await using Transaction fresh = await newer.BeginTransactionAsync();
        Assert.True(fresh.Epoch > stale.Epoch);
        await fresh.ProduceAsync("fence", 0, OutMessage.FromValue("from-newer"));

        var late = await Assert.ThrowsAsync<BrokerErrorException>(() => stale.ProduceAsync("fence", 0, OutMessage.FromValue("late")));
        Assert.Equal(ErrorCode.ProducerFenced, late.Code);
        var refused = await Assert.ThrowsAsync<MustAbortException>(() => stale.CommitAsync());
        Assert.Contains("fenced", refused.Message, StringComparison.OrdinalIgnoreCase);
        await stale.AbortAsync().ContinueWith(_ => { }); // the abort is refused as well; it only frees the client

        await fresh.CommitAsync();
        Assert.Equal(new[] { "from-newer" }, (await ReadUntilAsync(admin, "fence", 0, 1)).ToArray());
        Assert.Equal(new[] { "from-newer" }, (await ReadNowAsync(admin, "fence", 0)).Values.ToArray());
    }

    [LotseFact]
    public async Task ExactlyOnceConsumeTransformProduceFromAStream()
    {
        LotseClient admin = NewClient();
        await RealWaits.CreateStreamEverywhereAsync(_cluster, admin, "eos-in", 1);
        await admin.CreateStreamAsync("eos-out", 1, 3);
        for (int i = 0; i < 10; i++)
        {
            await admin.ProduceAsync("eos-in", 0, OutMessage.FromValue("m" + i));
        }

        LotseClient worker = NewClient("eos-worker");
        GroupConfig config = GroupConfig.CreateBuilder().WithAutoCommit(false).Build();
        await using (GroupConsumer g = await worker.JoinGroupAsync("eos-group", new[] { "eos-in" }, config))
        {
            int done = 0;
            bool failedOnce = false;
            DateTime deadline = DateTime.UtcNow.AddSeconds(90);
            while (done < 10)
            {
                Assert.True(DateTime.UtcNow < deadline, "timed out; the group consumer saw " + done);
                IReadOnlyList<GroupRecord> records = await g.PollAsync(TimeSpan.FromMilliseconds(500));
                if (records.Count == 0)
                {
                    continue;
                }
                await using Transaction txn = await worker.BeginTransactionAsync();
                foreach (GroupRecord r in records)
                {
                    await txn.ProduceAsync("eos-out", 0, OutMessage.FromValue(r.Record.Value!.ToUtf8().ToUpperInvariant()));
                }
                await txn.SendOffsetsAsync(g);
                if (!failedOnce)
                {
                    // The work fails after it wrote and sent the offsets: none of it may count.
                    failedOnce = true;
                    await txn.AbortAsync();
                    g.Rewind();
                    continue;
                }
                await txn.CommitAsync();
                done += records.Count;
            }
        }

        List<string> output = await ReadUntilAsync(admin, "eos-out", 0, 10);
        await Task.Delay(500);
        output = (await ReadNowAsync(admin, "eos-out", 0)).Values;
        Assert.Equal(Enumerable.Range(0, 10).Select(i => "M" + i).ToArray(), output.ToArray()); // every message once, in order

        // The offsets went with the transaction: the next member starts after the last message, and not before.
        await using GroupConsumer next = await worker.JoinGroupAsync("eos-group", new[] { "eos-in" }, config);
        DateTime until = DateTime.UtcNow.AddSeconds(60);
        while (!next.Positions.ContainsKey("eos-in"))
        {
            Assert.True(DateTime.UtcNow < until, "the next member got no partition");
            Assert.Empty(await next.PollAsync(TimeSpan.FromMilliseconds(300)));
        }
        Assert.Equal(await admin.ListOffsetAsync("eos-in", 0, OffsetAt.Latest), next.Positions["eos-in"][0]);
    }

    [LotseFact]
    public async Task OffsetsOfAnAbortedTransactionAreNotCommitted()
    {
        LotseClient admin = NewClient();
        await RealWaits.CreateStreamEverywhereAsync(_cluster, admin, "abort-in", 1);
        for (int i = 0; i < 3; i++)
        {
            await admin.ProduceAsync("abort-in", 0, OutMessage.FromValue("m" + i));
        }
        LotseClient worker = NewClient("abort-worker");
        GroupConfig config = GroupConfig.CreateBuilder().WithAutoCommit(false).Build();
        await using (GroupConsumer g = await worker.JoinGroupAsync("abort-group", new[] { "abort-in" }, config))
        {
            IReadOnlyList<GroupRecord> records = Array.Empty<GroupRecord>();
            DateTime deadline = DateTime.UtcNow.AddSeconds(60);
            while (records.Count == 0)
            {
                Assert.True(DateTime.UtcNow < deadline, "no records");
                records = await g.PollAsync(TimeSpan.FromMilliseconds(300));
            }
            await using Transaction txn = await worker.BeginTransactionAsync();
            await txn.SendOffsetsAsync(g);
            await txn.AbortAsync();
        }
        await using GroupConsumer again = await worker.JoinGroupAsync("abort-group", new[] { "abort-in" }, config);
        Assert.Equal(3, await CountAsync(again));
    }

    private static async Task<int> CountAsync(GroupConsumer g)
    {
        int seen = 0;
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        while (seen < 3 && DateTime.UtcNow < deadline)
        {
            seen += (await g.PollAsync(TimeSpan.FromMilliseconds(300))).Count;
        }
        return seen;
    }

    [LotseFact]
    public async Task ExactlyOnceConsumeTransformProduceFromAQueue()
    {
        LotseClient admin = NewClient();
        await CreateQueueAsync(admin, "eos-q");
        await admin.CreateStreamAsync("eos-q-out", 1, 3);
        for (int i = 0; i < 5; i++)
        {
            await admin.ProduceAsync("eos-q", 0, OutMessage.FromValue("job-" + i));
        }

        LotseClient worker = NewClient("eos-q-worker");
        await using (QueueConsumer c = await worker.SubscribeAsync("eos-q", SubscribeOptions.WithCredit(5)))
        {
            int settled = 0;
            DateTime deadline = DateTime.UtcNow.AddSeconds(90);
            while (settled < 5)
            {
                Assert.True(DateTime.UtcNow < deadline, "timed out after " + settled);
                Delivery d = (await TakeAsync(c))!;
                await using Transaction txn = await worker.BeginTransactionAsync();
                try
                {
                    await c.AckInAsync(txn, d);
                    await txn.ProduceAsync("eos-q-out", 0, OutMessage.FromValue("done-" + d.Value!.ToUtf8()));
                    await txn.CommitAsync();
                    settled++;
                }
                catch (BrokerErrorException e) when (e.Code.Equals(ErrorCode.NotLeader) || e.Code.Equals(ErrorCode.MessageNotHeld))
                {
                    // The queue's leader changed under load: the message went back, and the transaction is aborted.
                    // The work is done again for whatever the queue delivers next.
                    await txn.AbortAsync();
                }
            }
        }
        List<string> output = await ReadUntilAsync(admin, "eos-q-out", 0, 5);
        Assert.Equal(5, output.Count);
        Assert.Equal(5, output.Distinct().Count());

        // Every ack was committed: the queue holds nothing for the next consumer.
        await using QueueConsumer again = await admin.SubscribeAsync("eos-q");
        Assert.Null(await again.PollAsync(TimeSpan.FromSeconds(2)));
    }

    [LotseFact]
    public async Task AnAbortedAckGivesTheMessageBack()
    {
        LotseClient admin = NewClient();
        await CreateQueueAsync(admin, "eos-q2");
        await admin.CreateStreamAsync("eos-q2-out", 1, 3);
        await admin.ProduceAsync("eos-q2", 0, OutMessage.FromValue("only"));

        LotseClient worker = NewClient("eos-q2-worker");
        await using QueueConsumer c = await worker.SubscribeAsync("eos-q2");
        Delivery first = (await TakeAsync(c))!;
        Assert.Equal(1, first.DeliveryCount);
        await using (Transaction txn = await worker.BeginTransactionAsync())
        {
            await c.AckInAsync(txn, first);
            await txn.ProduceAsync("eos-q2-out", 0, OutMessage.FromValue("done"));
            await txn.AbortAsync();
        }
        Delivery again = (await TakeAsync(c))!;
        Assert.Equal("only", again.Value!.ToUtf8());
        Assert.True(again.DeliveryCount >= 2);
        Assert.Empty((await ReadNowAsync(admin, "eos-q2-out", 0)).Values); // the aborted write is not seen
        await c.AckAsync(again);
    }

    [LotseFact]
    public async Task TwoTransactionsCannotHoldTheSameMessage()
    {
        LotseClient admin = NewClient();
        await CreateQueueAsync(admin, "held");
        await admin.ProduceAsync("held", 0, OutMessage.FromValue("one"));

        LotseClient x = NewClient("held-x");
        LotseClient y = NewClient("held-y");
        await using QueueConsumer c = await admin.SubscribeAsync("held");
        Delivery d = (await TakeAsync(c))!;

        await using Transaction first = await x.BeginTransactionAsync();
        await using Transaction second = await y.BeginTransactionAsync();
        await c.AckInAsync(first, d);
        var refused = await Assert.ThrowsAsync<BrokerErrorException>(() => c.AckInAsync(second, d));
        Assert.Equal(ErrorCode.MessageNotHeld, refused.Code);
        await Assert.ThrowsAsync<MustAbortException>(() => second.CommitAsync());
        await second.AbortAsync();
        await first.CommitAsync();

        await using QueueConsumer after = await admin.SubscribeAsync("held");
        Assert.Null(await after.PollAsync(TimeSpan.FromSeconds(2))); // the first transaction settled it
    }

    // ---- publishing ----

    [LotseFact]
    public async Task APublishThroughATopicExchangeReachesBothQueues()
    {
        LotseClient admin = NewClient();
        await admin.DeclareExchangeAsync("events", Messages.ExchangeType.Topic);
        await CreateQueueAsync(admin, "audit");
        await CreateQueueAsync(admin, "billing");
        await CreateQueueAsync(admin, "other");
        await admin.BindAsync("events", "audit", "order.#");
        await admin.BindAsync("events", "billing", "*.paid");
        await admin.BindAsync("events", "other", "refund.*");

        // A node routes by the bindings it has applied; the binding may take a moment to reach it.
        Published published = await PublishUntilRoutedAsync(admin, "events", "order.paid", OutMessage.FromValue("o-1"));
        Assert.Equal(new[] { "audit", "billing" }, published.Routed.Select(r => r.Queue).ToArray());

        foreach (RoutedQueue routed in published.Routed)
        {
            await using QueueConsumer c = await admin.SubscribeAsync(routed.Queue);
            Delivery d = (await TakeAsync(c))!;
            Assert.Equal("o-1", d.Value!.ToUtf8());
            Assert.Equal(routed.Offset, d.Offset); // the offset the publish named
            await c.AckAsync(d);
        }
        await using QueueConsumer none = await admin.SubscribeAsync("other");
        Assert.Null(await none.PollAsync(TimeSpan.FromSeconds(1)));

        await Assert.ThrowsAsync<UnroutableException>(() => admin.PublishAsync("events", "nothing.here", OutMessage.FromValue("x"), mandatory: true));
        Published dropped = await admin.PublishAsync("events", "nothing.here", OutMessage.FromValue("x"));
        Assert.Empty(dropped.Routed);
        await Assert.ThrowsAsync<UnknownStreamException>(() => admin.PublishAsync("no-such-exchange", "k", OutMessage.FromValue("x")));
    }

    private static async Task<Published> PublishUntilRoutedAsync(LotseClient client, string exchange, string key, OutMessage message)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            try
            {
                return await client.PublishAsync(exchange, key, message, mandatory: true);
            }
            catch (UnroutableException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(100);
            }
        }
    }

    [LotseFact]
    public async Task APublishInATransactionIsRoutedByTheClientAndAtomic()
    {
        LotseClient admin = NewClient();
        await admin.DeclareExchangeAsync("txn-events", Messages.ExchangeType.Topic);
        await CreateQueueAsync(admin, "txn-a");
        await CreateQueueAsync(admin, "txn-b");
        await admin.BindAsync("txn-events", "txn-a", "t.#");
        await admin.BindAsync("txn-events", "txn-b", "t.*");

        LotseClient lotse = NewClient("publish-txn");
        await using QueueConsumer a = await admin.SubscribeAsync("txn-a");
        await using QueueConsumer b = await admin.SubscribeAsync("txn-b");

        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            Published p = await txn.PublishAsync("txn-events", "t.x", OutMessage.FromValue("aborted"));
            Assert.Equal(new[] { "txn-a", "txn-b" }, p.Routed.Select(r => r.Queue).ToArray());
            await txn.AbortAsync();
        }
        Assert.Null(await a.PollAsync(TimeSpan.FromSeconds(1)));
        Assert.Null(await b.PollAsync(TimeSpan.FromSeconds(1)));

        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.PublishAsync("txn-events", "t.y", OutMessage.FromValue("kept"));
            Assert.Null(await a.PollAsync(TimeSpan.FromMilliseconds(500))); // not before the commit
            await txn.CommitAsync();
        }
        Delivery da = (await TakeAsync(a))!;
        Delivery db = (await TakeAsync(b))!;
        Assert.Equal("kept", da.Value!.ToUtf8());
        Assert.Equal("kept", db.Value!.ToUtf8());
        await a.AckAsync(da);
        await b.AckAsync(db);

        // A binding made through this client is seen by its next transactional publish, whatever the metadata age.
        await CreateQueueAsync(lotse, "txn-c");
        await lotse.BindAsync("txn-events", "txn-c", "t.#");
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            Published p = await txn.PublishAsync("txn-events", "t.z", OutMessage.FromValue("three"));
            Assert.Equal(new[] { "txn-a", "txn-b", "txn-c" }, p.Routed.Select(r => r.Queue).ToArray());
            await txn.AbortAsync();
        }
    }

    [LotseFact]
    public async Task APublishWhoseAnswerIsLostIsUnknownAndWrittenOnce()
    {
        LotseClient admin = NewClient();
        await admin.DeclareExchangeAsync("lost-x", Messages.ExchangeType.Fanout);
        await CreateQueueAsync(admin, "lost-q");
        await admin.BindAsync("lost-x", "lost-q", string.Empty);
        await PublishUntilRoutedAsync(admin, "lost-x", "k", OutMessage.FromValue("warm-up"));

        // A node in this process that forwards every request to a real node and tells the client that the real node
        // is the only one there is. When armed, it drops the connection instead of relaying a publish's answer.
        using var proxy = new FakeNode(1);
        using var pool = new ConnectionPool(ConnectionOptions.CreateBuilder().PlaintextOnLoopback().Build());
        Connection upstream = await pool.GetAsync(_cluster.AddressOf(1));
        int armed = 0;
        proxy.OnAsync(async (n, request) =>
        {
            Response response = await upstream.CallAsync(request);
            if (request is Request.Publish && Volatile.Read(ref armed) == 1)
            {
                throw new FakeNode.Drop();
            }
            if (response is Response.Metadata m)
            {
                response = m with { Brokers = m.Brokers.Select(b => b with { Host = "127.0.0.1", Port = (ushort)proxy.Address.Port }).ToList() };
            }
            return response;
        });
        using var through = new LotseClient(ClientConfig.CreateBuilder(proxy.HostPort).PlaintextOnLoopback()
            .WithRequestTimeout(TimeSpan.FromSeconds(5)).WithRetryFor(TimeSpan.FromSeconds(20)).Build());
        await through.PublishAsync("lost-x", "k", OutMessage.FromValue("seen"));
        Volatile.Write(ref armed, 1);
        await Assert.ThrowsAsync<OutcomeUnknownException>(() => through.PublishAsync("lost-x", "k", OutMessage.FromValue("lost-answer")));
        Assert.Equal(2, proxy.Count<Request.Publish>()); // the one that was answered, and the one that was not sent again

        // The node did write the one whose answer was lost, once: the unknown outcome was honest, and not repeated.
        await using QueueConsumer c = await admin.SubscribeAsync("lost-q", SubscribeOptions.WithCredit(10));
        var got = new List<string>();
        for (Delivery? d = await c.PollAsync(TimeSpan.FromSeconds(3)); d is not null; d = await c.PollAsync(TimeSpan.FromSeconds(1)))
        {
            got.Add(d.Value!.ToUtf8());
            await c.AckAsync(d);
        }
        Assert.Equal(new[] { "lost-answer", "seen", "warm-up" }, got.OrderBy(x => x, StringComparer.Ordinal).ToArray());
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// Queues against three real <c>lotse</c> processes on loopback (plain TCP, no authentication): consuming, settling,
/// redelivery after a nack, a delayed nack, an ack timeout and a dropped consumer, dead letters, a single active
/// consumer with a standby, priorities, publish delays and a leader that dies mid-consumption. The pushes of phase 2
/// are tested here against a real node for the first time. Skipped, with a message, when <c>LOTSE_BIN</c> is not set.
/// </summary>
[Collection("real cluster")]
public sealed class RealQueueTests : IClassFixture<RealCluster>, IAsyncLifetime
{
    private readonly RealCluster _cluster;
    private LotseClient? _client;

    public RealQueueTests(RealCluster cluster) => _cluster = cluster;

    public Task InitializeAsync()
    {
        if (_cluster.Available)
        {
            _client = NewClient();
        }
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        _client?.Dispose();
        return Task.CompletedTask;
    }

    private LotseClient Client => _client!;

    private LotseClient NewClient() =>
        new(ClientConfig.CreateBuilder(_cluster.Bootstrap)
            .PlaintextOnLoopback()
            .WithRequestTimeout(TimeSpan.FromSeconds(5))
            .WithRetryFor(TimeSpan.FromSeconds(60))
            .Build());

    private async Task<int> LeaderOfAsync(string queue)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            Response.Metadata m = await Client.MetadataAsync(new[] { queue });
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
    /// Waits until the queue's leader has stayed the same for half a second. A queue's group elects its leader in its
    /// first moments, and a leader that changes later restarts the delivery counts (they are a lower bound after a
    /// failover), which these tests do not want to measure.
    /// </summary>
    private async Task SettledAsync(string queue)
    {
        int leader = await LeaderOfAsync(queue);
        int same = 0;
        while (same < 5)
        {
            await Task.Delay(100);
            int now = await LeaderOfAsync(queue);
            same = now == leader ? same + 1 : 0;
            leader = now;
        }
    }

    private async Task ProduceAsync(string queue, int count)
    {
        for (int i = 0; i < count; i++)
        {
            await Client.ProduceAsync(queue, 0, OutMessage.FromValue(queue + "-" + i).WithKey("k" + i).WithHeader("n", i.ToString()));
        }
    }

    private static async Task<Delivery> Take(QueueConsumer c, int seconds = 30)
    {
        Delivery? d = await c.PollAsync(TimeSpan.FromSeconds(seconds));
        Assert.NotNull(d);
        return d!;
    }

    [LotseFact]
    public async Task EveryMessageIsDeliveredOnceAckedAndTheQueueDrains()
    {
        await Client.CreateQueueAsync("drain");
        await ProduceAsync("drain", 20);
        var seen = new HashSet<string>();
        await using (QueueConsumer c = await Client.SubscribeAsync("drain", SubscribeOptions.WithCredit(3)))
        {
            for (int i = 0; i < 20; i++)
            {
                Delivery d = await Take(c);
                Assert.Equal(1, d.DeliveryCount);
                Assert.True(seen.Add(d.Value!.ToUtf8()), "no message twice");
                Assert.Equal("k" + d.Value.ToUtf8()["drain-".Length..], d.Key!.ToUtf8());
                Assert.Single(d.Headers);
                await c.AckAsync(d);
            }
            Assert.Null(await c.PollAsync(TimeSpan.FromMilliseconds(500))); // nothing more
        }
        Assert.Equal(20, seen.Count);
        await using QueueConsumer again = await Client.SubscribeAsync("drain");
        Assert.Null(await again.PollAsync(TimeSpan.FromSeconds(1))); // acked messages do not come back
    }

    [LotseFact]
    public async Task TheMessagesCanBeReadAsAnAsyncSequence()
    {
        await Client.CreateQueueAsync("sequence");
        await ProduceAsync("sequence", 5);
        var values = new List<string>();
        await using QueueConsumer c = await Client.SubscribeAsync("sequence", SubscribeOptions.WithCredit(2));
        using var stop = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(30));
        await foreach (Delivery d in c.DeliveriesAsync(stop.Token))
        {
            values.Add(d.Value!.ToUtf8());
            await c.AckAsync(d, stop.Token);
            if (values.Count == 5)
            {
                break;
            }
        }
        Assert.Equal(Enumerable.Range(0, 5).Select(i => "sequence-" + i), values);
    }

    [LotseFact]
    public async Task ASubscriptionToAQueueThatDoesNotExistFails()
    {
        await Assert.ThrowsAsync<UnknownStreamException>(() => Client.SubscribeAsync("no-such-queue"));
    }

    [LotseFact]
    public async Task ARequeuedMessageComesBackWithACountOfTwo()
    {
        await Client.CreateQueueAsync("requeue");
        await ProduceAsync("requeue", 1);
        await SettledAsync("requeue");
        await using QueueConsumer c = await Client.SubscribeAsync("requeue", SubscribeOptions.WithCredit(2));
        Delivery first = await Take(c);
        Assert.Equal(1, first.DeliveryCount);
        await c.NackAsync(first, true);
        Delivery second = await Take(c);
        Assert.Equal(first.Offset, second.Offset);
        Assert.Equal(2, second.DeliveryCount);
        await c.AckAsync(second);
        Assert.Null(await c.PollAsync(TimeSpan.FromMilliseconds(500)));
    }

    [LotseFact]
    public async Task ADiscardedMessageIsGone()
    {
        await Client.CreateQueueAsync("discard");
        await ProduceAsync("discard", 1);
        await SettledAsync("discard");
        await using QueueConsumer c = await Client.SubscribeAsync("discard");
        await c.NackAsync(await Take(c), false);
        Assert.Null(await c.PollAsync(TimeSpan.FromSeconds(1)));
    }

    [LotseFact]
    public async Task ADelayedRequeueWaitsForItsDelay()
    {
        await Client.CreateQueueAsync("backoff");
        await ProduceAsync("backoff", 1);
        await SettledAsync("backoff");
        await using QueueConsumer c = await Client.SubscribeAsync("backoff");
        Delivery first = await Take(c);
        var clock = Stopwatch.StartNew();
        await c.NackAfterAsync(first, TimeSpan.FromMilliseconds(1500));
        Assert.Null(await c.PollAsync(TimeSpan.FromMilliseconds(700))); // not due yet
        Delivery second = await Take(c);
        Assert.True(clock.ElapsedMilliseconds >= 1200, $"came back after only {clock.ElapsedMilliseconds} ms");
        Assert.Equal(first.Offset, second.Offset);
        Assert.Equal(2, second.DeliveryCount);
        await c.AckAsync(second);
    }

    [LotseFact]
    public async Task AMessageNotAckedInTimeIsDeliveredAgain()
    {
        await Client.CreateQueueAsync("timeout", QueueOptions.CreateBuilder().WithAckTimeout(TimeSpan.FromMilliseconds(700)).Build());
        await ProduceAsync("timeout", 1);
        await SettledAsync("timeout");
        await using QueueConsumer c = await Client.SubscribeAsync("timeout");
        Delivery first = await Take(c);
        // Held without an ack: the broker ends the subscription and the consumer subscribes again by itself. If the
        // queue's leader happens to change meanwhile, the message comes back at once with a count that started over
        // (counts are a lower bound after a failover, ADR 0031); it is held again until the ack timeout of the leader
        // that stayed brings it back with a count that rose.
        Delivery again = await Take(c);
        for (int tries = 0; again.DeliveryCount < 2 && tries < 4; tries++)
        {
            Assert.Equal(first.Offset, again.Offset);
            again = await Take(c);
        }
        Assert.Equal(first.Offset, again.Offset);
        Assert.True(again.DeliveryCount >= 2, "was " + again.DeliveryCount);
        await c.AckAsync(again);
    }

    [LotseFact]
    public async Task AConsumerThatDisappearsLeavesItsMessagesToTheNext()
    {
        await Client.CreateQueueAsync("vanish");
        await ProduceAsync("vanish", 3);
        await SettledAsync("vanish");
        using (LotseClient other = NewClient())
        {
            QueueConsumer gone = await other.SubscribeAsync("vanish", SubscribeOptions.WithCredit(10));
            for (int i = 0; i < 3; i++)
            {
                await Take(gone); // delivered to it, never acked
            }
        } // closing the client closes the connection, and the messages go back
        var offsets = new List<ulong>();
        await using QueueConsumer c = await Client.SubscribeAsync("vanish");
        for (int i = 0; i < 3; i++)
        {
            Delivery d = await Take(c);
            Assert.Equal(2, d.DeliveryCount);
            offsets.Add(d.Offset);
            await c.AckAsync(d);
        }
        Assert.Equal(3, offsets.Distinct().Count());
    }

    [LotseFact]
    public async Task AMessageThatAnotherConsumerHoldsIsAckedByNameNotByDelivery()
    {
        // An ack names an offset, so the second consumer's ack settles the message for the first too.
        await Client.CreateQueueAsync("by-name");
        await ProduceAsync("by-name", 1);
        await SettledAsync("by-name");
        await using QueueConsumer first = await Client.SubscribeAsync("by-name");
        Delivery d = await Take(first);
        await first.AckAsync(d);
        await first.AckAsync(d); // twice is harmless
        await using QueueConsumer second = await Client.SubscribeAsync("by-name");
        Assert.Null(await second.PollAsync(TimeSpan.FromSeconds(1)));
    }

    [LotseFact]
    public async Task KillingTheLeaderMidConsumptionLosesNothing()
    {
        await Client.CreateQueueAsync("failover");
        const int total = 60;
        await ProduceAsync("failover", total);
        int leader = await LeaderOfAsync("failover");
        var acked = new HashSet<string>();
        bool killed = false;
        await using (QueueConsumer c = await Client.SubscribeAsync("failover", SubscribeOptions.WithCredit(5)))
        {
            DateTime deadline = DateTime.UtcNow.AddSeconds(120);
            while (acked.Count < total)
            {
                Assert.True(DateTime.UtcNow < deadline, $"only {acked.Count} of {total} acked");
                Delivery? d = await c.PollAsync(TimeSpan.FromSeconds(10));
                if (d is null)
                {
                    continue;
                }
                string v = d.Value!.ToUtf8();
                if (acked.Contains(v))
                {
                    // Redelivered before its ack settled: allowed, and then the count says so.
                    Assert.True(d.DeliveryCount > 1, $"{v} came twice with a delivery count of {d.DeliveryCount}");
                }
                await c.AckAsync(d);
                acked.Add(v);
                if (!killed && acked.Count >= 20)
                {
                    await _cluster.KillAsync(leader);
                    killed = true;
                }
            }
        }
        Assert.True(killed, "the leader was killed while messages were left");
        for (int i = 0; i < total; i++)
        {
            Assert.Contains("failover-" + i, acked);
        }
        Assert.NotEqual(leader, await LeaderOfAsync("failover")); // another node leads now
        await _cluster.RestartAsync(leader);
    }

    [LotseFact]
    public async Task AMessageDeliveredTooOftenLandsInTheDeadLetterQueueWithItsOrigin()
    {
        await Client.CreateQueueAsync("dead-letters");
        ulong id = await Client.CreateQueueAsync(
            "capped", QueueOptions.CreateBuilder().WithMaxDeliveries(2).WithDeadLetterQueue("dead-letters").Build());
        ulong offset = (await Client.ProduceAsync("capped", 0, OutMessage.FromValue("poison").WithKey("k"))).BaseOffset;
        await SettledAsync("capped");
        await using (QueueConsumer c = await Client.SubscribeAsync("capped"))
        {
            Delivery d1 = await Take(c);
            Assert.Equal(1, d1.DeliveryCount);
            Assert.Null(d1.DeadLetter);
            await c.NackAsync(d1, true);
            Delivery d2 = await Take(c);
            Assert.Equal(2, d2.DeliveryCount);
            await c.NackAsync(d2, true);
        }
        await using (QueueConsumer dl = await Client.SubscribeAsync("dead-letters"))
        {
            Delivery d = await Take(dl);
            Assert.Equal("poison", d.Value!.ToUtf8());
            Assert.Equal("k", d.Key!.ToUtf8());
            DeadLetter origin = d.DeadLetter!;
            Assert.Equal("max-deliveries", origin.Reason);
            Assert.Equal("capped", origin.Queue);
            Assert.Equal(id, origin.QueueId);
            Assert.Equal(offset, origin.Offset);
            Assert.Equal(2, origin.Deliveries);
            await dl.AckAsync(d);
        }
        await using QueueConsumer again = await Client.SubscribeAsync("capped");
        Assert.Null(await again.PollAsync(TimeSpan.FromSeconds(1))); // retired, not delivered a third time
    }

    [LotseFact]
    public async Task AMessageThatWaitsTooLongExpiresIntoTheDeadLetterQueue()
    {
        await Client.CreateQueueAsync("expired-letters");
        await Client.CreateQueueAsync(
            "short-lived",
            QueueOptions.CreateBuilder().WithMessageTtl(TimeSpan.FromMilliseconds(600)).WithDeadLetterQueue("expired-letters").Build());
        await Client.ProduceAsync("short-lived", 0, OutMessage.FromValue("stale"));
        await using QueueConsumer dl = await Client.SubscribeAsync("expired-letters");
        Delivery d = await Take(dl);
        Assert.Equal("stale", d.Value!.ToUtf8());
        Assert.Equal("expired", d.DeadLetter!.Reason);
        Assert.Equal("short-lived", d.DeadLetter.Queue);
        await dl.AckAsync(d);
    }

    [LotseFact]
    public async Task ASingleActiveConsumerGetsTheMessagesAndTheStandbyTakesOverWhenItEnds()
    {
        await Client.CreateQueueAsync("ledger", QueueOptions.CreateBuilder().WithSingleActiveConsumer().Build());
        await SettledAsync("ledger");
        await using QueueConsumer active = await Client.SubscribeAsync("ledger");
        await using QueueConsumer standby = await Client.SubscribeAsync("ledger");
        Assert.False(active.IsStandby);
        Assert.True(standby.IsStandby);
        await ProduceAsync("ledger", 2);
        for (int i = 0; i < 2; i++)
        {
            Delivery d = await Take(active);
            Assert.Equal("ledger-" + i, d.Value!.ToUtf8()); // in order
            await active.AckAsync(d);
        }
        Assert.Null(await standby.PollAsync(TimeSpan.FromMilliseconds(500))); // the standby gets nothing
        Assert.True(standby.IsStandby);

        await active.DisposeAsync();
        await Client.ProduceAsync("ledger", 0, OutMessage.FromValue("ledger-after"));
        Delivery next = await Take(standby);
        Assert.Equal("ledger-after", next.Value!.ToUtf8());
        Assert.False(standby.IsStandby);
        await standby.AckAsync(next);
    }

    [LotseFact]
    public async Task AStandbyTakesOverFromAConsumerThatDisappearsAndGetsItsMessage()
    {
        await Client.CreateQueueAsync("ledger-2", QueueOptions.CreateBuilder().WithSingleActiveConsumer().Build());
        await SettledAsync("ledger-2");
        using LotseClient other = NewClient();
        QueueConsumer first = await other.SubscribeAsync("ledger-2");
        await using QueueConsumer second = await Client.SubscribeAsync("ledger-2");
        Assert.False(first.IsStandby);
        Assert.True(second.IsStandby);
        await ProduceAsync("ledger-2", 1);
        Delivery held = await Take(first); // out with the first, never acked
        Assert.Equal(1, held.DeliveryCount);
        other.Dispose(); // the connection ends: the message goes back and the standby is next
        Delivery again = await Take(second);
        Assert.Equal(held.Offset, again.Offset);
        Assert.True(again.DeliveryCount >= 2, "was " + again.DeliveryCount);
        Assert.False(second.IsStandby);
        await second.AckAsync(again);
    }

    [LotseFact]
    public async Task ClosingGivesBackWhatWasPushedButNotHandedOut()
    {
        // A new queue leader redelivers everything that is not acked and starts the counts again (ADR 0031), so
        // "the one handed out stays with the first consumer" holds only while the leader stays the same. A run in
        // which it changed says nothing about close and is repeated on a fresh queue; a failure with the same leader
        // before and after is a real one and reports both.
        for (int attempt = 0; ; attempt++)
        {
            string queue = "give-back-" + attempt;
            await Client.CreateQueueAsync(queue);
            await ProduceAsync(queue, 3);
            await SettledAsync(queue);
            int leaderBefore = await LeaderOfAsync(queue);
            QueueConsumer c = await Client.SubscribeAsync(queue, SubscribeOptions.WithCredit(10));
            Delivery kept = await Take(c); // the other two are pushed and buffered
            // Close only once the client holds all three pushes, so that the two unread ones are what the close gives back.
            await RealWaits.UntilAsync("the node's pushes", () => Task.FromResult(c.PushedForTests >= 3), 30);
            await c.DisposeAsync();
            await using QueueConsumer next = await Client.SubscribeAsync(queue, SubscribeOptions.WithCredit(10));
            var offsets = new List<ulong>();
            for (int i = 0; i < 2; i++)
            {
                Delivery d = await Take(next, 10);
                offsets.Add(d.Offset);
                await next.AckAsync(d);
            }
            Assert.Equal(2, offsets.Distinct().Count());
            int leaderAfter = await LeaderOfAsync(queue);
            if (leaderBefore != leaderAfter && attempt < 2)
            {
                continue;
            }
            Assert.True(
                !offsets.Contains(kept.Offset),
                $"the one handed out stays with the first consumer: kept {kept.Offset}, got {string.Join(",", offsets)}, "
                    + $"leader {leaderBefore} before and {leaderAfter} after");
            return;
        }
    }

    [LotseFact]
    public async Task AQueueWithPrioritiesDeliversTheHighestLevelFirst()
    {
        await Client.CreateQueueAsync("urgent", QueueOptions.CreateBuilder().WithPriorityLevels(3).Build());
        await SettledAsync("urgent");
        // Produced low first; the consumer subscribes afterwards, so the backlog is ordered by level.
        for (int i = 0; i < 3; i++)
        {
            await Client.ProduceAsync("urgent", 0, OutMessage.FromValue("low-" + i).WithHeader("x-lotse-priority", "0"));
        }
        await Client.ProduceAsync("urgent", 0, OutMessage.FromValue("high").WithHeader("x-lotse-priority", "2"));
        await Client.ProduceAsync("urgent", 0, OutMessage.FromValue("mid").WithHeader("x-lotse-priority", "1"));
        await using QueueConsumer c = await Client.SubscribeAsync("urgent", SubscribeOptions.WithCredit(1));
        var order = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            Delivery d = await Take(c);
            order.Add(d.Value!.ToUtf8());
            await c.AckAsync(d);
        }
        Assert.Equal(new[] { "high", "mid", "low-0", "low-1", "low-2" }, order);
    }

    [LotseFact]
    public async Task APublishDelayHoldsAMessageBack()
    {
        await Client.CreateQueueAsync("later");
        await SettledAsync("later");
        await using QueueConsumer c = await Client.SubscribeAsync("later");
        var clock = Stopwatch.StartNew();
        await Client.ProduceAsync("later", 0, OutMessage.FromValue("not yet").WithHeader("x-lotse-delay-ms", "1500"));
        Assert.Null(await c.PollAsync(TimeSpan.FromMilliseconds(700)));
        Delivery d = await Take(c);
        Assert.True(clock.ElapsedMilliseconds >= 1300, $"came after only {clock.ElapsedMilliseconds} ms");
        Assert.Equal("not yet", d.Value!.ToUtf8());
        await c.AckAsync(d);
    }

    [LotseFact]
    public async Task AnAckAfterTheConsumerIsClosedStillSettles()
    {
        await Client.CreateQueueAsync("after-close");
        await ProduceAsync("after-close", 1);
        await SettledAsync("after-close");
        QueueConsumer c = await Client.SubscribeAsync("after-close");
        Delivery d = await Take(c);
        await c.DisposeAsync();
        await c.AckAsync(d);
        await using QueueConsumer again = await Client.SubscribeAsync("after-close");
        Assert.Null(await again.PollAsync(TimeSpan.FromSeconds(1)));
    }
}

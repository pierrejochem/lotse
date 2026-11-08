using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Client.Tests.Net;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>A queue consumer against scripted brokers: what it sends, and what it does with what it is sent.</summary>
public sealed class QueueConsumerTests : IDisposable
{
    private const ulong QueueId = 9;
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(300);

    private readonly FakeNode _a = new(1);
    private readonly FakeNode _b = new(2);
    private LotseClient? _client;
    private long _nextSub = 100;
    private volatile FakeNode _leader;

    public QueueConsumerTests()
    {
        _leader = _a;
    }

    public void Dispose()
    {
        _client?.Dispose();
        _a.Dispose();
        _b.Dispose();
    }

    // ---- scripts ----

    /// <summary>Where a script saw the last subscription: the connection it came on and its id.</summary>
    private sealed class Seen
    {
        private long _id;

        internal FakeNode.Conn? Conn { get; set; }

        internal uint Id
        {
            get => (uint)Interlocked.Read(ref _id);
            set => Interlocked.Exchange(ref _id, value);
        }

        internal List<FakeNode.Conn> All { get; } = new();
    }

    private Messages.ResourceMetadata QueueMetadata() =>
        new(
            ErrorCode.None,
            "jobs",
            "default",
            QueueId,
            Messages.ResourceKind.Queue,
            new[] { Fx.Partition(0, _leader.Id, 1, 1, 2) },
            null);

    private void Script(FakeNode node, Func<FakeNode, FakeNode.Conn, Request, Response?> script) =>
        node.OnConn((n, conn, request) => request is Request.Metadata
            ? Fx.Metadata(new[] { _a, _b }, QueueMetadata())
            : script(n, conn, request));

    private uint NewSub() => (uint)Interlocked.Increment(ref _nextSub);

    /// <summary>The answers of a healthy queue leader that does not push anything of its own.</summary>
    private Response? Plain(FakeNode n, FakeNode.Conn conn, Request r) => r switch
    {
        Request.Subscribe => new Response.Subscribe(ErrorCode.None, NewSub(), null),
        Request.Credit => null,
        _ => new Response.Outcome(ErrorCode.None, null),
    };

    /// <summary>Like <see cref="Plain"/>, remembering the subscription; <paramref name="more"/> may answer first.</summary>
    private Func<FakeNode, FakeNode.Conn, Request, Response?> Recording(
        Seen seen, bool standby = false, Func<FakeNode, FakeNode.Conn, Request, Response?>? more = null) =>
        (n, conn, r) =>
        {
            if (r is Request.Subscribe)
            {
                seen.Conn = conn;
                seen.All.Add(conn);
                seen.Id = NewSub();
                return new Response.Subscribe(ErrorCode.None, seen.Id, null, standby);
            }
            return more?.Invoke(n, conn, r) ?? Plain(n, conn, r);
        };

    private void Connect(params FakeNode[] nodes) =>
        _client = new LotseClient(Fx.Config(nodes)
            .WithRequestTimeout(TimeSpan.FromSeconds(2))
            .WithRetryFor(TimeSpan.FromSeconds(3))
            .Build());

    private LotseClient Client => _client!;

    private static Frames.DeliverPush Push(uint sub, ulong baseOffset, int count, params ulong[] delivered)
    {
        string[] values = Enumerable.Range(0, count).Select(i => "m-" + (baseOffset + (ulong)i)).ToArray();
        ByteString batch = FetchTests.Data(baseOffset, 0, false, values);
        return new Frames.DeliverPush(sub, ErrorCode.None, batch, delivered.Select(o => new Messages.DeliveredOffset(o, 1)).ToList(), null);
    }

    private static Frames.DeliverPush PushAll(uint sub, ulong baseOffset, int count) =>
        Push(sub, baseOffset, count, Enumerable.Range(0, count).Select(i => baseOffset + (ulong)i).ToArray());

    private static Frames.DeliverPush Failure(uint sub, ErrorCode code, LeaderHint? hint = null) =>
        new(sub, code, ByteString.Empty, Array.Empty<Messages.DeliveredOffset>(), hint);

    private static async Task<Delivery> Take(QueueConsumer c)
    {
        Delivery? d = await c.PollAsync(TimeSpan.FromSeconds(3));
        Assert.NotNull(d);
        return d!;
    }

    private static List<Messages.OffsetRange> Ranges(params (ulong From, ulong To)[] r) =>
        r.Select(x => new Messages.OffsetRange(x.From, x.To)).ToList();

    // ---- subscribing ----

    [Fact]
    public async Task SubscribeSendsTheQueueAndTheCredit()
    {
        Script(_a, Plain);
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(8));
        Request.Subscribe s = _a.Requests<Request.Subscribe>()[0];
        Assert.Equal(QueueId, s.QueueId);
        Assert.Equal(string.Empty, s.ConsumerTag);
        Assert.Equal(8u, s.CreditMsgs);
        Assert.Equal(uint.MaxValue, s.CreditBytes);
        Assert.False(s.Exclusive);
        Assert.True(s.NotifyActive, "asks to be told when it becomes the single active consumer");
    }

    [Fact]
    public async Task TheDefaultCreditIs64()
    {
        Script(_a, Plain);
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        Assert.Equal(64u, _a.Requests<Request.Subscribe>()[0].CreditMsgs);
        Assert.Equal(64, SubscribeOptions.DefaultCredit);
    }

    [Fact]
    public void ACreditBelowOneIsOne()
    {
        Assert.Equal(1, SubscribeOptions.WithCredit(0).Credit);
        Assert.Equal(1, SubscribeOptions.WithCredit(-5).Credit);
    }

    [Fact]
    public async Task AnUnknownQueueIsReported()
    {
        _a.OnConn((n, conn, r) => r is Request.Metadata ? Fx.Metadata(new[] { _a }, Fx.Unknown("jobs")) : null);
        Connect(_a);
        await Assert.ThrowsAsync<UnknownStreamException>(() => Client.SubscribeAsync("jobs"));
    }

    [Fact]
    public async Task ARefusedSubscriptionFailsAtOnce()
    {
        Script(_a, (n, conn, r) => new Response.Subscribe(ErrorCode.InvalidRequest, 0, null));
        Connect(_a);
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => Client.SubscribeAsync("jobs"));
        Assert.Equal(ErrorCode.InvalidRequest, e.Code);
        Assert.Equal(1, _a.Count<Request.Subscribe>()); // not tried again
    }

    [Fact]
    public async Task AnUnknownResourceAnswerDropsTheRoute()
    {
        Script(_a, (n, conn, r) => new Response.Subscribe(ErrorCode.UnknownResource, 0, null));
        Connect(_a);
        await Assert.ThrowsAsync<BrokerErrorException>(() => Client.SubscribeAsync("jobs"));
        Assert.Null(Client.RouteOf("jobs", 0)); // the name may come back as a new queue
    }

    [Fact]
    public async Task ASubscriptionToANodeThatIsNotTheLeaderFollowsTheHint()
    {
        Script(_a, (n, conn, r) => new Response.Subscribe(ErrorCode.NotLeader, 0, new LeaderHint(2, 2)));
        Script(_b, Plain);
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        Assert.Equal(1, _b.Count<Request.Subscribe>());
        Assert.Equal(1, _a.Count<Request.Subscribe>());
        Assert.Equal(_b.Address, c.SubscribedAddress);
    }

    [Fact]
    public async Task ACancelledSubscribeEndsTheConnectionItMayHaveSubscribedOn()
    {
        // The broker never answers: the subscription may exist there, and nobody would read it.
        _a.OnConn((n, conn, r) => r is Request.Metadata ? Fx.Metadata(new[] { _a }, QueueMetadata()) : null);
        Connect(_a);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Client.SubscribeAsync("jobs", null, cts.Token));
        Assert.Equal(1, _a.Count<Request.Subscribe>());
        Assert.Single(_a.Conns());
        await Client.MetadataAsync(new[] { "jobs" });
        Assert.Equal(2, _a.Conns().Count); // the next call needed a new connection
    }

    // ---- receiving ----

    [Fact]
    public async Task PushesThatArriveBeforeTheAnswerToSubscribeAreNotLost()
    {
        Script(_a, (n, conn, r) =>
        {
            if (r is Request.Subscribe)
            {
                uint id = NewSub();
                // Sent before the answer, so the client has no subscription to give them to yet.
                conn.Push(PushAll(id, 10, 2));
                conn.Push(PushAll(id, 12, 1));
                return new Response.Subscribe(ErrorCode.None, id, null);
            }
            return Plain(n, conn, r);
        });
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        Assert.Equal(10UL, (await Take(c)).Offset);
        Assert.Equal(11UL, (await Take(c)).Offset);
        Assert.Equal(12UL, (await Take(c)).Offset);
        Assert.Null(await c.PollAsync(Short));
    }

    [Fact]
    public async Task OnlyTheOffsetsInTheDeliveriesListAreHandedOut()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(10));
        seen.Conn!.Push(Push(seen.Id, 10, 4, 11, 13));
        Delivery first = await Take(c);
        Delivery second = await Take(c);
        Assert.Equal(11UL, first.Offset);
        Assert.Equal(13UL, second.Offset);
        Assert.Equal("m-11", first.Value!.ToUtf8());
        Assert.Equal("m-13", second.Value!.ToUtf8());
        Assert.Null(await c.PollAsync(Short)); // the records 10 and 12 are somebody else's
    }

    [Fact]
    public async Task TheDeliveryCountComesFromTheDeliveriesList()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(new Frames.DeliverPush(
            seen.Id, ErrorCode.None, FetchTests.Data(5, 0, false, "x"), new[] { new Messages.DeliveredOffset(5, 4) }, null));
        Delivery d = await Take(c);
        Assert.Equal(4, d.DeliveryCount);
        Assert.Equal(1_000, d.Timestamp);
    }

    [Fact]
    public async Task PollReturnsNullWhenNothingComes()
    {
        Script(_a, Plain);
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        var clock = System.Diagnostics.Stopwatch.StartNew();
        Assert.Null(await c.PollAsync(TimeSpan.FromMilliseconds(200)));
        Assert.True(clock.ElapsedMilliseconds >= 150);
        Assert.Equal(1, _a.Count<Request.Subscribe>()); // waiting does not subscribe again
    }

    [Fact]
    public async Task ACancelledWaitThrowsAndLeavesTheSubscription()
    {
        Script(_a, Plain);
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(150));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => c.NextAsync(cts.Token));
        Assert.Equal(1, _a.Count<Request.Subscribe>());
        Assert.Null(await c.PollAsync(TimeSpan.FromMilliseconds(50)));
    }

    [Fact]
    public async Task TheDeliveriesAreAnAsyncSequenceThatEndsWhenTheConsumerIsClosed()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(10));
        seen.Conn!.Push(PushAll(seen.Id, 10, 3));
        var got = new List<ulong>();
        await foreach (Delivery d in c.DeliveriesAsync())
        {
            got.Add(d.Offset);
            await c.AckAsync(d);
            if (got.Count == 3)
            {
                _ = Task.Run(async () =>
                {
                    await Task.Delay(100);
                    c.Dispose();
                });
            }
        }
        Assert.Equal(new ulong[] { 10, 11, 12 }, got);
        Assert.Equal(3, _a.Count<Request.Ack>());
    }

    [Fact]
    public async Task TheSequenceStopsOnCancellation()
    {
        Script(_a, Plain);
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(150));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (Delivery _ in c.DeliveriesAsync(cts.Token))
            {
            }
        });
    }

    // ---- credit ----

    /// <summary>Credit <paramref name="credit"/>: pushes that many messages in one batch and hands <paramref name="taken"/> out.</summary>
    private async Task<(List<Request.Credit> Credits, int BatchSize, uint SubId)> CreditsAfter(int credit, int taken)
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(credit));
        Frames.DeliverPush p = PushAll(seen.Id, 10, credit);
        seen.Conn!.Push(p);
        for (int i = 0; i < taken; i++)
        {
            await Take(c);
        }
        await Task.Delay(150); // credit is fire-and-forget: give it time to arrive
        List<Request.Credit> credits = _a.Requests<Request.Credit>();
        c.Dispose();
        return (credits, p.Batch.Size, seen.Id);
    }

    [Fact]
    public async Task CreditIsAddedOnceHalfOfItIsUsed()
    {
        // credit 5: half rounded up is 3
        Assert.Empty((await CreditsAfter(5, 2)).Credits); // 2 of 5 used: not yet
    }

    [Fact]
    public async Task CreditCarriesTheMessagesAndBytesUsed()
    {
        var (credits, size, sub) = await CreditsAfter(5, 3);
        Request.Credit credit = Assert.Single(credits); // 3 of 5 used: now
        Assert.Equal(sub, credit.SubscriptionId);
        Assert.Equal(3u, credit.AddMsgs);
        Assert.Equal((uint)size, credit.AddBytes); // the bytes of the batch that was received
    }

    [Fact]
    public async Task AnEvenCreditRefillsAtExactlyHalf()
    {
        Assert.Empty((await CreditsAfter(4, 1)).Credits);
    }

    [Fact]
    public async Task AnEvenCreditRefillsAtHalf()
    {
        var (credits, _, _) = await CreditsAfter(4, 2);
        Assert.Equal(2u, Assert.Single(credits).AddMsgs);
    }

    [Fact]
    public async Task ACreditOfOneRefillsAfterEveryMessage()
    {
        var (credits, size, _) = await CreditsAfter(1, 1);
        Request.Credit credit = Assert.Single(credits);
        Assert.Equal(1u, credit.AddMsgs);
        Assert.Equal((uint)size, credit.AddBytes);
    }

    [Fact]
    public async Task UsedCountsStartOverAfterARefill()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(4));
        Frames.DeliverPush p1 = PushAll(seen.Id, 10, 2);
        Frames.DeliverPush p2 = PushAll(seen.Id, 12, 2);
        seen.Conn!.Push(p1);
        await Take(c);
        await Take(c);
        seen.Conn!.Push(p2);
        await Take(c);
        await Take(c);
        await Eventually.AwaitAsync("two credits", () => _a.Count<Request.Credit>() >= 2);
        await Task.Delay(100);
        List<Request.Credit> credits = _a.Requests<Request.Credit>();
        Assert.Equal(2, credits.Count); // two refills for four messages
        Assert.Equal(2u, credits[0].AddMsgs);
        Assert.Equal((uint)p1.Batch.Size, credits[0].AddBytes);
        Assert.Equal(2u, credits[1].AddMsgs);
        Assert.Equal((uint)p2.Batch.Size, credits[1].AddBytes); // bytes of the first batch are not counted twice
    }

    [Fact]
    public async Task CreditGoesOnTheSubscriptionsConnection()
    {
        await CreditsAfter(2, 1);
        Assert.Single(_a.Requests<Request.Credit>());
        Assert.Single(_a.Conns()[0].Requests<Request.Credit>());
    }

    // ---- settling ----

    [Fact]
    public async Task AnAckNamesTheOffsetAsAHalfOpenRange()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(10));
        seen.Conn!.Push(PushAll(seen.Id, 10, 3));
        Delivery d10 = await Take(c);
        await Take(c);
        Delivery d12 = await Take(c);
        await c.AckAsync(d10);
        Request.Ack ack = _a.Requests<Request.Ack>()[0];
        Assert.Equal(QueueId, ack.QueueId);
        Assert.Equal(Ranges((10, 11)), ack.Ranges);
        Assert.Null(ack.TxnId);

        await c.AckAsync(new[] { d12, d10, d12 });
        Assert.Equal(Ranges((10, 11), (12, 13)), _a.Requests<Request.Ack>()[1].Ranges);
    }

    [Fact]
    public async Task AckedNeighboursAreOneRange()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(10));
        seen.Conn!.Push(PushAll(seen.Id, 10, 3));
        var all = new List<Delivery> { await Take(c), await Take(c), await Take(c) };
        await c.AckAsync(all);
        Assert.Equal(Ranges((10, 13)), _a.Requests<Request.Ack>()[0].Ranges);
    }

    [Fact]
    public async Task AStandbyIsToldWhenItsTurnComes()
    {
        var seen = new Seen();
        Script(_a, Recording(seen, standby: true));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        Assert.True(c.IsStandby);
        seen.Conn!.Push(new Frames.DeliverPush(seen.Id, ErrorCode.None, ByteString.Empty, Array.Empty<Messages.DeliveredOffset>(), null));
        Assert.Null(await c.PollAsync(Short)); // the activation is no message
        Assert.False(c.IsStandby);
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        Assert.Equal(10UL, (await Take(c)).Offset);
    }

    [Fact]
    public async Task AStandbyThatGetsAMessageIsActive()
    {
        var seen = new Seen();
        Script(_a, Recording(seen, standby: true));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        Assert.True(c.IsStandby);
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        await Take(c);
        Assert.False(c.IsStandby);
    }

    [Fact]
    public async Task AStandbyGetsNoMessagesBeforeItsTurn()
    {
        var seen = new Seen();
        Script(_a, Recording(seen, standby: true));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        Assert.Null(await c.PollAsync(Short));
        Assert.True(c.IsStandby, "silence does not make a standby active");
    }

    [Fact]
    public async Task ADelayedRequeueCarriesItsDelayOnTheSubscriptionsConnection()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        Assert.False(c.IsStandby);
        seen.Conn!.Push(PushAll(seen.Id, 10, 2));
        await c.NackAfterAsync(await Take(c), TimeSpan.FromMilliseconds(1500));
        Request.Nack nack = seen.Conn!.Requests<Request.Nack>()[0];
        Assert.Equal(1500u, nack.DelayMs);
        Assert.True(nack.Requeue);
        Assert.Equal(Ranges((10, 11)), nack.Ranges);
        await c.NackAsync(await Take(c), true);
        Assert.Equal(0u, seen.Conn!.Requests<Request.Nack>()[1].DelayMs); // a plain requeue waits not
    }

    [Fact]
    public async Task ADelayIsRoundedDownToMillisecondsAndCappedAtU32()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 3));
        await c.NackAfterAsync(await Take(c), TimeSpan.FromTicks(25_000)); // 2.5 ms
        await c.NackAfterAsync(await Take(c), TimeSpan.FromDays(100));
        await c.NackAfterAsync(await Take(c), TimeSpan.FromSeconds(-3));
        List<Request.Nack> nacks = seen.Conn!.Requests<Request.Nack>();
        Assert.Equal(new[] { 2u, uint.MaxValue, 0u }, nacks.Select(x => x.DelayMs));
    }

    [Fact]
    public async Task AnAckThatIsRefusedAsNotLeaderIsRetriedAtTheNewLeader()
    {
        var seen = new Seen();
        Script(_a, Recording(seen, more: (n, conn, r) =>
            r is Request.Ack ? new Response.Outcome(ErrorCode.NotLeader, new LeaderHint(2, 2)) : null));
        Script(_b, Plain);
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        Delivery d = await Take(c);
        await c.AckAsync(d);
        Assert.Equal(1, _a.Count<Request.Ack>());
        Assert.Equal(Ranges((10, 11)), _b.Requests<Request.Ack>()[0].Ranges);
    }

    [Fact]
    public async Task AnAckWhoseAnswerIsLostIsSentAgain()
    {
        var seen = new Seen();
        int dropped = 0;
        Script(_a, Recording(seen, more: (n, conn, r) =>
            r is Request.Ack && Interlocked.Exchange(ref dropped, 1) == 0 ? throw new FakeNode.Drop() : null));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        Delivery d = await Take(c);
        await c.AckAsync(d);
        Assert.Equal(2, _a.Count<Request.Ack>()); // idempotent: sent again
    }

    [Fact]
    public async Task ARefusedAckIsAnError()
    {
        var seen = new Seen();
        Script(_a, Recording(seen, more: (n, conn, r) =>
            r is Request.Ack ? new Response.Outcome(ErrorCode.InvalidRequest, null) : null));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        Delivery d = await Take(c);
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => c.AckAsync(d));
        Assert.Equal(ErrorCode.InvalidRequest, e.Code);
        Assert.Equal(1, _a.Count<Request.Ack>()); // not retried
    }

    [Fact]
    public async Task NackWithoutRequeueDiscardsAtTheLeader()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        await c.NackAsync(await Take(c), false);
        Request.Nack nack = _a.Requests<Request.Nack>()[0];
        Assert.False(nack.Requeue);
        Assert.Equal(0u, nack.DelayMs);
        Assert.Equal(QueueId, nack.QueueId);
        Assert.Equal(Ranges((10, 11)), nack.Ranges);
    }

    [Fact]
    public async Task NackWithRequeueGoesOnTheSubscriptionsConnection()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 2));
        Delivery d = await Take(c);
        await c.NackAsync(d, true);
        Request.Nack nack = Assert.Single(seen.Conn!.Requests<Request.Nack>()); // on the connection of the subscription
        Assert.True(nack.Requeue);
        Assert.Equal(0u, nack.DelayMs);
        Assert.Equal(Ranges((10, 11)), nack.Ranges);
    }

    [Fact]
    public async Task ADiscardingNackIsRetriedLikeAnAck()
    {
        var seen = new Seen();
        int refused = 0;
        Script(_a, Recording(seen, more: (n, conn, r) =>
            r is Request.Nack && Interlocked.Exchange(ref refused, 1) == 0
                ? new Response.Outcome(ErrorCode.NotLeader, new LeaderHint(2, 2))
                : null));
        Script(_b, Plain);
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        await c.NackAsync(await Take(c), false);
        Assert.False(Assert.Single(_b.Requests<Request.Nack>()).Requeue);
    }

    [Fact]
    public async Task ARequeueThatTheBrokerRefusesIsAnError()
    {
        var seen = new Seen();
        Script(_a, Recording(seen, more: (n, conn, r) =>
            r is Request.Nack ? new Response.Outcome(ErrorCode.InvalidRequest, null) : null));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        Delivery d = await Take(c);
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => c.NackAsync(d, true));
        Assert.Equal(ErrorCode.InvalidRequest, e.Code);
    }

    [Fact]
    public async Task ARequeueWhoseConnectionIsGoneDoesNothing()
    {
        var seen = new Seen();
        Script(_a, Recording(seen, more: (n, conn, r) => r is Request.Nack ? throw new FakeNode.Drop() : null));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        await c.NackAsync(await Take(c), true); // the message went back with the connection
        Assert.Equal(1, _a.Count<Request.Nack>());
    }

    [Fact]
    public async Task AMessageOfAnEndedSubscriptionIsNotRequeuedOnTheNewOne()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        Delivery old = await Take(c);
        seen.Conn!.Push(Failure(seen.Id, ErrorCode.AckTimedOut));
        Assert.Null(await c.PollAsync(Short)); // subscribes again
        Assert.Equal(2, _a.Count<Request.Subscribe>());
        await c.NackAsync(old, true);
        Assert.Equal(0, _a.Count<Request.Nack>()); // the broker took it back when the subscription ended
        // An ack still settles it: it names an offset.
        await c.AckAsync(old);
        Assert.Equal(1, _a.Count<Request.Ack>());
    }

    // ---- ending and subscribing again ----

    [Fact]
    public async Task APushWithNotLeaderMovesTheSubscriptionToTheHintedLeader()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Script(_b, (n, conn, r) =>
        {
            if (r is Request.Subscribe)
            {
                uint id = NewSub();
                conn.Push(PushAll(id, 30, 1));
                return new Response.Subscribe(ErrorCode.None, id, null);
            }
            return Plain(n, conn, r);
        });
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(Failure(seen.Id, ErrorCode.NotLeader, new LeaderHint(2, 2)));
        Delivery d = await Take(c);
        Assert.Equal(30UL, d.Offset);
        Assert.Equal(1, _b.Count<Request.Subscribe>());
        Assert.Equal(1, _a.Count<Request.Subscribe>());
        Assert.Equal(0, _a.Count<Request.Unsubscribe>()); // the push ended it: nothing to unsubscribe
    }

    [Fact]
    public async Task APushWithUnknownResourceForgetsTheRoute()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        int before = _a.Count<Request.Metadata>();
        seen.Conn!.Push(Failure(seen.Id, ErrorCode.UnknownResource));
        Assert.Null(await c.PollAsync(Short));
        Assert.True(_a.Count<Request.Metadata>() > before, "the route was looked up again");
        Assert.Equal(2, _a.Count<Request.Subscribe>());
    }

    [Fact]
    public async Task AnAckTimeoutEndsTheSubscriptionAndTheNextCallSubscribesAgain()
    {
        var seen = new Seen();
        int subscribes = 0;
        Script(_a, (n, conn, r) =>
        {
            if (r is Request.Subscribe)
            {
                seen.Conn = conn;
                uint id = NewSub();
                seen.Id = id;
                if (Interlocked.Increment(ref subscribes) > 1)
                {
                    conn.Push(PushAll(id, 50, 1));
                }
                return new Response.Subscribe(ErrorCode.None, id, null);
            }
            return Plain(n, conn, r);
        });
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(Failure(seen.Id, ErrorCode.AckTimedOut));
        Delivery d = await Take(c);
        Assert.Equal(50UL, d.Offset); // the message of the new subscription
        Assert.Equal(2, _a.Count<Request.Subscribe>());
        Assert.Single(_a.Conns()); // the connection is still good: the same one is used
    }

    [Fact]
    public async Task ABrokenConnectionEndsTheSubscriptionAndTheNextCallSubscribesAgain()
    {
        var seen = new Seen();
        Script(_a, (n, conn, r) =>
        {
            if (r is Request.Subscribe)
            {
                seen.All.Add(conn);
                uint id = NewSub();
                if (seen.All.Count > 1)
                {
                    conn.Push(PushAll(id, 50, 1));
                }
                return new Response.Subscribe(ErrorCode.None, id, null);
            }
            return Plain(n, conn, r);
        });
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.All[0].Drop();
        Delivery d = await Take(c);
        Assert.Equal(50UL, d.Offset);
        Assert.Equal(2, _a.Count<Request.Subscribe>());
        Assert.Equal(2, _a.Conns().Count); // on a new connection
    }

    [Fact]
    public async Task MessagesAlreadyReceivedAreStillHandedOutWhenTheSubscriptionEnds()
    {
        // The consumer reads the end only after it has handed out everything it holds; the broker gives those messages
        // to others (the delivery count then rises), so they may come twice but not zero times.
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(10));
        seen.Conn!.Push(PushAll(seen.Id, 10, 3));
        Assert.Equal(10UL, (await Take(c)).Offset);
        seen.All[0].Drop();
        await Task.Delay(100);
        Assert.Equal(11UL, (await Take(c)).Offset);
        Assert.Equal(12UL, (await Take(c)).Offset);
        Assert.Equal(1, _a.Count<Request.Subscribe>()); // not subscribed again while messages are held
        Assert.Null(await c.PollAsync(Short));
        Assert.Equal(2, _a.Count<Request.Subscribe>());
    }

    [Fact]
    public async Task AFailedResubscribeGivesUpAfterRetryFor()
    {
        var seen = new Seen();
        int subscribes = 0;
        Script(_a, (n, conn, r) =>
        {
            if (r is Request.Subscribe)
            {
                seen.Conn = conn;
                seen.Id = NewSub();
                return Interlocked.Increment(ref subscribes) == 1
                    ? new Response.Subscribe(ErrorCode.None, seen.Id, null)
                    : new Response.Subscribe(ErrorCode.NotLeader, 0, null);
            }
            return Plain(n, conn, r);
        });
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(Failure(seen.Id, ErrorCode.AckTimedOut));
        await Assert.ThrowsAsync<GaveUpException>(() => c.PollAsync(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task APushWhoseBatchCannotBeReadIsCorrupt()
    {
        // A batch that is not one at all is damage; a missing codec library is named instead (WithoutCodecsTests).
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        await using QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(new Frames.DeliverPush(
            seen.Id, ErrorCode.None, ByteString.CopyOf(new byte[100]), new[] { new Messages.DeliveredOffset(0, 1) }, null));
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => c.PollAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal(ErrorCode.CorruptBatch, e.Code);
    }

    // ---- closing ----

    [Fact]
    public async Task CloseUnsubscribesAndGivesBackWhatWasNotHandedOut()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(10));
        seen.Conn!.Push(PushAll(seen.Id, 10, 4));
        await Take(c); // offset 10 is handed out and stays with the consumer
        await c.DisposeAsync();
        await Eventually.AwaitAsync("the unsubscribe", () => _a.Count<Request.Unsubscribe>() == 1);
        await Task.Delay(150);
        Request.Nack nack = Assert.Single(seen.Conn!.Requests<Request.Nack>()); // the three not handed out go back
        List<Request> sent = seen.Conn!.Requests();
        Request.Unsubscribe un = seen.Conn!.Requests<Request.Unsubscribe>()[0];
        Assert.Equal(seen.Id, un.SubscriptionId);
        Assert.True(nack.Requeue);
        Assert.Equal(QueueId, nack.QueueId);
        Assert.Equal(Ranges((11, 14)), nack.Ranges); // the three not handed out, as one range
        Assert.True(sent.IndexOf(un) < sent.IndexOf(nack), "unsubscribe first");
        c.Dispose(); // twice is harmless
        await c.DisposeAsync();
        Assert.Equal(1, _a.Count<Request.Unsubscribe>());
    }

    [Fact]
    public async Task ClosingAlsoGivesBackWhatWasPushedAndNotReadYet()
    {
        // Pushes are read only once the buffer is empty: the second one is still unread when the consumer closes, and
        // the broker keeps the messages of an ending subscription until they are settled.
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(10));
        seen.Conn!.Push(PushAll(seen.Id, 10, 2));
        seen.Conn!.Push(PushAll(seen.Id, 12, 2));
        await Take(c); // offset 10 handed out; 11 is buffered, 12 and 13 are unread
        await Task.Delay(150);
        await c.DisposeAsync();
        await Eventually.AwaitAsync("the nack", () => seen.Conn!.Requests<Request.Nack>().Count == 1);
        Assert.Equal(Ranges((11, 14)), seen.Conn!.Requests<Request.Nack>()[0].Ranges);
    }

    [Fact]
    public async Task APushTakenByTheReaderWhileTheConsumerClosesIsGivenBack()
    {
        // The reading task has taken the push from the events when close runs; close cannot see it, so the reader
        // gives its messages back when it finds the subscription gone.
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        QueueConsumer c = await Client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(10));
        object sub = c.SubscriptionForTests!;
        await c.DisposeAsync();
        c.HandleForTests(sub, PushAll(seen.Id, 20, 3));
        await Eventually.AwaitAsync("the nack", () => seen.Conn!.Requests<Request.Nack>().Count == 1);
        Request.Nack nack = seen.Conn!.Requests<Request.Nack>()[0];
        Assert.True(nack.Requeue);
        Assert.Equal(Ranges((20, 23)), nack.Ranges);
    }

    [Fact]
    public async Task CloseWithNothingBufferedOnlyUnsubscribes()
    {
        Script(_a, Plain);
        Connect(_a);
        QueueConsumer c = await Client.SubscribeAsync("jobs");
        await c.DisposeAsync();
        await Eventually.AwaitAsync("the unsubscribe", () => _a.Count<Request.Unsubscribe>() == 1);
        await Task.Delay(100);
        Assert.Equal(0, _a.Count<Request.Nack>());
    }

    [Fact]
    public async Task AClosedConsumerRefusesFurtherReads()
    {
        Script(_a, Plain);
        Connect(_a);
        QueueConsumer c = await Client.SubscribeAsync("jobs");
        await c.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => c.PollAsync(Short));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => c.NextAsync());
    }

    [Fact]
    public async Task AMessageHandedOutBeforeCloseCanStillBeAcked()
    {
        var seen = new Seen();
        Script(_a, Recording(seen));
        Connect(_a);
        QueueConsumer c = await Client.SubscribeAsync("jobs");
        seen.Conn!.Push(PushAll(seen.Id, 10, 1));
        Delivery d = await Take(c);
        await c.DisposeAsync();
        await c.AckAsync(d);
        Assert.Equal(Ranges((10, 11)), _a.Requests<Request.Ack>()[0].Ranges);
    }

    [Fact]
    public async Task CloseWakesATaskWaitingForAMessage()
    {
        Script(_a, Plain);
        Connect(_a);
        QueueConsumer c = await Client.SubscribeAsync("jobs");
        Task<Delivery?> waiting = c.PollAsync(TimeSpan.FromSeconds(30));
        await Task.Delay(150);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        c.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => waiting);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5));
    }

    // ---- settings of a queue ----

    [Fact]
    public async Task CreateQueueSendsTheTenantTheReplicationTheLimitsAndTheDeadLetterQueue()
    {
        _a.On((n, r) => r switch
        {
            Request.Metadata => Fx.Metadata(new[] { n }),
            Request.CreateQueue => new Response.Create(ErrorCode.None, 77, null, null),
            _ => null,
        });
        _client = new LotseClient(Fx.Config(_a).WithTenant("acme").Build());
        QueueOptions options = QueueOptions.CreateBuilder()
            .WithReplication(5)
            .WithAckTimeout(TimeSpan.FromMilliseconds(700))
            .WithMaxDeliveries(4)
            .WithMessageTtl(TimeSpan.FromSeconds(2))
            .WithDeadLetterQueue("dead")
            .WithSingleActiveConsumer()
            .WithPriorityLevels(3)
            .WithSetting("x.other", "1")
            .Build();
        Assert.Equal(77UL, await Client.CreateQueueAsync("jobs", options));
        Request.CreateQueue q = _a.Requests<Request.CreateQueue>().Single();
        Assert.Equal("jobs", q.Name);
        Assert.Equal("acme", q.Tenant);
        Assert.Equal((byte)5, q.Replication);
        Assert.Equal("dead", q.Dlq);
        Assert.True(q.SingleActive);
        Assert.Equal((byte)3, q.PriorityLevels);
        Assert.Equal(
            new[] { ("ack.timeout.ms", "700"), ("max.deliveries", "4"), ("message.ttl.ms", "2000"), ("x.other", "1") },
            q.Configs.Select(c => (c.Key, c.Value)));
    }

    [Fact]
    public async Task CreateQueueDefaultsToThreeReplicasAndOneLevel()
    {
        _a.On((n, r) => r switch
        {
            Request.Metadata => Fx.Metadata(new[] { n }),
            Request.CreateQueue => new Response.Create(ErrorCode.None, 78, null, null),
            _ => null,
        });
        Connect(_a);
        await Client.CreateQueueAsync("jobs");
        Request.CreateQueue q = _a.Requests<Request.CreateQueue>().Single();
        Assert.Equal("default", q.Tenant);
        Assert.Equal((byte)3, q.Replication);
        Assert.Equal((byte)1, q.PriorityLevels); // the broker refuses 0
        Assert.False(q.SingleActive);
        Assert.Null(q.Dlq);
        Assert.Empty(q.Configs);
    }

    [Fact]
    public async Task ACreateThatFindsTheNameTakenNamesTheExistingQueue()
    {
        _a.On((n, r) => r switch
        {
            Request.Metadata => Fx.Metadata(new[] { n }),
            Request.CreateQueue => new Response.Create(ErrorCode.ResourceExists, 12, null, null),
            _ => null,
        });
        Connect(_a);
        var e = await Assert.ThrowsAsync<ResourceExistsException>(() => Client.CreateQueueAsync("jobs"));
        Assert.Equal(12UL, e.ResourceId);
    }

    [Fact]
    public void QueueOptionsRefuseWhatTheBrokerWould()
    {
        QueueOptions.Builder b = QueueOptions.CreateBuilder();
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithReplication(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithReplication(256));
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithPriorityLevels(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithPriorityLevels(11));
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithMaxDeliveries(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithAckTimeout(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithMessageTtl(TimeSpan.FromTicks(5)));
    }
}

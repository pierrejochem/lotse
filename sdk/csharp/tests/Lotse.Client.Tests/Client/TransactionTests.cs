using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Client.Tests.Net;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>Transactions against a scripted broker that is coordinator and leader at once, as the Rust client does them.</summary>
public sealed class TransactionTests : IDisposable
{
    private const ulong StreamId = 7;
    private const ulong ExchangeId = 20;
    private const ulong QueueA = 21;
    private const ulong QueueB = 22;

    private readonly FakeNode _node = new(1);
    private LotseClient? _client;
    private long _producerIds = 99;
    private long _offsets = -1;

    // What a produce to a resource id gets; absent: written at the next offset.
    private readonly ConcurrentDictionary<ulong, Func<Request.Produce, Response?>> _produce = new();
    private volatile Func<Request.EndTxn, Response> _endTxn =
        r => new Response.EndTxn(ErrorCode.None, r.ProducerId, (ushort)(r.ProducerEpoch + 1));
    private ErrorCode _offsetsError = ErrorCode.None;
    private ErrorCode _ackError = ErrorCode.None;
    private long _subscriptions = 499;

    /// <summary>The consumer id subscriptions are answered with (ADR 0109); 0 gives none.</summary>
    private ulong _consumerId;

    /// <summary>Acks to refuse as <c>NOT_LEADER</c> before taking them.</summary>
    private int _acksNotLed;
    private volatile Func<Request.AddPartitionsToTxn, ErrorCode> _addError = _ => ErrorCode.None;

    public TransactionTests() => _node.OnConn((n, conn, request) => Answer(n, request));

    public void Dispose()
    {
        _client?.Dispose();
        _node.Dispose();
    }

    private static Messages.ResourceMetadata Queue(string name, ulong id) =>
        new(ErrorCode.None, name, "default", id, Messages.ResourceKind.Queue, new[] { Fx.Partition(0, 1, 1, 1) }, null);

    private Response? Answer(FakeNode n, Request request)
    {
        switch (request)
        {
            case Request.Metadata:
                // A topic exchange: "a.#" to queue a, "*.b" to queue b.
                var x = new Messages.ExchangeMetadata(
                    Messages.ExchangeType.Topic,
                    new[]
                    {
                        new Messages.BindingMetadata(QueueA, "a.#", Array.Empty<Messages.HeaderMatch>(), true),
                        new Messages.BindingMetadata(QueueB, "*.b", Array.Empty<Messages.HeaderMatch>(), true),
                    });
                return Fx.Metadata(
                    new[] { n },
                    Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1), Fx.Partition(1, 1, 1, 1)),
                    new Messages.ResourceMetadata(
                        ErrorCode.None, "x", "default", ExchangeId, Messages.ResourceKind.Exchange, Array.Empty<Messages.PartitionMetadata>(), x),
                    Queue("qa", QueueA),
                    Queue("qb", QueueB));
            case Request.FindCoordinator:
                return Fx.Coordinator(n);
            case Request.InitProducer:
                return Fx.Producer((ulong)Interlocked.Increment(ref _producerIds), 4);
            case Request.AddPartitionsToTxn a:
                ErrorCode addError = _addError(a);
                return new Response.AddPartitionsToTxn(
                    a.Resources.Select(r => new Messages.TxnResourceResult(
                        r.ResourceId, r.Partitions.Select(p => new Messages.TxnPartitionResult(p, addError)).ToList())).ToList());
            case Request.Produce p:
                ulong id = p.Resources[0].ResourceId;
                if (_produce.TryGetValue(id, out Func<Request.Produce, Response?>? f))
                {
                    return f(p);
                }
                return Fx.Produced(id, p.Resources[0].Partitions[0].Partition, (ulong)Interlocked.Increment(ref _offsets));
            case Request.EndTxn e:
                return _endTxn(e);
            case Request.AddOffsetsToTxn o:
                return new Response.CommitOffsets(
                    o.Resources.Select(r => new Messages.TxnResourceResult(
                        r.ResourceId, r.Partitions.Select(p => new Messages.TxnPartitionResult(p.Partition, _offsetsError)).ToList())).ToList());
            case Request.Subscribe:
                return new Response.Subscribe(ErrorCode.None, (uint)Interlocked.Increment(ref _subscriptions), null, false, _consumerId);
            case Request.Ack:
                if (Interlocked.Decrement(ref _acksNotLed) >= 0)
                {
                    return new Response.Outcome(ErrorCode.NotLeader, null);
                }
                return new Response.Outcome(_ackError, null);
            case Request.Unsubscribe or Request.Nack:
                return new Response.Outcome(ErrorCode.None, null);
            default:
                throw new InvalidOperationException("unexpected " + request);
        }
    }

    private LotseClient Client(TimeSpan? retryFor = null)
    {
        _client = new LotseClient(Fx.Config(_node).WithTransactionalId("t").WithRetryFor(retryFor ?? TimeSpan.FromMilliseconds(400)).Build());
        return _client;
    }

    private List<Batch> Produced(ulong resourceId) =>
        _node.Requests<Request.Produce>().Where(p => p.Resources[0].ResourceId == resourceId).Select(Fx.BatchOf).ToList();

    [Fact]
    public async Task AClientWithoutATransactionalIdCannotBegin()
    {
        _client = new LotseClient(Fx.Config(_node).Build());
        await Assert.ThrowsAsync<NotTransactionalException>(() => _client.BeginTransactionAsync());
    }

    [Fact]
    public async Task BatchesCarryTheTransactionAndEachPartitionIsAddedOnce()
    {
        LotseClient lotse = Client();
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("s", 0, new[] { OutMessage.FromValue("a"), OutMessage.FromValue("b") });
            await txn.ProduceAsync("s", 0, OutMessage.FromValue("c"));
            await txn.ProduceAsync("s", 1, OutMessage.FromValue("d"));
            await txn.CommitAsync();
        }
        List<Request.AddPartitionsToTxn> adds = _node.Requests<Request.AddPartitionsToTxn>();
        Assert.Equal(2, adds.Count);
        Assert.Equal("t", adds[0].TxnId);
        List<Batch> batches = Produced(StreamId);
        Assert.Equal(3, batches.Count);
        foreach (Batch b in batches)
        {
            Assert.True(b.Attributes.Transactional);
            Assert.Equal(100UL, b.ProducerId);
            Assert.Equal(4, b.ProducerEpoch);
        }
        Assert.Equal(0, batches[0].BaseSequence);
        Assert.Equal(2, batches[1].BaseSequence); // counted by records
        Assert.Equal(0, batches[2].BaseSequence); // per partition
        foreach (Request.Produce p in _node.Requests<Request.Produce>())
        {
            Assert.Equal("t", p.TxnId);
        }
        Request.EndTxn end = Assert.Single(_node.Requests<Request.EndTxn>());
        Assert.True(end.Commit);
        Assert.Equal(100UL, end.ProducerId);
    }

    [Fact]
    public async Task TheNextTransactionGoesOnWithTheRaisedEpoch()
    {
        LotseClient lotse = Client();
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("s", 0, OutMessage.FromValue("a"));
            await txn.CommitAsync();
        }
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("s", 0, OutMessage.FromValue("b"));
            await txn.AbortAsync();
        }
        Assert.Single(_node.Requests<Request.InitProducer>()); // the producer id is kept
        List<Batch> batches = Produced(StreamId);
        Assert.Equal(5, batches[1].ProducerEpoch); // the epoch EndTxn raised
        Assert.Equal(0, batches[1].BaseSequence); // a new epoch starts at 0
        Assert.False(_node.Requests<Request.EndTxn>()[1].Commit);
    }

    [Fact]
    public async Task ATransactionThatWroteNothingEndsWithoutTheCoordinator()
    {
        LotseClient lotse = Client();
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.CommitAsync();
        }
        Assert.Empty(_node.Requests<Request.EndTxn>());
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.AbortAsync();
        }
        Assert.Single(_node.Requests<Request.InitProducer>());
    }

    [Fact]
    public async Task ATransactionThatIsDisposedOpenIsAborted()
    {
        LotseClient lotse = Client();
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("s", 0, OutMessage.FromValue("a"));
        }
        Request.EndTxn end = Assert.Single(_node.Requests<Request.EndTxn>());
        Assert.False(end.Commit);
        // The abort raised the epoch and the id is kept: the next transaction needs no new producer id.
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("s", 0, OutMessage.FromValue("b"));
            await txn.CommitAsync();
        }
        Assert.Single(_node.Requests<Request.InitProducer>());
        Assert.Equal(5, Produced(StreamId)[1].ProducerEpoch);
    }

    [Fact]
    public async Task ABlockThatEndsWithAnExceptionAbortsTheTransaction()
    {
        LotseClient lotse = Client();
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using Transaction txn = await lotse.BeginTransactionAsync();
            await txn.ProduceAsync("s", 0, OutMessage.FromValue("a"));
            throw new InvalidOperationException("the work failed");
        });
        Assert.False(Assert.Single(_node.Requests<Request.EndTxn>()).Commit);
    }

    [Fact]
    public async Task ACancelledProduceLeavesOnlyAnAbortAndDisposeAbortsIt()
    {
        _produce[StreamId] = _ => null; // never answers
        LotseClient lotse = Client(TimeSpan.FromSeconds(5));
        using var cancel = new CancellationTokenSource();
        await using Transaction txn = await lotse.BeginTransactionAsync();
        Task produce = txn.ProduceAsync("s", 0, OutMessage.FromValue("a"), cancel.Token);
        await Eventually.AwaitAsync("the produce", () => _node.Count<Request.Produce>() > 0);
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => produce);
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
        await Assert.ThrowsAsync<MustAbortException>(() => txn.ProduceAsync("s", 0, OutMessage.FromValue("b")));
        Assert.True(txn.IsOpen);
        await txn.DisposeAsync();
        Assert.False(txn.IsOpen);
        Assert.False(Assert.Single(_node.Requests<Request.EndTxn>()).Commit);
    }

    [Fact]
    public async Task ACancelledAddLeavesTheCoordinatorToBeToldOnAbort()
    {
        // The coordinator may hold the partition although the call was cancelled: the abort must reach it.
        _node.OnConn((n, conn, request) =>
            request is Request.AddPartitionsToTxn ? null : Answer(n, request));
        LotseClient lotse = Client(TimeSpan.FromSeconds(5));
        using var cancel = new CancellationTokenSource();
        await using Transaction txn = await lotse.BeginTransactionAsync();
        Task produce = txn.ProduceAsync("s", 0, OutMessage.FromValue("a"), cancel.Token);
        await Eventually.AwaitAsync("the add", () => _node.Count<Request.AddPartitionsToTxn>() > 0);
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => produce);
        _node.OnConn((n, conn, request) => Answer(n, request));
        await txn.AbortAsync();
        Assert.False(Assert.Single(_node.Requests<Request.EndTxn>()).Commit);
    }

    [Fact]
    public async Task AnUnknownWriteLeavesOnlyAnAbort()
    {
        _produce[StreamId] = _ => Fx.Produced(StreamId, 0, ErrorCode.RequestTimedOut, 0);
        LotseClient lotse = Client();
        await using Transaction txn = await lotse.BeginTransactionAsync();
        await Assert.ThrowsAsync<OutcomeUnknownException>(() => txn.ProduceAsync("s", 0, OutMessage.FromValue("a")));
        await Assert.ThrowsAsync<MustAbortException>(() => txn.ProduceAsync("s", 1, OutMessage.FromValue("b")));
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
        Assert.Empty(_node.Requests<Request.EndTxn>()); // the coordinator was never asked to commit
        Assert.True(txn.IsOpen); // open for the abort
        await txn.AbortAsync();
        Assert.False(txn.IsOpen);
        Assert.False(Assert.Single(_node.Requests<Request.EndTxn>()).Commit);
    }

    [Fact]
    public async Task AProduceThatEndsWithoutAnOutcomeLeavesOnlyAnAbort()
    {
        // The leader never answers; the client is closed while the produce waits.
        _produce[StreamId] = _ => null;
        var lotse = new LotseClient(Fx.Config(_node).WithTransactionalId("t")
            .WithRequestTimeout(TimeSpan.FromSeconds(5)).WithRetryFor(TimeSpan.FromSeconds(5)).Build());
        _client = lotse;
        Transaction txn = await lotse.BeginTransactionAsync();
        Task produce = txn.ProduceAsync("s", 0, OutMessage.FromValue("a"));
        await Eventually.AwaitAsync("the produce", () => _node.Count<Request.Produce>() > 0);
        lotse.Dispose();
        await Assert.ThrowsAnyAsync<Exception>(() => produce);
        await Assert.ThrowsAsync<MustAbortException>(() => txn.ProduceAsync("s", 0, OutMessage.FromValue("b")));
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
    }

    [Fact]
    public async Task ARefusedWriteLeavesOnlyAnAbort()
    {
        _produce[StreamId] = _ => Fx.Produced(StreamId, 0, ErrorCode.InvalidRequest, 0);
        LotseClient lotse = Client();
        await using Transaction txn = await lotse.BeginTransactionAsync();
        await Assert.ThrowsAsync<BrokerErrorException>(() => txn.ProduceAsync("s", 0, OutMessage.FromValue("a")));
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
        await txn.AbortAsync();
    }

    [Fact]
    public async Task AWriteThatCertainlyFailedLeavesTheTransactionOpen()
    {
        LotseClient lotse = Client();
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await Assert.ThrowsAsync<UnknownStreamException>(() => txn.ProduceAsync("nope", 0, OutMessage.FromValue("a")));
            await txn.ProduceAsync("s", 0, OutMessage.FromValue("b"));
            await txn.CommitAsync();
        }
        Assert.True(Assert.Single(_node.Requests<Request.EndTxn>()).Commit);
    }

    [Fact]
    public async Task ALostEndIsAskedAgainAndCountsAsDecided()
    {
        int ends = 0;
        _endTxn = r => Interlocked.Increment(ref ends) == 1
            ? new Response.EndTxn(ErrorCode.CoordinatorNotAvailable, 0, 0)
            : new Response.EndTxn(ErrorCode.None, r.ProducerId, (ushort)(r.ProducerEpoch + 1));
        LotseClient lotse = Client();
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("s", 0, OutMessage.FromValue("a"));
            await txn.CommitAsync();
        }
        Assert.Equal(2, ends);
    }

    [Fact]
    public async Task AnEndThatStaysUnknownReportsItAsSuch()
    {
        _endTxn = _ => new Response.EndTxn(ErrorCode.CoordinatorNotAvailable, 0, 0);
        LotseClient lotse = Client();
        await using Transaction txn = await lotse.BeginTransactionAsync();
        await txn.ProduceAsync("s", 0, OutMessage.FromValue("a"));
        await Assert.ThrowsAsync<OutcomeUnknownException>(() => txn.CommitAsync());
    }

    [Fact]
    public async Task AFailedEndStartsTheNextTransactionWithAFreshProducerId()
    {
        _endTxn = _ => new Response.EndTxn(ErrorCode.ProducerFenced, 0, 0);
        LotseClient lotse = Client();
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.ProduceAsync("s", 0, OutMessage.FromValue("a"));
            await Assert.ThrowsAsync<BrokerErrorException>(() => txn.CommitAsync());
        }
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            await txn.AbortAsync();
        }
        Assert.Equal(2, _node.Requests<Request.InitProducer>().Count);
    }

    [Fact]
    public async Task AnAddThatGaveUpDoesNotEndTheTransactionWhileTheLastOneIsCompleting()
    {
        // The coordinator as the server has it (coord.rs): after EndTxn the decision is being completed (markers are
        // written) and the epoch is already raised. Until then it refuses AddPartitionsToTxn with
        // CONCURRENT_TRANSACTIONS, which is retriable, and an EndTxn of the next transaction with INVALID_TXN_STATE.
        bool completing = false;
        _endTxn = r =>
        {
            if (completing)
            {
                return new Response.EndTxn(ErrorCode.InvalidTxnState, 0, 0);
            }
            completing = true;
            return new Response.EndTxn(ErrorCode.None, r.ProducerId, (ushort)(r.ProducerEpoch + 1));
        };
        _addError = _ => completing ? ErrorCode.ConcurrentTransactions : ErrorCode.None;
        LotseClient lotse = Client();
        await using (Transaction first = await lotse.BeginTransactionAsync())
        {
            await first.ProduceAsync("s", 0, OutMessage.FromValue("a"));
            await first.CommitAsync();
        }
        await using Transaction second = await lotse.BeginTransactionAsync();
        // Nothing was written to the partition: the add gave up, so the transaction stays open ...
        await Assert.ThrowsAsync<GaveUpException>(() => second.ProduceAsync("s", 0, OutMessage.FromValue("b")));
        Assert.Single(Produced(StreamId));
        // ... and, as nothing is at the coordinator that this client knows of, commits without asking it.
        await second.CommitAsync();
        Assert.Single(_node.Requests<Request.EndTxn>());
    }

    [Fact]
    public async Task AFencedProducerCannotAddPartitions()
    {
        _addError = _ => ErrorCode.ProducerFenced;
        LotseClient lotse = Client();
        await using Transaction txn = await lotse.BeginTransactionAsync();
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => txn.ProduceAsync("s", 0, OutMessage.FromValue("a")));
        Assert.Equal(ErrorCode.ProducerFenced, e.Code);
        Assert.Empty(_node.Requests<Request.Produce>()); // nothing was written
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
    }

    [Fact]
    public async Task OneTransactionAtATime()
    {
        LotseClient lotse = Client();
        Transaction first = await lotse.BeginTransactionAsync();
        Task<Transaction> second = lotse.BeginTransactionAsync();
        await Task.Delay(100);
        Assert.False(second.IsCompleted); // waits for the first
        await first.AbortAsync();
        Transaction later = await second.WaitAsync(TimeSpan.FromSeconds(5));
        await later.AbortAsync();
    }

    [Fact]
    public async Task WaitingForTheTurnCanBeCancelled()
    {
        LotseClient lotse = Client();
        await using Transaction first = await lotse.BeginTransactionAsync();
        using var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lotse.BeginTransactionAsync(cancel.Token));
        await first.AbortAsync();
        await using Transaction again = await lotse.BeginTransactionAsync(); // the turn was not lost
    }

    // ---- publishing: routed by this client ----

    [Fact]
    public async Task APublishIsRoutedHereAndWrittenToEveryQueueInTheTransaction()
    {
        LotseClient lotse = Client();
        await using (Transaction txn = await lotse.BeginTransactionAsync())
        {
            Published p = await txn.PublishAsync("x", "a.b", OutMessage.FromValue("m"));
            Assert.Equal(new[] { "qa", "qb" }, p.Routed.Select(r => r.Queue));
            Published only = await txn.PublishAsync("x", "a.c", OutMessage.FromValue("n"), mandatory: true);
            Assert.Equal(new[] { "qa" }, only.Routed.Select(r => r.Queue));
            Assert.Empty((await txn.PublishAsync("x", "c", OutMessage.FromValue("o"))).Routed);
            await Assert.ThrowsAsync<UnroutableException>(() => txn.PublishAsync("x", "c", OutMessage.FromValue("o"), mandatory: true));
            await txn.CommitAsync();
        }
        Assert.Equal(2, Produced(QueueA).Count);
        Assert.Single(Produced(QueueB));
        Assert.True(Produced(QueueB)[0].Attributes.Transactional);
        Assert.Equal(0, _node.Count<Request.Publish>()); // not routed by a node
    }

    [Fact]
    public async Task APublishRoutesByTheHeadersToo()
    {
        LotseClient lotse = Client();
        await using Transaction txn = await lotse.BeginTransactionAsync();
        // The topic exchange ignores headers: a.b still goes to both.
        Published p = await txn.PublishAsync("x", "a.b", OutMessage.FromValue("m").WithHeader("h", "v"));
        Assert.Equal(2, p.Routed.Count);
        await txn.AbortAsync();
    }

    [Fact]
    public async Task APublishToAnUnknownExchangeIsRefusedAndLeavesTheTransactionOpen()
    {
        LotseClient lotse = Client();
        await using Transaction txn = await lotse.BeginTransactionAsync();
        await Assert.ThrowsAsync<UnknownStreamException>(() => txn.PublishAsync("nope", "k", OutMessage.FromValue("m")));
        await txn.ProduceAsync("s", 0, OutMessage.FromValue("ok"));
        await txn.CommitAsync();
    }

    [Fact]
    public async Task APublishThatReachedSomeQueuesOnlyLeavesOnlyAnAbort()
    {
        _produce[QueueB] = _ => Fx.Produced(QueueB, 0, ErrorCode.NotLeader, 0);
        LotseClient lotse = Client();
        await using Transaction txn = await lotse.BeginTransactionAsync();
        await Assert.ThrowsAsync<GaveUpException>(() => txn.PublishAsync("x", "a.b", OutMessage.FromValue("m")));
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
        await txn.AbortAsync();
        Assert.Single(Produced(QueueA));
    }

    // ---- queue acks in a transaction (ADR 0075) ----

    [Fact]
    public async Task AnAckInATransactionCarriesItOnTheSubscriptionsConnection()
    {
        LotseClient lotse = Client();
        await using QueueConsumer c = await lotse.SubscribeAsync("qa");
        await using Transaction txn = await lotse.BeginTransactionAsync();
        FakeNode.Conn on = _node.Conns()[^1];
        on.Push(new Frames.DeliverPush(500, ErrorCode.None, FetchTests.Data(3, 0, false, "m"), new[] { new Messages.DeliveredOffset(3, 1) }, null));
        Delivery d = (await c.PollAsync(TimeSpan.FromSeconds(3)))!;
        await c.AckInAsync(txn, d);
        await txn.CommitAsync();
        Request.Ack ack = on.Requests<Request.Ack>().Single(); // on the subscription's connection
        Assert.Equal("t", ack.TxnId);
        Assert.Equal(100UL, ack.ProducerId);
        Assert.Equal(4, ack.ProducerEpoch);
        Assert.Equal(new Messages.OffsetRange(3, 4), ack.Ranges.Single());
        Request.AddPartitionsToTxn add = _node.Requests<Request.AddPartitionsToTxn>().First();
        Assert.Equal(QueueA, add.Resources[0].ResourceId); // the queue joins the transaction first
        Assert.True(Assert.Single(_node.Requests<Request.EndTxn>()).Commit);
    }

    /// <summary>ADR 0109: the leader moved; the consumer resumes as itself and sends the ack on the new subscription.</summary>
    [Fact]
    public async Task AnAckRefusedAsNotLeaderGoesOnASubscriptionThatResumes()
    {
        _consumerId = 77;
        _acksNotLed = 1;
        LotseClient lotse = Client();
        await using QueueConsumer c = await lotse.SubscribeAsync("qa");
        await using Transaction txn = await lotse.BeginTransactionAsync();
        FakeNode.Conn on = _node.Conns()[^1];
        on.Push(new Frames.DeliverPush(500, ErrorCode.None, FetchTests.Data(3, 0, false, "m"), new[] { new Messages.DeliveredOffset(3, 1) }, null));
        Delivery d = (await c.PollAsync(TimeSpan.FromSeconds(3)))!;
        await c.AckInAsync(txn, d);
        await txn.CommitAsync();
        List<Request.Subscribe> subs = _node.Requests<Request.Subscribe>();
        Assert.Equal(2, subs.Count); // subscribed again
        Assert.Equal(77UL, subs[1].Resume); // as the consumer it was
        Assert.Equal(2, _node.Requests<Request.Ack>().Count); // sent again on the new subscription
    }

    [Fact]
    public async Task AnAckTheLeaderRefusesLeavesOnlyAnAbort()
    {
        _ackError = ErrorCode.MessageNotHeld;
        LotseClient lotse = Client();
        await using QueueConsumer c = await lotse.SubscribeAsync("qa");
        await using Transaction txn = await lotse.BeginTransactionAsync();
        FakeNode.Conn on = _node.Conns()[^1];
        on.Push(new Frames.DeliverPush(500, ErrorCode.None, FetchTests.Data(3, 0, false, "m"), new[] { new Messages.DeliveredOffset(3, 1) }, null));
        Delivery d = (await c.PollAsync(TimeSpan.FromSeconds(3)))!;
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => c.AckInAsync(txn, d));
        Assert.Equal(ErrorCode.MessageNotHeld, e.Code);
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
        await txn.AbortAsync();
        Assert.False(Assert.Single(_node.Requests<Request.EndTxn>()).Commit);
    }

    [Fact]
    public async Task AnAckLostWithItsConnectionIsNotHeld()
    {
        int acks = 0;
        _node.OnConn((n, conn, request) =>
        {
            if (request is Request.Ack && Interlocked.Increment(ref acks) == 1)
            {
                throw new FakeNode.Drop();
            }
            return Answer(n, request);
        });
        LotseClient lotse = Client(TimeSpan.FromSeconds(2));
        await using QueueConsumer c = await lotse.SubscribeAsync("qa");
        await using Transaction txn = await lotse.BeginTransactionAsync();
        FakeNode.Conn on = _node.Conns()[^1];
        on.Push(new Frames.DeliverPush(500, ErrorCode.None, FetchTests.Data(3, 0, false, "m"), new[] { new Messages.DeliveredOffset(3, 1) }, null));
        Delivery d = (await c.PollAsync(TimeSpan.FromSeconds(3)))!;
        // The connection is dropped with the ack: the message went back, and the ack must not claim otherwise.
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => c.AckInAsync(txn, d));
        Assert.Equal(ErrorCode.MessageNotHeld, e.Code);
        Assert.Equal(1, acks); // not sent again on a connection that is gone
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
    }
}

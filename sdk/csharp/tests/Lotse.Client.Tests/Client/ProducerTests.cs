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

/// <summary>Idempotent and plain produce against a scripted broker.</summary>
public sealed class ProducerTests : IDisposable
{
    private const ulong StreamId = 7;

    private readonly FakeNode _node = new(1);
    private LotseClient? _client;
    private long _nextProducerId = 100;
    private long _offsets;

    public void Dispose()
    {
        _client?.Dispose();
        _node.Dispose();
    }

    /// <summary>The broker serves metadata (partitions 0 and 1, both led by node 1), a coordinator, and <paramref name="produce"/>.</summary>
    private void Serve(Func<int, Response> produce)
    {
        int produces = 0;
        _node.On((n, request) =>
        {
            switch (request)
            {
                case Request.Metadata:
                    return Fx.Metadata(new[] { n }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1), Fx.Partition(1, 1, 1, 1)));
                case Request.FindCoordinator:
                    return Fx.Coordinator(n);
                case Request.InitProducer:
                    return Fx.Producer((ulong)Interlocked.Increment(ref _nextProducerId) - 1, 3);
                case Request.Produce:
                    return produce(Interlocked.Increment(ref produces));
                default:
                    throw new InvalidOperationException("unexpected " + request);
            }
        });
    }

    private Response Ok() => Fx.Produced(StreamId, 0, (ulong)Interlocked.Increment(ref _offsets) - 1);

    private LotseClient Client(bool idempotent)
    {
        _client = new LotseClient(Fx.Config(_node).WithIdempotence(idempotent).WithRetryFor(TimeSpan.FromMilliseconds(400)).Build());
        return _client;
    }

    /// <summary>
    /// Stream <c>s</c> has the resource id in <c>current[0]</c>. A produce to another id is refused with NOT_LEADER
    /// (nothing written), as a node that applied the delete would; with <c>swallowThen[0]</c> set, the next produce's
    /// answer is lost and the stream moves to that id meanwhile (ADR 0107).
    /// </summary>
    private void ServeIncarnations(long[] current, long[] swallowThen)
    {
        _node.On((n, request) =>
        {
            switch (request)
            {
                case Request.Metadata:
                    return Fx.Metadata(new[] { n }, Fx.Stream("s", (ulong)Interlocked.Read(ref current[0]), Fx.Partition(0, 1, 1, 1)));
                case Request.FindCoordinator:
                    return Fx.Coordinator(n);
                case Request.InitProducer:
                    return Fx.Producer((ulong)Interlocked.Increment(ref _nextProducerId) - 1, 3);
                case Request.Produce p:
                    ulong id = p.Resources[0].ResourceId;
                    long then = Interlocked.Exchange(ref swallowThen[0], 0);
                    if (then != 0)
                    {
                        Interlocked.Exchange(ref current[0], then);
                        throw new FakeNode.Drop();
                    }
                    return id == (ulong)Interlocked.Read(ref current[0])
                        ? Fx.Produced(id, 0, (ulong)Interlocked.Increment(ref _offsets) - 1)
                        : Fx.Produced(id, 0, ErrorCode.NotLeader, 0);
                default:
                    throw new InvalidOperationException("unexpected " + request);
            }
        });
    }

    private List<string> IncarnationsSent() =>
        _node.Requests<Request.Produce>()
            .Select(p => $"{p.Resources[0].ResourceId}/{Fx.BatchOf(p).ProducerId}/{Fx.BatchOf(p).BaseSequence}")
            .ToList();

    [Fact]
    public async Task EachIncarnationOfAStreamCountsItsOwnSequences()
    {
        long[] current = { 7 };
        ServeIncarnations(current, new long[] { 0 });
        LotseClient lotse = Client(true);
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        // Deleted and created again: the cached route still names 7, which is refused; the produce does not follow the
        // name to 9.
        Interlocked.Exchange(ref current[0], 9);
        await Assert.ThrowsAsync<UnknownStreamException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        // A node behind still serves 7: back there, its sequences go on.
        Interlocked.Exchange(ref current[0], 7);
        await Assert.ThrowsAsync<UnknownStreamException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        // A refused batch keeps its sequence; 9 starts at 0; 7 goes on at 2.
        Assert.Equal(new[] { "7/100/0", "7/100/1", "7/100/2", "9/100/0", "9/100/1", "7/100/2" }, IncarnationsSent());
    }

    [Fact]
    public async Task AProduceInDoubtWhenItsStreamMovesStartsANewProducer()
    {
        long[] current = { 7 };
        long[] swallowThen = { 0 };
        ServeIncarnations(current, swallowThen);
        LotseClient lotse = Client(true);
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        // The next produce's answer is lost while the stream is created again: it may be in 7.
        Interlocked.Exchange(ref swallowThen[0], 9);
        await Assert.ThrowsAsync<UnknownStreamException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        List<string> sent = IncarnationsSent();
        Assert.Equal("7/100/1", sent[1]);
        Assert.Equal("9/101/0", sent[^1]); // a new producer id
    }

    [Fact]
    public async Task AnIdempotentProducerAsksForItsIdOnceAndCountsRecordsPerPartition()
    {
        Serve(_ => Ok());
        LotseClient lotse = Client(true);
        await lotse.ProduceAsync("s", 0, Fx.Msgs(2));
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        await lotse.ProduceAsync("s", 0, Fx.Msgs(3));
        await lotse.ProduceAsync("s", 1, Fx.Msgs(1));

        Request.FindCoordinator find = _node.Requests<Request.FindCoordinator>()[0];
        Assert.Equal(Messages.CoordinatorType.Producer, find.KeyType);
        Assert.Equal(string.Empty, find.Key);
        Assert.Equal(1, _node.Count<Request.FindCoordinator>());
        List<Request.InitProducer> inits = _node.Requests<Request.InitProducer>();
        Assert.Single(inits);
        Assert.Null(inits[0].TxnId); // an idempotent producer has no transactional id

        List<Request.Produce> sent = _node.Requests<Request.Produce>();
        Assert.Equal(new[] { 0, 2, 3 }, new[] { 0, 1, 2 }.Select(i => Fx.BatchOf(sent[i]).BaseSequence));
        Assert.Equal(0, Fx.BatchOf(sent[3]).BaseSequence); // another partition has its own sequence
        foreach (Request.Produce p in sent)
        {
            Batch b = Fx.BatchOf(p);
            Assert.Equal(100UL, b.ProducerId);
            Assert.Equal((ushort)3, b.ProducerEpoch);
            Assert.False(b.Attributes.Transactional);
        }
        Assert.Equal(5000u, sent[0].TimeoutMs); // produce_timeout goes to the leader
    }

    [Fact]
    public async Task ABatchWhoseOutcomeIsUnknownIsSentAgainWithTheSameSequence()
    {
        // The first answer is lost with the connection, the second says the quorum timed out, the third works.
        Serve(n => n switch
        {
            1 => throw new FakeNode.Drop(),
            2 => Fx.Produced(StreamId, 0, ErrorCode.RequestTimedOut, 0),
            _ => Ok(),
        });
        LotseClient lotse = Client(true);
        await lotse.ProduceAsync("s", 0, Fx.Msgs(2));
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));

        List<Request.Produce> sent = _node.Requests<Request.Produce>();
        Assert.Equal(4, sent.Count);
        Assert.Equal(Fx.BytesOf(sent[0]), Fx.BytesOf(sent[1])); // the same bytes again
        Assert.Equal(Fx.BytesOf(sent[0]), Fx.BytesOf(sent[2]));
        Assert.Equal(0, Fx.BatchOf(sent[2]).BaseSequence);
        Assert.Equal(2, Fx.BatchOf(sent[3]).BaseSequence); // advanced once, by the two records
        Assert.Equal(1, _node.Count<Request.InitProducer>()); // the producer id stayed
    }

    [Fact]
    public async Task AnOutcomeStillUnknownWhenTimeRunsOutIsReportedAndTheNextProduceGetsANewProducerId()
    {
        int phase = 0;
        Serve(_ => Volatile.Read(ref phase) == 0 ? Fx.Produced(StreamId, 0, ErrorCode.RequestTimedOut, 0) : Ok());
        LotseClient lotse = Client(true);

        var unknown = await Assert.ThrowsAsync<OutcomeUnknownException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(2)));
        Assert.Contains("still unknown", unknown.Message);
        List<Request.Produce> sent = _node.Requests<Request.Produce>();
        Assert.True(sent.Count >= 2, "it retried while it could");
        foreach (Request.Produce p in sent)
        {
            Assert.Equal(Fx.BytesOf(sent[0]), Fx.BytesOf(p)); // always the same sequence
        }
        Assert.Equal(0UL, lotse.ProducerId); // the session was discarded

        Volatile.Write(ref phase, 1);
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        Assert.Equal(2, _node.Count<Request.InitProducer>()); // a new producer id
        Batch next = Fx.BatchOf(_node.Requests<Request.Produce>()[sent.Count]);
        Assert.Equal(101UL, next.ProducerId);
        Assert.Equal(0, next.BaseSequence); // the new id starts at 0
    }

    [Theory]
    [InlineData(7)]
    [InlineData(8)]
    public async Task AnOutOfOrderSequenceOrAFencedProducerIsReportedAndStartsOver(ushort codeValue)
    {
        var code = new ErrorCode(codeValue);
        int phase = 0;
        Serve(_ => Volatile.Read(ref phase) == 0 ? Fx.Produced(StreamId, 0, code, 0) : Ok());
        using var lotse = new LotseClient(Fx.Config(_node).WithRetryFor(TimeSpan.FromMilliseconds(400)).Build());
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
        Assert.Equal(code, e.Code);
        Assert.Equal(0UL, lotse.ProducerId); // discarded after the error
        Volatile.Write(ref phase, 1);
        long before = Interlocked.Read(ref _nextProducerId);
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        Assert.Equal(before + 1, Interlocked.Read(ref _nextProducerId)); // a new producer id
    }

    [Fact]
    public async Task ANamedProducerAsksUnderItsNameAndStaysFencedOnceANewerInstanceTookOver()
    {
        int phase = 0;
        Serve(_ => Volatile.Read(ref phase) == 0 ? Ok() : Fx.Produced(StreamId, 0, ErrorCode.ProducerFenced, 0));
        using (var lotse = new LotseClient(
            Fx.Config(_node).WithProducerName("orders-writer").WithRetryFor(TimeSpan.FromMilliseconds(400)).Build()))
        {
            await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
            Assert.Equal("orders-writer", _node.Requests<Request.InitProducer>()[0].TxnId); // asked by name
            Assert.Equal("orders-writer", _node.Requests<Request.FindCoordinator>()[0].Key);
            Volatile.Write(ref phase, 1);
            for (int i = 0; i < 2; i++)
            {
                var e = await Assert.ThrowsAsync<BrokerErrorException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
                Assert.Equal(ErrorCode.ProducerFenced, e.Code);
            }
            Assert.Equal(1, _node.Count<Request.InitProducer>()); // no new epoch that would fence the newer one
        }
        Assert.Throws<ArgumentException>(() => Fx.Config(_node).WithProducerName("a\0b"));
    }

    [Fact]
    public async Task ADefiniteFailureKeepsTheSessionAndTheSequence()
    {
        int phase = 0;
        Serve(_ => Volatile.Read(ref phase) == 0 ? Fx.Produced(StreamId, 0, ErrorCode.QuotaExceeded, 0) : Ok());
        LotseClient lotse = Client(true);
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(2)));
        Assert.Equal(ErrorCode.QuotaExceeded, e.Code);
        Assert.Equal(100UL, lotse.ProducerId); // the producer id is kept
        Volatile.Write(ref phase, 1);
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        Assert.Equal(1, _node.Count<Request.InitProducer>());
        List<Request.Produce> sent = _node.Requests<Request.Produce>();
        Assert.Equal(0, Fx.BatchOf(sent[1]).BaseSequence); // the sequence of a batch that was not written is free
    }

    [Fact]
    public async Task ProduceToTheSamePartitionIsSerialisedAndToOthersIsNot()
    {
        var release = new TaskCompletionSource();
        int inFlight = 0;
        int maxInFlight = 0;
        _node.OnAsync(async (n, request) =>
        {
            switch (request)
            {
                case Request.Metadata:
                    return Fx.Metadata(new[] { n }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1), Fx.Partition(1, 1, 1, 1)));
                case Request.FindCoordinator:
                    return Fx.Coordinator(n);
                case Request.InitProducer:
                    return Fx.Producer(100, 1);
            }
            var p = (Request.Produce)request;
            uint partition = p.Resources[0].Partitions[0].Partition;
            if (partition == 0)
            {
                int now = Interlocked.Increment(ref inFlight);
                int seen;
                while ((seen = Volatile.Read(ref maxInFlight)) < now && Interlocked.CompareExchange(ref maxInFlight, now, seen) != seen)
                {
                }
                await release.Task.WaitAsync(TimeSpan.FromSeconds(10));
                Interlocked.Decrement(ref inFlight);
            }
            return Fx.Produced(StreamId, partition, 0);
        });
        LotseClient lotse = Client(true);
        await lotse.MetadataAsync(new[] { "s" }); // route and producer id are settled before the race
        await lotse.ProduceAsync("s", 1, Fx.Msgs(1));
        int sentBefore = _node.Count<Request.Produce>();

        Task<ProduceResult> first = lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        await Net.Eventually.AwaitAsync("the first produce to arrive", () => _node.Count<Request.Produce>() == sentBefore + 1);
        Task<ProduceResult> second = lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        // Another partition is not held up by the first.
        await lotse.ProduceAsync("s", 1, Fx.Msgs(1));
        await Task.Delay(300);
        Assert.Equal(sentBefore + 2, _node.Count<Request.Produce>()); // the second produce to partition 0 waits its turn

        release.SetResult();
        await first.WaitAsync(TimeSpan.FromSeconds(5));
        await second.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, Volatile.Read(ref maxInFlight)); // never two in flight to one partition
        List<int> seqs = _node.Requests<Request.Produce>()
            .Where(p => p.Resources[0].Partitions[0].Partition == 0)
            .Select(p => Fx.BatchOf(p).BaseSequence)
            .ToList();
        Assert.Equal(new[] { 0, 1 }, seqs); // sequences reach the broker in order
    }

    [Fact]
    public async Task WithoutIdempotenceThereIsNoProducerIdAndAnUnknownOutcomeIsNeverRetried()
    {
        Serve(_ => Fx.Produced(StreamId, 0, ErrorCode.RequestTimedOut, 0));
        LotseClient lotse = Client(false);
        await Assert.ThrowsAsync<OutcomeUnknownException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
        Assert.Equal(1, _node.Count<Request.Produce>()); // not sent again
        Assert.Equal(0, _node.Count<Request.FindCoordinator>() + _node.Count<Request.InitProducer>());
        Assert.Equal(0UL, Fx.BatchOf(_node.Requests<Request.Produce>()[0]).ProducerId); // not stamped
    }

    [Fact]
    public async Task WithoutIdempotenceALostConnectionAfterSendingIsNeverRetried()
    {
        Serve(_ => throw new FakeNode.Drop());
        LotseClient lotse = Client(false);
        await Assert.ThrowsAsync<OutcomeUnknownException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
        Assert.Equal(1, _node.Count<Request.Produce>());
    }

    [Fact]
    public async Task WithoutIdempotenceALeaderErrorStillIsRetriedBecauseNothingWasWritten()
    {
        Serve(n => n == 1 ? Fx.Produced(StreamId, 0, ErrorCode.NotLeader, 0) : Ok());
        await Client(false).ProduceAsync("s", 0, Fx.Msgs(1));
        Assert.Equal(2, _node.Count<Request.Produce>());
    }

    [Fact]
    public async Task AConnectionThatCouldNotBeOpenedIsRetriedEvenWithoutIdempotence()
    {
        using var dead = new FakeNode(2);
        dead.Kill();
        // The metadata names a leader that is down for the first attempts: nothing is sent, so a retry is safe.
        int asked = 0;
        _node.On((n, request) =>
        {
            if (request is Request.Metadata)
            {
                bool first = Interlocked.Increment(ref asked) == 1;
                return Fx.Metadata(new[] { n, dead }, Fx.Stream("s", StreamId, Fx.Partition(0, first ? 2u : 1u, 1, 1, 2)));
            }
            return Fx.Produced(StreamId, 0, 11);
        });
        Assert.Equal(11UL, (await Client(false).ProduceAsync("s", 0, Fx.Msgs(1))).BaseOffset);
        Assert.Equal(1, _node.Count<Request.Produce>());
    }

    [Fact]
    public async Task AnAuthenticationFailureAfterALostAttemptIsReportedAsUnknown()
    {
        int n = 0;
        _node.On((self, request) =>
        {
            switch (request)
            {
                case Request.Metadata:
                    return Fx.Metadata(new[] { self }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1)));
                case Request.FindCoordinator:
                    return Fx.Coordinator(self);
                case Request.InitProducer:
                    return Fx.Producer(100, 1);
            }
            if (Interlocked.Increment(ref n) == 1)
            {
                throw new FakeNode.Drop();
            }
            return new Response.Failed(ErrorCode.Unauthorized);
        });
        LotseClient lotse = Client(true);
        var e = await Assert.ThrowsAsync<OutcomeUnknownException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
        Assert.Contains("may have been carried out", e.Message);
    }

    [Fact]
    public async Task TheTimestampDefaultsToNowAndAnExplicitOneIsKept()
    {
        Serve(_ => Ok());
        LotseClient lotse = Client(true);
        long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await lotse.ProduceAsync("s", 0, new[] { OutMessage.FromValue("a"), OutMessage.FromValue("b").WithTimestamp(1234) });
        long after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        List<BatchRecord> records = Fx.BatchOf(_node.Requests<Request.Produce>()[0]).Records();
        Assert.InRange(records[0].Timestamp, before, after); // now, not 0
        Assert.Equal(1234, records[1].Timestamp);
    }

    [Fact]
    public async Task KeysValuesAndHeadersArriveInTheBatch()
    {
        Serve(_ => Ok());
        await Client(true).ProduceAsync("s", 0, OutMessage.FromValue("v").WithKey("k").WithHeader("h", "x").WithHeader("n", (byte[]?)null));
        BatchRecord rec = Fx.BatchOf(_node.Requests<Request.Produce>()[0]).Records()[0];
        Assert.Equal(ByteString.Utf8("k"), rec.Key);
        Assert.Equal(ByteString.Utf8("v"), rec.Value);
        Assert.Equal(2, rec.Headers.Count);
        Assert.Equal(ByteString.Utf8("x"), rec.Headers[0].Value);
        Assert.Null(rec.Headers[1].Value);
    }

    [Fact]
    public async Task AnEmptyListIsRefused()
    {
        Serve(_ => Ok());
        LotseClient lotse = Client(true);
        await Assert.ThrowsAsync<ArgumentException>(() => lotse.ProduceAsync("s", 0, new List<OutMessage>()));
    }

    [Fact]
    public async Task ACancelledProduceLeavesItsSequenceInDoubtAndTheNextOneGetsANewProducerId()
    {
        // The answer never comes. The caller gives up: the batch may be written, so the sequence is not reused.
        int phase = 0;
        _node.OnAsync(async (n, request) =>
        {
            switch (request)
            {
                case Request.Metadata:
                    return Fx.Metadata(new[] { n }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1)));
                case Request.FindCoordinator:
                    return Fx.Coordinator(n);
                case Request.InitProducer:
                    return Fx.Producer((ulong)Interlocked.Increment(ref _nextProducerId) - 1, 3);
            }
            if (Volatile.Read(ref phase) == 0)
            {
                await Task.Delay(Timeout.Infinite);
            }
            return Ok();
        });
        LotseClient lotse = Client(true);
        using var cts = new CancellationTokenSource();
        Task<ProduceResult> stuck = lotse.ProduceAsync("s", 0, Fx.Msgs(1), cts.Token);
        await Net.Eventually.AwaitAsync("the produce to arrive", () => _node.Count<Request.Produce>() == 1);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stuck);
        Assert.Equal(0UL, lotse.ProducerId); // the session was discarded

        Volatile.Write(ref phase, 1);
        await lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        Assert.Equal(2, _node.Count<Request.InitProducer>()); // a new producer id
        Assert.Equal(101UL, Fx.BatchOf(_node.Requests<Request.Produce>()[1]).ProducerId);
        Assert.Equal(0, Fx.BatchOf(_node.Requests<Request.Produce>()[1]).BaseSequence);
    }

    [Fact]
    public async Task ACancelledProduceWithoutIdempotenceJustStops()
    {
        _node.OnAsync(async (n, request) =>
        {
            if (request is Request.Metadata)
            {
                return Fx.Metadata(new[] { n }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1)));
            }
            await Task.Delay(Timeout.Infinite);
            return null;
        });
        LotseClient lotse = Client(false);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1), cts.Token));
        Assert.Equal(1, _node.Count<Request.Produce>()); // not sent again
    }

    [Fact]
    public async Task AFailedCallCarriesTheClientException()
    {
        Serve(_ => Fx.Produced(StreamId, 0, ErrorCode.QuotaExceeded, 0));
        LotseClient lotse = Client(true);
        Task<ProduceResult> task = lotse.ProduceAsync("s", 0, Fx.Msgs(1));
        await Assert.ThrowsAsync<BrokerErrorException>(() => task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task APlainTransportThatWasNeverConfiguredIsRefusedNotRetried()
    {
        _node.On((n, r) => throw new InvalidOperationException("nothing may be sent"));
        using var lotse = new LotseClient(ClientConfig.CreateBuilder(_node.HostPort).WithRetryFor(TimeSpan.FromSeconds(5)).Build());
        await Assert.ThrowsAsync<InsecureTransportException>(() => lotse.ProduceAsync("s", 0, Fx.Msgs(1)));
        Assert.Empty(_node.Requests());
    }
}

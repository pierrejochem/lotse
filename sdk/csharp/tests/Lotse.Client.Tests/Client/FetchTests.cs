using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    private LotseClient? _client;

    public void Dispose()
    {
        _client?.Dispose();
        _a.Dispose();
        _b.Dispose();
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

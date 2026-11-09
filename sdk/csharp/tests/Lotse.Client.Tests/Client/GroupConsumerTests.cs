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

/// <summary>A consumer group member against a scripted coordinator and leader, as the Rust client's member behaves.</summary>
public sealed class GroupConsumerTests : IDisposable
{
    private const ulong StreamId = 7;
    private static readonly TimeSpan Wait = TimeSpan.FromMilliseconds(20);

    // The member's clock: time passes only when a test says so.
    private sealed class ManualTime : TimeProvider
    {
        private long _ms;

        public override long TimestampFrequency => 1000;

        public override long GetTimestamp() => Interlocked.Read(ref _ms);

        public void Advance(TimeSpan by) => Interlocked.Add(ref _ms, (long)by.TotalMilliseconds);
    }

    private readonly ManualTime _time = new();
    private readonly FakeNode _node = new(1);
    private LotseClient? _client;

    // The answer to the n-th heartbeat, counted from 1.
    private volatile Func<int, Response> _heartbeat = _ => Assign(0);
    private int _heartbeats;

    // Committed offsets per partition; absent: none.
    private readonly ConcurrentDictionary<uint, ulong> _committed = new();

    // Fetch answers per partition; absent: two records from the offset asked for.
    private readonly ConcurrentDictionary<uint, Func<ulong, Task<Response>>> _fetch = new();
    private ErrorCode _commitError = ErrorCode.None;

    public GroupConsumerTests() => _node.OnAsync(Answer);

    public void Dispose()
    {
        _client?.Dispose();
        _node.Dispose();
    }

    private static Response Assign(params uint[] partitions) =>
        // An interval of 0: every poll heartbeats.
        new Response.GroupHeartbeat(0, ErrorCode.None, "m1", 3, 0, new[] { new Messages.TxnResource(StreamId, partitions) });

    // Assign with the next heartbeat due only after ms.
    private static Response AssignEvery(uint ms, params uint[] partitions) =>
        new Response.GroupHeartbeat(0, ErrorCode.None, "m1", 3, ms, new[] { new Messages.TxnResource(StreamId, partitions) });

    private static Response Failed(ErrorCode code) => new Response.GroupHeartbeat(0, code, string.Empty, 0, 0, null);

    private Task<Response?> Answer(FakeNode n, Request request)
    {
        switch (request)
        {
            case Request.Metadata:
                return Done(Fx.Metadata(new[] { n }, Fx.Stream("s", StreamId, Fx.Partition(0, 1, 1, 1), Fx.Partition(1, 1, 1, 1))));
            case Request.FindCoordinator:
                return Done(Fx.Coordinator(n));
            case Request.GroupHeartbeat h:
                return Done(h.MemberEpoch == -1 ? Failed(ErrorCode.None) : _heartbeat(Interlocked.Increment(ref _heartbeats)));
            case Request.OffsetFetch f:
                return Done(new Response.OffsetFetch(f.Resources.Select(r => new Messages.OffsetFetchResource(
                    r.ResourceId,
                    r.Partitions.Select(p => new Messages.OffsetFetchPartition(
                        p, ErrorCode.None, _committed.GetValueOrDefault(p, Messages.NoOffset), 0, null)).ToList())).ToList()));
            case Request.Fetch f:
                Messages.FetchPartition fp = f.Resources[0].Partitions[0];
                if (_fetch.TryGetValue(fp.Partition, out Func<ulong, Task<Response>>? script))
                {
                    return Wrap(script(fp.FetchOffset));
                }
                ByteString records = FetchTests.Data(fp.FetchOffset, 0, false, "r" + fp.FetchOffset, "r" + (fp.FetchOffset + 1));
                return Done(Fx.Fetched(StreamId, fp.Partition, ErrorCode.None, records, 100, 100, 0));
            case Request.CommitOffsets c:
                return Done(CommitAnswer(c.Resources));
            case Request.AddOffsetsToTxn a:
                return Done(CommitAnswer(a.Resources));
            case Request.ListOffsets l:
                Messages.ListOffsetsPartition lp = l.Resources[0].Partitions[0];
                return Done(new Response.ListOffsets(new[]
                {
                    new Messages.ListOffsetsResourceResponse(
                        StreamId,
                        new[] { new Messages.ListOffsetsPartitionResponse(lp.Partition, ErrorCode.None, lp.Timestamp == -1 ? 42UL : 30UL, -1, null) }),
                }));
            case Request.InitProducer:
                return Done(Fx.Producer(100, 1));
            case Request.AddPartitionsToTxn a:
                return Done(new Response.AddPartitionsToTxn(new[]
                {
                    new Messages.TxnResourceResult(a.Resources[0].ResourceId, new[] { new Messages.TxnPartitionResult(0, ErrorCode.None) }),
                }));
            case Request.Produce p:
                return Done(Fx.Produced(p.Resources[0].ResourceId, 0, 0));
            case Request.EndTxn e:
                return Done(new Response.EndTxn(ErrorCode.None, e.ProducerId, (ushort)(e.ProducerEpoch + 1)));
            default:
                throw new InvalidOperationException("unexpected " + request);
        }
    }

    private static Task<Response?> Done(Response r) => Task.FromResult<Response?>(r);

    private static async Task<Response?> Wrap(Task<Response> r) => await r;

    private Response CommitAnswer(IReadOnlyList<Messages.OffsetCommitResource> resources) =>
        new Response.CommitOffsets(resources.Select(r => new Messages.TxnResourceResult(
            r.ResourceId, r.Partitions.Select(p => new Messages.TxnPartitionResult(p.Partition, _commitError)).ToList())).ToList());

    private LotseClient Client()
    {
        _client = new LotseClient(Fx.Config(_node).WithTimeProvider(_time).WithTransactionalId("t").WithRetryFor(TimeSpan.FromMilliseconds(400)).Build());
        return _client;
    }

    private List<Request.GroupHeartbeat> HeartbeatsSent() => _node.Requests<Request.GroupHeartbeat>();

    private static string[] Values(IReadOnlyList<GroupRecord> records) => records.Select(r => r.Record.Value!.ToUtf8()).ToArray();

    private static string Flat(IReadOnlyDictionary<string, IReadOnlyDictionary<int, ulong>> positions) =>
        string.Join(",", positions.SelectMany(s => s.Value.Select(p => $"{s.Key}/{p.Key}={p.Value}")));

    private static string Flat(IReadOnlyDictionary<string, IReadOnlyList<int>> assignment) =>
        string.Join(",", assignment.SelectMany(s => s.Value.Select(p => $"{s.Key}/{p}")));

    [Fact]
    public async Task AMemberJoinsReadsFromTheCommittedOffsetAndCommitsWhereItGot()
    {
        _committed[0] = 5;
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" }, GroupConfig.CreateBuilder().WithAutoCommit(false).Build());
        Assert.Equal(new[] { "r5", "r6" }, Values(await g.PollAsync(Wait)));
        Request.GroupHeartbeat join = HeartbeatsSent()[0];
        Assert.Equal(0, join.MemberEpoch);
        Assert.Equal(string.Empty, join.MemberId);
        Assert.Equal(new[] { StreamId }, join.Subscribed);
        Assert.Null(join.SubscribedPattern);
        Assert.Equal("m1", g.MemberId);
        Assert.Equal(3, g.MemberEpoch);
        Assert.Equal("s/0=7", Flat(g.Positions));
        await g.CommitAsync();
        Request.CommitOffsets c = _node.Requests<Request.CommitOffsets>()[0];
        Assert.Equal("m1", c.MemberId);
        Assert.Equal(3, c.MemberEpoch);
        Assert.Equal(7UL, c.Resources[0].Partitions[0].Offset);
        Assert.True(_node.Requests<Request.OffsetFetch>()[0].RequireStable); // waits for open transactions
    }

    [Fact]
    public async Task AMemberHeartbeatsAgainOnlyWhenDueAndReportsWhatItOwns()
    {
        _heartbeat = _ => new Response.GroupHeartbeat(0, ErrorCode.None, "m1", 3, 200, new[] { new Messages.TxnResource(StreamId, new uint[] { 0 }) });
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" });
        await g.PollAsync(Wait);
        await g.PollAsync(Wait);
        Assert.Single(HeartbeatsSent()); // not due yet
        _time.Advance(TimeSpan.FromMilliseconds(250));
        await g.PollAsync(Wait);
        Assert.Equal(2, HeartbeatsSent().Count);
        Request.GroupHeartbeat second = HeartbeatsSent()[1];
        Assert.Equal(3, second.MemberEpoch);
        Assert.Equal("m1", second.MemberId);
        Assert.Null(second.Subscribed); // the subscription goes with the join only
        Assert.Equal(new Messages.TxnResource(StreamId, new uint[] { 0 }).Partitions, second.Owned!.Single().Partitions);
    }

    [Fact]
    public async Task ARevokedPartitionIsCommittedThenReportedReleasedAtOnce()
    {
        // The revoking answer schedules the next heartbeat 10 s later; the release must not wait for it.
        _heartbeat = n => n == 1 ? Assign(0, 1) : AssignEvery(10_000, 1);
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" });
        await g.PollAsync(Wait);
        await g.PollAsync(Wait);
        Assert.Equal("s/1", Flat(g.Assignment));
        await g.PollAsync(Wait);
        Assert.Equal(3, HeartbeatsSent().Count); // released with the next poll
        List<Request> all = _node.Requests();
        int commit = -1;
        int release = -1;
        for (int i = 0; i < all.Count; i++)
        {
            if (all[i] is Request.CommitOffsets c && commit < 0)
            {
                Assert.Equal(0u, c.Resources[0].Partitions[0].Partition);
                commit = i;
            }
            if (all[i] is Request.GroupHeartbeat h && h.Owned is { Count: 1 } o && o[0].Partitions.SequenceEqual(new uint[] { 1 }) && release < 0)
            {
                release = i;
            }
        }
        Assert.True(commit >= 0, "the revoked partition's position is committed");
        Assert.True(release > commit, "and only then reported released");
    }

    [Fact]
    public async Task WithoutAutoCommitARevokedPartitionIsReleasedWithoutACommit()
    {
        _heartbeat = n => n == 1 ? Assign(0, 1) : Assign(1);
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" }, GroupConfig.CreateBuilder().WithAutoCommit(false).Build());
        await g.PollAsync(Wait);
        await g.PollAsync(Wait);
        await g.PollAsync(Wait);
        Assert.Empty(_node.Requests<Request.CommitOffsets>());
    }

    [Fact]
    public async Task APartitionRewoundWhileItsFetchIsOutHandsOutNothing()
    {
        // The position the fetch was made for is gone when the answer comes: its records would be read twice.
        var release = new TaskCompletionSource();
        _fetch[0] = async offset =>
        {
            await release.Task;
            return Fx.Fetched(StreamId, 0, ErrorCode.None, FetchTests.Data(offset, 0, false, "stale"), 100, 100, 0);
        };
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" }, GroupConfig.CreateBuilder().WithAutoCommit(false).Build());
        Task<IReadOnlyList<GroupRecord>> poll = g.PollAsync(TimeSpan.FromSeconds(5));
        await Eventually.AwaitAsync("the fetch", () => _node.Count<Request.Fetch>() > 0);
        g.Rewind();
        release.SetResult();
        Assert.Empty(await poll);
        Assert.Equal(string.Empty, Flat(g.Positions));
    }

    [Fact]
    public async Task AMemberWithoutAHeartbeatForTheSessionTimeoutDropsEverythingAndJoinsAgain()
    {
        _heartbeat = n => n == 1 ? Assign(0) : n < 100 ? Failed(ErrorCode.CoordinatorNotAvailable) : Assign(0);
        await using GroupConsumer g = await Client().JoinGroupAsync(
            "g", new[] { "s" }, GroupConfig.CreateBuilder().WithSessionTimeout(TimeSpan.FromMilliseconds(300)).Build());
        await g.PollAsync(Wait);
        Assert.Equal("s/0", Flat(g.Assignment));
        _time.Advance(TimeSpan.FromMilliseconds(350));
        await g.PollAsync(Wait);
        Assert.Equal(string.Empty, Flat(g.Assignment)); // the coordinator may have given it away
        Assert.Equal(string.Empty, g.MemberId);
        Request.GroupHeartbeat last = HeartbeatsSent()[^1];
        Assert.Equal(0, last.MemberEpoch); // joins again as a new member
        Assert.Empty(_node.Requests<Request.CommitOffsets>()); // no commit for what was lost
    }

    [Fact]
    public async Task AFetchThatOutlastsTheSessionHandsOutNothing()
    {
        // The fetch answer comes after the session timeout: the coordinator may have given the partition away.
        _fetch[0] = async offset =>
        {
            _time.Advance(TimeSpan.FromMilliseconds(400));
            return Fx.Fetched(StreamId, 0, ErrorCode.None, FetchTests.Data(offset, 0, false, "late"), 100, 100, 0);
        };
        await using GroupConsumer g = await Client().JoinGroupAsync(
            "g", new[] { "s" }, GroupConfig.CreateBuilder().WithSessionTimeout(TimeSpan.FromMilliseconds(300)).Build());
        Assert.Empty(await g.PollAsync(TimeSpan.FromMilliseconds(50)));
        Assert.Equal(string.Empty, Flat(g.Assignment));
        Assert.Equal(string.Empty, g.MemberId);
    }

    [Fact]
    public async Task AnUnknownMemberJoinsAgain()
    {
        _heartbeat = n => n == 2 ? Failed(ErrorCode.UnknownMemberId) : Assign(0);
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" });
        await g.PollAsync(Wait);
        await g.PollAsync(Wait);
        Assert.Equal(string.Empty, Flat(g.Assignment));
        await g.PollAsync(Wait);
        Request.GroupHeartbeat third = HeartbeatsSent()[2];
        Assert.Equal(0, third.MemberEpoch);
        Assert.Equal(string.Empty, third.MemberId);
        Assert.Equal(new[] { StreamId }, third.Subscribed);
    }

    [Fact]
    public async Task AFencedMemberEpochJoinsAgain()
    {
        _heartbeat = n => n == 2 ? Failed(ErrorCode.FencedMemberEpoch) : Assign(0);
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" });
        await g.PollAsync(Wait);
        await g.PollAsync(Wait);
        Assert.Equal(string.Empty, g.MemberId);
        await g.PollAsync(Wait);
        Assert.Equal(0, HeartbeatsSent()[2].MemberEpoch);
    }

    [Fact]
    public async Task AFencedInstanceStaysOut()
    {
        _heartbeat = _ => Failed(ErrorCode.FencedInstanceId);
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" }, GroupConfig.CreateBuilder().WithInstanceId("i1").Build());
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => g.PollAsync(Wait));
        Assert.Equal(ErrorCode.FencedInstanceId, e.Code);
        Assert.Equal("i1", HeartbeatsSent()[0].InstanceId);
        await Assert.ThrowsAsync<BrokerErrorException>(() => g.PollAsync(Wait));
        Assert.Single(HeartbeatsSent()); // it does not join again
    }

    [Fact]
    public async Task APartitionWithoutACommittedOffsetStartsWhereTheConfigSays()
    {
        await using GroupConsumer latest = await Client().JoinGroupAsync(
            "g", new[] { "s" }, GroupConfig.CreateBuilder().WithStartFrom(OffsetAt.Latest).Build());
        await latest.PollAsync(Wait);
        Assert.Equal("s/0=44", Flat(latest.Positions)); // 42, then the two records read
        Request.ListOffsets asked = _node.Requests<Request.ListOffsets>()[0];
        Assert.Equal(-1, asked.Resources[0].Partitions[0].Timestamp);
        Assert.Equal(Messages.Isolation.Committed, asked.Isolation);
        await using GroupConsumer earliest = await _client!.JoinGroupAsync("g2", new[] { "s" });
        await earliest.PollAsync(Wait);
        Assert.Equal("s/0=2", Flat(earliest.Positions)); // from 0, without asking
        Assert.Single(_node.Requests<Request.ListOffsets>());
    }

    [Fact]
    public async Task APositionRetentionDeletedGoesOnFromTheLogStart()
    {
        _committed[0] = 3;
        _fetch[0] = offset => Task.FromResult<Response>(offset < 10
            ? Fx.Fetched(StreamId, 0, ErrorCode.OffsetOutOfRange, ByteString.Empty, 100, 100, 10)
            : Fx.Fetched(StreamId, 0, ErrorCode.None, FetchTests.Data(offset, 0, false, "x"), 100, 100, 10));
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" });
        Assert.Empty(await g.PollAsync(Wait));
        Assert.Equal("s/0=10", Flat(g.Positions));
        Assert.Equal(new[] { "x" }, Values(await g.PollAsync(Wait)));
    }

    [Fact]
    public async Task RewindForgetsThePositions()
    {
        _committed[0] = 5;
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" }, GroupConfig.CreateBuilder().WithAutoCommit(false).Build());
        await g.PollAsync(Wait);
        g.Rewind();
        Assert.Equal(string.Empty, Flat(g.Positions));
        Assert.Equal(new[] { "r5", "r6" }, Values(await g.PollAsync(Wait))); // from the committed offset again
    }

    [Fact]
    public async Task DisposingCommitsAndLeaves()
    {
        GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" });
        await g.PollAsync(Wait);
        await g.DisposeAsync();
        Assert.Equal(2UL, _node.Requests<Request.CommitOffsets>()[0].Resources[0].Partitions[0].Offset);
        Request.GroupHeartbeat leave = HeartbeatsSent()[^1];
        Assert.Equal(-1, leave.MemberEpoch);
        Assert.Equal("m1", leave.MemberId);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => g.PollAsync(Wait));
    }

    [Fact]
    public async Task APatternGoesWithTheJoinInsteadOfStreams()
    {
        await using GroupConsumer g = await Client().JoinGroupMatchingAsync("g", "s.*");
        await g.PollAsync(Wait);
        Request.GroupHeartbeat join = HeartbeatsSent()[0];
        Assert.Equal("s.*", join.SubscribedPattern);
        Assert.Null(join.Subscribed);
        Assert.Equal("s/0", Flat(g.Assignment));
    }

    [Fact]
    public async Task TheRecordsCanBeReadAsAnAsyncSequence()
    {
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" });
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var seen = new List<string>();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (GroupRecord r in g.RecordsAsync(stop.Token))
            {
                seen.Add(r.Record.Value!.ToUtf8());
                if (seen.Count == 4)
                {
                    stop.Cancel();
                }
            }
        });
        Assert.Equal(new[] { "r0", "r1", "r2", "r3" }, seen);
    }

    [Fact]
    public async Task OffsetsSentWithATransactionCarryTheMember()
    {
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" }, GroupConfig.CreateBuilder().WithAutoCommit(false).Build());
        await g.PollAsync(Wait);
        await using (Transaction txn = await _client!.BeginTransactionAsync())
        {
            await txn.ProduceAsync("s", 1, OutMessage.FromValue("out"));
            await txn.SendOffsetsAsync(g);
            await txn.CommitAsync();
        }
        Request.AddOffsetsToTxn a = _node.Requests<Request.AddOffsetsToTxn>()[0];
        Assert.Equal("g", a.GroupId);
        Assert.Equal("m1", a.MemberId);
        Assert.Equal(3, a.MemberEpoch);
        Assert.Equal("t", a.TxnId);
        Assert.Equal(2UL, a.Resources[0].Partitions[0].Offset);
        Assert.Empty(_node.Requests<Request.CommitOffsets>()); // not committed on their own
    }

    [Fact]
    public async Task ATransactionThatOnlySentOffsetsIsEnded()
    {
        // The coordinator holds the offsets as pending until it hears, so a commit without a partition added still
        // sends EndTxn (the same for an abort after offsets whose answer was refused).
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" }, GroupConfig.CreateBuilder().WithAutoCommit(false).Build());
        await g.PollAsync(Wait);
        await using (Transaction txn = await _client!.BeginTransactionAsync())
        {
            await txn.SendOffsetsAsync(g);
            await txn.CommitAsync();
        }
        Assert.Empty(_node.Requests<Request.AddPartitionsToTxn>());
        Assert.True(Assert.Single(_node.Requests<Request.EndTxn>()).Commit);
        _commitError = ErrorCode.FencedMemberEpoch;
        await using Transaction refused = await _client!.BeginTransactionAsync();
        await Assert.ThrowsAsync<BrokerErrorException>(() => refused.SendOffsetsAsync(g));
        await refused.AbortAsync();
        Assert.Equal(2, _node.Requests<Request.EndTxn>().Count);
        Assert.False(_node.Requests<Request.EndTxn>()[1].Commit);
    }

    [Fact]
    public async Task RefusedOffsetsLeaveTheTransactionOnlyAnAbort()
    {
        await using GroupConsumer g = await Client().JoinGroupAsync("g", new[] { "s" }, GroupConfig.CreateBuilder().WithAutoCommit(false).Build());
        await g.PollAsync(Wait);
        _commitError = ErrorCode.FencedMemberEpoch;
        await using Transaction txn = await _client!.BeginTransactionAsync();
        await txn.ProduceAsync("s", 1, OutMessage.FromValue("out"));
        await Assert.ThrowsAsync<BrokerErrorException>(() => txn.SendOffsetsAsync(g));
        await Assert.ThrowsAsync<MustAbortException>(() => txn.CommitAsync());
        await txn.AbortAsync();
        Assert.False(Assert.Single(_node.Requests<Request.EndTxn>()).Commit);
    }

    [Fact]
    public void AGroupConfigRefusesWhatCannotWork()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => GroupConfig.CreateBuilder().WithSessionTimeout(TimeSpan.Zero));
        Assert.Throws<ArgumentException>(() => GroupConfig.CreateBuilder().WithInstanceId(string.Empty));
        Assert.Equal(TimeSpan.FromSeconds(45), GroupConfig.Defaults.SessionTimeout);
        Assert.True(GroupConfig.Defaults.AutoCommit);
    }
}

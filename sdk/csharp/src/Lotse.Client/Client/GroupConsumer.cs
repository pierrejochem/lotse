using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client;

/// <summary>
/// A member of a consumer group, from <see cref="LotseClient.JoinGroupAsync"/> (ADR 0033). The coordinator assigns
/// partitions; the member reads them with read_committed from the group's committed offsets. It follows the Rust
/// client's <c>GroupConsumer</c> rule for rule:
/// <list type="bullet">
/// <item>it heartbeats only from <see cref="PollAsync"/>, when one is due: call <c>PollAsync</c> more often than the
/// session timeout, or the member loses its partitions;</item>
/// <item>a revoked partition is no longer read from the moment the answer that revokes it arrives; with auto-commit its
/// position is committed, then the next heartbeat, at once, reports it released, and only then may another member get
/// it;</item>
/// <item>when no heartbeat has succeeded for the session timeout, counted from when the last successful one was sent,
/// every partition is dropped at once, since the coordinator may give them to others by then, and the member joins
/// again as a new member;</item>
/// <item>a partition whose committed offset retention has deleted goes on from the oldest record left.</item>
/// </list>
/// <para>Not for use by several tasks at once.</para>
/// </summary>
public sealed class GroupConsumer : IAsyncDisposable
{
    internal readonly record struct Part(string Stream, int Partition);

    private sealed class PartOrder : IComparer<Part>
    {
        internal static readonly PartOrder Instance = new();

        public int Compare(Part x, Part y)
        {
            int c = string.CompareOrdinal(x.Stream, y.Stream);
            return c != 0 ? c : x.Partition.CompareTo(y.Partition);
        }
    }

    private static readonly TimeSpan OneMillisecond = TimeSpan.FromMilliseconds(1);

    private readonly LotseClient _client;
    private readonly string[] _streams;
    private readonly string? _pattern; // subscribed by pattern instead (ADR 0073)
    private readonly GroupConfig _cfg;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private string _memberId = string.Empty;
    private int _epoch;

    // Owned partitions and the next offset to read; null until the committed offset is known.
    private readonly SortedDictionary<Part, ulong?> _owned = new(PartOrder.Instance);

    private TimeSpan? _lastOk; // when the last heartbeat that succeeded was sent
    private TimeSpan _nextHeartbeat = TimeSpan.Zero;
    private int _turn; // the partitions are read round robin
    private bool _fenced; // a newer instance with the same instance id took this member's place (ADR 0072): it stays out
    private bool _closed;

    internal GroupConsumer(LotseClient client, string group, string[] streams, string? pattern, GroupConfig cfg)
    {
        _client = client;
        Group = group;
        _streams = streams;
        _pattern = pattern;
        _cfg = cfg;
    }

    private static ClientException FencedInstance() =>
        new BrokerErrorException(ErrorCode.FencedInstanceId, "another member joined with this instance id");

    /// <summary>The group's id.</summary>
    public string Group { get; }

    /// <summary>The member id, empty until joined.</summary>
    public string MemberId => _memberId;

    /// <summary>The member epoch.</summary>
    public int MemberEpoch => _epoch;

    /// <summary>The partitions this member owns now, as <c>stream -&gt; partitions</c>, in order.</summary>
    public IReadOnlyDictionary<string, IReadOnlyList<int>> Assignment
    {
        get
        {
            var output = new SortedDictionary<string, IReadOnlyList<int>>(StringComparer.Ordinal);
            foreach (Part p in _owned.Keys)
            {
                if (!output.TryGetValue(p.Stream, out IReadOnlyList<int>? list))
                {
                    list = new List<int>();
                    output[p.Stream] = list;
                }
                ((List<int>)list).Add(p.Partition);
            }
            return output;
        }
    }

    /// <summary>The next offset to read per owned partition whose position is known, as <c>stream -&gt; partition -&gt; offset</c>.</summary>
    public IReadOnlyDictionary<string, IReadOnlyDictionary<int, ulong>> Positions
    {
        get
        {
            var output = new SortedDictionary<string, IReadOnlyDictionary<int, ulong>>(StringComparer.Ordinal);
            foreach (KeyValuePair<Part, ulong> e in PositionMap())
            {
                if (!output.TryGetValue(e.Key.Stream, out IReadOnlyDictionary<int, ulong>? map))
                {
                    map = new SortedDictionary<int, ulong>();
                    output[e.Key.Stream] = map;
                }
                ((SortedDictionary<int, ulong>)map)[e.Key.Partition] = e.Value;
            }
            return output;
        }
    }

    internal SortedDictionary<Part, ulong> PositionMap()
    {
        var output = new SortedDictionary<Part, ulong>(PartOrder.Instance);
        foreach (KeyValuePair<Part, ulong?> e in _owned)
        {
            if (e.Value is ulong at)
            {
                output[e.Key] = at;
            }
        }
        return output;
    }

    /// <summary>
    /// Heartbeats when one is due, then reads what one owned partition has, waiting up to <paramref name="wait"/>.
    /// Returns the records in offset order; empty if there is nothing yet.
    /// </summary>
    /// <exception cref="BrokerErrorException">With <c>FENCED_INSTANCE_ID</c> once a newer instance took this member's place.</exception>
    /// <exception cref="ObjectDisposedException">The member left the group.</exception>
    public async Task<IReadOnlyList<GroupRecord>> PollAsync(TimeSpan wait, CancellationToken cancellationToken = default)
    {
        if (wait < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(wait), "the wait is not negative");
        }
        if (_closed)
        {
            throw new ObjectDisposedException(nameof(GroupConsumer), "the consumer is closed");
        }
        if (_fenced)
        {
            throw FencedInstance();
        }
        TimeSpan now = _clock.Elapsed;
        if (_lastOk is TimeSpan ok && now - ok >= _cfg.SessionTimeout)
        {
            // The coordinator may have given everything away by now.
            Lost();
        }
        if (now >= _nextHeartbeat || _memberId.Length == 0)
        {
            await HeartbeatAsync(cancellationToken).ConfigureAwait(false);
        }
        await LoadPositionsAsync(cancellationToken).ConfigureAwait(false);
        List<KeyValuePair<Part, ulong>> ready = PositionMap().ToList();
        TimeSpan untilHeartbeat = Max(TimeSpan.Zero, _nextHeartbeat - now);
        if (ready.Count == 0)
        {
            TimeSpan pause = Min(Max(TimeSpan.Zero, _nextHeartbeat - _clock.Elapsed), wait);
            await Task.Delay(Max(pause, OneMillisecond), cancellationToken).ConfigureAwait(false);
            return Array.Empty<GroupRecord>();
        }
        _turn = unchecked(_turn + 1);
        KeyValuePair<Part, ulong> pick = ready[((_turn % ready.Count) + ready.Count) % ready.Count];
        Part part = pick.Key;
        ulong offset = pick.Value;
        TimeSpan fetchWait = Max(Min(wait, untilHeartbeat), OneMillisecond);
        Fetched f;
        try
        {
            f = await _client.FetchAsync(part.Stream, part.Partition, offset, fetchWait, cancellationToken).ConfigureAwait(false);
        }
        catch (GaveUpException)
        {
            return Array.Empty<GroupRecord>();
        }
        catch (OffsetOutOfRangeException e)
        {
            // Retention deleted what the group had not read: go on from the oldest record left, as Kafka's earliest
            // reset does.
            if (e.LogStart <= offset)
            {
                throw;
            }
            if (_owned.TryGetValue(part, out ulong? at) && at == offset)
            {
                _owned[part] = e.LogStart;
            }
            return Array.Empty<GroupRecord>();
        }
        if (_lastOk is not TimeSpan sent || _clock.Elapsed - sent >= _cfg.SessionTimeout)
        {
            Lost();
            return Array.Empty<GroupRecord>();
        }
        // Revoked or lost meanwhile: nothing of it is handed out.
        if (!_owned.TryGetValue(part, out ulong? current) || current != offset)
        {
            return Array.Empty<GroupRecord>();
        }
        var output = new List<GroupRecord>();
        foreach (Batch b in f.CommittedBatches())
        {
            List<BatchRecord> records;
            try
            {
                records = b.Records();
            }
            catch (RecordFormatException e)
            {
                // A codec library missing here is no damage: say which one to add (ADR 0084).
                if (e.Kind == RecordFormatErrorKind.UnsupportedCompression)
                {
                    throw;
                }
                throw new BrokerErrorException(ErrorCode.CorruptBatch, e.Message);
            }
            foreach (BatchRecord r in records)
            {
                output.Add(new GroupRecord(part.Stream, part.Partition, r));
            }
        }
        _owned[part] = f.NextOffset(offset);
        return output;
    }

    /// <summary>
    /// The records as an async sequence: polls with a wait of half a second and yields what comes. It ends when the
    /// token is cancelled (with <see cref="OperationCanceledException"/>) or the member is disposed (with
    /// <see cref="ObjectDisposedException"/>), and throws what <see cref="PollAsync"/> throws.
    /// </summary>
    public async IAsyncEnumerable<GroupRecord> RecordsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (GroupRecord r in await PollAsync(TimeSpan.FromMilliseconds(500), cancellationToken).ConfigureAwait(false))
            {
                yield return r;
            }
        }
    }

    private static TimeSpan Max(TimeSpan a, TimeSpan b) => a > b ? a : b;

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;

    /// <summary>Everything is gone: no commits, join again as a new member.</summary>
    private void Lost()
    {
        _owned.Clear();
        _memberId = string.Empty;
        _epoch = 0;
        _lastOk = null;
    }

    /// <summary>One heartbeat. Only what retrying cannot fix (a refusal, a login that fails) is an error.</summary>
    private async Task HeartbeatAsync(CancellationToken ct)
    {
        TimeSpan sent = _clock.Elapsed;
        bool joining = _memberId.Length == 0;
        var ids = new List<ulong>();
        foreach (string s in _streams)
        {
            if (_client.StreamId(s) is ulong id)
            {
                ids.Add(id);
            }
        }
        var byStream = new SortedDictionary<ulong, List<uint>>();
        foreach (Part p in _owned.Keys)
        {
            if (_client.StreamId(p.Stream) is ulong id)
            {
                if (!byStream.TryGetValue(id, out List<uint>? parts))
                {
                    parts = new List<uint>();
                    byStream[id] = parts;
                }
                parts.Add((uint)p.Partition);
            }
        }
        List<Messages.TxnResource> ownedNow = byStream.Select(e => new Messages.TxnResource(e.Key, e.Value)).ToList();
        var req = new Request.GroupHeartbeat(
            Group,
            _memberId,
            joining ? 0 : _epoch,
            (uint)Math.Min(_cfg.RebalanceTimeout.TotalMilliseconds, uint.MaxValue),
            joining && _pattern is null ? ids : null,
            ownedNow,
            _client.Config.Tenant,
            _cfg.InstanceId,
            joining ? _pattern : null);
        TimeSpan retrySoon = sent + _client.Config.Backoff;
        Response.GroupHeartbeat resp;
        try
        {
            if (await _client.CallCoordinatorAsync(Group, req, ct).ConfigureAwait(false) is not Response.GroupHeartbeat r)
            {
                _client.ForgetCoordinator();
                _nextHeartbeat = retrySoon;
                return;
            }
            resp = r;
        }
        catch (Attempt a)
        {
            if (a.Outcome == Attempt.Kind.Fail)
            {
                throw a.Error!;
            }
            _client.ForgetCoordinator();
            _nextHeartbeat = retrySoon;
            return;
        }
        ErrorCode code = resp.Error;
        if (code.Equals(ErrorCode.UnknownMemberId) || code.Equals(ErrorCode.FencedMemberEpoch))
        {
            Lost();
            _nextHeartbeat = retrySoon;
            return;
        }
        if (code.Equals(ErrorCode.FencedInstanceId))
        {
            // A newer instance with this instance id took the place: stop, rather than join again and take it back.
            Lost();
            _fenced = true;
            throw FencedInstance();
        }
        if (!code.IsOk)
        {
            if (code.Retriable)
            {
                _client.ForgetCoordinator();
            }
            _nextHeartbeat = retrySoon;
            return;
        }
        _lastOk = sent;
        _memberId = resp.MemberId;
        _epoch = resp.MemberEpoch;
        TimeSpan interval = Min(TimeSpan.FromMilliseconds(resp.HeartbeatIntervalMs), _cfg.SessionTimeout / 3);
        _nextHeartbeat = sent + interval;
        IReadOnlyList<Messages.TxnResource>? assignment = resp.Assignment;
        if (assignment is null)
        {
            return;
        }
        // A pattern may bring streams this client has not looked up yet.
        foreach (Messages.TxnResource r in assignment)
        {
            if (_client.StreamName(r.ResourceId) is null)
            {
                try
                {
                    await _client.MetadataAsync(null, ct).ConfigureAwait(false);
                }
                catch (ClientException)
                {
                    // the names come with a later heartbeat
                }
                break;
            }
        }
        var target = new SortedSet<Part>(PartOrder.Instance);
        foreach (Messages.TxnResource r in assignment)
        {
            if (_client.StreamName(r.ResourceId) is not string name)
            {
                continue;
            }
            foreach (uint p in r.Partitions)
            {
                target.Add(new Part(name, (int)p));
            }
        }
        List<Part> revoked = _owned.Keys.Where(p => !target.Contains(p)).ToList();
        if (revoked.Count > 0)
        {
            // Stop reading them now; commit where they got to, then report them released with the next heartbeat, which
            // comes at once.
            var positions = new SortedDictionary<Part, ulong>(PartOrder.Instance);
            foreach (Part p in revoked)
            {
                ulong? at = _owned[p];
                _owned.Remove(p);
                if (at is ulong position)
                {
                    positions[p] = position;
                }
            }
            if (_cfg.AutoCommit && positions.Count > 0)
            {
                try
                {
                    await CommitPositionsAsync(positions, ct).ConfigureAwait(false);
                }
                catch (ClientException)
                {
                    // the next owner starts from the last commit
                }
            }
            _nextHeartbeat = _clock.Elapsed;
        }
        foreach (Part p in target)
        {
            _owned.TryAdd(p, null);
        }
    }

    /// <summary>
    /// Learns the committed offsets of partitions whose position is not known yet; a partition whose offset an open
    /// transaction holds waits for it.
    /// </summary>
    private async Task LoadPositionsAsync(CancellationToken ct)
    {
        var byStream = new SortedDictionary<ulong, List<uint>>();
        foreach (KeyValuePair<Part, ulong?> e in _owned)
        {
            if (e.Value is not null)
            {
                continue;
            }
            if (_client.StreamId(e.Key.Stream) is ulong id)
            {
                if (!byStream.TryGetValue(id, out List<uint>? parts))
                {
                    parts = new List<uint>();
                    byStream[id] = parts;
                }
                parts.Add((uint)e.Key.Partition);
            }
        }
        if (byStream.Count == 0)
        {
            return;
        }
        List<Messages.TxnResource> resources = byStream.Select(e => new Messages.TxnResource(e.Key, e.Value)).ToList();
        var req = new Request.OffsetFetch(Group, true, resources, _client.Config.Tenant);
        Response.OffsetFetch resp;
        try
        {
            if (await _client.CallCoordinatorAsync(Group, req, ct).ConfigureAwait(false) is not Response.OffsetFetch r)
            {
                _client.ForgetCoordinator();
                return;
            }
            resp = r;
        }
        catch (Attempt)
        {
            _client.ForgetCoordinator();
            return;
        }
        foreach (Messages.OffsetFetchResource r in resp.Resources)
        {
            if (_client.StreamName(r.ResourceId) is not string name)
            {
                continue;
            }
            foreach (Messages.OffsetFetchPartition p in r.Partitions)
            {
                if (!p.Error.IsOk)
                {
                    continue;
                }
                ulong start;
                if (p.Offset != Messages.NoOffset)
                {
                    start = p.Offset;
                }
                else if (_cfg.StartFrom.Equals(OffsetAt.Earliest))
                {
                    // From the start: a position below the log start goes on from there anyway.
                    start = 0;
                }
                else
                {
                    try
                    {
                        start = await _client.ListOffsetAsync(name, (int)p.Partition, _cfg.StartFrom, ct).ConfigureAwait(false);
                    }
                    catch (ClientException)
                    {
                        // asked again with the next fetch of positions
                        continue;
                    }
                }
                var key = new Part(name, (int)p.Partition);
                if (_owned.ContainsKey(key))
                {
                    _owned[key] = start;
                }
            }
        }
    }

    /// <summary>Commits the position of every owned partition whose position is known.</summary>
    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        SortedDictionary<Part, ulong> positions = PositionMap();
        return positions.Count == 0 ? Task.CompletedTask : CommitPositionsAsync(positions, cancellationToken);
    }

    private Task CommitPositionsAsync(SortedDictionary<Part, ulong> positions, CancellationToken ct)
    {
        var req = new Request.CommitOffsets(Group, _memberId, _epoch, Resources(_client, positions), _client.Config.Tenant);
        return _client.RetryingAsync(() => CommitOnceAsync(_client, Group, req, ct), ct);
    }

    /// <summary>The positions as commit resources, by stream id.</summary>
    internal static List<Messages.OffsetCommitResource> Resources(LotseClient client, SortedDictionary<Part, ulong> positions)
    {
        var by = new SortedDictionary<ulong, List<Messages.OffsetCommitPartition>>();
        foreach (KeyValuePair<Part, ulong> e in positions)
        {
            if (client.StreamId(e.Key.Stream) is ulong id)
            {
                if (!by.TryGetValue(id, out List<Messages.OffsetCommitPartition>? list))
                {
                    list = new List<Messages.OffsetCommitPartition>();
                    by[id] = list;
                }
                list.Add(new Messages.OffsetCommitPartition((uint)e.Key.Partition, e.Value, 0, null));
            }
        }
        return by.Select(e => new Messages.OffsetCommitResource(e.Key, e.Value)).ToList();
    }

    /// <summary>One <c>CommitOffsets</c> or <c>AddOffsetsToTxn</c> to the coordinator, keyed by <paramref name="key"/>.</summary>
    internal static async Task<int> CommitOnceAsync(LotseClient client, string key, Request req, CancellationToken ct)
    {
        if (await client.CallCoordinatorAsync(key, req, ct).ConfigureAwait(false) is not Response.CommitOffsets r)
        {
            throw Attempt.Retry("unexpected response");
        }
        foreach (Messages.TxnResourceResult res in r.Resources)
        {
            foreach (Messages.TxnPartitionResult p in res.Partitions)
            {
                ErrorCode code = p.Error;
                if (code.IsOk)
                {
                    continue;
                }
                if (code.Retriable)
                {
                    client.ForgetCoordinator();
                    throw Attempt.RetryMaybeDone(code.ToString());
                }
                throw Attempt.Fail(new BrokerErrorException(code, null));
            }
        }
        return 0;
    }

    /// <summary>
    /// Forgets the read positions: every owned partition starts again from the group's committed offset, as after an
    /// aborted transaction.
    /// </summary>
    public void Rewind()
    {
        foreach (Part p in _owned.Keys.ToList())
        {
            _owned[p] = null;
        }
    }

    /// <summary>Leaves the group, committing positions first when auto-commit is on. Failures are not thrown: leaving matters more.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_closed)
        {
            return;
        }
        _closed = true;
        if (_cfg.AutoCommit)
        {
            try
            {
                await CommitAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // leaving matters more
            }
        }
        if (_memberId.Length == 0)
        {
            return;
        }
        var leave = new Request.GroupHeartbeat(
            Group, _memberId, -1, 0, null, null, _client.Config.Tenant, _cfg.InstanceId, null);
        try
        {
            await _client.CallCoordinatorAsync(Group, leave, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // the coordinator drops the member after the session timeout
        }
        _owned.Clear();
    }
}

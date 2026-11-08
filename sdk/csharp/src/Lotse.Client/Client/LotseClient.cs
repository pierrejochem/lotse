using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Auth;
using Lotse.Net;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client;

/// <summary>
/// A client for Lotse streams, written to behave like the Rust client (<c>lotse-client</c>) and the Java one.
/// <para>
/// It looks up streams with <c>Metadata</c>, sends <c>Produce</c> and <c>Fetch</c> straight to each partition's
/// leader, and follows <c>NOT_LEADER</c> hints and metadata changes on its own. It connects on first use, keeps one
/// connection per broker address, and is safe to share between threads and tasks.
/// </para>
/// <para>
/// <b>Produce is idempotent by default</b> (<see cref="ClientConfig.Builder.WithIdempotence"/>). The client gets a
/// producer id from the coordinator (<c>FindCoordinator</c>, <c>InitProducer</c>), stamps every batch with it and a
/// sequence number per partition, and retries a batch whose outcome is unknown (the connection broke after sending,
/// or <c>REQUEST_TIMED_OUT</c>) with the <em>same</em> sequence; the leader recognises a batch it already has and
/// answers with its offset. Produces to one partition go out one at a time, so sequences arrive in order. Only when
/// retrying runs out of time with the outcome still unknown does the caller get <see cref="OutcomeUnknownException"/>;
/// the client then starts over with a new producer id, so later batches do not wait for a gap that may never fill. A
/// produce abandoned before its outcome is known, by cancelling its token, does the same. Without idempotence an
/// unknown outcome is never retried, since a retry could write the batch twice.
/// </para>
/// <para>
/// A <see cref="InsecureTransportException"/> (no TLS configured, or plain TCP to an address that is not loopback) is a
/// mistake in the configuration and is thrown as it is, not retried. Cancelling a token throws
/// <see cref="OperationCanceledException"/>.
/// </para>
/// </summary>
public sealed class LotseClient : IDisposable, IAsyncDisposable
{
    private const uint U32Max = uint.MaxValue;

    private readonly ClientConfig _cfg;
    private readonly ConnectionPool _pool;
    private volatile bool _closed;

    // Guards _brokers, _routes and _names.
    private readonly object _state = new();
    private readonly SortedDictionary<uint, Address> _brokers = new();
    private readonly Dictionary<PartitionKey, Route> _routes = new();
    private readonly Dictionary<ulong, string> _names = new();

    private readonly object _sessionLock = new();
    private Session? _session;
    private volatile Address? _coordinatorAddress;
    private readonly ConcurrentDictionary<PartitionKey, SemaphoreSlim> _partitionLocks = new();

    private long _metadataLeader = -1;
    private int _adminTurn;
    private ClientException? _fatal;
    private readonly ConditionalWeakTable<Connection, object> _seen = new();

    /// <summary>One partition's place in the cluster, as metadata last said.</summary>
    internal readonly record struct Route(ulong ResourceId, uint Leader, ulong Term);

    /// <summary>A route and the leader's address.</summary>
    internal readonly record struct Routed(Route Route, Address Address);

    private readonly record struct PartitionKey(string Stream, int Partition);

    /// <summary>The producer id this client writes with.</summary>
    private sealed class Session
    {
        internal Session(ulong producerId, ushort epoch)
        {
            ProducerId = producerId;
            Epoch = epoch;
        }

        internal ulong ProducerId { get; }

        internal ushort Epoch { get; }

        internal Dictionary<PartitionKey, int> Next { get; } = new();
    }

    /// <summary>What a produce is stamped with.</summary>
    private readonly record struct Stamp(ulong ProducerId, ushort Epoch, int Sequence);

    /// <summary>Creates a client. It connects on first use.</summary>
    public LotseClient(ClientConfig config)
    {
        _cfg = config ?? throw new ArgumentNullException(nameof(config));
        _pool = new ConnectionPool(config.ConnectionOptions());
    }

    /// <summary>Closes every connection. Calls in progress fail.</summary>
    public void Dispose()
    {
        _closed = true;
        _pool.Dispose();
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    // ---- metadata ----

    /// <summary>
    /// Asks any reachable broker for metadata and remembers the leaders.
    /// </summary>
    /// <param name="names">The resources to ask for; none asks for all.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    /// <exception cref="GaveUpException">No broker answers within <see cref="ClientConfig.RetryFor"/>.</exception>
    /// <exception cref="ClientAuthenticationException">A broker does not accept the credentials.</exception>
    public async Task<Response.Metadata> MetadataAsync(
        IReadOnlyList<string>? names = null, CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> asked = names ?? Array.Empty<string>();
        var clock = Stopwatch.StartNew();
        string why = "no broker known";
        while (true)
        {
            foreach (Address addr in Candidates())
            {
                try
                {
                    Response r = await CallAsync(addr, new Request.Metadata(asked), cancellationToken).ConfigureAwait(false);
                    if (r is Response.Metadata m)
                    {
                        Learn(m);
                        return m;
                    }
                    why = "unexpected response " + r;
                }
                catch (CallFailure e)
                {
                    if (e.Outcome == CallFailure.Kind.Denied)
                    {
                        throw new ClientAuthenticationException(e.Message);
                    }
                    why = addr + ": " + e.Describe();
                }
            }
            if (clock.Elapsed >= _cfg.RetryFor)
            {
                throw new GaveUpException(_cfg.RetryFor, why);
            }
            await Task.Delay(_cfg.Backoff, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Brokers known so far, then the bootstrap list, without repeats.</summary>
    private List<Address> Candidates()
    {
        var output = new List<Address>();
        lock (_state)
        {
            output.AddRange(_brokers.Values);
        }
        foreach (Address a in _cfg.Bootstrap)
        {
            if (!output.Contains(a))
            {
                output.Add(a);
            }
        }
        return output;
    }

    /// <summary>Asks every known broker and merges what they know: per name, the newest resource any of them knows.</summary>
    private async Task<List<Messages.ResourceMetadata>> MetadataFromAllAsync(string[] asked, CancellationToken ct)
    {
        var sorted = Candidates().OrderBy(a => a.ToString(), StringComparer.Ordinal).ToList();
        var merged = new List<Messages.ResourceMetadata>();
        foreach (Address addr in sorted)
        {
            try
            {
                if (await CallAsync(addr, new Request.Metadata(asked), ct).ConfigureAwait(false) is Response.Metadata m)
                {
                    Learn(m);
                    MergeNewest(merged, m.Resources);
                }
            }
            catch (CallFailure)
            {
                // a broker that does not answer knows nothing
            }
        }
        return merged;
    }

    /// <summary>
    /// Adds what one broker knows to <paramref name="merged"/>, keeping per name the newest resource: a name deleted
    /// and created again has a new, higher id (ids grow with the metadata log), and a broker that lags may still
    /// report the old one. A resource with an error names nothing.
    /// </summary>
    internal static void MergeNewest(List<Messages.ResourceMetadata> merged, IReadOnlyList<Messages.ResourceMetadata> more)
    {
        foreach (Messages.ResourceMetadata r in more)
        {
            if (!r.Error.IsOk)
            {
                continue;
            }
            int at = merged.FindIndex(x => x.Name == r.Name);
            if (at < 0)
            {
                merged.Add(r);
            }
            else if (merged[at].ResourceId < r.ResourceId)
            {
                merged[at] = r;
            }
        }
    }

    private void Learn(Response.Metadata m)
    {
        lock (_state)
        {
            foreach (Messages.Broker b in m.Brokers)
            {
                if (b.Port != 0)
                {
                    _brokers[b.NodeId] = new Address(b.Host, b.Port);
                }
            }
            foreach (Messages.ResourceMetadata r in m.Resources)
            {
                if (!r.Error.IsOk)
                {
                    continue;
                }
                _names[r.ResourceId] = r.Name;
                foreach (Messages.PartitionMetadata p in r.Partitions)
                {
                    _routes[new PartitionKey(r.Name, (int)p.Partition)] = new Route(r.ResourceId, p.Leader, p.LeaderTerm);
                }
            }
        }
    }

    // ---- routing ----

    /// <summary>The leader's address, refreshing metadata if none is known.</summary>
    internal async Task<Routed> RouteAsync(string stream, int partition, CancellationToken ct)
    {
        Route? known;
        lock (_state)
        {
            known = _routes.TryGetValue(new PartitionKey(stream, partition), out Route r) ? r : null;
        }
        Route route = known is { Leader: not 0 } k ? k : await ResolveAsync(stream, partition, ct).ConfigureAwait(false);
        Address? addr;
        lock (_state)
        {
            addr = _brokers.GetValueOrDefault(route.Leader);
        }
        if (addr is null)
        {
            Forget(stream, partition);
            throw Attempt.Retry("no address for node " + route.Leader);
        }
        return new Routed(route, addr);
    }

    private async Task<Route> ResolveAsync(string stream, int partition, CancellationToken ct)
    {
        Response.Metadata m;
        try
        {
            m = await MetadataAsync(new[] { stream }, ct).ConfigureAwait(false);
        }
        catch (ClientException e)
        {
            throw Attempt.Fail(e);
        }
        IReadOnlyList<Messages.ResourceMetadata> resources = m.Resources;
        if (Find(resources, stream) is null)
        {
            // A resource created a moment ago may not have reached that broker yet: any broker that knows it will do.
            resources = await MetadataFromAllAsync(new[] { stream }, ct).ConfigureAwait(false);
        }
        Messages.ResourceMetadata? res = Find(resources, stream);
        Messages.PartitionMetadata? p = res?.Partitions.FirstOrDefault(c => c.Partition == (uint)partition);
        if (res is null || p is null)
        {
            throw Attempt.Fail(new UnknownStreamException(stream, partition));
        }
        uint leader = p.Leader;
        ulong term = p.LeaderTerm;
        if (leader == 0)
        {
            // Partitions created at runtime live on some nodes only: their voters know the leader.
            foreach (uint voter in p.Voters)
            {
                Address? addr;
                lock (_state)
                {
                    addr = _brokers.GetValueOrDefault(voter);
                }
                if (addr is null)
                {
                    continue;
                }
                try
                {
                    if (await CallAsync(addr, new Request.Metadata(new[] { stream }), ct).ConfigureAwait(false) is not Response.Metadata vm)
                    {
                        continue;
                    }
                    Learn(vm);
                    Messages.ResourceMetadata? vr = Find(vm.Resources, stream, mustBeOk: false);
                    if (vr is null)
                    {
                        continue;
                    }
                    foreach (Messages.PartitionMetadata vp in vr.Partitions)
                    {
                        if (vp.Partition == (uint)partition && vp.Leader != 0)
                        {
                            leader = vp.Leader;
                            term = vp.LeaderTerm;
                        }
                    }
                    if (leader != 0)
                    {
                        break;
                    }
                }
                catch (CallFailure)
                {
                    // the next voter may know
                }
            }
        }
        if (leader == 0)
        {
            throw Attempt.Retry("the partition has no leader");
        }
        return new Route(res.ResourceId, leader, term);
    }

    private static Messages.ResourceMetadata? Find(
        IReadOnlyList<Messages.ResourceMetadata> resources, string name, bool mustBeOk = true) =>
        resources.FirstOrDefault(r => r.Name == name && (!mustBeOk || r.Error.IsOk));

    internal void Forget(string stream, int partition)
    {
        lock (_state)
        {
            _routes.Remove(new PartitionKey(stream, partition));
        }
    }

    /// <summary>
    /// Follows a leader hint, or forgets the route so metadata is reloaded. A hint is followed only if its term is
    /// not older than the route's and it names another leader: an older hint comes from a node that lags.
    /// </summary>
    internal void Redirect(string stream, int partition, LeaderHint? hint)
    {
        var key = new PartitionKey(stream, partition);
        lock (_state)
        {
            if (hint is not null && _routes.TryGetValue(key, out Route r) && hint.Term >= r.Term && hint.Leader != r.Leader)
            {
                _routes[key] = new Route(r.ResourceId, hint.Leader, hint.Term);
            }
            else
            {
                _routes.Remove(key);
            }
        }
    }

    // ---- the retry loop ----

    internal async Task<T> RetryingAsync<T>(Func<Task<T>> attempt, CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        bool maybeDone = false;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            string why;
            try
            {
                return await attempt().ConfigureAwait(false);
            }
            catch (Attempt a)
            {
                switch (a.Outcome)
                {
                    case Attempt.Kind.Fail:
                        // A failed login or a refusal now says nothing of an earlier attempt whose answer was lost:
                        // that one may have been carried out. (An answer about the request itself, such as
                        // RESOURCE_EXISTS, is the caller's to read.)
                        ClientException e = a.Error!;
                        bool refusal = e is ClientAuthenticationException
                            || (e is BrokerErrorException b && b.Code.Equals(ErrorCode.Unauthorized));
                        if (maybeDone && refusal)
                        {
                            throw new OutcomeUnknownException("an earlier attempt may have been carried out; then " + e.Message);
                        }
                        throw e;
                    case Attempt.Kind.Retry:
                        why = a.Message;
                        break;
                    default:
                        maybeDone = true;
                        why = a.Message;
                        break;
                }
            }
            // A connection that could not authenticate: the same credentials will not do better on the next try.
            ClientException? failedLogin = Interlocked.Exchange(ref _fatal, null);
            if (failedLogin is not null)
            {
                throw maybeDone
                    ? new OutcomeUnknownException("an earlier attempt may have been carried out; then " + failedLogin.Message)
                    : failedLogin;
            }
            if (clock.Elapsed >= _cfg.RetryFor)
            {
                throw maybeDone
                    ? new OutcomeUnknownException($"still unknown after {_cfg.RetryFor}: {why}")
                    : new GaveUpException(_cfg.RetryFor, why);
            }
            await Task.Delay(_cfg.Backoff, ct).ConfigureAwait(false);
        }
    }

    // ---- produce ----

    /// <summary>
    /// Appends messages to a partition as one batch and returns where it went once it is committed.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="partition">The partition; Lotse's client does not choose one for you.</param>
    /// <param name="messages">At least one message.</param>
    /// <param name="cancellationToken">
    /// Stops the call. A produce cancelled before its outcome is known leaves the sequence in doubt, so an idempotent
    /// client starts over with a new producer id.
    /// </param>
    /// <exception cref="OutcomeUnknownException">The outcome is unknown after retrying; see the class description.</exception>
    /// <exception cref="GaveUpException">Nothing was written and no broker could be reached in time.</exception>
    /// <exception cref="UnknownStreamException">The stream or partition does not exist.</exception>
    public Task<ProduceResult> ProduceAsync(
        string stream, int partition, IReadOnlyList<OutMessage> messages, CancellationToken cancellationToken = default) =>
        ProduceBatchAsync(stream, partition, Build(messages), cancellationToken);

    /// <summary>Appends one message to a partition; see <see cref="ProduceAsync(string, int, IReadOnlyList{OutMessage}, CancellationToken)"/>.</summary>
    public Task<ProduceResult> ProduceAsync(
        string stream, int partition, OutMessage message, CancellationToken cancellationToken = default) =>
        ProduceAsync(stream, partition, new[] { message }, cancellationToken);

    internal static ByteString Build(IReadOnlyList<OutMessage> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            throw new ArgumentException("there is no message to produce", nameof(messages));
        }
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var b = new BatchBuilder();
        foreach (OutMessage m in messages)
        {
            b.Push(m.Timestamp ?? now, m.Key, m.Value, m.Headers);
        }
        try
        {
            return b.Finish();
        }
        catch (RecordFormatException e)
        {
            throw new ArgumentException(e.Message, nameof(messages), e);
        }
    }

    /// <summary>
    /// Appends a record batch built with <see cref="BatchBuilder"/> to a partition. Returns the offset of its first
    /// record once the batch is committed. The batch is sent as it is, with the producer fields of this client written
    /// into a copy. A compressed batch is sent compressed.
    /// </summary>
    public async Task<ProduceResult> ProduceBatchAsync(
        string stream, int partition, ByteString batch, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(batch);
        if (!_cfg.Idempotent)
        {
            return await RetryingAsync(() => ProduceOnceAsync(stream, partition, batch, false, null, cancellationToken), cancellationToken)
                .ConfigureAwait(false);
        }
        uint records;
        try
        {
            records = Batch.Parse(batch).Batch.RecordCount;
        }
        catch (RecordFormatException e)
        {
            throw new BrokerErrorException(ErrorCode.CorruptBatch, e.Message);
        }
        var key = new PartitionKey(stream, partition);
        // One produce per partition at a time, so sequence numbers reach the leader in order.
        SemaphoreSlim turn = _partitionLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
        await turn.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Stamp stamp = await NextSequenceAsync(key, cancellationToken).ConfigureAwait(false);
            byte[] bytes = batch.ToByteArray();
            Batch.SetProducer(bytes, stamp.ProducerId, stamp.Epoch, stamp.Sequence);
            ByteString stamped = ByteString.View(bytes, 0, bytes.Length);
            ProduceResult result;
            // Abandoned before the outcome is known (a cancelled token, a client closed meanwhile) counts as unknown:
            // reusing the sequence would let the leader take the next batch for this one, answer with its offset and
            // write nothing.
            bool decided = false;
            try
            {
                // Every attempt sends the same bytes: the same producer id and the same sequence.
                result = await RetryingAsync(
                    () => ProduceOnceAsync(stream, partition, stamped, true, null, cancellationToken), cancellationToken)
                    .ConfigureAwait(false);
                decided = true;
            }
            catch (ClientException e)
            {
                decided = true;
                // Written or not, nobody can tell: later batches would wait behind a gap that may never fill. Start
                // over with a new producer id. Anything else was definitely not written, and the sequence is free for
                // the next batch. A named producer that is fenced has a newer instance: it keeps its fenced id rather
                // than start over, which would fence that one in turn (ADR 0071).
                bool fencedByName = _cfg.ProducerName is not null
                    && e is BrokerErrorException f
                    && f.Code.Equals(ErrorCode.ProducerFenced);
                bool startOver = !fencedByName
                    && (e is OutcomeUnknownException
                        || (e is BrokerErrorException b
                            && (b.Code.Equals(ErrorCode.OutOfOrderSequence) || b.Code.Equals(ErrorCode.ProducerFenced))));
                if (startOver)
                {
                    EndSession(stamp.ProducerId);
                }
                throw;
            }
            finally
            {
                if (!decided)
                {
                    EndSession(stamp.ProducerId);
                }
            }
            lock (_sessionLock)
            {
                if (_session is not null && _session.ProducerId == stamp.ProducerId)
                {
                    _session.Next[key] = unchecked(stamp.Sequence + (int)records);
                }
            }
            return result;
        }
        finally
        {
            turn.Release();
        }
    }

    private void EndSession(ulong producerId)
    {
        lock (_sessionLock)
        {
            if (_session is not null && _session.ProducerId == producerId)
            {
                _session = null;
            }
        }
    }

    /// <summary>The producer id, epoch and next sequence for a partition, getting a producer id first if there is none.</summary>
    private async Task<Stamp> NextSequenceAsync(PartitionKey key, CancellationToken ct)
    {
        lock (_sessionLock)
        {
            if (_session is not null)
            {
                return StampOf(_session, key);
            }
        }
        (ulong id, ushort epoch) = await RetryingAsync(() => InitProducerOnceAsync(_cfg.ProducerName, ct), ct).ConfigureAwait(false);
        lock (_sessionLock)
        {
            // Another partition's produce may have started a session meanwhile.
            _session ??= new Session(id, epoch);
            return StampOf(_session, key);
        }
    }

    private static Stamp StampOf(Session s, PartitionKey key) => new(s.ProducerId, s.Epoch, s.Next.GetValueOrDefault(key, 0));

    internal async Task<ProduceResult> ProduceOnceAsync(
        string stream, int partition, ByteString batch, bool idempotent, string? txnId, CancellationToken ct)
    {
        // An idempotent batch may be sent again whatever happened to it.
        Routed routed = await RouteAsync(stream, partition, ct).ConfigureAwait(false);
        var req = new Request.Produce(
            txnId,
            (uint)Math.Min(_cfg.ProduceTimeout.TotalMilliseconds, U32Max),
            new[] { new Messages.ProduceResource(routed.Route.ResourceId, new[] { new Messages.ProducePartition((uint)partition, batch) }) },
            _cfg.Tenant);
        Response resp;
        try
        {
            resp = await CallAsync(routed.Address, req, ct).ConfigureAwait(false);
        }
        catch (CallFailure e)
        {
            switch (e.Outcome)
            {
                case CallFailure.Kind.NotSent:
                    // Not sent at all: safe to try again.
                    Forget(stream, partition);
                    throw Attempt.Retry(e.Message);
                case CallFailure.Kind.Lost:
                    Forget(stream, partition);
                    throw Unknown(idempotent, e.Message);
                default:
                    throw e.ToAttempt();
            }
        }
        if (resp is not Response.Produce produced)
        {
            throw Unknown(idempotent, "unexpected response " + resp);
        }
        if (produced.Resources.Count == 0 || produced.Resources[0].Partitions.Count == 0)
        {
            throw Unknown(idempotent, "empty produce response");
        }
        Messages.ProducePartitionResponse p = produced.Resources[0].Partitions[0];
        ErrorCode code = p.Error;
        if (code.IsOk)
        {
            return new ProduceResult(p.BaseOffset, p.AppendTs);
        }
        if (code.Equals(ErrorCode.NotLeader) || code.Equals(ErrorCode.FencedLeaderTerm))
        {
            // Refused before it was proposed, or definitely not written.
            Redirect(stream, partition, p.LeaderHint);
            throw Attempt.Retry(code.ToString());
        }
        if (code.Equals(ErrorCode.RequestTimedOut))
        {
            Forget(stream, partition);
            throw Unknown(idempotent, p.ErrorMsg ?? code.ToString());
        }
        if (code.Equals(ErrorCode.UnknownResource))
        {
            // Deleted: the name may come back as a new resource.
            Forget(stream, partition);
        }
        throw Attempt.Fail(new BrokerErrorException(code, p.ErrorMsg));
    }

    private static Attempt Unknown(bool idempotent, string why) =>
        idempotent ? Attempt.RetryMaybeDone(why) : Attempt.Fail(new OutcomeUnknownException(why));

    // ---- the producer id ----

    /// <summary>The coordinator's address, asking any broker for it if needed.</summary>
    private async Task<Address> CoordinatorAsync(string key, CancellationToken ct)
    {
        Address? known = _coordinatorAddress;
        if (known is not null)
        {
            return known;
        }
        var find = new Request.FindCoordinator(Messages.CoordinatorType.Producer, key);
        string why = "no broker answered";
        foreach (Address addr in Candidates())
        {
            try
            {
                Response r = await CallAsync(addr, find, ct).ConfigureAwait(false);
                if (r is Response.FindCoordinator f)
                {
                    if (f.Error.IsOk)
                    {
                        Address a;
                        try
                        {
                            a = new Address(f.Host, f.Port);
                        }
                        catch (ArgumentException)
                        {
                            why = $"the coordinator's address {f.Host}:{f.Port} is not valid";
                            continue;
                        }
                        _coordinatorAddress = a;
                        return a;
                    }
                    why = f.ErrorMsg ?? f.Error.ToString();
                    if (!f.Error.Retriable)
                    {
                        throw Attempt.Fail(new BrokerErrorException(f.Error, why));
                    }
                }
                else
                {
                    why = "unexpected response " + r;
                }
            }
            catch (CallFailure e)
            {
                why = e.Describe();
            }
        }
        throw Attempt.Retry(why);
    }

    internal void ForgetCoordinator() => _coordinatorAddress = null;

    /// <summary>One request to the coordinator; failures become attempts (every coordinator request may be repeated).</summary>
    internal async Task<Response> CallCoordinatorAsync(string key, Request req, CancellationToken ct)
    {
        Address addr = await CoordinatorAsync(key, ct).ConfigureAwait(false);
        try
        {
            return await CallAsync(addr, req, ct).ConfigureAwait(false);
        }
        catch (CallFailure e)
        {
            ForgetCoordinator();
            throw e.ToAttempt();
        }
    }

    internal async Task<(ulong ProducerId, ushort Epoch)> InitProducerOnceAsync(string? txn, CancellationToken ct)
    {
        var init = new Request.InitProducer(txn, (uint)Math.Min(_cfg.TxnTimeout.TotalMilliseconds, U32Max), _cfg.Tenant);
        Response resp = await CallCoordinatorAsync(txn ?? string.Empty, init, ct).ConfigureAwait(false);
        if (resp is not Response.InitProducer r)
        {
            throw Attempt.Retry("unexpected response " + resp);
        }
        if (r.Error.IsOk)
        {
            return (r.ProducerId, r.ProducerEpoch);
        }
        if (r.Error.Retriable)
        {
            // An id that was handed out but never received is just unused.
            ForgetCoordinator();
            throw Attempt.Retry(r.Error.ToString());
        }
        throw Attempt.Fail(new BrokerErrorException(r.Error, null));
    }

    // ---- list offsets (ADR 0059) ----

    /// <summary>
    /// Where <paramref name="at"/> falls in a partition, as a read_committed reader sees it (ADR 0059):
    /// <see cref="OffsetAt.Earliest"/> is the first offset still stored, <see cref="OffsetAt.Latest"/> the offset after
    /// the last one it may read, and <see cref="OffsetAt.Time"/> the first offset of the first batch with a record at
    /// or after that time, or the latest offset if there is none yet.
    /// </summary>
    public Task<ulong> ListOffsetAsync(string stream, int partition, OffsetAt at, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(stream);
        return RetryingAsync(() => ListOffsetOnceAsync(stream, partition, at, cancellationToken), cancellationToken);
    }

    private async Task<ulong> ListOffsetOnceAsync(string stream, int partition, OffsetAt at, CancellationToken ct)
    {
        Routed routed = await RouteAsync(stream, partition, ct).ConfigureAwait(false);
        var req = new Request.ListOffsets(
            Messages.Isolation.Committed,
            new[]
            {
                new Messages.ListOffsetsResource(
                    routed.Route.ResourceId, new[] { new Messages.ListOffsetsPartition((uint)partition, at.Timestamp) }),
            });
        Response resp;
        try
        {
            resp = await CallAsync(routed.Address, req, ct).ConfigureAwait(false);
        }
        catch (CallFailure e)
        {
            Forget(stream, partition);
            throw e.ToAttempt();
        }
        if (resp is not Response.ListOffsets r)
        {
            throw Attempt.Retry("unexpected response " + resp);
        }
        if (r.Resources.Count == 0 || r.Resources[0].Partitions.Count == 0)
        {
            throw Attempt.Retry("empty list-offsets response");
        }
        Messages.ListOffsetsPartitionResponse p = r.Resources[0].Partitions[0];
        ErrorCode code = p.Error;
        if (code.IsOk)
        {
            return p.Offset;
        }
        if (code.Retriable)
        {
            Redirect(stream, partition, p.LeaderHint);
            throw Attempt.Retry(code.ToString());
        }
        if (code.Equals(ErrorCode.UnknownResource))
        {
            Forget(stream, partition);
        }
        throw Attempt.Fail(new BrokerErrorException(code, null));
    }

    // ---- fetch ----

    /// <summary>
    /// Reads committed batches from <paramref name="offset"/>, waiting up to <paramref name="maxWait"/> at the broker
    /// for some to arrive (a long poll). Keep <paramref name="maxWait"/> well below <see cref="ClientConfig.RequestTimeout"/>.
    /// <para>
    /// This is read_committed: reading stops at the last stable offset. Use <see cref="Fetched.CommittedBatches"/> or
    /// <see cref="Fetched.CommittedMessages"/> to leave out control batches and aborted transactions.
    /// </para>
    /// </summary>
    /// <exception cref="OffsetOutOfRangeException">Retention deleted the offset, or it is past the end; the exception says where the partition starts.</exception>
    public Task<Fetched> FetchAsync(
        string stream, int partition, ulong offset, TimeSpan maxWait, CancellationToken cancellationToken = default) =>
        FetchAsync(stream, partition, offset, maxWait, Messages.Isolation.Committed, cancellationToken);

    /// <summary>
    /// Like <see cref="FetchAsync(string, int, ulong, TimeSpan, CancellationToken)"/>, but also past the last stable
    /// offset: batches of open and aborted transactions and their markers, up to the high watermark. For inspecting a
    /// log, not for processing it.
    /// </summary>
    public Task<Fetched> FetchUncommittedAsync(
        string stream, int partition, ulong offset, TimeSpan maxWait, CancellationToken cancellationToken = default) =>
        FetchAsync(stream, partition, offset, maxWait, Messages.Isolation.Uncommitted, cancellationToken);

    private Task<Fetched> FetchAsync(
        string stream, int partition, ulong offset, TimeSpan maxWait, byte isolation, CancellationToken ct)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(stream);
        if (maxWait < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(maxWait), "the wait is not negative");
        }
        return RetryingAsync(() => FetchOnceAsync(stream, partition, offset, maxWait, isolation, ct), ct);
    }

    private async Task<Fetched> FetchOnceAsync(
        string stream, int partition, ulong offset, TimeSpan maxWait, byte isolation, CancellationToken ct)
    {
        Routed routed = await RouteAsync(stream, partition, ct).ConfigureAwait(false);
        var req = new Request.Fetch(
            (uint)Math.Min(maxWait.TotalMilliseconds, U32Max),
            1,
            4u << 20,
            isolation,
            new[]
            {
                new Messages.FetchResource(
                    routed.Route.ResourceId,
                    new[] { new Messages.FetchPartition((uint)partition, offset, routed.Route.Term, 1u << 20) }),
            });
        Response resp;
        try
        {
            resp = await CallAsync(routed.Address, req, ct).ConfigureAwait(false);
        }
        catch (CallFailure e)
        {
            Forget(stream, partition);
            throw e.ToAttempt();
        }
        if (resp is not Response.Fetch fetched)
        {
            throw Attempt.Retry("unexpected response " + resp);
        }
        if (fetched.Resources.Count == 0 || fetched.Resources[0].Partitions.Count == 0)
        {
            throw Attempt.Retry("empty fetch response");
        }
        Messages.FetchPartitionResponse p = fetched.Resources[0].Partitions[0];
        ErrorCode code = p.Error;
        if (code.IsOk)
        {
            return new Fetched(p.Records, p.HighWatermark, p.LastStableOffset, p.LogStartOffset, p.AbortedTxns);
        }
        if (code.Retriable)
        {
            Redirect(stream, partition, p.LeaderHint);
            throw Attempt.Retry(code.ToString());
        }
        if (code.Equals(ErrorCode.OffsetOutOfRange))
        {
            throw Attempt.Fail(new OffsetOutOfRangeException(stream, partition, offset, p.LogStartOffset));
        }
        if (code.Equals(ErrorCode.UnknownResource))
        {
            Forget(stream, partition);
        }
        throw Attempt.Fail(new BrokerErrorException(code, null));
    }

    // ---- admin (ADR 0034) ----
    //
    // Admin requests go to the metadata group's leader. Any broker answers NOT_LEADER with a hint to it, which the
    // client follows. A create whose outcome is unknown is retried; if the first attempt did create the resource, the
    // retry finds it and the call fails with ResourceExistsException with its id.

    /// <summary>
    /// Creates a stream in the configured tenant and returns its id.
    /// <para>
    /// A create whose outcome is unknown is retried. If the first attempt did create the stream, the retry finds it and
    /// the call fails with <see cref="ResourceExistsException"/>, which carries the id.
    /// </para>
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="partitions">The number of partitions.</param>
    /// <param name="replication">The number of nodes holding each partition.</param>
    /// <param name="configs">Settings such as <c>retention.ms</c> and <c>retention.bytes</c>; may be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task<ulong> CreateStreamAsync(
        string name,
        int partitions,
        int replication,
        IReadOnlyDictionary<string, string>? configs = null,
        CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        if (partitions < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(partitions), "a stream has at least one partition");
        }
        if (replication < 1 || replication > byte.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(replication), "replication is from 1 to 255");
        }
        List<Messages.Config> list = configs is null
            ? new List<Messages.Config>()
            : configs.Select(kv => new Messages.Config(kv.Key, kv.Value)).ToList();
        return CreateAsync(
            name, new Request.CreateStream(name, _cfg.Tenant, (uint)partitions, (byte)replication, list), cancellationToken);
    }

    /// <summary>
    /// Creates a queue in the configured tenant and returns its id. A create whose outcome is unknown is retried and
    /// may then fail with <see cref="ResourceExistsException"/>, as for <see cref="CreateStreamAsync"/>.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="options">Replication, limits and the dead-letter queue; <c>null</c> for <see cref="QueueOptions.Defaults"/>.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task<ulong> CreateQueueAsync(string name, QueueOptions? options = null, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        QueueOptions o = options ?? QueueOptions.Defaults;
        return CreateAsync(
            name,
            new Request.CreateQueue(
                name, _cfg.Tenant, (byte)o.PriorityLevels, o.SingleActiveConsumer, o.DeadLetterQueue, (byte)o.Replication, o.Configs()),
            cancellationToken);
    }

    // ---- queues ----

    /// <summary>
    /// Subscribes to a queue. The consumer is connected to the queue's leader when this returns. Messages are produced to
    /// a queue with <see cref="ProduceAsync(string, int, OutMessage, CancellationToken)"/> to partition 0.
    /// </summary>
    /// <param name="queue">The queue.</param>
    /// <param name="options">The credit; <c>null</c> for <see cref="SubscribeOptions.Default"/>.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    /// <exception cref="UnknownStreamException">The queue does not exist.</exception>
    /// <exception cref="GaveUpException">No leader can be reached within <see cref="ClientConfig.RetryFor"/>.</exception>
    public async Task<QueueConsumer> SubscribeAsync(
        string queue, SubscribeOptions? options = null, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(queue);
        var consumer = new QueueConsumer(this, queue, (options ?? SubscribeOptions.Default).Credit);
        try
        {
            await consumer.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            consumer.Dispose();
            throw;
        }
        return consumer;
    }

    /// <summary>Sends a create request to the metadata leader and returns the id of what was created.</summary>
    internal Task<ulong> CreateAsync(string name, Request req, CancellationToken ct) =>
        RetryingAsync(
            async () =>
            {
                Response r = await CallAdminAsync(req, ct).ConfigureAwait(false);
                if (r is not Response.Create c)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                ErrorCode code = c.Error;
                if (code.IsOk)
                {
                    return c.ResourceId;
                }
                if (code.Equals(ErrorCode.ResourceExists))
                {
                    throw Attempt.Fail(new ResourceExistsException(name, c.ResourceId));
                }
                if (code.Retriable)
                {
                    Interlocked.Exchange(ref _metadataLeader, c.LeaderHint is null ? -1 : c.LeaderHint.Leader);
                    throw Attempt.RetryMaybeDone(code.ToString());
                }
                throw Attempt.Fail(new BrokerErrorException(code, c.ErrorMsg));
            },
            ct);

    /// <summary>Sends an admin request to the metadata leader, or any broker if it is not known.</summary>
    private async Task<Response> CallAdminAsync(Request req, CancellationToken ct)
    {
        long leader = Interlocked.Read(ref _metadataLeader);
        // A leader whose address this client has not seen (a node added since it last asked): learn the cluster's
        // nodes first, or it keeps asking the others.
        if (leader >= 0 && AddressOf((uint)leader) is null)
        {
            try
            {
                await MetadataAsync(null, ct).ConfigureAwait(false);
            }
            catch (ClientException)
            {
                // the retry loop reports what is still wrong
            }
        }
        Address? addr = leader >= 0 ? AddressOf((uint)leader) : null;
        if (addr is null)
        {
            List<Address> targets = KnownBrokers();
            if (targets.Count == 0)
            {
                targets = _cfg.Bootstrap.ToList();
            }
            int turn = Interlocked.Increment(ref _adminTurn) - 1;
            addr = targets[((turn % targets.Count) + targets.Count) % targets.Count];
        }
        try
        {
            return await CallAsync(addr, req, ct).ConfigureAwait(false);
        }
        catch (CallFailure e)
        {
            if (e.Outcome is CallFailure.Kind.Denied or CallFailure.Kind.Refused)
            {
                throw e.ToAttempt();
            }
            Interlocked.Exchange(ref _metadataLeader, -1);
            // The request may have arrived: a retry finds what it did.
            throw Attempt.RetryMaybeDone(e.Describe());
        }
    }

    private Address? AddressOf(uint node)
    {
        lock (_state)
        {
            return _brokers.GetValueOrDefault(node);
        }
    }

    private List<Address> KnownBrokers()
    {
        lock (_state)
        {
            return _brokers.Values.ToList();
        }
    }

    // ---- one request to one broker ----

    /// <summary>
    /// Sends one request to <paramref name="addr"/> and waits for its response. A connection that failed once is not
    /// trusted again: it is closed and the next call opens a new one.
    /// </summary>
    /// <exception cref="CallFailure">There is no response.</exception>
    internal async Task<Response> CallAsync(Address addr, Request req, CancellationToken ct)
    {
        Connection c = await ConnectAsync(addr, ct).ConfigureAwait(false);
        return await CallOnAsync(c, req, ct).ConfigureAwait(false);
    }

    /// <summary>The pooled connection to <paramref name="addr"/>, logged in.</summary>
    /// <exception cref="CallFailure">There is none.</exception>
    internal async Task<Connection> ConnectAsync(Address addr, CancellationToken ct)
    {
        CheckOpen();
        Connection c;
        try
        {
            c = await _pool.GetAsync(addr, ct).ConfigureAwait(false);
        }
        catch (InsecureTransportException)
        {
            throw;
        }
        catch (AuthenticationException e)
        {
            CallFailure f = CallFailure.Denied(e.Message);
            NoteFatal(f);
            throw f;
        }
        catch (ConnectionException e)
        {
            throw CallFailure.NotSent(e.Message);
        }
        if (_seen.TryAdd(c, c))
        {
            // A new connection logged in (if it has credentials): an earlier failed login no longer holds.
            Interlocked.Exchange(ref _fatal, null);
        }
        return c;
    }

    /// <summary>Sends one request on a given connection and waits for its response; failures are handled as in <see cref="CallAsync"/>.</summary>
    internal async Task<Response> CallOnAsync(Connection c, Request req, CancellationToken ct)
    {
        try
        {
            return await c.CallAsync(req, ct).ConfigureAwait(false);
        }
        catch (BrokerException e)
        {
            _pool.Discard(c);
            // Not authenticated, or the session ended: sending again with the same credentials gets the same answer.
            // Refused as a whole with a reason: not carried out, and the same request would be refused again.
            CallFailure f;
            if (e.Code.Equals(ErrorCode.Unauthorized) && e.Reason is null)
            {
                f = CallFailure.Denied("the node requires authentication");
            }
            else if (e.Reason is null)
            {
                f = CallFailure.NotSent("refused: " + e.Code);
            }
            else
            {
                f = CallFailure.Refused(e.Code, e.Reason);
            }
            NoteFatal(f);
            throw f;
        }
        catch (ConnectionException e)
        {
            _pool.Discard(c);
            throw e.RequestSent ? CallFailure.Lost(e.Message) : CallFailure.NotSent(e.Message);
        }
    }

    /// <summary>
    /// Keeps a failed login for the retry loop to report: it is the client's credentials that fail, so it holds for
    /// every request, also one whose attempt only saw a metadata lookup fail. A refused request is not kept: its own
    /// caller gets the refusal, and keeping it would report it for another request.
    /// </summary>
    private void NoteFatal(CallFailure f)
    {
        if (f.Outcome == CallFailure.Kind.Denied)
        {
            Interlocked.Exchange(ref _fatal, new ClientAuthenticationException(f.Message));
        }
    }

    /// <summary>Closes a connection that must not be trusted any more, and forgets it.</summary>
    internal void DiscardConnection(Connection c) => _pool.Discard(c);

    /// <summary>How long a request waits for its response; the consumer bounds its fire-and-forget writes with it.</summary>
    internal TimeSpan RequestTimeout => _cfg.RequestTimeout;

    private void CheckOpen()
    {
        if (_closed)
        {
            throw new ObjectDisposedException(nameof(LotseClient), "the client is closed");
        }
    }

    // ---- for tests ----

    /// <summary>The route this client holds for a partition, or <c>null</c>.</summary>
    internal Route? RouteOf(string stream, int partition)
    {
        lock (_state)
        {
            return _routes.TryGetValue(new PartitionKey(stream, partition), out Route r) ? r : null;
        }
    }

    /// <summary>The producer id of the current session, or 0 if there is none.</summary>
    internal ulong ProducerId
    {
        get
        {
            lock (_sessionLock)
            {
                return _session?.ProducerId ?? 0;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client;

/// <summary>
/// A subscription to a queue, from <see cref="LotseClient.SubscribeAsync"/>. Read messages with
/// <see cref="NextAsync"/>, <see cref="PollAsync"/> or <see cref="DeliveriesAsync"/> and settle each with
/// <see cref="AckAsync(Delivery, CancellationToken)"/> or <see cref="NackAsync(Delivery, bool, CancellationToken)"/>.
/// <para>
/// A subscription lives on one connection to the queue's leader. The leader pushes batches within the consumer's
/// credit; the consumer hands the messages out one at a time and adds credit once half of it is used up, so at most
/// <see cref="SubscribeOptions.Credit"/> messages wait unprocessed on this side.
/// </para>
/// <para>
/// When the subscription ends (the leader changed, the connection broke, or the consumer held a message longer than
/// the queue's ack timeout) the leader gives its unacknowledged messages to other consumers, and the consumer
/// subscribes again on its next read once it has handed out the messages it already holds. Those may meanwhile have
/// gone to someone else, so a message can arrive more than once (<see cref="Delivery.DeliveryCount"/> then exceeds
/// 1); it is not lost. Acknowledging a message that went to someone else still settles it: an ack names an offset,
/// not a delivery.
/// </para>
/// <para>
/// The reading methods are for one reader at a time. Acking, nacking and closing may be done from any task, also while
/// another task waits for a message.
/// </para>
/// </summary>
public sealed class QueueConsumer : IAsyncDisposable, IDisposable
{
    private const uint U32Max = uint.MaxValue;

    /// <summary>Marks a closed consumer in a <see cref="Sub"/>'s events, so that a task waiting for a message wakes.</summary>
    private static readonly object ClosedEvent = new();

    /// <summary>One subscription on one connection; its events are pushes and the end of the connection.</summary>
    private sealed class Sub : IPushListener
    {
        private readonly Channel<object> _events = Channel.CreateUnbounded<object>(
            new UnboundedChannelOptions { SingleReader = false, SingleWriter = false, AllowSynchronousContinuations = false });

        internal Sub(Connection conn, uint id, bool standby)
        {
            Conn = conn;
            Id = id;
            Standby = standby;
        }

        internal Connection Conn { get; }

        internal uint Id { get; }

        /// <summary>Waits as a standby of the queue's single active consumer (ADR 0076).</summary>
        internal volatile bool Standby;

        internal ChannelReader<object> Events => _events.Reader;

        private long _pushed;

        /// <summary>Messages the node pushed to this subscription so far, whether read from the events or not; for tests.</summary>
        internal long Pushed => Interlocked.Read(ref _pushed);

        internal void Add(object ev)
        {
            if (ev is Frames.DeliverPush { Error.IsOk: true } p)
            {
                Interlocked.Add(ref _pushed, p.Deliveries.Count);
            }
            _events.Writer.TryWrite(ev);
        }

        public void OnPush(Frames.DeliverPush push) => Add(push);

        public void OnClose(ConnectionException reason) => Add(reason);
    }

    private readonly LotseClient _client;
    private readonly string _queue;
    private readonly int _credit;

    private readonly object _mu = new();
    private Sub? _sub; // guarded by _mu
    private ulong _resourceId; // guarded by _mu
    private readonly Queue<Delivery> _buffered = new(); // guarded by _mu
    private ulong _usedMsgs; // guarded by _mu
    private ulong _usedBytes; // guarded by _mu
    private bool _closed; // guarded by _mu

    internal QueueConsumer(LotseClient client, string queue, int credit)
    {
        _client = client;
        _queue = queue;
        _credit = credit;
    }

    /// <summary>Subscribes for the first time, so that a queue that cannot be reached fails <see cref="LotseClient.SubscribeAsync"/>.</summary>
    internal Task StartAsync(CancellationToken ct) => EnsureSubscribedAsync(ct);

    // ---- subscribing ----

    private async Task EnsureSubscribedAsync(CancellationToken ct)
    {
        lock (_mu)
        {
            CheckOpen();
            if (_sub is not null)
            {
                return;
            }
        }
        (Sub made, ulong resourceId) = await _client.RetryingAsync(() => SubscribeOnceAsync(ct), ct).ConfigureAwait(false);
        bool abandon;
        lock (_mu)
        {
            abandon = _closed;
            if (!abandon)
            {
                _sub = made;
                _resourceId = resourceId;
                _usedMsgs = 0;
                _usedBytes = 0;
            }
        }
        if (abandon)
        {
            // Closed while subscribing: the broker has the subscription, so end it.
            made.Conn.Unregister(made.Id);
            await SendQuietlyAsync(made.Conn, new Request.Unsubscribe(made.Id)).ConfigureAwait(false);
            throw new ObjectDisposedException(nameof(QueueConsumer), "the consumer is closed");
        }
    }

    private async Task<(Sub, ulong)> SubscribeOnceAsync(CancellationToken ct)
    {
        LotseClient.Routed routed = await _client.RouteAsync(_queue, 0, ct).ConfigureAwait(false);
        Connection conn;
        try
        {
            conn = await _client.ConnectAsync(routed.Address, ct).ConfigureAwait(false);
        }
        catch (CallFailure e)
        {
            throw e.ToAttempt();
        }
        // Asks to be told when it becomes the single active consumer, should the queue have one (ADR 0076).
        var request = new Request.Subscribe(routed.Route.ResourceId, string.Empty, (uint)_credit, U32Max, false, true);
        Response response;
        try
        {
            response = await _client.CallOnAsync(conn, request, ct).ConfigureAwait(false);
        }
        catch (CallFailure e)
        {
            // A subscription the broker may have made ends with the connection, which CallOnAsync has closed.
            _client.Forget(_queue, 0);
            throw e.ToAttempt();
        }
        catch (OperationCanceledException)
        {
            // Cancelled with the answer outstanding: the broker may have subscribed, and nobody would read it. Closing
            // the connection ends the subscription.
            _client.DiscardConnection(conn);
            throw;
        }
        if (response is not Response.Subscribe s)
        {
            throw Attempt.Retry("unexpected response " + response);
        }
        ErrorCode code = s.Error;
        if (code.IsOk)
        {
            var made = new Sub(conn, s.SubscriptionId, s.Standby);
            try
            {
                conn.Register(made.Id, made);
            }
            catch (ConnectionException)
            {
                throw Attempt.Retry("the connection broke");
            }
            return (made, routed.Route.ResourceId);
        }
        if (code.Retriable)
        {
            _client.Redirect(_queue, 0, s.LeaderHint);
            throw Attempt.Retry(code.ToString());
        }
        if (code.Equals(ErrorCode.UnknownResource))
        {
            // Deleted: the name may come back as a new queue.
            _client.Forget(_queue, 0);
        }
        throw Attempt.Fail(new BrokerErrorException(code, null));
    }

    // ---- receiving ----

    /// <summary>
    /// The next message, waiting for one as long as it takes. Subscribes again first if the subscription has ended.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed, also while this call waits.</exception>
    /// <exception cref="GaveUpException">The queue's leader cannot be reached within <see cref="ClientConfig.RetryFor"/>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public async Task<Delivery> NextAsync(CancellationToken cancellationToken = default)
    {
        Delivery? d = await PollAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        return d!; // an infinite wait returns a message or throws
    }

    /// <summary>
    /// The next message, waiting up to <paramref name="wait"/> for one; <c>null</c> if none came. Subscribes again first
    /// if the subscription has ended. <see cref="Timeout.InfiniteTimeSpan"/> waits as long as it takes.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    public async Task<Delivery?> PollAsync(TimeSpan wait, CancellationToken cancellationToken = default)
    {
        if (wait < TimeSpan.Zero && wait != Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(nameof(wait), "the wait is not negative");
        }
        bool forever = wait == Timeout.InfiniteTimeSpan;
        var clock = Stopwatch.StartNew();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Delivery? output = null;
            Request? creditRequest = null;
            Sub? on = null;
            lock (_mu)
            {
                CheckOpen();
                if (_buffered.Count > 0)
                {
                    output = _buffered.Dequeue();
                    _usedMsgs++;
                    creditRequest = Refill();
                    on = _sub;
                }
            }
            if (output is not null)
            {
                if (creditRequest is not null)
                {
                    // A lost credit is noticed as a broken connection by the next wait.
                    _ = SendQuietlyAsync(on!.Conn, creditRequest);
                }
                return output;
            }
            await EnsureSubscribedAsync(cancellationToken).ConfigureAwait(false);
            TimeSpan left = forever ? Timeout.InfiniteTimeSpan : wait - clock.Elapsed;
            if (!forever && left <= TimeSpan.Zero)
            {
                return null;
            }
            Sub? waitingOn;
            lock (_mu)
            {
                CheckOpen();
                waitingOn = _sub;
            }
            if (waitingOn is null)
            {
                continue;
            }
            object ev;
            using (var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                timeout.CancelAfter(left);
                try
                {
                    ev = await waitingOn.Events.ReadAsync(timeout.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    return null;
                }
            }
            Handle(waitingOn, ev);
        }
    }

    /// <summary>
    /// The messages as they come, for <c>await foreach</c>. The sequence has no end of its own: it goes on until the
    /// token is cancelled (an <see cref="OperationCanceledException"/>) or the consumer is closed, which ends it. A
    /// message is yielded before it is settled; settle it with <see cref="AckAsync(Delivery, CancellationToken)"/> or
    /// <see cref="NackAsync(Delivery, bool, CancellationToken)"/>.
    /// </summary>
    public async IAsyncEnumerable<Delivery> DeliveriesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (true)
        {
            Delivery d;
            try
            {
                d = await NextAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                yield break;
            }
            yield return d;
        }
    }

    private void Handle(Sub from, object ev)
    {
        if (ReferenceEquals(ev, ClosedEvent))
        {
            lock (_mu)
            {
                CheckOpen();
            }
            return;
        }
        if (ev is ConnectionException)
        {
            Ended(from);
            return;
        }
        var push = (Frames.DeliverPush)ev;
        if (!push.Error.IsOk)
        {
            if (push.Error.Equals(ErrorCode.NotLeader))
            {
                _client.Redirect(_queue, 0, push.LeaderHint);
            }
            else if (push.Error.Equals(ErrorCode.UnknownResource))
            {
                _client.Forget(_queue, 0);
            }
            Ended(from);
            return;
        }
        // Its turn as the single active consumer, or messages, which come to the active consumer only.
        from.Standby = false;
        if (push.IsActivation)
        {
            return;
        }
        List<BatchRecord> records;
        try
        {
            records = Batch.Parse(push.Batch).Batch.Records();
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
        // The batch is as stored: it can hold messages that are not for this subscription.
        var mine = new Dictionary<ulong, int>();
        foreach (Messages.DeliveredOffset d in push.Deliveries)
        {
            mine[d.Offset] = d.DeliveryCount;
        }
        Request? giveBack = null;
        lock (_mu)
        {
            if (!ReferenceEquals(_sub, from))
            {
                // Closed while this push was being read: close could not know about it, so give its messages back here.
                if (_closed && push.Deliveries.Count > 0)
                {
                    giveBack = new Request.Nack(
                        _resourceId, OffsetRanges.Of(push.Deliveries.Select(d => d.Offset).ToList()), true, 0);
                }
            }
            else
            {
                foreach (BatchRecord r in records)
                {
                    if (mine.TryGetValue(r.Offset, out int count))
                    {
                        _buffered.Enqueue(new Delivery(r, count, from));
                    }
                }
                _usedBytes = Math.Min(U32Max, _usedBytes + (ulong)push.Batch.Size);
            }
        }
        if (giveBack is not null)
        {
            _ = SendQuietlyAsync(from.Conn, giveBack);
        }
    }

    /// <summary>
    /// The subscription is over. Called only when nothing is buffered (the buffer is read before the events), so the
    /// clearing is for a consumer closed meanwhile.
    /// </summary>
    private void Ended(Sub which)
    {
        lock (_mu)
        {
            if (!ReferenceEquals(_sub, which))
            {
                return;
            }
            _sub = null;
            _buffered.Clear();
        }
        which.Conn.Unregister(which.Id);
    }

    /// <summary>The request that adds credit once half of it is used up, or <c>null</c>; called with the lock held.</summary>
    private Request? Refill()
    {
        if (_usedMsgs < (ulong)((_credit + 1) / 2) || _sub is null)
        {
            return null;
        }
        var request = new Request.Credit(_sub.Id, (uint)Math.Min(_usedMsgs, U32Max), (uint)Math.Min(_usedBytes, U32Max));
        _usedMsgs = 0;
        _usedBytes = 0;
        return request;
    }

    /// <summary>
    /// Whether the subscription waits as a standby of its queue's single active consumer (ADR 0076): subscribed, and
    /// not its turn yet. The broker tells a standby when it becomes active; this is false from the next read on that
    /// sees it.
    /// </summary>
    public bool IsStandby
    {
        get
        {
            lock (_mu)
            {
                return _sub is not null && _sub.Standby;
            }
        }
    }

    // ---- settling ----

    /// <summary>Acknowledges a message: once this returns, it is never delivered again.</summary>
    public Task AckAsync(Delivery delivery, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        return SettleAsync(OffsetRanges.Of(new[] { delivery.Offset }), nack: false, cancellationToken);
    }

    /// <summary>
    /// Acknowledges a message within a transaction (ADR 0075): the queue joins the transaction, the leader takes the
    /// message out of flight, and the transaction holds it. The commit settles it; an abort gives it back. This goes over
    /// the subscription's own connection, since only the connection that holds a message can ack it so, so only a
    /// message of the current subscription can be acknowledged this way. Any error leaves the transaction to be aborted.
    /// <c>MESSAGE_NOT_HELD</c> means the message has gone back, or another transaction holds it.
    /// </summary>
    /// <exception cref="BrokerErrorException">With <c>MESSAGE_NOT_HELD</c> or another refusal.</exception>
    public async Task AckInAsync(Transaction txn, Delivery delivery, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(txn);
        ArgumentNullException.ThrowIfNull(delivery);
        await txn.AddAsync(_queue, 0, cancellationToken).ConfigureAwait(false);
        try
        {
            await AckInOnceAsync(txn, delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is ClientException or OperationCanceledException or ObjectDisposedException)
        {
            txn.MustAbort(e.Message);
            throw;
        }
    }

    private async Task AckInOnceAsync(Transaction txn, Delivery delivery, CancellationToken ct)
    {
        Sub? on;
        ulong queueId;
        lock (_mu)
        {
            on = _sub;
            queueId = _resourceId;
        }
        // The messages are the subscription's connection's: on another one they are not held.
        if (on is null)
        {
            throw new BrokerErrorException(ErrorCode.MessageNotHeld, "the subscription ended; the message went back");
        }
        var req = new Request.Ack(
            queueId, txn.TxnId, OffsetRanges.Of(new[] { delivery.Offset }), _client.Config.Tenant, txn.ProducerId, txn.Epoch);
        await _client.RetryingAsync(
            async () =>
            {
                Response resp;
                try
                {
                    resp = await _client.CallOnAsync(on.Conn, req, ct).ConfigureAwait(false);
                }
                catch (CallFailure e)
                {
                    switch (e.Outcome)
                    {
                        // Sent again on the same connection: the leader takes a retry for what the transaction holds.
                        case CallFailure.Kind.Lost:
                            throw Attempt.RetryMaybeDone(e.Message);
                        case CallFailure.Kind.NotSent:
                            throw Attempt.Fail(new BrokerErrorException(ErrorCode.MessageNotHeld, "the connection broke; the message went back"));
                        default:
                            throw e.ToAttempt();
                    }
                }
                if (resp is Response.Outcome o)
                {
                    if (o.Error.IsOk)
                    {
                        return 0;
                    }
                    throw Attempt.Fail(new BrokerErrorException(o.Error, null));
                }
                throw Attempt.RetryMaybeDone("unexpected response " + resp);
            },
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Acknowledges several messages in as few ranges as their offsets allow. The outcome of an ack that was lost is
    /// retried: settling twice is harmless.
    /// </summary>
    public Task AckAsync(IReadOnlyCollection<Delivery> deliveries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(deliveries);
        return deliveries.Count == 0
            ? Task.CompletedTask
            : SettleAsync(OffsetRanges.Of(Offsets(deliveries)), nack: false, cancellationToken);
    }

    /// <summary>
    /// Rejects a message. With <paramref name="requeue"/> it is delivered again, to any consumer; without it, it is
    /// discarded like an acknowledged one (or dead-lettered, if the queue has a dead-letter queue and the limits say so).
    /// </summary>
    public Task NackAsync(Delivery delivery, bool requeue, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        return NackAsync(new[] { delivery }, requeue, cancellationToken);
    }

    /// <summary><see cref="NackAsync(Delivery, bool, CancellationToken)"/> for several messages.</summary>
    public Task NackAsync(IReadOnlyCollection<Delivery> deliveries, bool requeue, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(deliveries);
        if (deliveries.Count == 0)
        {
            return Task.CompletedTask;
        }
        return requeue
            ? RequeueAsync(deliveries, 0, cancellationToken)
            : SettleAsync(OffsetRanges.Of(Offsets(deliveries)), nack: true, cancellationToken);
    }

    /// <summary>
    /// Gives a message back to be delivered again, to any consumer, once <paramref name="delay"/> has passed (ADR 0077):
    /// a retry with backoff. Rounded down to whole milliseconds, at most about 49 days; a delay of 0 is
    /// <see cref="NackAsync(Delivery, bool, CancellationToken)"/> with requeue. If the subscription has ended, the
    /// message went back with it, without a delay.
    /// </summary>
    public Task NackAfterAsync(Delivery delivery, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        double ms = Math.Min(Math.Max(delay.TotalMilliseconds, 0), U32Max);
        return RequeueAsync(new[] { delivery }, (uint)ms, cancellationToken);
    }

    private async Task RequeueAsync(IReadOnlyCollection<Delivery> deliveries, uint delayMs, CancellationToken ct)
    {
        // Only the subscription's connection can give a message back; if it is gone, so is the message (it went back
        // with the subscription). A message of an earlier subscription is gone with it.
        Sub? on;
        ulong queueId;
        lock (_mu)
        {
            on = _sub;
            queueId = _resourceId;
        }
        if (on is null)
        {
            return;
        }
        var held = new List<Delivery>();
        foreach (Delivery d in deliveries)
        {
            if (ReferenceEquals(d.Subscription, on))
            {
                held.Add(d);
            }
        }
        if (held.Count == 0)
        {
            return;
        }
        var request = new Request.Nack(queueId, OffsetRanges.Of(Offsets(held)), true, delayMs);
        Response response;
        try
        {
            response = await _client.CallOnAsync(on.Conn, request, ct).ConfigureAwait(false);
        }
        catch (CallFailure)
        {
            // Not done (no leader, broken connection): the subscription ends with it and the message goes back anyway.
            return;
        }
        if (response is Response.Outcome o && !o.Error.IsOk && !o.Error.Retriable)
        {
            throw new BrokerErrorException(o.Error, null);
        }
    }

    private static List<ulong> Offsets(IEnumerable<Delivery> deliveries)
    {
        var output = new List<ulong>();
        foreach (Delivery d in deliveries)
        {
            output.Add(d.Offset);
        }
        return output;
    }

    /// <summary>An <c>Ack</c> or a discarding <c>Nack</c> to the queue's leader, retried.</summary>
    private Task SettleAsync(List<Messages.OffsetRange> ranges, bool nack, CancellationToken ct) =>
        _client.RetryingAsync(
            async () =>
            {
                LotseClient.Routed routed = await _client.RouteAsync(_queue, 0, ct).ConfigureAwait(false);
                ulong queueId = routed.Route.ResourceId;
                Request request = nack
                    ? new Request.Nack(queueId, ranges, false, 0)
                    : new Request.Ack(queueId, null, ranges);
                Response response;
                try
                {
                    response = await _client.CallAsync(routed.Address, request, ct).ConfigureAwait(false);
                }
                catch (CallFailure e)
                {
                    _client.Forget(_queue, 0);
                    if (e.Outcome is CallFailure.Kind.Denied or CallFailure.Kind.Refused)
                    {
                        throw e.ToAttempt();
                    }
                    throw Attempt.RetryMaybeDone(e.Describe());
                }
                if (response is not Response.Outcome o)
                {
                    throw Attempt.Retry("unexpected response " + response);
                }
                if (o.Error.IsOk)
                {
                    return 0;
                }
                if (o.Error.Retriable)
                {
                    _client.Redirect(_queue, 0, o.LeaderHint);
                    throw Attempt.RetryMaybeDone(o.Error.ToString());
                }
                throw Attempt.Fail(new BrokerErrorException(o.Error, null));
            },
            ct);

    // ---- closing ----

    /// <summary>
    /// Ends the subscription. Messages handed out stay with this consumer until acknowledged (or until this client's
    /// connection closes); the ones not handed out yet go back to the queue. A task waiting for a message gets an
    /// <see cref="ObjectDisposedException"/>. This does not wait for the broker; <see cref="DisposeAsync"/> waits until
    /// the requests are written.
    /// </summary>
    public void Dispose()
    {
        (Sub? ending, List<Request> requests) = BeginClose();
        if (ending is not null)
        {
            _ = SendAllQuietlyAsync(ending.Conn, requests);
        }
    }

    /// <summary>Like <see cref="Dispose"/>, and completes once the requests that end the subscription are written.</summary>
    public async ValueTask DisposeAsync()
    {
        (Sub? ending, List<Request> requests) = BeginClose();
        if (ending is not null)
        {
            await SendAllQuietlyAsync(ending.Conn, requests).ConfigureAwait(false);
        }
    }

    private (Sub?, List<Request>) BeginClose()
    {
        Sub? ending;
        var requests = new List<Request>();
        lock (_mu)
        {
            if (_closed)
            {
                return (null, requests);
            }
            _closed = true;
            ending = _sub;
            _sub = null;
            if (ending is not null)
            {
                requests.Add(new Request.Unsubscribe(ending.Id));
                // What was handed out stays with the consumer. What it holds, and what was pushed and not read yet (the
                // events are read only once the buffer is empty), goes back; the broker keeps the messages of a
                // subscription that is ending until they are settled, so they would otherwise wait for the ack timeout.
                List<ulong> back = Offsets(_buffered);
                while (ending.Events.TryRead(out object? ev))
                {
                    if (ev is Frames.DeliverPush { Error.IsOk: true } p)
                    {
                        back.AddRange(p.Deliveries.Select(d => d.Offset));
                    }
                }
                if (back.Count > 0)
                {
                    requests.Add(new Request.Nack(_resourceId, OffsetRanges.Of(back), true, 0));
                }
            }
            _buffered.Clear();
        }
        if (ending is not null)
        {
            ending.Conn.Unregister(ending.Id);
            ending.Add(ClosedEvent);
        }
        return (ending, requests);
    }

    private void CheckOpen()
    {
        if (_closed)
        {
            throw new ObjectDisposedException(nameof(QueueConsumer), "the consumer is closed");
        }
    }

    private async Task SendAllQuietlyAsync(Connection conn, List<Request> requests)
    {
        foreach (Request r in requests)
        {
            await SendQuietlyAsync(conn, r).ConfigureAwait(false);
        }
    }

    /// <summary>Sends without waiting for an answer; the connection breaking is noticed elsewhere.</summary>
    private async Task SendQuietlyAsync(Connection conn, Request request)
    {
        using var timeout = new CancellationTokenSource(_client.RequestTimeout);
        try
        {
            await conn.SendOnlyAsync(request, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception e) when (e is ConnectionException or OperationCanceledException)
        {
            // the subscription ends with the connection
        }
    }

    /// <summary>The current subscription, or <c>null</c>; for tests.</summary>
    internal object? SubscriptionForTests
    {
        get
        {
            lock (_mu)
            {
                return _sub;
            }
        }
    }

    /// <summary>Handles a push as if the reading task had taken it from <paramref name="subscription"/>'s events; for tests.</summary>
    internal void HandleForTests(object subscription, Frames.DeliverPush push) => Handle((Sub)subscription, push);

    /// <summary>How many messages the node has pushed to this consumer's current subscription; for tests.</summary>
    internal long PushedForTests
    {
        get
        {
            lock (_mu)
            {
                return _sub?.Pushed ?? 0;
            }
        }
    }

    /// <summary>The address this consumer's subscription is on, or <c>null</c>; for tests.</summary>
    internal Address? SubscribedAddress
    {
        get
        {
            lock (_mu)
            {
                return _sub?.Conn.Address;
            }
        }
    }
}

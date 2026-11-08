using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client;

/// <summary>
/// An open transaction, from <see cref="LotseClient.BeginTransactionAsync"/> (ADR 0029). Everything written in it
/// becomes visible to read_committed readers together when it commits, and never if it aborts. It follows the Rust
/// client's <c>Transaction</c> rule for rule:
/// <list type="bullet">
/// <item>a partition (or queue) is added to the transaction with <c>AddPartitionsToTxn</c> before the first batch is
/// written to it;</item>
/// <item>batches carry the transaction's producer id and epoch, a sequence per partition, and the transactional flag;
/// a batch whose outcome is unknown is retried with the same bytes;</item>
/// <item>after an error that may have left something written, or a write that never got its outcome (also a
/// cancelled token), only <see cref="AbortAsync"/> is left: every other call throws <see cref="MustAbortException"/>.
/// Errors that prove nothing was written (<see cref="GaveUpException"/>, <see cref="UnknownStreamException"/>) leave
/// the transaction open;</item>
/// <item><see cref="CommitAsync"/> returns once the decision is durable. A transaction that wrote nothing ends without
/// asking the coordinator.</item>
/// </list>
/// <para>
/// A transaction that is disposed without <see cref="CommitAsync"/> or <see cref="AbortAsync"/> is aborted, also when
/// the block it was used in ended with an exception or a cancellation. If that abort fails, the client's next
/// transaction starts with a fresh producer id, which makes the coordinator abort this one.
/// </para>
/// <para>Not for use by several tasks at once.</para>
/// </summary>
public sealed class Transaction : IAsyncDisposable
{
    private readonly record struct Part(string Name, int Partition);

    private readonly LotseClient _client;
    private readonly Dictionary<Part, int> _next = new();
    private readonly HashSet<Part> _added = new();
    private string? _poisoned;
    private bool _coordinatorMayHold;
    private bool _ended;

    internal Transaction(LotseClient client, string txnId, ulong producerId, ushort epoch)
    {
        _client = client;
        TxnId = txnId;
        ProducerId = producerId;
        Epoch = epoch;
    }

    internal string TxnId { get; }

    internal ulong ProducerId { get; }

    internal ushort Epoch { get; }

    /// <summary>
    /// Whether the transaction may still be committed or aborted: neither has been called and it is not disposed. A
    /// commit refused with <see cref="MustAbortException"/> leaves it open for an abort.
    /// </summary>
    public bool IsOpen => !_ended;

    private void CheckUsable()
    {
        if (_ended)
        {
            throw new InvalidOperationException("the transaction has ended");
        }
        if (_poisoned is not null)
        {
            throw new MustAbortException(_poisoned);
        }
    }

    /// <summary>Appends messages to a partition as one batch within the transaction; see <see cref="ProduceBatchAsync"/>.</summary>
    public Task<ProduceResult> ProduceAsync(
        string stream, int partition, IReadOnlyList<OutMessage> messages, CancellationToken cancellationToken = default) =>
        ProduceBatchAsync(stream, partition, LotseClient.Build(messages), cancellationToken);

    /// <summary>Appends one message to a partition within the transaction; see <see cref="ProduceBatchAsync"/>.</summary>
    public Task<ProduceResult> ProduceAsync(
        string stream, int partition, OutMessage message, CancellationToken cancellationToken = default) =>
        ProduceAsync(stream, partition, new[] { message }, cancellationToken);

    /// <summary>
    /// Appends a batch to a partition within the transaction and returns the offset of its first record. Read_committed
    /// readers see it only once the transaction commits.
    /// </summary>
    /// <exception cref="MustAbortException">An earlier call left the transaction to be aborted.</exception>
    public async Task<ProduceResult> ProduceBatchAsync(
        string stream, int partition, ByteString batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(batch);
        CheckUsable();
        await AddAsync(stream, partition, cancellationToken).ConfigureAwait(false);
        uint records;
        try
        {
            records = Batch.Parse(batch).Batch.RecordCount;
        }
        catch (RecordFormatException e)
        {
            throw new BrokerErrorException(ErrorCode.CorruptBatch, e.Message);
        }
        var key = new Part(stream, partition);
        int seq = _next.GetValueOrDefault(key, 0);
        byte[] bytes = batch.ToByteArray();
        Batch.SetProducer(bytes, ProducerId, Epoch, seq);
        Batch.SetTransactional(bytes, true);
        ByteString stamped = ByteString.View(bytes, 0, bytes.Length);
        // Until the outcome is known the sequence is in doubt: a call that ends some other way (a cancelled token, the
        // client closed) leaves the transaction to be aborted, rather than let the next batch take the same sequence
        // and be taken for this one.
        _poisoned = "a produce ended before its outcome was known";
        ProduceResult result;
        try
        {
            result = await _client.RetryingAsync(
                () => _client.ProduceOnceAsync(stream, partition, stamped, true, TxnId, cancellationToken), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ClientException e)
        {
            _poisoned = null;
            Poison(e);
            throw;
        }
        _poisoned = null;
        _next[key] = unchecked(seq + (int)records);
        return result;
    }

    /// <summary>
    /// Publishes a message within the transaction (ADR 0032): this client routes it by the exchange's bindings, as
    /// metadata gave them within <see cref="ClientConfig.MetadataMaxAge"/>, and produces it to every queue it goes to,
    /// so it reaches them all if the transaction commits and none if it aborts. A message no queue takes is dropped, or
    /// refused with <see cref="UnroutableException"/> when <paramref name="mandatory"/>. If some queues hold it already
    /// when a write fails, only an abort is left.
    /// </summary>
    public async Task<Published> PublishAsync(
        string exchange, string routingKey, OutMessage message, bool mandatory = false, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exchange);
        ArgumentNullException.ThrowIfNull(routingKey);
        ArgumentNullException.ThrowIfNull(message);
        CheckUsable();
        List<string> queues = await _client.RouteMessageAsync(exchange, routingKey, message, mandatory, cancellationToken).ConfigureAwait(false);
        ByteString batch = LotseClient.Build(new[] { message });
        var routed = new List<RoutedQueue>();
        foreach (string q in queues)
        {
            try
            {
                routed.Add(new RoutedQueue(q, (await ProduceBatchAsync(q, 0, batch, cancellationToken).ConfigureAwait(false)).BaseOffset));
            }
            catch (Exception e) when (e is ClientException or OperationCanceledException or ObjectDisposedException)
            {
                // Some queues may hold it already: committing would deliver it to those only.
                if (routed.Count > 0 && _poisoned is null)
                {
                    _poisoned = $"published to [{string.Join(", ", routed.ConvertAll(r => r.Queue))}] only: {e.Message}";
                }
                throw;
            }
        }
        return new Published(routed);
    }

    /// <summary>Adds a partition (a queue: partition 0) to the transaction once.</summary>
    internal async Task AddAsync(string name, int partition, CancellationToken ct)
    {
        CheckUsable();
        var key = new Part(name, partition);
        if (_added.Contains(key))
        {
            return;
        }
        try
        {
            await _client.RetryingAsync(() => AddPartitionOnceAsync(name, partition, ct), ct).ConfigureAwait(false);
        }
        catch (ClientException e)
        {
            Poison(e);
            throw;
        }
        _added.Add(key);
    }

    /// <summary>After an error only an abort is left, unless nothing was written and nothing else went wrong.</summary>
    internal void Poison(ClientException e)
    {
        if (e is not (GaveUpException or UnknownStreamException))
        {
            _poisoned = e.Message;
        }
    }

    /// <summary>Leaves only an abort, for <paramref name="why"/>.</summary>
    internal void MustAbort(string why) => _poisoned = why;

    private async Task<int> AddPartitionOnceAsync(string name, int partition, CancellationToken ct)
    {
        LotseClient.Routed routed = await _client.RouteAsync(name, partition, ct).ConfigureAwait(false);
        var req = new Request.AddPartitionsToTxn(
            TxnId,
            ProducerId,
            Epoch,
            new[] { new Messages.TxnResource(routed.Route.ResourceId, new[] { (uint)partition }) },
            _client.Config.Tenant);
        // From here on the coordinator may hold the partition, whatever becomes of the call.
        _coordinatorMayHold = true;
        if (await _client.CallCoordinatorAsync(TxnId, req, ct).ConfigureAwait(false) is not Response.AddPartitionsToTxn r)
        {
            throw Attempt.Retry("unexpected response");
        }
        ErrorCode error = ErrorCode.None;
        if (r.Resources.Count > 0 && r.Resources[0].Partitions.Count > 0)
        {
            error = r.Resources[0].Partitions[0].Error;
        }
        if (error.IsOk)
        {
            return 0;
        }
        if (error.Retriable)
        {
            _client.ForgetCoordinator();
            throw Attempt.Retry(error.ToString());
        }
        throw Attempt.Fail(new BrokerErrorException(error, null));
    }

    /// <summary>
    /// Commits the consumer's read positions with this transaction (exactly-once consume-transform-produce): they become
    /// the group's committed offsets if it commits, and nothing changes if it aborts. A consumer that no longer owns a
    /// partition is fenced, and the transaction can only be aborted.
    /// </summary>
    public async Task SendOffsetsAsync(GroupConsumer consumer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        CheckUsable();
        // From here on the coordinator may hold the offsets, so the transaction must be ended there even if no
        // partition was added: otherwise the offsets stay pending until the transaction times out, and every reader of
        // the group's offsets waits for them. (The Rust and Java clients skip the end here.)
        _coordinatorMayHold = true;
        var req = new Request.AddOffsetsToTxn(
            TxnId,
            ProducerId,
            Epoch,
            consumer.Group,
            consumer.MemberId,
            consumer.MemberEpoch,
            GroupConsumer.Resources(_client, consumer.PositionMap()),
            _client.Config.Tenant);
        try
        {
            await _client.RetryingAsync(() => GroupConsumer.CommitOnceAsync(_client, TxnId, req, cancellationToken), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception e) when (e is ClientException or OperationCanceledException or ObjectDisposedException)
        {
            // Unknown or refused: committing could commit offsets nobody knows, or a zombie's. Only an abort is left.
            _poisoned = "offsets not added: " + e.Message;
            throw;
        }
    }

    /// <summary>
    /// Commits: once this returns, the decision is durable and every partition will show the transaction to
    /// read_committed readers. If the call fails, the transaction has ended (the decision may or may not have been
    /// made, and the next transaction starts with a fresh producer id); a call refused with
    /// <see cref="MustAbortException"/> leaves it open for <see cref="AbortAsync"/>.
    /// </summary>
    /// <exception cref="MustAbortException">An earlier call left the transaction to be aborted; it is still open.</exception>
    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        CheckUsable();
        return EndAsync(true, cancellationToken);
    }

    /// <summary>Aborts: none of the transaction's batches will be visible to read_committed readers.</summary>
    public Task AbortAsync(CancellationToken cancellationToken = default)
    {
        if (_ended)
        {
            throw new InvalidOperationException("the transaction has ended");
        }
        return EndAsync(false, cancellationToken);
    }

    private async Task EndAsync(bool commit, CancellationToken ct)
    {
        _ended = true;
        if (_added.Count == 0 && !_coordinatorMayHold)
        {
            _client.TransactionEnded((ProducerId, Epoch));
            return;
        }
        var req = new Request.EndTxn(TxnId, ProducerId, Epoch, commit, _client.Config.Tenant);
        (ulong, ushort)? next = null;
        try
        {
            next = await _client.RetryingAsync(
                async () =>
                {
                    Response resp;
                    try
                    {
                        resp = await _client.CallCoordinatorAsync(TxnId, req, ct).ConfigureAwait(false);
                    }
                    catch (Attempt a) when (a.Outcome == Attempt.Kind.Retry)
                    {
                        // A lost request may have been decided: retry as maybe done.
                        throw Attempt.RetryMaybeDone(a.Message);
                    }
                    if (resp is not Response.EndTxn r)
                    {
                        throw Attempt.Retry("unexpected response " + resp);
                    }
                    if (r.Error.IsOk)
                    {
                        return ((ulong, ushort)?)(r.ProducerId, r.ProducerEpoch);
                    }
                    if (r.Error.Retriable)
                    {
                        _client.ForgetCoordinator();
                        // The coordinator may have decided before the answer was lost.
                        throw Attempt.RetryMaybeDone(r.Error.ToString());
                    }
                    throw Attempt.Fail(new BrokerErrorException(r.Error, null));
                },
                ct).ConfigureAwait(false);
        }
        finally
        {
            // The next transaction uses the raised epoch; after a failure, a fresh producer id (which also aborts
            // whatever is left open).
            _client.TransactionEnded(next);
        }
    }

    /// <summary>
    /// Ends this handle: a transaction that was neither committed nor aborted is aborted. If the abort fails, the
    /// client's next transaction starts with a fresh producer id and the coordinator aborts this one. The failure is not
    /// thrown, since the block that is ending may be unwinding from an exception of its own.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_ended)
        {
            return;
        }
        try
        {
            // Not tied to the caller's token: a cancelled block must still give the transaction up. RetryFor bounds it.
            await EndAsync(false, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // EndAsync released the client already, and the next transaction asks for a fresh producer id.
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client;

// Transactions (ADR 0029), exchanges routed by the client inside a transaction (ADR 0032) and consumer groups
// (ADR 0033): what the client holds for them. The rules are in Transaction and GroupConsumer.
public sealed partial class LotseClient
{
    // One transaction at a time. The producer id and epoch for the next one, or null to ask anew; only the holder of
    // the turn reads or writes it.
    private readonly SemaphoreSlim _txnTurn = new(1, 1);
    private (ulong ProducerId, ushort Epoch)? _txnProducer;

    // ---- transactions (ADR 0029) ----

    /// <summary>
    /// Starts a transaction for the configured transactional id (<see cref="ClientConfig.Builder.WithTransactionalId"/>),
    /// getting its producer id from the coordinator first, which fences any older client with the same id. One
    /// transaction at a time per client: a second call waits until the first ends. End it with
    /// <see cref="Transaction.CommitAsync"/> or <see cref="Transaction.AbortAsync"/>, or dispose it: a transaction that
    /// is disposed without either is aborted.
    /// </summary>
    /// <exception cref="NotTransactionalException">No transactional id is configured.</exception>
    public async Task<Transaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        string txnId = _cfg.TransactionalId ?? throw new NotTransactionalException();
        await _txnTurn.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            (ulong ProducerId, ushort Epoch)? ids = _txnProducer;
            if (ids is null)
            {
                ids = await RetryingAsync(() => InitProducerOnceAsync(txnId, cancellationToken), cancellationToken).ConfigureAwait(false);
                _txnProducer = ids;
            }
            return new Transaction(this, txnId, ids.Value.ProducerId, ids.Value.Epoch);
        }
        catch
        {
            _txnTurn.Release();
            throw;
        }
    }

    /// <summary>A transaction ended: <paramref name="next"/> is the producer id for the next one, <c>null</c> to ask anew.</summary>
    internal void TransactionEnded((ulong ProducerId, ushort Epoch)? next)
    {
        _txnProducer = next;
        _txnTurn.Release();
    }

    internal ClientConfig Config => _cfg;

    // ---- exchanges routed by the client, in a transaction (ADR 0032) ----

    /// <summary>The exchange as metadata last said within <see cref="ClientConfig.MetadataMaxAge"/>, asking for it otherwise.</summary>
    internal async Task<Exchange> ExchangeAsync(string name, CancellationToken ct)
    {
        lock (_state)
        {
            if (_exchanges.TryGetValue(name, out (Exchange Exchange, long At) known)
                && Stopwatch.GetElapsedTime(known.At) < _cfg.MetadataMaxAge)
            {
                return known.Exchange;
            }
        }
        // Learn keeps the exchange and the queues bound to it.
        Response.Metadata m = await MetadataAsync(new[] { name }, ct).ConfigureAwait(false);
        if (Find(m.Resources, name) is null)
        {
            // Declared a moment ago: not every broker knows it yet.
            await MetadataFromAllAsync(new[] { name }, ct).ConfigureAwait(false);
        }
        lock (_state)
        {
            if (!_exchanges.TryGetValue(name, out (Exchange Exchange, long At) learnt))
            {
                throw new UnknownStreamException(name, 0);
            }
            return learnt.Exchange;
        }
    }

    /// <summary>The queues <paramref name="message"/> goes to, by name, sorted. With <paramref name="mandatory"/>, a message no queue takes is refused.</summary>
    internal async Task<List<string>> RouteMessageAsync(
        string exchange, string routingKey, OutMessage message, bool mandatory, CancellationToken ct)
    {
        Exchange x = await ExchangeAsync(exchange, ct).ConfigureAwait(false);
        SortedSet<ulong> ids = x.Route(routingKey, message.Headers);
        if (ids.Count == 0 && mandatory)
        {
            throw new UnroutableException(exchange, routingKey);
        }
        var queues = new List<string>();
        lock (_state)
        {
            foreach (ulong id in ids)
            {
                if (!_names.TryGetValue(id, out string? n))
                {
                    throw new UnknownStreamException($"queue {id} bound to {exchange}", 0);
                }
                queues.Add(n);
            }
        }
        queues.Sort(StringComparer.Ordinal);
        return queues;
    }

    private void ForgetExchange(string name)
    {
        lock (_state)
        {
            _exchanges.Remove(name);
        }
    }

    // ---- consumer groups (ADR 0033) ----

    /// <summary>
    /// Joins <paramref name="group"/> to read <paramref name="streams"/>; partitions arrive with the first calls to
    /// <see cref="GroupConsumer.PollAsync"/>. The group's id lives in the configured tenant (ADR 0062).
    /// </summary>
    public async Task<GroupConsumer> JoinGroupAsync(
        string group, IReadOnlyList<string> streams, GroupConfig? config = null, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(group);
        ArgumentNullException.ThrowIfNull(streams);
        // Learn the streams' ids (and names) first.
        await MetadataAsync(streams, cancellationToken).ConfigureAwait(false);
        return new GroupConsumer(this, group, streams.ToArray(), null, config ?? GroupConfig.Defaults);
    }

    /// <summary>
    /// Joins <paramref name="group"/> to read every stream of the tenant whose whole name matches the regular
    /// expression <paramref name="pattern"/> and that this principal may read (ADR 0073). The coordinator matches it with
    /// every heartbeat, so streams created later join in.
    /// </summary>
    public async Task<GroupConsumer> JoinGroupMatchingAsync(
        string group, string pattern, GroupConfig? config = null, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(group);
        ArgumentNullException.ThrowIfNull(pattern);
        await MetadataAsync(null, cancellationToken).ConfigureAwait(false);
        return new GroupConsumer(this, group, Array.Empty<string>(), pattern, config ?? GroupConfig.Defaults);
    }

    /// <summary>The id of a stream this client has looked up, or <c>null</c>.</summary>
    internal ulong? StreamId(string stream)
    {
        lock (_state)
        {
            foreach (KeyValuePair<ulong, string> e in _names)
            {
                if (e.Value == stream)
                {
                    return e.Key;
                }
            }
            return null;
        }
    }

    /// <summary>The name of a resource this client has looked up, or <c>null</c>.</summary>
    internal string? StreamName(ulong id)
    {
        lock (_state)
        {
            return _names.GetValueOrDefault(id);
        }
    }
}

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client;

// Publishing through exchanges (ADR 0079) and the exchange calls of the admin API (ADR 0034).
public sealed partial class LotseClient
{
    private readonly ConcurrentDictionary<string, ulong> _exchangeIds = new();

    /// <summary>
    /// Publishes a message to an exchange, which routes it to queues by <paramref name="routingKey"/> (and headers). A
    /// node routes it by the bindings it knows and writes it to each queue, if this client's roles allow every one of
    /// them. Returns once every routed queue holds it; a message no queue takes is dropped, or refused with
    /// <see cref="UnroutableException"/> when <paramref name="mandatory"/>. If some queues could not be written,
    /// <see cref="NotAllWrittenException"/> says which hold it. If the node's answer is lost, the call throws
    /// <see cref="OutcomeUnknownException"/>: the publish is not sent again, since the node may have written it. So
    /// does a publish whose token is cancelled after it was sent.
    /// </summary>
    /// <exception cref="UnknownStreamException">There is no such exchange.</exception>
    public async Task<Published> PublishAsync(
        string exchange, string routingKey, OutMessage message, bool mandatory = false, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(exchange);
        ArgumentNullException.ThrowIfNull(routingKey);
        ArgumentNullException.ThrowIfNull(message);
        ByteString batch = Build(new[] { message });
        // A name deleted and made again has a new id: ask once more.
        for (int attempt = 0; attempt < 2; attempt++)
        {
            if (attempt > 0)
            {
                _exchangeIds.TryRemove(exchange, out _);
            }
            ulong id = await ExchangeIdAsync(exchange, cancellationToken).ConfigureAwait(false);
            Messages.PublishResult r = await PublishOnceAsync(new Messages.PublishEntry(id, routingKey, mandatory, batch), cancellationToken)
                .ConfigureAwait(false);
            ErrorCode code = r.Error;
            if (code.IsOk)
            {
                return ToPublished(r);
            }
            if (code.Equals(ErrorCode.UnknownResource) && attempt == 0)
            {
                continue;
            }
            if (code.Equals(ErrorCode.UnknownResource))
            {
                throw new UnknownStreamException(exchange, 0);
            }
            if (code.Equals(ErrorCode.Unroutable))
            {
                throw new UnroutableException(exchange, routingKey);
            }
            throw new BrokerErrorException(code, null);
        }
        throw new InvalidOperationException("the second attempt returns");
    }

    /// <summary>The exchange's id, from metadata the first time.</summary>
    private async Task<ulong> ExchangeIdAsync(string exchange, CancellationToken ct)
    {
        if (_exchangeIds.TryGetValue(exchange, out ulong known))
        {
            return known;
        }
        Response.Metadata m = await MetadataAsync(new[] { exchange }, ct).ConfigureAwait(false);
        Messages.ResourceMetadata? r = Find(m.Resources, exchange, mustBeOk: false);
        if (r is null || !r.Error.IsOk || r.Kind != Messages.ResourceKind.Exchange)
        {
            throw new UnknownStreamException(exchange, 0);
        }
        _exchangeIds[exchange] = r.ResourceId;
        return r.ResourceId;
    }

    /// <summary>One <c>Publish</c> to the first node that takes it.</summary>
    private async Task<Messages.PublishResult> PublishOnceAsync(Messages.PublishEntry entry, CancellationToken ct)
    {
        uint timeoutMs = (uint)Math.Min(_cfg.RequestTimeout.TotalMilliseconds, uint.MaxValue);
        var req = new Request.Publish(null, timeoutMs, new[] { entry });
        var clock = Stopwatch.StartNew();
        string why = "no broker known";
        while (true)
        {
            foreach (Address addr in Candidates())
            {
                try
                {
                    Response r = await CallAsync(addr, req, ct).ConfigureAwait(false);
                    if (r is Response.Publish p && p.Entries.Count == 1)
                    {
                        // Refused before anything was written, by a node that may not know the exchange yet:
                        // another node, or later.
                        if (!p.Entries[0].Error.Retriable)
                        {
                            return p.Entries[0];
                        }
                        why = $"{addr}: {p.Entries[0].Error}";
                        continue;
                    }
                    why = "unexpected response " + r;
                }
                catch (CallFailure e)
                {
                    switch (e.Outcome)
                    {
                        case CallFailure.Kind.Denied:
                            throw new ClientAuthenticationException(e.Message);
                        case CallFailure.Kind.Refused:
                            throw new BrokerErrorException(e.Code!.Value, e.Message);
                        case CallFailure.Kind.Lost:
                            // The node may have written it: not sent again.
                            throw new OutcomeUnknownException($"{addr}: {e.Message}");
                        default:
                            why = $"{addr}: {e.Describe()}";
                            break;
                    }
                }
            }
            if (clock.Elapsed >= _cfg.RetryFor)
            {
                throw new GaveUpException(_cfg.RetryFor, why);
            }
            await Task.Delay(_cfg.Backoff, ct).ConfigureAwait(false);
        }
    }

    /// <summary>A routed message's outcome by queue name.</summary>
    private Published ToPublished(Messages.PublishResult result)
    {
        // The node names each queue: this client may never have seen one bound and deleted between two of its lookups.
        // Only a queue the node no longer knows comes without a name.
        var written = new List<RoutedQueue>();
        var writtenNames = new List<string>();
        var failures = new List<ClientException>();
        foreach (Messages.RoutedMessage q in result.Routed)
        {
            string name = q.Name;
            if (name.Length == 0)
            {
                lock (_state)
                {
                    name = _names.GetValueOrDefault(q.QueueId) ?? $"queue {q.QueueId}";
                }
            }
            if (q.Error.IsOk)
            {
                written.Add(new RoutedQueue(name, q.BaseOffset));
                writtenNames.Add(name);
            }
            else if (q.Error.Equals(ErrorCode.RequestTimedOut))
            {
                failures.Add(new OutcomeUnknownException(name + ": the write's outcome is unknown"));
            }
            else
            {
                failures.Add(new BrokerErrorException(q.Error, name));
            }
        }
        written.Sort((a, b) => string.CompareOrdinal(a.Queue, b.Queue));
        if (failures.Count > 0)
        {
            throw new NotAllWrittenException(writtenNames, failures);
        }
        return new Published(written);
    }

    // ---- exchanges and bindings (ADR 0034) ----

    /// <summary>Declares an exchange in the configured tenant and returns its id.</summary>
    /// <param name="name">The name.</param>
    /// <param name="type">One of <see cref="Messages.ExchangeType"/>: direct, topic, fanout or headers.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task<ulong> DeclareExchangeAsync(string name, byte type, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        if (type > Messages.ExchangeType.Headers)
        {
            throw new ArgumentOutOfRangeException(nameof(type), "unknown exchange type " + type);
        }
        return CreateAsync(name, new Request.DeclareExchange(name, _cfg.Tenant, type, Array.Empty<Messages.Config>()), cancellationToken);
    }

    /// <summary>Binds <paramref name="queue"/> to <paramref name="exchange"/> with a binding key (direct and topic exchanges).</summary>
    public Task BindAsync(string exchange, string queue, string key, CancellationToken cancellationToken = default) =>
        BindingAsync(exchange, queue, key, Array.Empty<Messages.HeaderMatch>(), true, false, cancellationToken);

    /// <summary>
    /// Binds <paramref name="queue"/> to a headers exchange: all of <paramref name="headers"/> must match
    /// (<paramref name="matchAll"/>), or any one. A header with a <c>null</c> value matches only a header whose value is null.
    /// </summary>
    public Task BindHeadersAsync(
        string exchange, string queue, IReadOnlyList<Header> headers, bool matchAll, CancellationToken cancellationToken = default) =>
        BindingAsync(
            exchange, queue, string.Empty, headers.Select(h => new Messages.HeaderMatch(h.Key, h.Value)).ToList(), matchAll, false, cancellationToken);

    /// <summary>Removes a binding made by <see cref="BindAsync"/>.</summary>
    public Task UnbindAsync(string exchange, string queue, string key, CancellationToken cancellationToken = default) =>
        BindingAsync(exchange, queue, key, Array.Empty<Messages.HeaderMatch>(), true, true, cancellationToken);

    private async Task BindingAsync(
        string exchange,
        string queue,
        string key,
        IReadOnlyList<Messages.HeaderMatch> matches,
        bool matchAll,
        bool unbind,
        CancellationToken ct)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(exchange);
        ArgumentNullException.ThrowIfNull(queue);
        List<Messages.ResourceMetadata> m = await MetadataFromAllAsync(new[] { exchange, queue }, ct).ConfigureAwait(false);
        Messages.ResourceMetadata? x = Find(m, exchange);
        Messages.ResourceMetadata? q = Find(m, queue);
        if (x is null || q is null)
        {
            throw new UnknownStreamException(exchange + " or " + queue, 0);
        }
        Request req = unbind
            ? new Request.Unbind(x.ResourceId, q.ResourceId, key, matches, matchAll)
            : new Request.Bind(x.ResourceId, q.ResourceId, key, matches, matchAll);
        await RetryingAsync(
            async () =>
            {
                Response r = await CallAdminAsync(req, ct).ConfigureAwait(false);
                if (r is not Response.Outcome o)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(o.Error, o.LeaderHint, null);
                return 0;
            },
            ct).ConfigureAwait(false);
        // A transaction's publish routes by the bindings this client has seen: see the new ones.
        ForgetExchange(exchange);
    }

    /// <summary>An admin answer: retriable errors follow the leader hint and may have been carried out; others fail.</summary>
    private void AdminAnswer(ErrorCode error, LeaderHint? hint, string? message)
    {
        if (error.IsOk)
        {
            return;
        }
        if (error.Retriable)
        {
            Interlocked.Exchange(ref _metadataLeader, hint is null ? -1 : hint.Leader);
            throw Attempt.RetryMaybeDone(error.ToString());
        }
        throw Attempt.Fail(new BrokerErrorException(error, message));
    }
}

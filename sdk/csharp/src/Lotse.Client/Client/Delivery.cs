using System;
using System.Collections.Generic;
using System.Globalization;
using Lotse.Record;

namespace Lotse.Client;

/// <summary>
/// Where a dead-lettered message came from, read from the <c>x-lotse-dead-letter-*</c> headers the broker adds when it
/// moves a message to a dead-letter queue. They describe the latest move.
/// </summary>
/// <param name="Reason">Why: <c>max-deliveries</c> or <c>expired</c>.</param>
/// <param name="Queue">The name of the queue the message came from.</param>
/// <param name="QueueId">That queue's resource id.</param>
/// <param name="Offset">The message's offset there.</param>
/// <param name="Deliveries">How often it had been delivered there.</param>
/// <param name="Floor">That queue's ack floor when the message was sent: every offset below it is settled.</param>
public sealed record DeadLetter(string Reason, string Queue, ulong QueueId, ulong Offset, int Deliveries, ulong Floor)
{
    internal const string ReasonHeader = "x-lotse-dead-letter-reason";
    internal const string QueueHeader = "x-lotse-dead-letter-queue";
    internal const string QueueIdHeader = "x-lotse-dead-letter-queue-id";
    internal const string OffsetHeader = "x-lotse-dead-letter-offset";
    internal const string DeliveriesHeader = "x-lotse-dead-letter-deliveries";
    internal const string FloorHeader = "x-lotse-dead-letter-floor";
}

/// <summary>
/// One message from a queue, from <see cref="QueueConsumer.NextAsync"/>. Settle it with
/// <see cref="QueueConsumer.AckAsync(Delivery, System.Threading.CancellationToken)"/> or
/// <see cref="QueueConsumer.NackAsync(Delivery, bool, System.Threading.CancellationToken)"/>.
/// </summary>
public sealed class Delivery
{
    private readonly BatchRecord _record;

    internal Delivery(BatchRecord record, int deliveryCount, object subscription)
    {
        _record = record;
        DeliveryCount = deliveryCount;
        Subscription = subscription;
    }

    /// <summary>The message's offset in the queue; it identifies the message for acks.</summary>
    public ulong Offset => _record.Offset;

    /// <summary>How often the message has been delivered, this time included. More than 1 means an earlier delivery was not acknowledged.</summary>
    public int DeliveryCount { get; }

    /// <summary>The producer's timestamp, in milliseconds since the epoch.</summary>
    public long Timestamp => _record.Timestamp;

    /// <summary>The key, or <c>null</c>.</summary>
    public ByteString? Key => _record.Key;

    /// <summary>The value, or <c>null</c>.</summary>
    public ByteString? Value => _record.Value;

    /// <summary>The headers, in the order the producer set them.</summary>
    public IReadOnlyList<Header> Headers => _record.Headers;

    /// <summary>The record as stored.</summary>
    public BatchRecord Record => _record;

    /// <summary>The subscription this message came on; only that subscription's connection can give it back.</summary>
    internal object Subscription { get; }

    /// <summary>
    /// Where this message came from, if it was moved to this queue by a dead-letter move: present when the
    /// <c>x-lotse-dead-letter-reason</c> header is. A missing or unreadable number reads as 0, a missing name as the
    /// empty string.
    /// </summary>
    public DeadLetter? DeadLetter
    {
        get
        {
            string? reason = HeaderText(Lotse.Client.DeadLetter.ReasonHeader);
            if (reason is null)
            {
                return null;
            }
            return new DeadLetter(
                reason,
                HeaderText(Lotse.Client.DeadLetter.QueueHeader) ?? string.Empty,
                Number(Lotse.Client.DeadLetter.QueueIdHeader),
                Number(Lotse.Client.DeadLetter.OffsetHeader),
                (int)Math.Min(Number(Lotse.Client.DeadLetter.DeliveriesHeader), int.MaxValue),
                Number(Lotse.Client.DeadLetter.FloorHeader));
        }
    }

    private string? HeaderText(string name)
    {
        // The broker replaces the headers of an earlier move, so each appears once; the last wins if not.
        string? found = null;
        foreach (Header h in _record.Headers)
        {
            if (h.Value is not null && h.Key.ToUtf8() == name)
            {
                found = h.Value.ToUtf8();
            }
        }
        return found;
    }

    private ulong Number(string name)
    {
        string? v = HeaderText(name);
        return v is not null && ulong.TryParse(v, NumberStyles.None, CultureInfo.InvariantCulture, out ulong n) ? n : 0;
    }

    /// <inheritdoc/>
    public override string ToString() => $"Delivery[offset={Offset}, deliveryCount={DeliveryCount}]";
}

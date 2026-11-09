using System;
using System.Collections.Generic;
using System.Linq;
using Lotse.Proto;

namespace Lotse.Client;

/// <summary>
/// Settings of a queue for <see cref="LotseClient.CreateQueueAsync"/>.
/// <para>
/// Limits (<c>ack.timeout.ms</c>, <c>max.deliveries</c>, <c>message.ttl.ms</c>), the dead-letter queue, a single
/// active consumer (ADR 0076) and priority levels (ADR 0078) have their own members; anything else the broker knows
/// goes through <see cref="Builder.WithSetting"/>.
/// </para>
/// </summary>
public sealed class QueueOptions
{
    /// <summary>The most priority levels a queue has.</summary>
    public const int MaxPriorityLevels = 10;

    private readonly SortedDictionary<string, string> _settings;

    private QueueOptions(Builder b)
    {
        Replication = b.Replication;
        DeadLetterQueue = b.DeadLetterQueue;
        SingleActiveConsumer = b.SingleActive;
        PriorityLevels = b.Levels;
        _settings = new SortedDictionary<string, string>(b.Settings, StringComparer.Ordinal);
    }

    /// <summary>The default settings, on 3 nodes.</summary>
    public static QueueOptions Defaults { get; } = new Builder().Build();

    /// <summary>The number of nodes holding the queue.</summary>
    public int Replication { get; }

    /// <summary>The name of the queue that receives retired messages, or <c>null</c>.</summary>
    public string? DeadLetterQueue { get; }

    /// <summary>Whether the queue delivers to one subscription at a time (<c>single.active.consumer</c>).</summary>
    public bool SingleActiveConsumer { get; }

    /// <summary>The queue's priority levels (<c>priority.levels</c>): 1 is no priorities, 2 to 10 are levels.</summary>
    public int PriorityLevels { get; }

    /// <summary>The settings sent in the request's settings list, by key.</summary>
    public IReadOnlyDictionary<string, string> Settings => _settings;

    /// <summary>A builder; the queue is replicated on 3 nodes unless told otherwise.</summary>
    public static Builder CreateBuilder() => new();

    internal List<Messages.Config> Configs() => _settings.Select(kv => new Messages.Config(kv.Key, kv.Value)).ToList();

    /// <summary>Builds <see cref="QueueOptions"/>.</summary>
    public sealed class Builder
    {
        internal Builder()
        {
        }

        internal int Replication { get; private set; } = 3;

        internal string? DeadLetterQueue { get; private set; }

        internal bool SingleActive { get; private set; }

        internal int Levels { get; private set; } = 1;

        internal SortedDictionary<string, string> Settings { get; } = new(StringComparer.Ordinal);

        /// <summary>The number of nodes holding the queue (1 to 255).</summary>
        public Builder WithReplication(int nodes)
        {
            if (nodes < 1 || nodes > 255)
            {
                throw new ArgumentOutOfRangeException(nameof(nodes), "replication is from 1 to 255");
            }
            Replication = nodes;
            return this;
        }

        /// <summary>How long a consumer may hold a message unacknowledged before it goes to others (<c>ack.timeout.ms</c>).</summary>
        public Builder WithAckTimeout(TimeSpan timeout) => WithSetting("ack.timeout.ms", Millis(timeout, nameof(timeout)));

        /// <summary>How often a message is delivered before the queue retires it (<c>max.deliveries</c>), from 1.</summary>
        public Builder WithMaxDeliveries(uint deliveries)
        {
            if (deliveries < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(deliveries), "a message is delivered at least once");
            }
            return WithSetting("max.deliveries", deliveries.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        /// <summary>How long a message may wait in the queue before it is retired (<c>message.ttl.ms</c>).</summary>
        public Builder WithMessageTtl(TimeSpan ttl) => WithSetting("message.ttl.ms", Millis(ttl, nameof(ttl)));

        /// <summary>A queue of the same tenant that receives the messages the limits retire; without one they are dropped.</summary>
        public Builder WithDeadLetterQueue(string name)
        {
            DeadLetterQueue = name ?? throw new ArgumentNullException(nameof(name));
            return this;
        }

        /// <summary>
        /// Delivers to one subscription at a time; the others wait as standbys in the order they subscribed
        /// (<c>single.active.consumer</c>, ADR 0076).
        /// </summary>
        public Builder WithSingleActiveConsumer(bool single = true)
        {
            SingleActive = single;
            return this;
        }

        /// <summary>The number of priority levels (<c>priority.levels</c>, ADR 0078): 1 is no priorities, 2 to 10 are levels.</summary>
        public Builder WithPriorityLevels(int levels)
        {
            if (levels < 1 || levels > MaxPriorityLevels)
            {
                throw new ArgumentOutOfRangeException(nameof(levels), $"priority levels are from 1 to {MaxPriorityLevels}");
            }
            Levels = levels;
            return this;
        }

        /// <summary>A setting by its key, as the broker names it; the last value given for a key wins.</summary>
        public Builder WithSetting(string key, string value)
        {
            ArgumentNullException.ThrowIfNull(key);
            ArgumentNullException.ThrowIfNull(value);
            Settings[key] = value;
            return this;
        }

        /// <summary>Builds the options.</summary>
        public QueueOptions Build() => new(this);

        private static string Millis(TimeSpan span, string name)
        {
            long ms = (long)span.TotalMilliseconds;
            if (ms < 1)
            {
                throw new ArgumentOutOfRangeException(name, "at least 1 ms");
            }
            return ms.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
    }
}

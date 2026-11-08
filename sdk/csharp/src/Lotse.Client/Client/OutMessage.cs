using System;
using System.Collections.Generic;
using System.Linq;
using Lotse.Record;

namespace Lotse.Client;

/// <summary>
/// A message to produce: an optional key, a value, headers and a timestamp.
/// <para>
/// <b>The timestamp.</b> When none is set, the client uses the time of the produce call, in milliseconds since the
/// epoch. The Rust helper's default is 0, but a stream's retention judges a batch's age by the producer's timestamps,
/// so a message stamped 0 is old at once and may be deleted at the next retention pass. Set a timestamp yourself only
/// when it means something, such as the time of the event.
/// </para>
/// </summary>
/// <param name="Key">The key, or <c>null</c>.</param>
/// <param name="Value">The value, or <c>null</c> for a tombstone.</param>
/// <param name="Headers">Headers in order.</param>
/// <param name="Timestamp">Milliseconds since the epoch, or <c>null</c> for the time of the produce call.</param>
public sealed record OutMessage(ByteString? Key, ByteString? Value, IReadOnlyList<Header> Headers, long? Timestamp)
{
    /// <summary>A message with a value only.</summary>
    public static OutMessage FromValue(byte[] value) =>
        new(null, ByteString.CopyOf(value), Array.Empty<Header>(), null);

    /// <summary>A message whose value is <paramref name="value"/> in UTF-8.</summary>
    public static OutMessage FromValue(string value) =>
        new(null, ByteString.Utf8(value), Array.Empty<Header>(), null);

    /// <summary>This message with a key.</summary>
    public OutMessage WithKey(byte[] key) => this with { Key = ByteString.CopyOf(key) };

    /// <summary>This message with a key, in UTF-8.</summary>
    public OutMessage WithKey(string key) => this with { Key = ByteString.Utf8(key) };

    /// <summary>This message with one more header; the value may be <c>null</c>.</summary>
    public OutMessage WithHeader(string name, byte[]? value) =>
        this with { Headers = Headers.Append(new Header(ByteString.Utf8(name), value is null ? null : ByteString.CopyOf(value))).ToArray() };

    /// <summary>This message with one more header whose value is text, in UTF-8.</summary>
    public OutMessage WithHeader(string name, string? value) =>
        WithHeader(name, value is null ? null : System.Text.Encoding.UTF8.GetBytes(value));

    /// <summary>This message with a timestamp, in milliseconds since the epoch.</summary>
    public OutMessage WithTimestamp(long millis) => this with { Timestamp = millis };
}

/// <summary>Where a produced batch went.</summary>
/// <param name="BaseOffset">The offset of the batch's first record; the batch is committed.</param>
/// <param name="AppendTimestamp">The leader's append time in milliseconds since the epoch, or <c>-1</c>.</param>
public readonly record struct ProduceResult(ulong BaseOffset, long AppendTimestamp);

/// <summary>
/// A place in a partition, as a read_committed reader sees it (ADR 0059): <see cref="Earliest"/> the first offset
/// still stored, <see cref="Latest"/> the offset after the last one it may read, and <see cref="Time"/> the first
/// offset of the first batch with a record at or after a time, or the latest offset if there is none yet.
/// </summary>
public readonly record struct OffsetAt
{
    private OffsetAt(long timestamp)
    {
        Timestamp = timestamp;
    }

    /// <summary>The first offset still stored.</summary>
    public static OffsetAt Earliest => new(-2);

    /// <summary>The offset after the last one a read_committed reader may see.</summary>
    public static OffsetAt Latest => new(-1);

    /// <summary>The first batch with a record at or after <paramref name="epochMillis"/>; a negative time counts as 0.</summary>
    public static OffsetAt Time(long epochMillis) => new(Math.Max(0, epochMillis));

    /// <summary>What <c>ListOffsets</c> asks for: -2, -1 or milliseconds since the epoch.</summary>
    internal long Timestamp { get; }

    /// <inheritdoc/>
    public override string ToString() => Timestamp == -2 ? "Earliest" : Timestamp == -1 ? "Latest" : $"Time({Timestamp})";
}

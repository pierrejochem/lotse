using System.Collections.Generic;

namespace Lotse.Record;

/// <summary>One record of a batch, with its absolute offset and timestamp.</summary>
/// <param name="Offset">The batch's base offset plus the record's delta.</param>
/// <param name="Timestamp">The batch's base timestamp plus the record's delta, in milliseconds since the epoch.</param>
/// <param name="Key">The key, or <c>null</c>.</param>
/// <param name="Value">The value, or <c>null</c>.</param>
/// <param name="Headers">The headers, in order.</param>
public sealed record BatchRecord(
    ulong Offset, long Timestamp, ByteString? Key, ByteString? Value, IReadOnlyList<Header> Headers);

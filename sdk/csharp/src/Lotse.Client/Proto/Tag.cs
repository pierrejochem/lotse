using System;
using Lotse.Record;

namespace Lotse.Proto;

/// <summary>
/// One tagged field: a tag number and an opaque value. Later versions add fields as tags; readers skip the ones
/// they do not know.
/// </summary>
/// <param name="Number">The tag number, never negative.</param>
/// <param name="Value">Its bytes.</param>
public sealed record Tag(long Number, ByteString Value)
{
    /// <summary>The tag number, never negative.</summary>
    public long Number { get; } = Number >= 0 ? Number : throw new ArgumentException("negative tag", nameof(Number));

    /// <summary>Its bytes.</summary>
    public ByteString Value { get; } = Value ?? throw new ArgumentNullException(nameof(Value));
}

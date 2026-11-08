using System;
using Lotse.Record;

namespace Lotse.Client;

/// <summary>One record read by a group member.</summary>
/// <param name="Stream">The stream.</param>
/// <param name="Partition">The partition.</param>
/// <param name="Record">The record, its offset included.</param>
public sealed record GroupRecord(string Stream, int Partition, BatchRecord Record)
{
    /// <summary>The stream.</summary>
    public string Stream { get; } = Stream ?? throw new ArgumentNullException(nameof(Stream));

    /// <summary>The record, its offset included.</summary>
    public BatchRecord Record { get; } = Record ?? throw new ArgumentNullException(nameof(Record));
}

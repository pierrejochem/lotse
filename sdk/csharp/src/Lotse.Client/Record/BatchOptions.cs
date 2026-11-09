namespace Lotse.Record;

/// <summary>What a <see cref="BatchBuilder"/> puts into the header.</summary>
/// <param name="Attributes">Compression, transactional bit, control bit and kind.</param>
/// <param name="ProducerId">The producer's id; 0 for a batch that is not idempotent.</param>
/// <param name="ProducerEpoch">The producer's epoch.</param>
/// <param name="BaseSequence">The sequence of the first record, or -1 for none.</param>
public sealed record BatchOptions(Attributes Attributes, ulong ProducerId, ushort ProducerEpoch, int BaseSequence)
{
    /// <summary>A plain batch: no producer, no sequence.</summary>
    public static readonly BatchOptions Default = new(Attributes.Data, 0, 0, -1);
}

using System;

namespace Lotse.Record;

/// <summary>
/// A batch's attributes: <c>compression(3) | transactional(1) | control(1) | kind(3)</c>, bit 0 first. Bit 8, the
/// mark of a compacted batch, is not part of them; <see cref="Batch.IsCompacted"/> reads it.
/// </summary>
public sealed record Attributes(Compression Compression, bool Transactional, bool Control, BatchKind Kind)
{
    /// <summary>Plain data: not compressed, not transactional, not a control batch.</summary>
    public static readonly Attributes Data = new(Compression.None, false, false, BatchKind.Data);

    /// <summary>The packed 16 bits, with bit 8 clear.</summary>
    public int ToBits() =>
        (int)Compression | (Transactional ? 1 << 3 : 0) | (Control ? 1 << 4 : 0) | ((int)Kind << 5);

    /// <summary>Unpacks bits as they are in a batch header; bit 8 is the caller's to look at.</summary>
    /// <exception cref="RecordFormatException">The compression or kind is unknown, or a reserved bit is set.</exception>
    public static Attributes FromBits(int bits)
    {
        int compression = bits & 0b111;
        if (compression > (int)Compression.Lz4)
        {
            throw RecordFormatException.MalformedBatch("unknown compression");
        }
        int kind = (bits >> 5) & 0b111;
        if (kind > (int)BatchKind.Retire)
        {
            throw RecordFormatException.MalformedBatch("unknown batch kind");
        }
        // Bit 8 marks a compacted batch (ADR 0067); everything above it is reserved.
        if (bits >> 9 != 0)
        {
            throw RecordFormatException.MalformedBatch("reserved attribute bits set");
        }
        return new Attributes((Compression)compression, (bits & (1 << 3)) != 0, (bits & (1 << 4)) != 0, (BatchKind)kind);
    }
}

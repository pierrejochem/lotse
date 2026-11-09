namespace Lotse.Record;

/// <summary>The codec of a batch's record block. The numbers are the wire codes.</summary>
public enum Compression
{
    /// <summary>Not compressed.</summary>
    None = 0,

    /// <summary>One zstd frame.</summary>
    Zstd = 1,

    /// <summary>One LZ4 frame (the frame format, not the bare block).</summary>
    Lz4 = 2,
}

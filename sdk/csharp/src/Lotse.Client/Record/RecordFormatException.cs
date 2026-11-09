using System;

namespace Lotse.Record;

/// <summary>Why a record batch or record could not be read or written.</summary>
public enum RecordFormatErrorKind
{
    /// <summary>The buffer ends inside a batch, record or varint.</summary>
    Truncated,

    /// <summary>A varint does not fit 64 bits (or a value does not fit the 32 bits it is meant for).</summary>
    VarintOverflow,

    /// <summary>The batch format version is not 1.</summary>
    UnsupportedMagic,

    /// <summary>The checksum does not match the bytes.</summary>
    Checksum,

    /// <summary>A batch or buffer is larger than the limits allow.</summary>
    TooLarge,

    /// <summary>A field holds something no valid batch holds.</summary>
    Malformed,

    /// <summary>The batch is compressed with a codec this SDK cannot use; the message names what is missing.</summary>
    UnsupportedCompression,
}

/// <summary>A record batch or record that cannot be read or written.</summary>
public sealed class RecordFormatException : Exception
{
    /// <summary>Creates the exception.</summary>
    public RecordFormatException(RecordFormatErrorKind kind, string message)
        : base(message)
    {
        Kind = kind;
    }

    /// <summary>Why.</summary>
    public RecordFormatErrorKind Kind { get; }

    internal static RecordFormatException TruncatedBuffer() =>
        new(RecordFormatErrorKind.Truncated, "buffer ends inside a batch");

    internal static RecordFormatException MalformedBatch(string what) =>
        new(RecordFormatErrorKind.Malformed, "malformed batch: " + what);
}

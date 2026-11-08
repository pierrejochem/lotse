using System;

namespace Lotse.Proto;

/// <summary>Why a frame was refused.</summary>
public enum DecodeErrorKind
{
    /// <summary>The frame ends inside a field.</summary>
    Truncated,

    /// <summary>A field holds something no version allows.</summary>
    Malformed,

    /// <summary>The request header is fine, but the key or version is not known; see <see cref="DecodeException.Header"/>.</summary>
    Unsupported,
}

/// <summary>A frame that cannot be decoded.</summary>
public sealed class DecodeException : Exception
{
    /// <summary>Creates the exception.</summary>
    public DecodeException(DecodeErrorKind kind, string message)
        : this(kind, message, null)
    {
    }

    /// <summary>Creates the exception for an unsupported request, keeping its header so it can still be answered.</summary>
    public DecodeException(DecodeErrorKind kind, string message, RequestHeader? header)
        : base(message)
    {
        Kind = kind;
        Header = header;
    }

    /// <summary>Why.</summary>
    public DecodeErrorKind Kind { get; }

    /// <summary>The header of an <see cref="DecodeErrorKind.Unsupported"/> request, otherwise <c>null</c>.</summary>
    public RequestHeader? Header { get; }

    internal static DecodeException TruncatedFrame() => new(DecodeErrorKind.Truncated, "frame is truncated");

    internal static DecodeException Malformed(string what) => new(DecodeErrorKind.Malformed, "malformed frame: " + what);
}

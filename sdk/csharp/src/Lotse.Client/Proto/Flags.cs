namespace Lotse.Proto;

/// <summary>Frame flags, a <c>u16</c> bit set in every header after the correlation id.</summary>
public static class Flags
{
    /// <summary>A push frame (delivery to a queue consumer), not a response.</summary>
    public const ushort Push = 1;

    /// <summary>A response whose body is only an error code: the request could not be served at all.</summary>
    public const ushort Error = 2;
}

namespace Lotse.Proto;

/// <summary>The versions a node speaks for one API key.</summary>
/// <param name="ApiKey">The key.</param>
/// <param name="Min">Oldest version.</param>
/// <param name="Max">Newest version.</param>
public sealed record ApiVersionRange(ApiKey ApiKey, ushort Min, ushort Max)
{
    /// <summary>Whether <paramref name="version"/> is inside the range.</summary>
    public bool Contains(int version) => version >= Min && version <= Max;
}

using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Security.Cryptography;

namespace Lotse.Auth;

/// <summary>
/// An access token (ADR 0133): its id and its secret, as <c>lotse_tok_&lt;id&gt;_&lt;secret&gt;</c>. The id is 16
/// lower-case hex digits and names the token in the cluster's state; the secret is 32 random bytes in unpadded
/// base64url (43 characters). The cluster keeps only the SHA-256 of the secret bytes, which is all a client sends when
/// it creates a token. Nothing here draws randomness: the caller passes the secret in.
/// </summary>
public sealed class AccessToken : IEquatable<AccessToken>
{
    private const string Prefix = "lotse_tok_";
    private const int SecretBytes = 32;
    private const int SecretChars = 43;

    // What Rust's trim_ascii strips: space, tab, line feed, form feed and carriage return.
    private static readonly char[] AsciiWhitespace = { ' ', '\t', '\n', '\f', '\r' };

    private readonly byte[] _secret;

    /// <summary>A token from an id and a secret the caller drew.</summary>
    /// <param name="id">The token's id.</param>
    /// <param name="secret">The 32 secret bytes; copied.</param>
    public AccessToken(ulong id, byte[] secret)
    {
        ArgumentNullException.ThrowIfNull(secret);
        if (secret.Length != SecretBytes)
        {
            throw new ArgumentException("a token's secret is 32 bytes, got " + secret.Length, nameof(secret));
        }
        Id = id;
        _secret = (byte[])secret.Clone();
    }

    /// <summary>The token's id.</summary>
    public ulong Id { get; }

    /// <summary>A copy of the 32 secret bytes.</summary>
    public byte[] Secret() => (byte[])_secret.Clone();

    /// <summary>The text the user holds: <c>lotse_tok_&lt;id&gt;_&lt;secret&gt;</c>.</summary>
    public string Text() => Prefix + HexId(Id) + "_" + Base64Url(_secret);

    /// <summary>The SHA-256 of the secret bytes: all the cluster keeps, and all a client sends to create the token.</summary>
    public byte[] Hash() => SHA256.HashData(_secret);

    /// <summary>
    /// Reads a token's text, ignoring ASCII whitespace around it (a pasted token often carries a newline), as the node
    /// does. Anything that is not exactly a token's canonical text is refused: upper-case hex, a sign, another length,
    /// padding, or a last character whose spare bits are not zero, so one secret has one text.
    /// </summary>
    /// <param name="text">The text.</param>
    /// <param name="token">The token, or <c>null</c> if <paramref name="text"/> is not one.</param>
    /// <returns>Whether <paramref name="text"/> is a token.</returns>
    public static bool TryParse(string? text, [NotNullWhen(true)] out AccessToken? token)
    {
        token = null;
        if (text is null)
        {
            return false;
        }
        string t = text.Trim(AsciiWhitespace);
        if (!t.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return false;
        }
        string rest = t.Substring(Prefix.Length);
        int sep = rest.IndexOf('_', StringComparison.Ordinal);
        if (sep < 0)
        {
            return false;
        }
        string id = rest.Substring(0, sep);
        string secret = rest.Substring(sep + 1);
        if (id.Length != 16)
        {
            return false;
        }
        foreach (char c in id)
        {
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            {
                return false;
            }
        }
        if (secret.Length != SecretChars)
        {
            return false;
        }
        foreach (char c in secret)
        {
            bool b64 = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_';
            if (!b64)
            {
                return false;
            }
        }
        byte[] bytes = Convert.FromBase64String(secret.Replace('-', '+').Replace('_', '/') + "=");
        // The base class library's decoder ignores the 2 spare bits of the last character; only the canonical text is
        // a token.
        if (bytes.Length != SecretBytes || Base64Url(bytes) != secret)
        {
            return false;
        }
        token = new AccessToken(ulong.Parse(id, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture), bytes);
        return true;
    }

    /// <summary>Reads a token's text as <see cref="TryParse"/> does.</summary>
    /// <param name="text">The text.</param>
    /// <exception cref="FormatException"><paramref name="text"/> is not a token; the message does not repeat it.</exception>
    public static AccessToken Parse(string text) =>
        TryParse(text, out AccessToken? token) ? token : throw new FormatException("not an access token (lotse_tok_...)");

    private static string HexId(ulong id) => id.ToString("x16", CultureInfo.InvariantCulture);

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    /// <inheritdoc/>
    public bool Equals(AccessToken? other) =>
        other is not null && Id == other.Id && CryptographicOperations.FixedTimeEquals(_secret, other._secret);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is AccessToken other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => Id.GetHashCode();

    /// <summary>Names the token by its id only: the secret stays out of logs.</summary>
    public override string ToString() => "AccessToken[id=" + HexId(Id) + "]";
}

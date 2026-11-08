using System;
using System.Text;

namespace Lotse.Auth;

/// <summary>
/// A client's end of one SCRAM-SHA-512 exchange (RFC 5802, RFC 7677), without I/O.
/// <para>
/// Three calls in this order: <see cref="First"/>, <see cref="Respond"/> with the server's first message,
/// <see cref="Verify"/> with the server's final message. A call out of order fails. Channel binding is not used
/// (<c>n,,</c>): TLS ties the session to the server's certificate. The password is the PBKDF2 input as its UTF-8
/// bytes, with no SASLprep; a password that SASLprep would change gives a different key than other SCRAM clients
/// compute.
/// </para>
/// <para>
/// The client refuses a server whose nonce does not strictly extend its own, and an iteration count outside
/// <see cref="MinIterations"/> to <see cref="MaxIterations"/>, which would let a hostile server make it spin. Not
/// thread-safe.
/// </para>
/// </summary>
public sealed class ScramClient
{
    /// <summary>Fewest iterations a credential may have (RFC 7677 asks for at least 4096).</summary>
    public const int MinIterations = 4096;

    /// <summary>Most iterations a credential may have: a bound on the work one login costs either side.</summary>
    public const int MaxIterations = 1 << 20;

    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private enum State
    {
        Start,
        SentFirst,
        SentFinal,
        Done,
    }

    private readonly string _user;
    private readonly byte[] _password;
    private readonly string _nonce;
    private State _state = State.Start;
    private string _bare = "";
    private byte[] _serverSignature = Array.Empty<byte>();

    /// <summary>An exchange for <paramref name="user"/> with <paramref name="password"/>.</summary>
    /// <param name="user">The user name.</param>
    /// <param name="password">The password.</param>
    /// <param name="nonce">
    /// Fresh and unpredictable: printable ASCII without a comma. <see cref="Authenticator"/> uses 24 random bytes in hex.
    /// </param>
    public ScramClient(string user, string password, string nonce)
    {
        _user = user;
        _password = Encoding.UTF8.GetBytes(password);
        _nonce = nonce;
    }

    /// <summary><c>saslname</c>: <c>=</c> and <c>,</c> escaped as <c>=3D</c> and <c>=2C</c>.</summary>
    internal static string Escape(string name) => name.Replace("=", "=3D", StringComparison.Ordinal).Replace(",", "=2C", StringComparison.Ordinal);

    /// <summary>The client's first message: <c>n,,n=&lt;user&gt;,r=&lt;nonce&gt;</c>.</summary>
    public byte[] First()
    {
        _bare = "n=" + Escape(_user) + ",r=" + _nonce;
        _state = State.SentFirst;
        return Encoding.UTF8.GetBytes("n,," + _bare);
    }

    private static string Attribute(string[] parts, int index, char key)
    {
        if (index >= parts.Length)
        {
            throw new ScramException(ScramFailure.Malformed, "malformed SCRAM message: missing attribute " + key);
        }
        string part = parts[index];
        if (part.Length < 2 || part[0] != key || part[1] != '=')
        {
            throw new ScramException(ScramFailure.Malformed, "malformed SCRAM message: expected attribute " + key);
        }
        return part.Substring(2);
    }

    private static bool ValidNonce(string n)
    {
        if (n.Length == 0)
        {
            return false;
        }
        foreach (char c in n)
        {
            if (c < 0x21 || c > 0x7e || c == ',')
            {
                return false;
            }
        }
        return true;
    }

    private static string Utf8(byte[] bytes)
    {
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            throw new ScramException(ScramFailure.Malformed, "malformed SCRAM message: not UTF-8");
        }
    }

    /// <summary>Strict base64: the alphabet and padding only, no white space, which the BCL would skip.</summary>
    private static bool TryBase64(string text, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        foreach (char c in text)
        {
            bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=';
            if (!ok)
            {
                return false;
            }
        }
        try
        {
            bytes = Convert.FromBase64String(text);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    /// <summary>Answers the server's first message with the client's final one.</summary>
    /// <exception cref="ScramException">
    /// The message is malformed, its nonce does not extend ours, or its iteration count is out of bounds.
    /// </exception>
    public byte[] Respond(byte[] serverFirstBytes)
    {
        if (_state != State.SentFirst)
        {
            throw new ScramException(ScramFailure.OutOfOrder, "unexpected SCRAM step");
        }
        _state = State.Done;
        string serverFirst = Utf8(serverFirstBytes);
        string[] parts = serverFirst.Split(',');
        string serverNonce = Attribute(parts, 0, 'r');
        if (!TryBase64(Attribute(parts, 1, 's'), out byte[] salt))
        {
            throw new ScramException(ScramFailure.Malformed, "malformed SCRAM message: salt is not base64");
        }
        string count = Attribute(parts, 2, 'i');
        bool digits = count.Length > 0 && count.Length <= 10;
        foreach (char c in count)
        {
            digits &= c >= '0' && c <= '9';
        }
        if (!digits)
        {
            throw new ScramException(ScramFailure.Malformed, "malformed SCRAM message: bad iterations");
        }
        long iterations = long.Parse(count, System.Globalization.CultureInfo.InvariantCulture);
        // The server's nonce extends ours; a bound on the iterations keeps a hostile server from making the client spin.
        if (!serverNonce.StartsWith(_nonce, StringComparison.Ordinal)
            || serverNonce.Length == _nonce.Length
            || !ValidNonce(serverNonce)
            || iterations < MinIterations
            || iterations > MaxIterations)
        {
            throw new ScramException(
                ScramFailure.Malformed,
                "malformed SCRAM message: server nonce or iteration count not acceptable");
        }
        byte[] salted = Scram.SaltedPassword(_password, salt, (int)iterations);
        byte[] clientKey = Scram.Hmac(salted, Scram.ClientKeyLabel);
        byte[] storedKey = Scram.Sha512(clientKey);
        string withoutProof = "c=biws,r=" + serverNonce;
        byte[] authMessage = Encoding.UTF8.GetBytes(_bare + "," + serverFirst + "," + withoutProof);
        byte[] clientSignature = Scram.Hmac(storedKey, authMessage);
        byte[] proof = Scram.Xor(clientKey, clientSignature);
        byte[] serverKey = Scram.Hmac(salted, Scram.ServerKeyLabel);
        _serverSignature = Scram.Hmac(serverKey, authMessage);
        _state = State.SentFinal;
        return Encoding.UTF8.GetBytes(withoutProof + ",p=" + Convert.ToBase64String(proof));
    }

    /// <summary>Checks the server's final message: the server proves it knew the credential.</summary>
    /// <exception cref="ScramException">Of kind <see cref="ScramFailure.ServerNotVerified"/>: the signature is missing or wrong.</exception>
    public void Verify(byte[] serverFinal)
    {
        if (_state != State.SentFinal)
        {
            throw new ScramException(ScramFailure.OutOfOrder, "unexpected SCRAM step");
        }
        _state = State.Done;
        string message = Utf8(serverFinal);
        if (!message.StartsWith("v=", StringComparison.Ordinal))
        {
            throw NotVerified();
        }
        if (!TryBase64(message.Substring(2), out byte[] signature))
        {
            throw NotVerified();
        }
        if (!Scram.ConstantTimeEquals(signature, _serverSignature))
        {
            throw NotVerified();
        }
    }

    private static ScramException NotVerified() =>
        new(ScramFailure.ServerNotVerified, "the server could not prove it knows the credential");
}

using System;
using System.Security.Cryptography;
using System.Text;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Auth;

/// <summary>
/// What a node keeps for one SCRAM user: salt, iteration count, <c>StoredKey</c> and <c>ServerKey</c>. Neither the
/// password nor anything that lets one log in without it is stored or sent: a stolen credential lets an attacker
/// impersonate the server, not the user.
/// </summary>
public sealed class ScramCredential : IEquatable<ScramCredential>
{
    /// <summary>The salt length <see cref="ForPassword(string)"/> uses.</summary>
    public const int SaltBytes = 16;

    private readonly byte[] _salt;
    private readonly byte[] _storedKey;
    private readonly byte[] _serverKey;

    /// <summary>Creates a credential from its parts, copying them.</summary>
    /// <param name="salt">The salt.</param>
    /// <param name="iterations">PBKDF2 iterations, from <see cref="ScramClient.MinIterations"/> to <see cref="ScramClient.MaxIterations"/>.</param>
    /// <param name="storedKey"><c>H(ClientKey)</c>, 64 bytes.</param>
    /// <param name="serverKey"><c>HMAC(SaltedPassword, "Server Key")</c>, 64 bytes.</param>
    public ScramCredential(byte[] salt, int iterations, byte[] storedKey, byte[] serverKey)
    {
        ArgumentNullException.ThrowIfNull(salt);
        ArgumentNullException.ThrowIfNull(storedKey);
        ArgumentNullException.ThrowIfNull(serverKey);
        _salt = (byte[])salt.Clone();
        Iterations = iterations;
        _storedKey = (byte[])storedKey.Clone();
        _serverKey = (byte[])serverKey.Clone();
    }

    /// <summary>PBKDF2 iterations.</summary>
    public int Iterations { get; }

    /// <summary>A copy of the salt.</summary>
    public byte[] Salt => (byte[])_salt.Clone();

    /// <summary>A copy of <c>StoredKey</c>.</summary>
    public byte[] StoredKey => (byte[])_storedKey.Clone();

    /// <summary>A copy of <c>ServerKey</c>.</summary>
    public byte[] ServerKey => (byte[])_serverKey.Clone();

    /// <summary>
    /// The credential for <paramref name="password"/> with <paramref name="salt"/> and <paramref name="iterations"/>.
    /// The password is used as its UTF-8 bytes; there is no SASLprep.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="iterations"/> is outside the bounds a node accepts.</exception>
    public static ScramCredential Of(string password, byte[] salt, int iterations)
    {
        if (iterations < ScramClient.MinIterations || iterations > ScramClient.MaxIterations)
        {
            throw new ArgumentOutOfRangeException(
                nameof(iterations),
                $"iterations must be from {ScramClient.MinIterations} to {ScramClient.MaxIterations}, not {iterations}");
        }
        byte[] salted = Scram.SaltedPassword(Encoding.UTF8.GetBytes(password), salt, iterations);
        byte[] clientKey = Scram.Hmac(salted, Scram.ClientKeyLabel);
        return new ScramCredential(salt, iterations, Scram.Sha512(clientKey), Scram.Hmac(salted, Scram.ServerKeyLabel));
    }

    /// <summary>
    /// A credential for <paramref name="password"/> with a fresh 16-byte salt and <see cref="ScramClient.MinIterations"/>
    /// iterations, as <c>lotse-client</c>'s <c>set_user_password</c> makes it.
    /// </summary>
    public static ScramCredential ForPassword(string password) => ForPassword(password, b => RandomNumberGenerator.Fill(b));

    /// <summary>Like <see cref="ForPassword(string)"/> with another source of the salt; for tests.</summary>
    public static ScramCredential ForPassword(string password, Action<byte[]> random)
    {
        byte[] salt = new byte[SaltBytes];
        random(salt);
        return Of(password, salt, ScramClient.MinIterations);
    }

    /// <summary>The <c>AlterUser</c> request that sets this credential for <paramref name="user"/> (admin).</summary>
    public Request.AlterUser AlterUser(string user) =>
        new(user, new Messages.ScramCredentialData(
            (uint)Iterations, ByteString.CopyOf(_salt), ByteString.CopyOf(_storedKey), ByteString.CopyOf(_serverKey)));

    /// <summary>The <c>AlterUser</c> request that removes <paramref name="user"/>'s credential (admin).</summary>
    public static Request.AlterUser Removal(string user) => new(user, null);

    /// <inheritdoc/>
    public bool Equals(ScramCredential? other) =>
        other is not null
        && Iterations == other.Iterations
        && _salt.AsSpan().SequenceEqual(other._salt)
        && _storedKey.AsSpan().SequenceEqual(other._storedKey)
        && _serverKey.AsSpan().SequenceEqual(other._serverKey);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as ScramCredential);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Iterations);
        hash.AddBytes(_salt);
        hash.AddBytes(_storedKey);
        return hash.ToHashCode();
    }

    /// <inheritdoc/>
    public override string ToString() => $"ScramCredential[iterations={Iterations}]";
}

using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Lotse.Client;

/// <summary>A renewed client certificate (ADR 0123).</summary>
/// <param name="KeyPem">The new private key, PKCS#8 PEM.</param>
/// <param name="ChainPem">The new certificate, then the renewal CA's, PEM.</param>
/// <param name="NotAfterMs">Milliseconds since the Unix epoch at which the new certificate stops being valid.</param>
public sealed record RenewedCertificate(string KeyPem, string ChainPem, ulong NotAfterMs);

/// <summary>
/// Renewing a client certificate over a connection made with it (ADR 0123): when a certificate may be renewed, a new key
/// with its signing request, and writing the files a <see cref="Net.TlsConfig"/> reads.
/// <para>
/// A certificate may be renewed in its last 30 days or once two thirds of its validity have passed, whichever comes
/// first, until it expires. The new certificate is valid for 90 days.
/// </para>
/// </summary>
public static class CertificateRenewal
{
    /// <summary>The last stretch of a certificate's validity in which it may always be renewed: 30 days.</summary>
    public const ulong WindowMs = 30UL * 86_400_000UL;

    /// <summary>
    /// The millisecond since the Unix epoch from which a certificate valid from <paramref name="notBeforeMs"/> until
    /// <paramref name="notAfterMs"/> may be renewed: the earlier of its last 30 days and two thirds of its validity.
    /// </summary>
    public static ulong WindowOpensMs(ulong notBeforeMs, ulong notAfterMs)
    {
        ulong span = notAfterMs > notBeforeMs ? notAfterMs - notBeforeMs : 0;
        ulong twoThirds = notBeforeMs + span / 3 * 2 + span % 3 * 2 / 3;
        ulong lastDays = notAfterMs > WindowMs ? notAfterMs - WindowMs : 0;
        return Math.Min(lastDays, twoThirds);
    }

    /// <summary>Whether such a certificate may be renewed at <paramref name="nowMs"/>: its window is open and it has not expired.</summary>
    public static bool MayRenew(ulong notBeforeMs, ulong notAfterMs, ulong nowMs) =>
        nowMs >= WindowOpensMs(notBeforeMs, notAfterMs) && nowMs < notAfterMs;

    /// <summary>Whether the certificate in a PEM file (its first block, the leaf) may be renewed at <paramref name="nowMs"/>.</summary>
    public static bool Due(string certPemFile, ulong nowMs)
    {
        using var leaf = X509Certificate2.CreateFromPem(File.ReadAllText(certPemFile));
        return MayRenew(Ms(leaf.NotBefore), Ms(leaf.NotAfter), nowMs);
    }

    private static ulong Ms(DateTime t) => (ulong)new DateTimeOffset(t.ToUniversalTime()).ToUnixTimeMilliseconds();

    /// <summary>A new ECDSA P-256 key, PKCS#8 PEM, and a PKCS#10 signing request for it, DER; the node ignores its subject.</summary>
    public static (string KeyPem, byte[] Csr) NewKey()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=renewal", key, HashAlgorithmName.SHA256);
        return (key.ExportPkcs8PrivateKeyPem(), request.CreateSigningRequest());
    }

    /// <summary>PEM text of one DER block.</summary>
    public static string Pem(string label, ReadOnlySpan<byte> der) =>
        new string(PemEncoding.Write(label, der)) + "\n";

    /// <summary>
    /// Writes the renewed key and chain over the files: the key first, each through a file beside it that is then moved
    /// over the old one, so a reader sees one file or the other, never half of one. A <see cref="Net.TlsConfig"/> that
    /// reads them takes up the pair once both are written.
    /// </summary>
    public static void Write(RenewedCertificate renewed, string certPemFile, string keyPemFile)
    {
        ArgumentNullException.ThrowIfNull(renewed);
        Replace(keyPemFile, renewed.KeyPem, secret: true);
        Replace(certPemFile, renewed.ChainPem, secret: false);
    }

    private static void Replace(string path, string text, bool secret)
    {
        string tmp = path + ".new";
        File.WriteAllText(tmp, text, new UTF8Encoding(false));
        if (secret && !OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(tmp, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
        File.Move(tmp, path, overwrite: true);
    }
}

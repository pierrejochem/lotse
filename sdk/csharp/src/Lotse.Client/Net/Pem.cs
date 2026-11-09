using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Lotse.Net;

/// <summary>
/// PEM files for certificates and unencrypted private keys, which is what <c>lotse certs</c> writes (ECDSA P-256 keys
/// in <c>BEGIN PRIVATE KEY</c> blocks). The BCL reads PKCS#8, RSA and EC key blocks; an encrypted key is refused with
/// the command that decrypts it.
/// </summary>
internal static class Pem
{
    /// <summary>The certificates in <paramref name="pem"/>, in file order.</summary>
    /// <exception cref="ArgumentException">There are none or one cannot be parsed.</exception>
    internal static X509Certificate2Collection Certificates(string pem)
    {
        var certificates = new X509Certificate2Collection();
        try
        {
            certificates.ImportFromPem(pem);
        }
        catch (Exception e) when (e is CryptographicException or ArgumentException)
        {
            throw new ArgumentException("not a certificate: " + e.Message, nameof(pem), e);
        }
        if (certificates.Count == 0)
        {
            throw new ArgumentException("no -----BEGIN CERTIFICATE----- block", nameof(pem));
        }
        return certificates;
    }

    internal static X509Certificate2Collection CertificatesFile(string file) => Wrap(file, () => Certificates(File.ReadAllText(file)));

    /// <summary>The leader certificate with its private key, and the rest of the chain after it.</summary>
    internal static (X509Certificate2 Leaf, X509Certificate2Collection Chain) Identity(string certPem, string keyPem)
    {
        if (keyPem.Contains("-----BEGIN ENCRYPTED PRIVATE KEY-----", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "the key is encrypted; decrypt it first: openssl pkcs8 -in key.pem -out plain.pem -nocrypt",
                nameof(keyPem));
        }
        if (!keyPem.Contains("PRIVATE KEY-----", StringComparison.Ordinal))
        {
            throw new ArgumentException("no -----BEGIN PRIVATE KEY----- block", nameof(keyPem));
        }
        X509Certificate2Collection chain = Certificates(certPem);
        X509Certificate2 leaf;
        try
        {
            leaf = X509Certificate2.CreateFromPem(certPem, keyPem);
        }
        catch (Exception e) when (e is CryptographicException or ArgumentException)
        {
            throw new ArgumentException("cannot load the client identity: " + e.Message, nameof(keyPem), e);
        }
        var rest = new X509Certificate2Collection();
        for (int i = 1; i < chain.Count; i++)
        {
            rest.Add(chain[i]);
        }
        return (leaf, rest);
    }

    internal static (X509Certificate2 Leaf, X509Certificate2Collection Chain) IdentityFiles(string certFile, string keyFile) =>
        Wrap(certFile + " / " + keyFile, () => Identity(File.ReadAllText(certFile), File.ReadAllText(keyFile)));

    private static T Wrap<T>(string what, Func<T> read)
    {
        try
        {
            return read();
        }
        catch (ArgumentException e)
        {
            throw new ArgumentException(what + ": " + e.Message, e);
        }
    }
}

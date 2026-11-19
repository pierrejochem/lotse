using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;

namespace Lotse.Net;

/// <summary>
/// TLS for client connections: the cluster's CA, and optionally a client certificate.
/// <para>
/// Only TLS 1.3 is spoken, and the node's certificate must be valid for the host the client dialled (a name in its DNS
/// entries, or an IP address in its IP entries). Only the CA given here is trusted, not the system's roots. Nothing is
/// read from the system's trust store, and no revocation list is fetched: a node ends the connections of revoked
/// client certificates itself (ADR 0060).
/// </para>
/// <para>
/// A client identity given as files is read again for the next connection once either file changed, so a renewed
/// certificate (ADR 0123) is used without a restart. If the files cannot be read then, half written for instance, the
/// identity read before is kept.
/// </para>
/// </summary>
public sealed class TlsConfig
{
    /// <summary>The only protocol version.</summary>
    public const SslProtocols Protocol = SslProtocols.Tls13;

    private readonly X509Certificate2Collection _authorities;
    private readonly object _lock = new();
    private readonly string? _certFile;
    private readonly string? _keyFile;
    private SslStreamCertificateContext? _identity;
    private (DateTime, DateTime)? _stamps;

    private TlsConfig(
        X509Certificate2Collection authorities,
        SslStreamCertificateContext? identity,
        string? certFile,
        string? keyFile,
        (DateTime, DateTime)? stamps)
    {
        _authorities = authorities;
        _identity = identity;
        _certFile = certFile;
        _keyFile = keyFile;
        _stamps = stamps;
    }

    /// <summary>The identity to present: built again first if its files changed.</summary>
    private SslStreamCertificateContext? Identity()
    {
        if (_certFile is null || _keyFile is null)
        {
            return _identity;
        }
        lock (_lock)
        {
            var now = Stamps(_certFile, _keyFile);
            if (now is not null && now != _stamps)
            {
                try
                {
                    // CreateFromPem refuses a key that does not match the certificate: between the writes of a
                    // renewal the key may be new and the certificate not yet.
                    _identity = Context(Pem.IdentityFiles(_certFile, _keyFile));
                    _stamps = now;
                }
                catch (Exception e) when (e is ArgumentException or IOException or UnauthorizedAccessException
                    or System.Security.Cryptography.CryptographicException)
                {
                    // keep what worked, look again next time
                }
            }
            return _identity;
        }
    }

    private static (DateTime, DateTime)? Stamps(string cert, string key)
    {
        try
        {
            if (!File.Exists(cert) || !File.Exists(key))
            {
                return null;
            }
            return (File.GetLastWriteTimeUtc(cert), File.GetLastWriteTimeUtc(key));
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static SslStreamCertificateContext Context((X509Certificate2 Leaf, X509Certificate2Collection Chain) identity) =>
        // Offline: never fetch intermediates from the network.
        SslStreamCertificateContext.Create(identity.Leaf, identity.Chain, offline: true);

    /// <summary>Whether a client certificate is presented when a node asks for one.</summary>
    public bool HasClientCertificate => _identity is not null;

    /// <summary>A builder.</summary>
    public static Builder CreateBuilder() => new();

    /// <summary>Trusts the CA in <paramref name="caPemFile"/>, no client certificate.</summary>
    public static TlsConfig FromFiles(string caPemFile) => CreateBuilder().CaFile(caPemFile).Build();

    /// <summary>Trusts the CA in <paramref name="caPemFile"/> and presents the certificate and key as the client's identity.</summary>
    public static TlsConfig FromFiles(string caPemFile, string certPemFile, string keyPemFile) =>
        CreateBuilder().CaFile(caPemFile).ClientIdentityFiles(certPemFile, keyPemFile).Build();

    /// <summary>The options a handshake as a client uses.</summary>
    internal SslClientAuthenticationOptions Options(string targetHost)
    {
        var policy = new X509ChainPolicy
        {
            TrustMode = X509ChainTrustMode.CustomRootTrust,
            RevocationMode = X509RevocationMode.NoCheck,
        };
        policy.CustomTrustStore.AddRange(_authorities);
        return new SslClientAuthenticationOptions
        {
            TargetHost = targetHost,
            EnabledSslProtocols = Protocol,
            CertificateChainPolicy = policy,
            ClientCertificateContext = Identity(),
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
        };
    }

    /// <summary>Builds a <see cref="TlsConfig"/>.</summary>
    public sealed class Builder
    {
        private readonly X509Certificate2Collection _authorities = new();
        private (X509Certificate2 Leaf, X509Certificate2Collection Chain)? _identity;
        private string? _certFile;
        private string? _keyFile;
        private (DateTime, DateTime)? _stamps;

        internal Builder()
        {
        }

        /// <summary>Trusts the CA certificates in a PEM file.</summary>
        public Builder CaFile(string pemFile)
        {
            _authorities.AddRange(Pem.CertificatesFile(pemFile));
            return this;
        }

        /// <summary>Trusts the CA certificates in PEM text.</summary>
        public Builder Ca(string pem)
        {
            _authorities.AddRange(Pem.Certificates(pem));
            return this;
        }

        /// <summary>Presents a client certificate (the chain, leaf first) with its private key, from files.</summary>
        public Builder ClientIdentityFiles(string certPemFile, string keyPemFile)
        {
            _stamps = Stamps(certPemFile, keyPemFile);
            _identity = Pem.IdentityFiles(certPemFile, keyPemFile);
            _certFile = certPemFile;
            _keyFile = keyPemFile;
            return this;
        }

        /// <summary>Presents a client certificate (the chain, leaf first) with its private key, from PEM text.</summary>
        public Builder ClientIdentity(string certPem, string keyPem)
        {
            _identity = Pem.Identity(certPem, keyPem);
            _certFile = null;
            _keyFile = null;
            return this;
        }

        /// <summary>Builds the configuration.</summary>
        /// <exception cref="InvalidOperationException">No CA was given: a node's certificate cannot be checked against nothing.</exception>
        public TlsConfig Build()
        {
            if (_authorities.Count == 0)
            {
                throw new InvalidOperationException("no CA certificate: give the cluster's ca.pem");
            }
            SslStreamCertificateContext? context = _identity is { } identity ? Context(identity) : null;
            var copy = new X509Certificate2Collection();
            copy.AddRange(_authorities);
            return new TlsConfig(copy, context, _certFile, _keyFile, _stamps);
        }
    }
}

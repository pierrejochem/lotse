using System;
using System.Collections.Generic;
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
/// </summary>
public sealed class TlsConfig
{
    /// <summary>The only protocol version.</summary>
    public const SslProtocols Protocol = SslProtocols.Tls13;

    private readonly X509Certificate2Collection _authorities;
    private readonly SslStreamCertificateContext? _identity;

    private TlsConfig(X509Certificate2Collection authorities, SslStreamCertificateContext? identity)
    {
        _authorities = authorities;
        _identity = identity;
    }

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
            ClientCertificateContext = _identity,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
        };
    }

    /// <summary>Builds a <see cref="TlsConfig"/>.</summary>
    public sealed class Builder
    {
        private readonly X509Certificate2Collection _authorities = new();
        private (X509Certificate2 Leaf, X509Certificate2Collection Chain)? _identity;

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
            _identity = Pem.IdentityFiles(certPemFile, keyPemFile);
            return this;
        }

        /// <summary>Presents a client certificate (the chain, leaf first) with its private key, from PEM text.</summary>
        public Builder ClientIdentity(string certPem, string keyPem)
        {
            _identity = Pem.Identity(certPem, keyPem);
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
            SslStreamCertificateContext? context = null;
            if (_identity is { } identity)
            {
                // Offline: never fetch intermediates from the network.
                context = SslStreamCertificateContext.Create(identity.Leaf, identity.Chain, offline: true);
            }
            var copy = new X509Certificate2Collection();
            copy.AddRange(_authorities);
            return new TlsConfig(copy, context);
        }
    }
}

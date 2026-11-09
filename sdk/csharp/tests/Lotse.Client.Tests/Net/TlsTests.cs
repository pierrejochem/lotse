using System;
using System.Linq;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Net;

/// <summary>
/// TLS 1.3 against a server in this process with certificates made by the real <c>lotse certs</c> (a CA, a node
/// certificate with the IP entry 127.0.0.1 and a client certificate for "alice"). Skipped when <c>LOTSE_BIN</c> is
/// not set; see <see cref="LotseBinary"/>.
/// </summary>
public class TlsTests : IClassFixture<CertsFixture>
{
    private readonly CertsFixture _fixture;

    public TlsTests(CertsFixture fixture) => _fixture = fixture;

    private Certs Certs => _fixture.Certs;

    private static ConnectionOptions Options(TlsConfig tls) =>
        ConnectionOptions.CreateBuilder().Tls(tls).WithRequestTimeout(TimeSpan.FromSeconds(5)).Build();

    private static Address At(string host, TlsTestServer server) => new(host, server.Port);

    private static async Task<Response.ApiVersions> Ping(Connection c) => await c.ApiVersionsAsync();

    [LotseFact]
    public async Task ANodeWithACertificateFromTheClustersCaIsReachedAtItsIpAddress()
    {
        using var server = new TlsTestServer(Certs, SslProtocols.Tls13, false);
        await using Connection c = await Connection.OpenAsync(At("127.0.0.1", server), Options(TlsConfig.FromFiles(Certs.Ca)));
        Assert.True(c.IsSecure);
        Assert.Equal(new[] { "tls-test" }, (await Ping(c)).Features);
    }

    [LotseFact]
    public async Task TheNameMustBeOneTheCertificateIsValidFor()
    {
        // The certificate lists the IP address 127.0.0.1 and the node's URI, but not the name "localhost".
        using var server = new TlsTestServer(Certs, SslProtocols.Tls13, false);
        ConnectionException e = await Assert.ThrowsAsync<ConnectionException>(
            () => Connection.OpenAsync(At("localhost", server), Options(TlsConfig.FromFiles(Certs.Ca))));
        Assert.False(e.RequestSent);
        Assert.IsType<AuthenticationException>(e.InnerException);
    }

    [LotseFact]
    public async Task ACertificateFromAnotherCaIsRefused()
    {
        using var server = new TlsTestServer(Certs, SslProtocols.Tls13, false);
        ConnectionException e = await Assert.ThrowsAsync<ConnectionException>(
            () => Connection.OpenAsync(At("127.0.0.1", server), Options(TlsConfig.FromFiles(_fixture.OtherCa.Ca))));
        Assert.False(e.RequestSent);
        Assert.IsType<AuthenticationException>(e.InnerException);
    }

    [LotseFact]
    public async Task Tls12IsNotSpoken()
    {
        using var server = new TlsTestServer(Certs, SslProtocols.Tls12, false);
        ConnectionException e = await Assert.ThrowsAsync<ConnectionException>(
            () => Connection.OpenAsync(At("127.0.0.1", server), Options(TlsConfig.FromFiles(Certs.Ca))));
        Assert.NotNull(e.InnerException);
    }

    [LotseFact]
    public async Task AClientCertificateIsPresentedWhenTheNodeAsksForOne()
    {
        using var server = new TlsTestServer(Certs, SslProtocols.Tls13, true);
        await using (Connection c = await Connection.OpenAsync(
            At("127.0.0.1", server), Options(TlsConfig.FromFiles(Certs.Ca, Certs.ClientCert, Certs.ClientKey))))
        {
            Assert.Equal(new[] { "tls-test" }, (await Ping(c)).Features);
        }
        Assert.Equal(new[] { "alice" }, server.ClientSubjects.ToArray());
    }

    [LotseFact]
    public async Task WithoutAClientCertificateANodeThatRequiresOneRefuses()
    {
        using var server = new TlsTestServer(Certs, SslProtocols.Tls13, true);
        // In TLS 1.3 the client finishes its handshake before the server has checked the client's certificate, so
        // the refusal can arrive with the first request instead of the handshake.
        await Assert.ThrowsAsync<ConnectionException>(async () =>
        {
            await using Connection c = await Connection.OpenAsync(At("127.0.0.1", server), Options(TlsConfig.FromFiles(Certs.Ca)));
            await Ping(c);
        });
    }

    [LotseFact]
    public async Task AClientCertificateFromAnotherCaIsRefused()
    {
        using var server = new TlsTestServer(Certs, SslProtocols.Tls13, true);
        TlsConfig wrong = TlsConfig.FromFiles(Certs.Ca, _fixture.OtherCa.ClientCert, _fixture.OtherCa.ClientKey);
        await Assert.ThrowsAsync<ConnectionException>(async () =>
        {
            await using Connection c = await Connection.OpenAsync(At("127.0.0.1", server), Options(wrong));
            await Ping(c);
        });
    }

    [LotseFact]
    public async Task TheTlsRequestsGoThroughTheSameFraming()
    {
        using var server = new TlsTestServer(Certs, SslProtocols.Tls13, false);
        await using Connection c = await Connection.OpenAsync(At("127.0.0.1", server), Options(TlsConfig.FromFiles(Certs.Ca)));
        for (int i = 0; i < 20; i++)
        {
            Assert.IsType<Response.ApiVersions>(await c.CallAsync(new Request.ApiVersions()));
        }
    }

    [LotseFact]
    public void TheKeyThatLotseCertsWritesLoadsAndMatchesItsCertificate()
    {
        foreach ((string cert, string key) in new[] { (Certs.NodeCert, Certs.NodeKey), (Certs.ClientCert, Certs.ClientKey) })
        {
            using X509Certificate2 loaded = X509Certificate2.CreateFromPemFile(cert, key);
            Assert.True(loaded.HasPrivateKey);
            using var ecdsa = loaded.GetECDsaPrivateKey();
            Assert.NotNull(ecdsa); // rcgen's default is ECDSA P-256
            byte[] signature = ecdsa!.SignData(new byte[] { 1, 2, 3 }, System.Security.Cryptography.HashAlgorithmName.SHA256);
            using var pub = loaded.GetECDsaPublicKey();
            Assert.True(pub!.VerifyData(new byte[] { 1, 2, 3 }, signature, System.Security.Cryptography.HashAlgorithmName.SHA256), "the key belongs to the certificate");
        }
    }

    [Fact]
    public void ATlsConfigNeedsACaAndReadsPemWithAClearError()
    {
        Assert.Throws<InvalidOperationException>(() => TlsConfig.CreateBuilder().Build());
        Assert.Throws<ArgumentException>(() => TlsConfig.CreateBuilder().Ca("nothing"));
        string encrypted = "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n";
        ArgumentException e = Assert.Throws<ArgumentException>(() => Pem.Identity("x", encrypted));
        Assert.Contains("openssl pkcs8", e.Message);
        Assert.Throws<ArgumentException>(() => Pem.Identity("x", "nothing"));
        Assert.Equal(SslProtocols.Tls13, TlsConfig.Protocol);
    }
}

using System;
using System.Net;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Lotse.Net;
using Xunit;

namespace Lotse.Client.Tests.Net;

/// <summary>
/// A connection ends when the certificate its other end showed expires (ADR 0068). The certificates are made here,
/// so the test needs no <c>lotse</c> executable.
/// </summary>
public class CertificateExpiryTests
{
    private static (X509Certificate2 Ca, X509Certificate2 Server) Make(TimeSpan lifetime)
    {
        using var caKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var caRequest = new CertificateRequest("CN=expiry-test-ca", caKey, HashAlgorithmName.SHA256);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
        DateTimeOffset now = DateTimeOffset.UtcNow;
        using X509Certificate2 caWithKey = caRequest.CreateSelfSigned(now.AddHours(-1), now.AddHours(1));
        X509Certificate2 ca = X509CertificateLoader.LoadCertificate(caWithKey.Export(X509ContentType.Cert));

        using var serverKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=expiry-test-node", serverKey, HashAlgorithmName.SHA256);
        var san = new SubjectAlternativeNameBuilder();
        san.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
        using X509Certificate2 signed = request.Create(caWithKey, now.AddHours(-1), now + lifetime, new byte[] { 1, 2, 3, 4 });
        return (ca, signed.CopyWithPrivateKey(serverKey));
    }

    [Fact]
    public async Task TheConnectionEndsWhenTheNodesCertificateExpiresAndPendingRequestsFail()
    {
        (X509Certificate2 ca, X509Certificate2 serverCert) = Make(TimeSpan.FromSeconds(3));
        using var server = new TlsTestServer(serverCert, SslProtocols.Tls13, null);
        TlsConfig tls = TlsConfig.CreateBuilder().Ca(ca.ExportCertificatePem()).Build();
        ConnectionOptions options = ConnectionOptions.CreateBuilder().Tls(tls).WithRequestTimeout(TimeSpan.FromSeconds(30)).Build();
        await using Connection c = await Connection.OpenAsync(new Address("127.0.0.1", server.Port), options);
        Assert.NotEmpty((await c.ApiVersionsAsync()).Features);
        Assert.True(c.IsOpen);
        Assert.Null(c.Failure);

        // A request the server never answers is waiting when the certificate ends.
        Task<Lotse.Proto.Response> waiting = c.SendAsync(new Lotse.Proto.Request.Unsubscribe(1));
        await Eventually.AwaitAsync("the connection to end", () => !c.IsOpen, seconds: 10);
        ConnectionException e = await Assert.ThrowsAsync<ConnectionException>(() => waiting);
        Assert.Contains("certificate expired", e.Message);
        Assert.Contains("certificate expired", c.Failure!.Message);
        ConnectionException later = await Assert.ThrowsAsync<ConnectionException>(() => c.ApiVersionsAsync());
        Assert.False(later.RequestSent);
    }

    [Fact]
    public async Task ACertificateThatLastsLongerDoesNotEndTheConnection()
    {
        (X509Certificate2 ca, X509Certificate2 serverCert) = Make(TimeSpan.FromMinutes(30));
        using var server = new TlsTestServer(serverCert, SslProtocols.Tls13, null);
        TlsConfig tls = TlsConfig.CreateBuilder().Ca(ca.ExportCertificatePem()).Build();
        await using Connection c = await Connection.OpenAsync(
            new Address("127.0.0.1", server.Port), ConnectionOptions.CreateBuilder().Tls(tls).Build());
        await Task.Delay(1500);
        Assert.True(c.IsOpen);
        Assert.NotEmpty((await c.ApiVersionsAsync()).Features);
    }
}

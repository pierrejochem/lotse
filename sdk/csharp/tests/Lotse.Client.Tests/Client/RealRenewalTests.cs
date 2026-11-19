using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// Renewing a client certificate over a connection made with it (ADR 0123), against one real node with a renewal CA:
/// the call writes the new files, a <see cref="TlsConfig"/> built from them presents the new certificate at its next
/// connection, which is outside its window and so cannot be renewed again; and a client configured to renew by itself
/// does so once its certificate is in its window. Skipped, with a message, when <c>LOTSE_BIN</c> is not set.
/// </summary>
public sealed class RealRenewalTests : IClassFixture<AdminNodeFixture>
{
    private readonly AdminNodeFixture _f;

    public RealRenewalTests(AdminNodeFixture fixture) => _f = fixture;

    private ClientConfig.Builder Config(TlsConfig tls) =>
        ClientConfig.CreateBuilder(_f.Address.ToString()).Tls(tls).WithRetryFor(TimeSpan.FromSeconds(60));

    private TlsConfig TlsOf(string user) => TlsConfig.FromFiles(_f.Certs.Ca, _f.Certs.CertOf(user), _f.Certs.KeyOf(user));

    private static X509Certificate2 Leaf(string file) => X509Certificate2.CreateFromPem(File.ReadAllText(file));

    private static int Blocks(string file) => File.ReadAllText(file).Split("BEGIN CERTIFICATE").Length - 1;

    [LotseFact]
    public async Task ACertificateInItsWindowIsRenewedAndTheNewFilesAreUsedAtTheNextConnection()
    {
        string cert = _f.Certs.CertOf("carol");
        string key = _f.Certs.KeyOf("carol");
        TlsConfig tls = TlsOf("carol");
        string before = Leaf(cert).SerialNumber;
        using (var carol = new LotseClient(Config(tls).Build()))
        {
            RenewedCertificate renewed = await carol.RenewCertificateAsync(cert, key);
            using X509Certificate2 leaf = Leaf(cert);
            Assert.Equal(2, Blocks(cert));
            Assert.NotEqual(before, leaf.SerialNumber);
            Assert.Equal("CN=carol", leaf.Subject);
            Assert.Equal(renewed.NotAfterMs / 1000, (ulong)new DateTimeOffset(leaf.NotAfter.ToUniversalTime()).ToUnixTimeSeconds());
            ulong days = (renewed.NotAfterMs - (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) / 86_400_000UL;
            Assert.Equal(89UL, days);
        }
        // The same TlsConfig, on a new client's connections, shows the renewed certificate: 90 days long, it is not in its
        // window, so renewing again is refused.
        using var again = new LotseClient(Config(tls).Build());
        Assert.Equal("carol", (await again.WhoAmIAsync()).Name);
        var refused = await Assert.ThrowsAsync<BrokerErrorException>(() => again.RenewCertificateAsync());
        Assert.Equal(ErrorCode.InvalidRequest, refused.Code);
        Assert.Contains("two thirds", refused.BrokerMessage ?? string.Empty);
    }

    [LotseFact]
    public async Task AClientConfiguredToRenewByItselfRenewsInTheWindow()
    {
        string cert = _f.Certs.CertOf("dave");
        string key = _f.Certs.KeyOf("dave");
        string before = Leaf(cert).SerialNumber;
        using var dave = new LotseClient(Config(TlsOf("dave"))
            .RenewCertificate(cert, key)
            .WithRenewCheck(TimeSpan.FromMilliseconds(200))
            .Build());
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        while (Leaf(cert).SerialNumber == before)
        {
            Assert.True(DateTime.UtcNow < deadline, "never renewed: " + dave.RenewalFailure);
            await Task.Delay(100);
        }
        Assert.Equal(2, Blocks(cert));
        // Renewed, it is out of its window: the next looks change nothing.
        string renewed = Leaf(cert).SerialNumber;
        await Task.Delay(1000);
        Assert.Equal(renewed, Leaf(cert).SerialNumber);
        Assert.Null(dave.RenewalFailure);
    }
}

/// <summary>The renewal window, new keys and writing the files (ADR 0123), without a node.</summary>
public sealed class CertificateRenewalTests
{
    private const ulong Day = 86_400_000UL;

    [Fact]
    public void TheWindowOpensInTheLast30DaysOrAfterTwoThirds()
    {
        // A year: two thirds (day 243 and a third) come before the last 30 days.
        Assert.Equal(365 * Day / 3 * 2, CertificateRenewal.WindowOpensMs(0, 365 * Day));
        // 60 days: the last 30 days come first.
        Assert.Equal(30 * Day, CertificateRenewal.WindowOpensMs(0, 60 * Day));
        Assert.Equal(0UL, CertificateRenewal.WindowOpensMs(100, 103));
        Assert.True(CertificateRenewal.MayRenew(0, 60 * Day, 30 * Day));
        Assert.False(CertificateRenewal.MayRenew(0, 60 * Day, 30 * Day - 1));
        Assert.False(CertificateRenewal.MayRenew(0, 60 * Day, 60 * Day));
    }

    [Fact]
    public void ANewKeyIsAP256KeyWithASigningRequest()
    {
        (string keyPem, byte[] csr) = CertificateRenewal.NewKey();
        Assert.Contains("BEGIN PRIVATE KEY", keyPem);
        Assert.Equal(0x30, csr[0]);
        var request = CertificateRequest.LoadSigningRequest(csr, System.Security.Cryptography.HashAlgorithmName.SHA256);
        Assert.Equal("CN=renewal", request.SubjectName.Name);
    }

    [Fact]
    public void TheKeyIsWrittenThenTheChainEachWhole()
    {
        string dir = Directory.CreateTempSubdirectory("lotse-renewal").FullName;
        string cert = Path.Combine(dir, "c.pem");
        string key = Path.Combine(dir, "c.key");
        File.WriteAllText(cert, "old cert");
        File.WriteAllText(key, "old key");
        CertificateRenewal.Write(new RenewedCertificate("new key", "new chain", 1), cert, key);
        Assert.Equal("new chain", File.ReadAllText(cert));
        Assert.Equal("new key", File.ReadAllText(key));
        Assert.False(File.Exists(cert + ".new") || File.Exists(key + ".new"));
    }
}

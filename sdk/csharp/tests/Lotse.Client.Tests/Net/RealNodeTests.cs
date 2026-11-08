using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Lotse.Auth;
using Lotse.Net;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Net;

/// <summary>
/// One real <c>lotse</c> node, started from a generated cluster file with TLS, <c>alice</c> as cluster admin and
/// sessions limited to 4 s. The admin connects with a client certificate and creates a SCRAM user; the user then logs
/// in over SCRAM, renews its session, and loses it when it does not. Skipped when <c>LOTSE_BIN</c> is not set.
/// </summary>
public sealed class NodeFixture : IAsyncLifetime
{
    internal const int SessionMs = 4_000;

    private Process? _node;

    internal Certs Certs { get; private set; } = null!;

    internal Address Address { get; private set; } = null!;

    internal TlsConfig AdminTls { get; private set; } = null!;

    internal TlsConfig AnonymousTls { get; private set; } = null!;

    internal ConnectionOptions Options(TlsConfig tls, Credentials? credentials) =>
        ConnectionOptions.CreateBuilder().Tls(tls).WithCredentials(credentials).WithRequestTimeout(TimeSpan.FromSeconds(10)).Build();

    public async Task InitializeAsync()
    {
        string? bin = LotseBinary.Find();
        if (bin is null)
        {
            return;
        }
        Certs = await Certs.CreateAsync(bin, $"max_session_s = {SessionMs / 1000}\n");
        Address = new Address("127.0.0.1", Certs.ClientPort);
        AdminTls = TlsConfig.FromFiles(Certs.Ca, Certs.ClientCert, Certs.ClientKey);
        AnonymousTls = TlsConfig.FromFiles(Certs.Ca);
        var info = new ProcessStartInfo(bin) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string a in new[] { "run", "--config", Certs.Config, "--node", "1" })
        {
            info.ArgumentList.Add(a);
        }
        _node = Process.Start(info)!;
        // Both pipes drain into the log so the node never blocks on a full pipe.
        StreamWriter log = File.CreateText(Path.Combine(Certs.Dir, "node.log"));
        log.AutoFlush = true;
        _ = _node.StandardOutput.BaseStream.CopyToAsync(log.BaseStream);
        _ = _node.StandardError.BaseStream.CopyToAsync(log.BaseStream);

        // Ready when the node answers and has a leader for its groups: AlterUser is the proof.
        var credential = ScramCredential.ForPassword("correct horse");
        await Retry("the node to accept AlterUser", async () =>
        {
            await using Connection admin = await Connection.OpenAsync(Address, Options(AdminTls, null));
            Response.Admin r = await admin.CallAsync<Response.Admin>(credential.AlterUser("bob"));
            if (!r.Error.IsOk)
            {
                throw new InvalidOperationException($"AlterUser answered {r.Error} {r.ErrorMsg}");
            }
        });
    }

    private async Task Retry(string what, Func<Task> action)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception e)
            {
                last = e;
                await Task.Delay(200);
            }
        }
        string log = File.Exists(Path.Combine(Certs.Dir, "node.log")) ? await File.ReadAllTextAsync(Path.Combine(Certs.Dir, "node.log")) : "";
        throw new InvalidOperationException($"timed out waiting for {what}: {last}\nnode log:\n{log}", last);
    }

    public async Task DisposeAsync()
    {
        if (_node is null)
        {
            return;
        }
        try
        {
            _node.Kill(entireProcessTree: true);
            using var timeout = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10));
            await _node.WaitForExitAsync(timeout.Token);
        }
        catch (Exception)
        {
            // already gone
        }
    }
}

public class RealNodeTests : IClassFixture<NodeFixture>
{
    private readonly NodeFixture _f;

    public RealNodeTests(NodeFixture fixture) => _f = fixture;

    [LotseFact]
    public async Task ANodeAnswersApiVersionsAndMetadataOverTls()
    {
        await using Connection c = await Connection.OpenAsync(_f.Address, _f.Options(_f.AdminTls, null));
        Assert.True(c.IsSecure);
        Response.ApiVersions versions = await c.ApiVersionsAsync();
        Assert.Contains(versions.Versions, v => v.ApiKey.Equals(ApiKey.Metadata) && v.Min == 0);
        Assert.Contains(versions.Versions, v => v.ApiKey.Equals(ApiKey.SaslAuthenticate));
        Response.Metadata metadata = await c.CallAsync<Response.Metadata>(new Request.Metadata(Array.Empty<string>()));
        Assert.Single(metadata.Brokers);
        Assert.Equal(1ul, metadata.Brokers[0].NodeId);
        Assert.Equal(_f.Certs.ClientPort, metadata.Brokers[0].Port);
        Response.WhoAmI who = await c.CallAsync<Response.WhoAmI>(new Request.WhoAmI());
        Assert.Equal("alice", who.Name); // the principal comes from the certificate's common name
        Assert.Equal(SessionState.Unlimited, c.SessionState); // a certificate's session lasts the connection
    }

    [LotseFact]
    public async Task APipelinedBatchOfRequestsIsAnsweredToTheRightCaller()
    {
        await using Connection c = await Connection.OpenAsync(_f.Address, _f.Options(_f.AdminTls, null));
        var calls = new Task<Response>[40];
        for (int i = 0; i < calls.Length; i++)
        {
            calls[i] = i % 2 == 0 ? c.SendAsync(new Request.WhoAmI()) : c.SendAsync(new Request.ApiVersions());
        }
        Response[] answers = await Task.WhenAll(calls);
        for (int i = 0; i < answers.Length; i++)
        {
            if (i % 2 == 0)
            {
                Assert.Equal("alice", Assert.IsType<Response.WhoAmI>(answers[i]).Name);
            }
            else
            {
                Assert.IsType<Response.ApiVersions>(answers[i]);
            }
        }
    }

    [LotseFact]
    public async Task ANodeThatRequiresAuthenticationRefusesAnAnonymousClientWithTheErrorFlag()
    {
        await using Connection c = await Connection.OpenAsync(_f.Address, _f.Options(_f.AnonymousTls, null));
        BrokerException e = await Assert.ThrowsAsync<BrokerException>(() => c.CallAsync(new Request.Metadata(Array.Empty<string>())));
        Assert.Null(e.Reason); // an unauthenticated connection is told no more than the code
        Assert.Equal(ErrorCode.Unauthorized, e.Code);
        Assert.True(c.IsOpen, "a refused request does not end the connection");
        // ApiVersions is answered before anyone has authenticated: a client may look before it logs in.
        Assert.True((await c.ApiVersionsAsync()).Versions.Count > 10);
    }

    [LotseFact]
    public async Task TheAdminCreatedAScramUserAndTheUserLogsIn()
    {
        using var pool = new ConnectionPool(_f.Options(_f.AnonymousTls, Credentials.Scram("bob", "correct horse")));
        Connection bob = await pool.GetAsync(_f.Address);
        Response.WhoAmI who = await bob.CallAsync<Response.WhoAmI>(new Request.WhoAmI());
        Assert.Equal("bob", who.Name);
        Assert.Equal(SessionState.Active, bob.SessionState);
        Assert.Same(bob, await pool.GetAsync(_f.Address));
    }

    [LotseFact]
    public async Task AWrongPasswordAndAnUnknownUserAreRefusedTheSameWay()
    {
        AuthenticationException wrong = await Assert.ThrowsAsync<AuthenticationException>(
            () => Connection.OpenAsync(_f.Address, _f.Options(_f.AnonymousTls, Credentials.Scram("bob", "wrong"))));
        AuthenticationException unknown = await Assert.ThrowsAsync<AuthenticationException>(
            () => Connection.OpenAsync(_f.Address, _f.Options(_f.AnonymousTls, Credentials.Scram("nobody", "wrong"))));
        Assert.Equal(ErrorCode.AuthenticationFailed, wrong.Code);
        Assert.Equal(wrong.Message, unknown.Message); // the node does not say which was wrong
        Assert.Equal(ErrorCode.AuthenticationFailed, unknown.Code);
    }

    [LotseFact]
    public async Task AMechanismTheNodeDoesNotOfferIsNamedWithTheOnesItDoes()
    {
        AuthenticationException e = await Assert.ThrowsAsync<AuthenticationException>(
            () => Connection.OpenAsync(_f.Address, _f.Options(_f.AnonymousTls, Credentials.OAuthBearer(() => "token"))));
        Assert.Equal(ErrorCode.UnsupportedSaslMechanism, e.Code);
        Assert.Contains("OAUTHBEARER is not enabled", e.Message);
        Assert.Contains("SCRAM-SHA-512", e.Message);
    }

    [LotseFact]
    public async Task TheSessionIsRenewedOnTheSameConnectionAndEndsWhenItIsNot()
    {
        Credentials bob = Credentials.Scram("bob", "correct horse");
        using (var pool = new ConnectionPool(_f.Options(_f.AnonymousTls, bob)))
        {
            Connection c = await pool.GetAsync(_f.Address);
            await Task.Delay(NodeFixture.SessionMs * 4 / 5 + 200); // the node grants at most its maximum
            Assert.Equal(SessionState.RenewDue, c.SessionState);
            Assert.Same(c, await pool.GetAsync(_f.Address)); // renewed, same connection
            Assert.Equal(SessionState.Active, c.SessionState);
            // Past the first session's end, the renewed one carries the connection.
            await Task.Delay(NodeFixture.SessionMs / 5 + 300);
            Assert.Equal("bob", (await c.CallAsync<Response.WhoAmI>(new Request.WhoAmI())).Name);
        }

        // Without renewal the node ends the session and refuses what follows.
        await using Connection plain = await Connection.OpenAsync(_f.Address, _f.Options(_f.AnonymousTls, bob));
        Assert.Equal("bob", (await plain.CallAsync<Response.WhoAmI>(new Request.WhoAmI())).Name);
        await Task.Delay(NodeFixture.SessionMs + 300);
        Assert.Equal(SessionState.Ended, plain.SessionState);
        BrokerException e = await Assert.ThrowsAsync<BrokerException>(() => plain.CallAsync(new Request.WhoAmI()));
        Assert.Equal(ErrorCode.Unauthorized, e.Code);
    }

    [LotseFact]
    public async Task APoolReplacesAConnectionTheNodeOrTheCallerDropped()
    {
        using var pool = new ConnectionPool(_f.Options(_f.AnonymousTls, Credentials.Scram("bob", "correct horse")));
        Connection first = await pool.GetAsync(_f.Address);
        first.Dispose(); // as if the node had dropped it
        Connection second = await pool.GetAsync(_f.Address);
        Assert.NotSame(first, second);
        Assert.Equal("bob", (await second.CallAsync<Response.WhoAmI>(new Request.WhoAmI())).Name);
    }

    [LotseFact]
    public async Task DisposingAConnectionEndsItAndLaterSendsSayTheyWereNotSent()
    {
        Connection c = await Connection.OpenAsync(_f.Address, _f.Options(_f.AdminTls, null));
        await c.ApiVersionsAsync();
        c.Dispose();
        ConnectionException e = await Assert.ThrowsAsync<ConnectionException>(() => c.ApiVersionsAsync());
        Assert.False(e.RequestSent);
        Assert.False(c.IsOpen);
    }
}

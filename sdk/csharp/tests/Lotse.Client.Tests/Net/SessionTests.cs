using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Auth;
using Lotse.Net;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Net;

/// <summary>Logging in, renewing a session at four fifths of its lifetime, and replacing the connection when it ends.</summary>
public class SessionTests
{
    private const long Second = 1_000_000_000L;

    private static FakeBroker.Handler Serving(FakeSasl sasl) => (peer, req) =>
    {
        if (!sasl.Handle(peer, req))
        {
            peer.Reply(req.Header.CorrelationId, new Response.Outcome(ErrorCode.None, null));
        }
        return Task.CompletedTask;
    };

    private sealed class Clock
    {
        private long _now;

        internal Clock(long start) => _now = start;

        internal long Now
        {
            get => Interlocked.Read(ref _now);
            set => Interlocked.Exchange(ref _now, value);
        }

        internal long Add(long delta) => Interlocked.Add(ref _now, delta);
    }

    private static ConnectionOptions Options(Clock clock, Credentials credentials) =>
        ConnectionOptions.CreateBuilder()
            .PlaintextOnLoopback()
            .WithCredentials(credentials)
            .WithTicker(() => clock.Now)
            .WithRequestTimeout(TimeSpan.FromSeconds(5))
            .Build();

    [Fact]
    public async Task ASessionIsRenewedOnTheSameConnectionAtFourFifthsAndReplacedAtTheEnd()
    {
        var clock = new Clock(1_000_000 * Second); // an arbitrary origin
        long origin = clock.Now;
        var sasl = new FakeSasl(10_000);
        using var broker = new FakeBroker(Serving(sasl));
        using var pool = new ConnectionPool(Options(clock, Credentials.Scram("alice", "pw")));
        Connection first = await pool.GetAsync(broker.Address);
        Assert.Equal(1, sasl.Handshakes);
        Assert.Equal(SessionState.Active, first.SessionState);

        clock.Now = origin + 8 * Second - 1;
        Assert.Same(first, await pool.GetAsync(broker.Address));
        Assert.Equal(SessionState.Active, first.SessionState);
        Assert.Equal(1, sasl.Handshakes);

        clock.Now = origin + 8 * Second;
        Assert.Equal(SessionState.RenewDue, first.SessionState);
        Assert.Same(first, await pool.GetAsync(broker.Address)); // renewed on the same connection
        Assert.Equal(2, sasl.Handshakes);
        Assert.Equal(2, sasl.Finals);
        Assert.Equal(1, broker.Accepted);
        Assert.Equal(SessionState.Active, first.SessionState); // a new session of 10 s from 8 s

        clock.Now = origin + 18 * Second - 1;
        Assert.NotEqual(SessionState.Ended, first.SessionState);
        clock.Now = origin + 18 * Second;
        Assert.Equal(SessionState.Ended, first.SessionState);
        Connection second = await pool.GetAsync(broker.Address);
        Assert.NotSame(first, second); // an ended session is not renewed: the node refuses it
        Assert.Equal(2, broker.Accepted);
        Assert.False(first.IsOpen);
        Assert.Equal(3, sasl.Handshakes);
        Assert.Equal(SessionState.Active, second.SessionState);
    }

    [Fact]
    public async Task TheLifetimeComesFromTheLastResponse()
    {
        var clock = new Clock(5 * Second);
        long origin = clock.Now;
        var sasl = new FakeSasl(10_000);
        using var broker = new FakeBroker(Serving(sasl));
        using var pool = new ConnectionPool(Options(clock, Credentials.Scram("alice", "pw")));
        Connection c = await pool.GetAsync(broker.Address);
        sasl.LifetimeMs = 4_000; // the node grants less on renewal
        clock.Now = origin + 8 * Second;
        Assert.Same(c, await pool.GetAsync(broker.Address));
        clock.Now = origin + 8 * Second + 3_199_999_999L;
        Assert.Equal(SessionState.Active, c.SessionState); // renewal at 3.2 s of 4 s
        clock.Now = origin + 8 * Second + 3_200_000_000L;
        Assert.Equal(SessionState.RenewDue, c.SessionState);
        clock.Now = origin + 8 * Second + 4 * Second;
        Assert.Equal(SessionState.Ended, c.SessionState);
    }

    [Fact]
    public async Task TheLifetimeCountsFromBeforeTheHandshake()
    {
        var clock = new Clock(0);
        var sasl = new FakeSasl(10_000);
        // The log in takes a second: the node's clock started when it received the first message.
        sasl.OnHandshake = _ => clock.Add(Second);
        using var broker = new FakeBroker(Serving(sasl));
        using var pool = new ConnectionPool(Options(clock, Credentials.Scram("alice", "pw")));
        Connection c = await pool.GetAsync(broker.Address);
        Assert.Equal(1 * Second, clock.Now);
        clock.Now = 8 * Second - 1;
        Assert.Equal(SessionState.Active, c.SessionState);
        clock.Now = 8 * Second;
        Assert.Equal(SessionState.RenewDue, c.SessionState); // 4/5 of 10 s from time 0, not from time 1 s
        clock.Now = 10 * Second - 1;
        Assert.Equal(SessionState.RenewDue, c.SessionState);
        clock.Now = 10 * Second;
        Assert.Equal(SessionState.Ended, c.SessionState);
    }

    [Fact]
    public async Task ASessionWithoutALifetimeLastsTheConnection()
    {
        var clock = new Clock(0);
        var sasl = new FakeSasl(0);
        using var broker = new FakeBroker(Serving(sasl));
        using var pool = new ConnectionPool(Options(clock, Credentials.Scram("alice", "pw")));
        Connection c = await pool.GetAsync(broker.Address);
        Assert.Equal(SessionState.Unlimited, c.SessionState);
        clock.Now = 1_000_000 * Second;
        Assert.Same(c, await pool.GetAsync(broker.Address));
        Assert.Equal(1, sasl.Handshakes);
    }

    [Fact]
    public async Task AnAbsurdLifetimeIsClampedNotWrapped()
    {
        var clock = new Clock(0);
        var sasl = new FakeSasl(ulong.MaxValue);
        using var broker = new FakeBroker(Serving(sasl));
        using var pool = new ConnectionPool(Options(clock, Credentials.Scram("alice", "pw")));
        Connection c = await pool.GetAsync(broker.Address);
        Assert.Equal(SessionState.Active, c.SessionState);
        clock.Now = 1_000_000 * Second;
        Assert.Equal(SessionState.Active, c.SessionState);
    }

    [Fact]
    public async Task OauthBearerAsksForTheTokenOnEveryAuthentication()
    {
        var clock = new Clock(0);
        var sasl = new FakeSasl(10_000);
        int issued = 0;
        using var broker = new FakeBroker(Serving(sasl));
        using var pool = new ConnectionPool(Options(clock, Credentials.OAuthBearer(() => "token-" + Interlocked.Increment(ref issued))));
        Connection c = await pool.GetAsync(broker.Address);
        Assert.Equal(new[] { "token-1" }, sasl.Tokens.ToArray());
        clock.Now = 8 * Second;
        Assert.Same(c, await pool.GetAsync(broker.Address));
        Assert.Equal(new[] { "token-1", "token-2" }, sasl.Tokens.ToArray()); // a fresh token to renew
    }

    [Fact]
    public async Task AnAsynchronousTokenSourceIsAwaited()
    {
        var sasl = new FakeSasl(10_000);
        using var broker = new FakeBroker(Serving(sasl));
        Credentials credentials = Credentials.OAuthBearerAsync(async ct =>
        {
            await Task.Delay(10, ct);
            return "late-token";
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Options(new Clock(0), credentials));
        Assert.Equal(new[] { "late-token" }, sasl.Tokens.ToArray());
    }

    [Fact]
    public async Task AWrongPasswordFailsTheLoginAndLeavesNothingInThePool()
    {
        var sasl = new FakeSasl(10_000);
        using var broker = new FakeBroker(Serving(sasl));
        using var pool = new ConnectionPool(Options(new Clock(0), Credentials.Scram("alice", "wrong")));
        AuthenticationException e = await Assert.ThrowsAsync<AuthenticationException>(() => pool.GetAsync(broker.Address));
        Assert.Equal(ErrorCode.AuthenticationFailed, e.Code);
        Assert.Equal("authentication failed", e.Message);
        await Assert.ThrowsAsync<AuthenticationException>(() => pool.GetAsync(broker.Address));
        Assert.Equal(2, broker.Accepted); // each try is a new connection; the failed one was closed
    }

    [Fact]
    public async Task AMechanismTheNodeDoesNotOfferNamesTheOnesItDoes()
    {
        var sasl = new FakeSasl(10_000);
        sasl.Mechanisms = new[] { "OAUTHBEARER" };
        using var broker = new FakeBroker(Serving(sasl));
        AuthenticationException e = await Assert.ThrowsAsync<AuthenticationException>(
            () => Connection.OpenAsync(broker.Address, Options(new Clock(0), Credentials.Scram("alice", "pw"))));
        Assert.Equal(ErrorCode.UnsupportedSaslMechanism, e.Code);
        Assert.Contains("SCRAM-SHA-512 is not enabled", e.Message);
        Assert.Contains("OAUTHBEARER", e.Message);
    }

    [Fact]
    public async Task AServerThatCannotProveItselfIsRefused()
    {
        // A broker that accepts any proof and answers with a made up signature.
        using var broker = new FakeBroker((peer, req) =>
        {
            if (req.Request is Request.SaslHandshake)
            {
                peer.Reply(req.Header.CorrelationId, new Response.SaslHandshake(ErrorCode.None, new[] { "SCRAM-SHA-512" }));
            }
            else
            {
                string first = Encoding.UTF8.GetString(((Request.SaslAuthenticate)req.Request).AuthBytes.ToByteArray());
                byte[] output;
                if (first.StartsWith("n,,", StringComparison.Ordinal))
                {
                    string nonce = first.Substring(first.IndexOf("r=", StringComparison.Ordinal) + 2);
                    output = Encoding.UTF8.GetBytes("r=" + nonce + "xyz,s=MDEyMzQ1Njc4OWFiY2RlZg==,i=4096");
                }
                else
                {
                    output = Encoding.UTF8.GetBytes("v=" + Convert.ToBase64String(new byte[64]));
                }
                peer.Reply(req.Header.CorrelationId, new Response.SaslAuthenticate(ErrorCode.None, null, ByteString.CopyOf(output), 10_000));
            }
        });
        ScramException e = await Assert.ThrowsAsync<ScramException>(
            () => Connection.OpenAsync(broker.Address, Options(new Clock(0), Credentials.Scram("alice", "pw"))));
        Assert.Equal(ScramFailure.ServerNotVerified, e.Kind);
        Assert.Contains("could not prove", e.Message);
    }

    [Fact]
    public async Task AConnectionThatBreaksDuringRenewalIsReplacedWithAFreshLogin()
    {
        var clock = new Clock(0);
        var sasl = new FakeSasl(10_000);
        int handshakes = 0;
        FakeBroker.Handler inner = Serving(sasl);
        using var broker = new FakeBroker(async (peer, req) =>
        {
            // The second handshake is the renewal: the connection dies under it.
            if (req.Request is Request.SaslHandshake && Interlocked.Increment(ref handshakes) == 2)
            {
                peer.CloseSocket();
                return;
            }
            await inner(peer, req);
        });
        using var pool = new ConnectionPool(Options(clock, Credentials.Scram("alice", "pw")));
        Connection first = await pool.GetAsync(broker.Address);
        clock.Now = 8 * Second;
        Connection second = await pool.GetAsync(broker.Address);
        Assert.NotSame(first, second);
        Assert.Equal(2, broker.Accepted);
        Assert.False(first.IsOpen);
        Assert.Equal(SessionState.Active, second.SessionState);
    }

    [Fact]
    public async Task ARefusedRenewalIsReportedAndTheConnectionDropped()
    {
        var clock = new Clock(0);
        var sasl = new FakeSasl(10_000);
        int handshakes = 0;
        FakeBroker.Handler inner = Serving(sasl);
        using var broker = new FakeBroker(async (peer, req) =>
        {
            // On renewal the node has forgotten the mechanism.
            if (req.Request is Request.SaslHandshake && Interlocked.Increment(ref handshakes) > 1)
            {
                peer.Reply(req.Header.CorrelationId, new Response.SaslHandshake(ErrorCode.UnsupportedSaslMechanism, Array.Empty<string>()));
                return;
            }
            await inner(peer, req);
        });
        using var pool = new ConnectionPool(Options(clock, Credentials.Scram("alice", "pw")));
        Connection first = await pool.GetAsync(broker.Address);
        clock.Now = 8 * Second;
        await Assert.ThrowsAsync<AuthenticationException>(() => pool.GetAsync(broker.Address));
        Assert.False(first.IsOpen);
    }

    [Fact]
    public async Task OnlyOneCallerRenewsAtATime()
    {
        var clock = new Clock(0);
        var sasl = new FakeSasl(10_000);
        using var broker = new FakeBroker(Serving(sasl));
        using var pool = new ConnectionPool(Options(clock, Credentials.Scram("alice", "pw")));
        Connection c = await pool.GetAsync(broker.Address);
        clock.Now = 8 * Second;
        Connection[] got = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => pool.GetAsync(broker.Address)));
        Assert.All(got, g => Assert.Same(c, g));
        Assert.True(sasl.Handshakes <= 3, "renewals are not repeated by every caller");
        Assert.Equal(1, broker.Accepted);
    }

    [Fact]
    public async Task AClosedPoolRefuses()
    {
        var pool = new ConnectionPool(ConnectionOptions.CreateBuilder().PlaintextOnLoopback().Build());
        pool.Dispose();
        await Assert.ThrowsAsync<ConnectionException>(() => pool.GetAsync(new Address("127.0.0.1", 9)));
    }
}

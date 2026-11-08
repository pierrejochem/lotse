using System;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Net;

/// <summary>Plain TCP only on loopback and only when asked for (the rule of AGENTS.md, rule 7).</summary>
public class TransportTests
{
    [Fact]
    public async Task NothingIsConnectedUntilTheTransportIsChosen()
    {
        using var broker = new FakeBroker((peer, req) => { });
        ConnectionOptions none = ConnectionOptions.CreateBuilder().Build();
        await Assert.ThrowsAsync<InsecureTransportException>(() => Connection.OpenAsync(broker.Address, none));
        using var pool = new ConnectionPool(none);
        await Assert.ThrowsAsync<InsecureTransportException>(() => pool.GetAsync(broker.Address));
        await Task.Delay(100);
        Assert.Equal(0, broker.Accepted);
    }

    [Fact]
    public async Task PlainTcpIsRefusedOffLoopbackBeforeAnythingIsSent()
    {
        ConnectionOptions plain = ConnectionOptions.CreateBuilder().PlaintextOnLoopback().WithConnectTimeout(TimeSpan.FromSeconds(30)).Build();
        long start = Environment.TickCount64;
        foreach (string host in new[] { "192.0.2.1", "10.1.2.3", "0.0.0.0", "8.8.8.8", "2001:db8::1" })
        {
            InsecureTransportException e = await Assert.ThrowsAsync<InsecureTransportException>(
                () => Connection.OpenAsync(new Address(host, 9092), plain));
            Assert.Contains("loopback", e.Message);
            Assert.False(e.RequestSent);
        }
        Assert.True(Environment.TickCount64 - start < 5000, "refused without trying to connect");
    }

    [Fact]
    public async Task PlainTcpToLoopbackNamesWorks()
    {
        ConnectionOptions plain = ConnectionOptions.CreateBuilder().PlaintextOnLoopback().Build();
        using var broker = new FakeBroker((peer, req) => peer.Reply(req.Header.CorrelationId, new Response.Outcome(ErrorCode.None, null)));
        await using Connection c = await Connection.OpenAsync(new Address("localhost", broker.Address.Port), plain);
        Assert.IsType<Response.Outcome>(await c.CallAsync(new Request.Unsubscribe(1)));
        Assert.False(c.IsSecure);
    }

    [Fact]
    public void ChoosingTlsOrPlaintextReplacesTheOtherChoice()
    {
        // The last choice wins; neither is left half set.
        Assert.Throws<InvalidOperationException>(() => TlsConfig.CreateBuilder().Build());
    }

    [Fact]
    public void AddressesParse()
    {
        Assert.Equal(new Address("broker-1.example", 9092), Address.Parse("broker-1.example:9092"));
        Assert.Equal(new Address("127.0.0.1", 7), Address.Parse("127.0.0.1:7"));
        Assert.Equal(new Address("::1", 9092), Address.Parse("[::1]:9092"));
        Assert.Equal("[::1]:9092", Address.Parse("[::1]:9092").ToString());
        foreach (string bad in new[] { "nohost", "::1:9092", "host:", "host:abc", "host:0", "host:70000", ":9092", "host:-1" })
        {
            Assert.ThrowsAny<ArgumentException>(() => Address.Parse(bad));
        }
    }
}

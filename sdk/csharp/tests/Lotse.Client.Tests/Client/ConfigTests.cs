using System;
using System.Linq;
using Lotse.Net;
using Xunit;

namespace Lotse.Client.Tests.Client;

public sealed class ConfigTests
{
    [Fact]
    public void TheDefaultsAreTheRustClients()
    {
        ClientConfig c = ClientConfig.CreateBuilder("a:1", "[::1]:2").Build();
        Assert.Equal(new[] { Address.Parse("a:1"), Address.Parse("[::1]:2") }, c.Bootstrap.ToArray());
        Assert.Equal(TimeSpan.FromSeconds(10), c.RequestTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), c.RetryFor);
        Assert.Equal(TimeSpan.FromMilliseconds(50), c.Backoff);
        Assert.Equal(TimeSpan.FromSeconds(5), c.ProduceTimeout);
        Assert.True(c.Idempotent);
        Assert.Equal(TimeSpan.FromSeconds(60), c.TxnTimeout);
        Assert.Null(c.ProducerName);
        Assert.Null(c.TransactionalId);
        Assert.Equal(TimeSpan.FromSeconds(30), c.MetadataMaxAge);
        Assert.Equal("default", c.Tenant);
        Assert.Null(c.Credentials);
    }

    [Fact]
    public void ABootstrapListIsNeededAndEveryAddressMustParse()
    {
        Assert.Throws<ArgumentException>(() => ClientConfig.CreateBuilder(Array.Empty<string>()));
        Assert.Throws<ArgumentException>(() => ClientConfig.CreateBuilder("no-port"));
    }

    [Fact]
    public void DurationsMustBePositive()
    {
        ClientConfig.Builder b = ClientConfig.CreateBuilder("a:1");
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithRetryFor(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithBackoff(TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => b.WithRequestTimeout(TimeSpan.Zero));
    }

    [Fact]
    public void NamesAndIdsAreNotEmptyAndHaveNoNul()
    {
        ClientConfig.Builder b = ClientConfig.CreateBuilder("a:1");
        Assert.Throws<ArgumentException>(() => b.WithProducerName(string.Empty));
        Assert.Throws<ArgumentException>(() => b.WithTransactionalId("a\0"));
        Assert.Equal("w", b.WithProducerName("w").Build().ProducerName);
    }

    [Fact]
    public async System.Threading.Tasks.Task AClosedClientRefusesCalls()
    {
        var client = new LotseClient(ClientConfig.CreateBuilder("127.0.0.1:1").PlaintextOnLoopback().Build());
        client.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.MetadataAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.ProduceAsync("s", 0, OutMessage.FromValue("x")));
    }
}

using System.Threading.Tasks;
using Xunit;

namespace Lotse.Client.Tests.Net;

/// <summary>Two independent sets of certificates from the real <c>lotse certs</c>; empty when there is no executable.</summary>
public sealed class CertsFixture : IAsyncLifetime
{
    internal Certs Certs { get; private set; } = null!;

    internal Certs OtherCa { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        string? bin = LotseBinary.Find();
        if (bin is null)
        {
            return;
        }
        Certs = await Certs.CreateAsync(bin, "");
        OtherCa = await Certs.CreateAsync(bin, "");
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

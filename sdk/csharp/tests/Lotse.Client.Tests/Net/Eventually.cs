using System;
using System.Threading.Tasks;

namespace Lotse.Client.Tests.Net;

internal static class Eventually
{
    internal static async Task AwaitAsync(string what, Func<bool> condition, int seconds = 5)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new Xunit.Sdk.XunitException("timed out waiting for " + what);
            }
            await Task.Delay(5);
        }
    }
}

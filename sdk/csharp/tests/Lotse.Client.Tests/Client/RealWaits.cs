using System;
using System.Linq;
using System.Threading.Tasks;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>Waits that the real-cluster tests share.</summary>
internal static class RealWaits
{
    /// <summary>
    /// Creates a stream and waits until every node knows it. A node that coordinates a group keeps only the streams it
    /// knows from a member's subscription at the time it joins: a stream that node has not applied yet is dropped from
    /// the subscription for good (see <c>client/groups.rs</c>), so a member that joins in that moment never gets a
    /// partition. The Rust and Java clients meet the same race; the tests do not want to measure it.
    /// </summary>
    internal static async Task CreateStreamEverywhereAsync(RealCluster cluster, LotseClient client, string name, int partitions)
    {
        await client.CreateStreamAsync(name, partitions, 3);
        for (int node = 1; node <= 3; node++)
        {
            using var probe = new LotseClient(ClientConfig.CreateBuilder(cluster.AddressOf(node).ToString()).PlaintextOnLoopback()
                .WithRequestTimeout(TimeSpan.FromSeconds(5)).WithRetryFor(TimeSpan.FromSeconds(60)).Build());
            DateTime deadline = DateTime.UtcNow.AddSeconds(60);
            while (true)
            {
                Response.Metadata m = await probe.MetadataAsync(new[] { name });
                if (m.Resources.Any(r => r.Name == name && r.Error.IsOk && r.Partitions.Count == partitions))
                {
                    break;
                }
                Assert.True(DateTime.UtcNow < deadline, $"node {node} does not know {name}");
                await Task.Delay(50);
            }
        }
    }
}

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

    /// <summary>Polls <paramref name="condition"/> until it holds; fails with <paramref name="what"/> after <paramref name="seconds"/>.</summary>
    internal static async Task UntilAsync(string what, Func<Task<bool>> condition, int seconds = 60)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "timed out waiting for " + what);
            await Task.Delay(50);
        }
    }

    /// <summary>
    /// Waits until every node's own view of the cluster (its metadata, asked of that node alone) satisfies
    /// <paramref name="holds"/>. An admin change is answered when the metadata group has applied it; the other nodes
    /// apply it a moment later, and a test that looks at them must not race that.
    /// </summary>
    internal static async Task OnEveryNodeAsync(RealCluster cluster, string what, string name, Func<Messages.ResourceMetadata?, bool> holds)
    {
        for (int node = 1; node <= 3; node++)
        {
            using var probe = new LotseClient(ClientConfig.CreateBuilder(cluster.AddressOf(node).ToString()).PlaintextOnLoopback()
                .WithRequestTimeout(TimeSpan.FromSeconds(5)).WithRetryFor(TimeSpan.FromSeconds(60)).Build());
            await UntilAsync($"node {node}: {what}", async () =>
            {
                Response.Metadata m = await probe.MetadataAsync(new[] { name });
                return holds(m.Resources.FirstOrDefault(r => r.Name == name && r.Error.IsOk));
            });
        }
    }
}

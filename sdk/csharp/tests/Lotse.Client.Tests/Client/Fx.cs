using System;
using System.Collections.Generic;
using System.Linq;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client.Tests.Client;

/// <summary>Builders for the fake brokers' answers.</summary>
internal static class Fx
{
    /// <summary>A client configuration for fake brokers: plain TCP, short timeouts so that a test that should fail fails soon.</summary>
    internal static ClientConfig.Builder Config(params FakeNode[] bootstrap) =>
        ClientConfig.CreateBuilder(bootstrap.Select(n => n.HostPort))
            .PlaintextOnLoopback()
            .WithRequestTimeout(TimeSpan.FromSeconds(2))
            .WithRetryFor(TimeSpan.FromSeconds(2))
            .WithBackoff(TimeSpan.FromMilliseconds(5));

    internal static Response.Metadata Metadata(IEnumerable<FakeNode> nodes, params Messages.ResourceMetadata[] resources) =>
        new(nodes.Select(n => new Messages.Broker(n.Id, "127.0.0.1", (ushort)n.Address.Port, string.Empty)).ToList(), resources);

    internal static Messages.PartitionMetadata Partition(uint number, uint leader, ulong term, params uint[] voters) =>
        new(number, leader, term, voters);

    internal static Messages.ResourceMetadata Stream(string name, ulong id, params Messages.PartitionMetadata[] partitions) =>
        new(ErrorCode.None, name, "default", id, Messages.ResourceKind.Stream, partitions, null);

    internal static Messages.ResourceMetadata Unknown(string name) =>
        new(ErrorCode.UnknownResource, name, string.Empty, 0, Messages.ResourceKind.Stream, Array.Empty<Messages.PartitionMetadata>(), null);

    internal static Response.Produce Produced(ulong resourceId, uint partition, ErrorCode code, ulong baseOffset, LeaderHint? hint = null) =>
        new(0, new[] { new Messages.ProduceResourceResponse(resourceId, new[] { new Messages.ProducePartitionResponse(partition, code, baseOffset, -1, null, hint) }) });

    internal static Response.Produce Produced(ulong resourceId, uint partition, ulong baseOffset) =>
        Produced(resourceId, partition, ErrorCode.None, baseOffset);

    internal static Response.Fetch Fetched(
        ulong resourceId,
        uint partition,
        ErrorCode code,
        ByteString records,
        ulong highWatermark,
        ulong lastStable,
        ulong logStart,
        IReadOnlyList<Messages.AbortedTxn>? aborted = null,
        LeaderHint? hint = null) =>
        new(
            0,
            new[]
            {
                new Messages.FetchResourceResponse(
                    resourceId,
                    new[]
                    {
                        new Messages.FetchPartitionResponse(
                            partition, code, highWatermark, lastStable, logStart, aborted ?? Array.Empty<Messages.AbortedTxn>(), records, hint),
                    }),
            });

    internal static Response.FindCoordinator Coordinator(FakeNode n) =>
        new(ErrorCode.None, n.Id, "127.0.0.1", (ushort)n.Address.Port, null);

    internal static Response.InitProducer Producer(ulong id, ushort epoch) => new(ErrorCode.None, id, epoch, null);

    /// <summary>The batch of a produce request's only partition.</summary>
    internal static Batch BatchOf(Request.Produce p) => Batch.Parse(BytesOf(p)).Batch;

    internal static ByteString BytesOf(Request.Produce p) => p.Resources[0].Partitions[0].Batch;

    internal static IReadOnlyList<OutMessage> Msgs(int n) =>
        Enumerable.Range(0, n).Select(i => OutMessage.FromValue("m" + i)).ToList();
}

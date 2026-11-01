package dev.lotse.client;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.LeaderHint;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.AbortedTxn;
import dev.lotse.proto.Messages.Broker;
import dev.lotse.proto.Messages.FetchPartitionResponse;
import dev.lotse.proto.Messages.FetchResourceResponse;
import dev.lotse.proto.Messages.PartitionMetadata;
import dev.lotse.proto.Messages.ProducePartitionResponse;
import dev.lotse.proto.Messages.ProduceResourceResponse;
import dev.lotse.proto.Messages.ResourceMetadata;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.Batch;
import dev.lotse.record.ByteString;
import java.util.ArrayList;
import java.util.List;

/** Builders for the fake brokers' answers. */
final class Fx {
    private Fx() {}

    /** A client configuration for fake brokers: plain TCP, short timeouts so that a test that should fail fails soon. */
    static ClientConfig.Builder config(FakeNode... bootstrap) {
        List<String> hosts = new ArrayList<>();
        for (FakeNode n : bootstrap) {
            hosts.add(n.hostPort());
        }
        return ClientConfig.builder(hosts)
                .plaintextOnLoopback()
                .requestTimeout(java.time.Duration.ofSeconds(2))
                .retryFor(java.time.Duration.ofSeconds(2))
                .backoff(java.time.Duration.ofMillis(5));
    }

    static Response.Metadata metadata(List<FakeNode> nodes, ResourceMetadata... resources) {
        List<Broker> brokers = new ArrayList<>();
        for (FakeNode n : nodes) {
            brokers.add(new Broker(n.id, "127.0.0.1", n.address().port(), ""));
        }
        return new Response.Metadata(brokers, List.of(resources));
    }

    static PartitionMetadata partition(int number, long leader, long term, Long... voters) {
        return new PartitionMetadata(number, leader, term, List.of(voters));
    }

    static ResourceMetadata stream(String name, long id, PartitionMetadata... partitions) {
        return new ResourceMetadata(ErrorCode.NONE, name, "default", id, Messages.ResourceKind.STREAM, List.of(partitions), null);
    }

    static ResourceMetadata unknown(String name) {
        return new ResourceMetadata(ErrorCode.UNKNOWN_RESOURCE, name, "", 0, Messages.ResourceKind.STREAM, List.of(), null);
    }

    static Response.Produce produced(long resourceId, int partition, ErrorCode code, long baseOffset, LeaderHint hint) {
        return new Response.Produce(
                0,
                List.of(new ProduceResourceResponse(
                        resourceId,
                        List.of(new ProducePartitionResponse(partition, code, baseOffset, -1, null, hint)))));
    }

    static Response.Produce produced(long resourceId, int partition, long baseOffset) {
        return produced(resourceId, partition, ErrorCode.NONE, baseOffset, null);
    }

    static Response.Fetch fetched(
            long resourceId,
            int partition,
            ErrorCode code,
            ByteString records,
            long highWatermark,
            long lastStable,
            long logStart,
            List<AbortedTxn> aborted,
            LeaderHint hint) {
        return new Response.Fetch(
                0,
                List.of(new FetchResourceResponse(
                        resourceId,
                        List.of(new FetchPartitionResponse(
                                partition, code, highWatermark, lastStable, logStart, aborted, records, hint)))));
    }

    static Response.FindCoordinator coordinator(FakeNode n) {
        return new Response.FindCoordinator(ErrorCode.NONE, n.id, "127.0.0.1", n.address().port(), null);
    }

    static Response.InitProducer producer(long id, int epoch) {
        return new Response.InitProducer(ErrorCode.NONE, id, epoch, null);
    }

    /** The batch of a produce request's only partition. */
    static Batch batchOf(Request.Produce p) {
        return Batch.parse(p.resources().get(0).partitions().get(0).batch()).batch();
    }

    static ByteString bytesOf(Request.Produce p) {
        return p.resources().get(0).partitions().get(0).batch();
    }
}

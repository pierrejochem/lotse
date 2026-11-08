package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.net.LotseBinary;
import dev.lotse.proto.Messages.PartitionMetadata;
import dev.lotse.proto.Response;
import dev.lotse.record.BatchRecord;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Three real {@code lotse} processes on loopback (plain TCP, no authentication) and a client that creates streams,
 * produces, fetches, and keeps going when the leader of a partition is killed. Skipped when no executable is found;
 * see {@link LotseBinary}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RealClusterTest {
    private static RealCluster cluster;
    private static LotseClient client;

    @BeforeAll
    static void startCluster() throws Exception {
        Path bin = LotseBinary.require();
        cluster = new RealCluster(bin, 3, false);
        cluster.startAll();
        client = new LotseClient(ClientConfig.builder(cluster.bootstrap())
                .plaintextOnLoopback()
                .requestTimeout(Duration.ofSeconds(5))
                .retryFor(Duration.ofSeconds(60))
                .build());
    }

    @AfterAll
    static void stopCluster() {
        if (client != null) {
            client.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private static int leaderOf(String stream, int partition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            Response.Metadata m = client.metadata(stream);
            for (var r : m.resources()) {
                for (PartitionMetadata p : r.partitions()) {
                    if (r.name().equals(stream) && p.partition() == partition && p.leader() != 0) {
                        return (int) p.leader();
                    }
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("no leader for " + stream + "/" + partition + "\n" + cluster.log(1));
    }

    /** Reads a partition from its start until {@code count} records are seen. */
    private static List<BatchRecord> readAll(String stream, int partition, int count) {
        List<BatchRecord> out = new ArrayList<>();
        long offset = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (out.size() < count && System.nanoTime() < deadline) {
            Fetched f = client.fetch(stream, partition, offset, Duration.ofMillis(200));
            out.addAll(f.committedMessages());
            offset = f.nextOffset(offset);
        }
        return out;
    }

    private static void restart(int node) throws Exception {
        cluster.start(node);
        // Serving again: the node answers metadata for itself.
        try (LotseClient probe = new LotseClient(ClientConfig.builder(cluster.address(node).toString())
                .plaintextOnLoopback()
                .requestTimeout(Duration.ofSeconds(5))
                .retryFor(Duration.ofSeconds(60))
                .build())) {
            probe.metadata();
        }
    }

    @Test
    @Order(1)
    void aStreamIsCreatedProducedToAndReadBack() throws Exception {
        long id = client.createStream("orders", 2, 3);
        assertTrue(id != 0);
        ClientException.ResourceExists again =
                assertThrows(ClientException.ResourceExists.class, () -> client.createStream("orders", 2, 3));
        assertEquals(id, again.resourceId(), "creating it again says which one exists");

        long before = System.currentTimeMillis();
        List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ProduceResult r = client.produce("orders", i % 2, OutMessage.value("order-" + i).withKey("k" + i).withHeader("n", "" + i));
            offsets.add(r.baseOffset());
        }
        long after = System.currentTimeMillis();

        for (int partition = 0; partition < 2; partition++) {
            List<BatchRecord> got = readAll("orders", partition, 5);
            assertEquals(5, got.size());
            for (int j = 0; j < 5; j++) {
                int i = 2 * j + partition;
                BatchRecord rec = got.get(j);
                assertEquals("order-" + i, rec.value().toUtf8());
                assertEquals("k" + i, rec.key().toUtf8());
                assertEquals("" + i, rec.headers().get(0).value().toUtf8());
                assertTrue(rec.timestamp() >= before && rec.timestamp() <= after, "stamped with the time of the produce");
                assertEquals(offsets.get(i), rec.offset(), "the offset the produce returned");
            }
        }
    }

    @Test
    @Order(2)
    void aLongPollWaitsAtTheBrokerAndAnOffsetPastTheEndIsOutOfRange() {
        // The log holds control entries as well as records, so the end is found by reading, not by counting.
        long end = 0;
        for (Fetched f = client.fetch("orders", 0, 0, Duration.ofMillis(50));
                !f.records().isEmpty();
                f = client.fetch("orders", 0, end, Duration.ofMillis(50))) {
            end = f.nextOffset(end);
        }
        long start = System.nanoTime();
        Fetched f = client.fetch("orders", 0, end, Duration.ofMillis(300));
        long ms = Duration.ofNanos(System.nanoTime() - start).toMillis();
        // The broker may append an entry of its own meanwhile (a membership change), which ends the wait early:
        // either something came, or the broker held the request for the whole wait.
        assertTrue(!f.records().isEmpty() || ms >= 200, "returned empty after only " + ms + " ms");
        if (f.records().isEmpty()) {
            assertEquals(end, f.nextOffset(end), "nothing read: stays where it was");
        }

        ClientException.OffsetOutOfRange e = assertThrows(
                ClientException.OffsetOutOfRange.class,
                () -> client.fetch("orders", 0, 1_000_000, Duration.ofMillis(10)));
        assertEquals(0, e.logStart());
    }

    @Test
    @Order(3)
    void anUnknownStreamIsReportedAtOnce() {
        long start = System.nanoTime();
        assertThrows(ClientException.UnknownStream.class, () -> client.produce("nope", 0, OutMessage.value("x")));
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(10)) < 0);
    }

    @Test
    @Order(4)
    void killingTheLeaderMidStreamNeitherLosesNorDuplicatesAnIdempotentProduce() throws Exception {
        client.createStream("idem", 1, 3);
        int leader = leaderOf("idem", 0);
        int total = 150;
        AtomicInteger done = new AtomicInteger();
        Thread killer = new Thread(() -> {
            try {
                while (done.get() < 40) {
                    Thread.sleep(2);
                }
                cluster.kill(leader);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        killer.start();
        List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            offsets.add(client.produce("idem", 0, OutMessage.value("m-" + i)).baseOffset());
            done.incrementAndGet();
        }
        killer.join();

        List<BatchRecord> got = readAll("idem", 0, total);
        List<String> values = got.stream().map(r -> r.value().toUtf8()).toList();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            expected.add("m-" + i);
        }
        assertEquals(expected, values, "every message once, in order");
        assertEquals(total, new HashSet<>(offsets).size(), "each produce got its own offset");
        for (int i = 1; i < total; i++) {
            assertTrue(offsets.get(i) > offsets.get(i - 1), "offsets rise");
        }
        assertNotEquals(leader, leaderOf("idem", 0), "another node leads now");
        restart(leader);
    }

    @Test
    @Order(5)
    void aClientFollowsTheNewLeaderAfterAFailover() throws Exception {
        client.createStream("failover", 1, 3);
        client.produce("failover", 0, OutMessage.value("before"));
        int old = leaderOf("failover", 0);
        assertEquals(old, client.routeOf("failover", 0).leader());

        cluster.kill(old);
        client.produce("failover", 0, OutMessage.value("after"));
        assertNotEquals(old, client.routeOf("failover", 0).leader(), "the route moved to the new leader");

        List<String> values = readAll("failover", 0, 2).stream().map(r -> r.value().toUtf8()).toList();
        assertEquals(List.of("before", "after"), values);
        restart(old);
        client.produce("failover", 0, OutMessage.value("again"));
        assertEquals(3, readAll("failover", 0, 3).size());
    }

    @Test
    @Order(6)
    void aClientWithoutIdempotenceWritesTooAndNeverWritesTwice() throws Exception {
        client.createStream("plain", 1, 3);
        try (LotseClient plain = new LotseClient(ClientConfig.builder(cluster.bootstrap())
                .plaintextOnLoopback()
                .idempotent(false)
                .retryFor(Duration.ofSeconds(60))
                .build())) {
            for (int i = 0; i < 5; i++) {
                plain.produce("plain", 0, OutMessage.value("p-" + i));
            }
        }
        List<String> values = readAll("plain", 0, 5).stream().map(r -> r.value().toUtf8()).toList();
        assertEquals(List.of("p-0", "p-1", "p-2", "p-3", "p-4"), values);
    }

    @Test
    @Order(100)
    void aTransactionThatOnlySentOffsetsCommitsThemAtOnce() throws Exception {
        client.createStream("offsets-only", 1, 3);
        for (String v : List.of("a", "b")) {
            client.produce("offsets-only", 0, OutMessage.value(v));
        }
        GroupConfig manual = GroupConfig.builder().autoCommit(false).build();
        try (LotseClient processor = new LotseClient(ClientConfig.builder(cluster.bootstrap())
                .plaintextOnLoopback()
                .transactionalId("tx-offsets-only")
                .requestTimeout(Duration.ofSeconds(5))
                .retryFor(Duration.ofSeconds(60))
                .build())) {
            GroupConsumer first = processor.joinGroup("offsets-only", List.of("offsets-only"), manual);
            int seen = 0;
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (seen < 2 && System.nanoTime() < deadline) {
                seen += first.poll(Duration.ofMillis(200)).size();
            }
            assertEquals(2, seen);
            try (Transaction txn = processor.beginTransaction()) {
                txn.sendOffsets(first); // no produce, no partition added
                txn.commit();
            }
            first.close();
        }
        client.produce("offsets-only", 0, OutMessage.value("c"));
        // Not ended, the offsets would stay pending for the transaction's timeout and the group would read nothing.
        try (GroupConsumer next = client.joinGroup("offsets-only", List.of("offsets-only"), manual)) {
            List<String> got = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (got.isEmpty() && System.nanoTime() < deadline) {
                next.poll(Duration.ofMillis(200)).forEach(r -> got.add(r.record().value().toUtf8()));
            }
            assertEquals(List.of("c"), got, "only what came after the committed offsets");
        }
    }
}

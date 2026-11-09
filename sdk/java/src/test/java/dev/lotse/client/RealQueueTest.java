package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.net.LotseBinary;
import dev.lotse.proto.Messages.PartitionMetadata;
import dev.lotse.proto.Response;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Queues against three real {@code lotse} processes on loopback (plain TCP, no authentication): consuming,
 * settling, redelivery after a nack, an ack timeout and a dropped consumer, dead letters, and a leader that dies
 * mid-consumption. Skipped when no executable is found; see {@link LotseBinary}.
 */
class RealQueueTest {
    private static RealCluster cluster;
    private static LotseClient client;

    @BeforeAll
    static void startCluster() throws Exception {
        Path bin = LotseBinary.require();
        cluster = new RealCluster(bin, 3, false);
        cluster.startAll();
        client = newClient();
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

    private static LotseClient newClient() {
        return new LotseClient(ClientConfig.builder(cluster.bootstrap())
                .plaintextOnLoopback()
                .requestTimeout(Duration.ofSeconds(5))
                .retryFor(Duration.ofSeconds(60))
                .build());
    }

    private static int leaderOf(String queue) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            Response.Metadata m = client.metadata(queue);
            for (var r : m.resources()) {
                for (PartitionMetadata p : r.partitions()) {
                    if (r.name().equals(queue) && p.partition() == 0 && p.leader() != 0) {
                        return (int) p.leader();
                    }
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("no leader for " + queue + "\n" + cluster.log(1));
    }

    /**
     * Waits until the queue's leader has stayed the same for half a second. A queue's group elects its leader in
     * its first moments, and a leader that changes later restarts the delivery counts (they are a lower bound after
     * a failover), which these tests do not want to measure.
     */
    private static void settled(String queue) throws InterruptedException {
        int leader = leaderOf(queue);
        int same = 0;
        while (same < 5) {
            Thread.sleep(100);
            int now = leaderOf(queue);
            same = now == leader ? same + 1 : 0;
            leader = now;
        }
    }

    private static void restart(int node) throws Exception {
        cluster.start(node);
        try (LotseClient probe = new LotseClient(ClientConfig.builder(cluster.address(node).toString())
                .plaintextOnLoopback()
                .requestTimeout(Duration.ofSeconds(5))
                .retryFor(Duration.ofSeconds(60))
                .build())) {
            probe.metadata();
        }
    }

    private static void produce(String queue, int count) {
        for (int i = 0; i < count; i++) {
            client.produce(queue, 0, OutMessage.value(queue + "-" + i).withKey("k" + i).withHeader("n", "" + i));
        }
    }

    private static Delivery take(QueueConsumer c) {
        Delivery d = c.poll(Duration.ofSeconds(30));
        assertNotNull(d, "a message should have come");
        return d;
    }

    @Test
    void everyMessageIsDeliveredOnceAckedAndTheQueueDrains() {
        client.createQueue("drain");
        produce("drain", 20);
        Set<String> seen = new HashSet<>();
        try (QueueConsumer c = client.subscribe("drain", SubscribeOptions.withCredit(3))) {
            for (int i = 0; i < 20; i++) {
                Delivery d = take(c);
                assertEquals(1, d.deliveryCount());
                assertTrue(seen.add(d.value().toUtf8()), "no message twice");
                assertEquals("k" + d.value().toUtf8().substring("drain-".length()), d.key().toUtf8());
                assertEquals(1, d.headers().size());
                c.ack(d);
            }
            assertNull(c.poll(Duration.ofMillis(500)), "nothing more");
        }
        assertEquals(20, seen.size());
        try (QueueConsumer again = client.subscribe("drain")) {
            assertNull(again.poll(Duration.ofSeconds(1)), "acked messages do not come back");
        }
    }

    @Test
    void aRequeuedMessageComesBackWithACountOfTwo() throws Exception {
        client.createQueue("requeue");
        produce("requeue", 1);
        settled("requeue");
        try (QueueConsumer c = client.subscribe("requeue", SubscribeOptions.withCredit(2))) {
            Delivery first = take(c);
            assertEquals(1, first.deliveryCount());
            c.nack(first, true);
            Delivery second = take(c);
            assertEquals(first.offset(), second.offset());
            assertEquals(2, second.deliveryCount());
            c.ack(second);
            assertNull(c.poll(Duration.ofMillis(500)));
        }
    }

    @Test
    void aMessageNotAckedInTimeIsDeliveredAgain() throws Exception {
        client.createQueue("timeout", QueueOptions.builder().ackTimeout(Duration.ofMillis(700)).build());
        produce("timeout", 1);
        settled("timeout");
        try (QueueConsumer c = client.subscribe("timeout")) {
            Delivery first = take(c);
            // Held without an ack: the broker ends the subscription and the consumer subscribes again by itself.
            // If the queue's leader happens to change meanwhile, the message comes back at once with a count that
            // started over (counts are a lower bound after a failover, ADR 0031); it is held again until the ack
            // timeout of the leader that stayed brings it back with a count that rose.
            Delivery again = take(c);
            for (int tries = 0; again.deliveryCount() < 2 && tries < 4; tries++) {
                assertEquals(first.offset(), again.offset());
                again = take(c);
            }
            assertEquals(first.offset(), again.offset());
            assertTrue(again.deliveryCount() >= 2, "was " + again.deliveryCount());
            c.ack(again);
        }
    }

    @Test
    void aConsumerThatDisappearsLeavesItsMessagesToTheNext() throws Exception {
        client.createQueue("vanish");
        produce("vanish", 3);
        settled("vanish");
        try (LotseClient other = newClient()) {
            QueueConsumer gone = other.subscribe("vanish", SubscribeOptions.withCredit(10));
            for (int i = 0; i < 3; i++) {
                take(gone); // delivered to it, never acked
            }
        } // closing the client closes the connection, and the messages go back
        List<Long> offsets = new ArrayList<>();
        try (QueueConsumer c = client.subscribe("vanish")) {
            for (int i = 0; i < 3; i++) {
                Delivery d = take(c);
                assertEquals(2, d.deliveryCount());
                offsets.add(d.offset());
                c.ack(d);
            }
        }
        assertEquals(3, new HashSet<>(offsets).size());
    }

    @Test
    void closingGivesBackWhatWasPushedButNotHandedOut() throws Exception {
        // A new queue leader redelivers everything that is not acked and starts the counts again (ADR 0031), so
        // "the one handed out stays with the first consumer" holds only while the leader stays the same. A run in
        // which it changed says nothing about close and is repeated on a fresh queue; a failure with the same leader
        // before and after is a real one and reports both.
        for (int attempt = 0; ; attempt++) {
            String queue = "give-back-" + attempt;
            client.createQueue(queue);
            produce(queue, 3);
            settled(queue);
            int leaderBefore = leaderOf(queue);
            long kept;
            QueueConsumer first = client.subscribe(queue, SubscribeOptions.withCredit(10));
            kept = take(first).offset(); // the other two are pushed, some of them perhaps still unread
            Thread.sleep(300);
            first.close();
            Set<Long> offsets = new HashSet<>();
            try (QueueConsumer next = client.subscribe(queue, SubscribeOptions.withCredit(10))) {
                for (int i = 0; i < 2; i++) {
                    Delivery d = next.poll(Duration.ofSeconds(10));
                    assertNotNull(d, "given back at once, not after the ack timeout");
                    offsets.add(d.offset());
                    next.ack(d);
                }
            }
            assertEquals(2, offsets.size());
            int leaderAfter = leaderOf(queue);
            if (leaderBefore != leaderAfter && attempt < 2) {
                continue;
            }
            assertTrue(
                    !offsets.contains(kept),
                    "the one handed out stays with the first consumer: kept " + kept + ", got " + offsets
                            + ", leader " + leaderBefore + " before and " + leaderAfter + " after");
            return;
        }
    }

    @Test
    void killingTheLeaderMidConsumptionLosesNothing() throws Exception {
        client.createQueue("failover");
        int total = 60;
        produce("failover", total);
        int leader = leaderOf("failover");
        Set<String> acked = new HashSet<>();
        Map<String, Integer> duplicates = new HashMap<>();
        boolean killed = false;
        try (QueueConsumer c = client.subscribe("failover", SubscribeOptions.withCredit(5))) {
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            while (acked.size() < total) {
                assertTrue(System.nanoTime() < deadline, "only " + acked.size() + " of " + total + " acked");
                Delivery d = c.poll(Duration.ofSeconds(10));
                if (d == null) {
                    continue;
                }
                String v = d.value().toUtf8();
                if (acked.contains(v)) {
                    // Redelivered before its ack settled: allowed, and then the count says so.
                    assertTrue(d.deliveryCount() > 1, v + " came twice with a delivery count of " + d.deliveryCount());
                    duplicates.merge(v, 1, Integer::sum);
                }
                c.ack(d);
                acked.add(v);
                if (!killed && acked.size() >= 20) {
                    cluster.kill(leader);
                    killed = true;
                }
            }
        }
        assertTrue(killed, "the leader was killed while messages were left");
        for (int i = 0; i < total; i++) {
            assertTrue(acked.contains("failover-" + i), "lost: failover-" + i);
        }
        assertNotEquals(leader, leaderOf("failover"), "another node leads now");
        restart(leader);
    }

    @Test
    void aMessageDeliveredTooOftenLandsInTheDeadLetterQueueWithItsOrigin() throws Exception {
        client.createQueue("dead-letters");
        long jobsId = client.createQueue(
                "capped", QueueOptions.builder().maxDeliveries(2).deadLetterQueue("dead-letters").build());
        long offset = client.produce("capped", 0, OutMessage.value("poison").withKey("k")).baseOffset();
        settled("capped");
        try (QueueConsumer c = client.subscribe("capped")) {
            Delivery d1 = take(c);
            assertEquals(1, d1.deliveryCount());
            assertTrue(d1.deadLetter().isEmpty());
            c.nack(d1, true);
            Delivery d2 = take(c);
            assertEquals(2, d2.deliveryCount());
            c.nack(d2, true);
        }
        try (QueueConsumer dl = client.subscribe("dead-letters")) {
            Delivery d = take(dl);
            assertEquals("poison", d.value().toUtf8());
            assertEquals("k", d.key().toUtf8());
            DeadLetter origin = d.deadLetter().orElseThrow();
            assertEquals("max-deliveries", origin.reason());
            assertEquals("capped", origin.queue());
            assertEquals(jobsId, origin.queueId());
            assertEquals(offset, origin.offset());
            assertEquals(2, origin.deliveries());
            dl.ack(d);
        }
        try (QueueConsumer c = client.subscribe("capped")) {
            assertNull(c.poll(Duration.ofSeconds(1)), "retired, not delivered a third time");
        }
    }

    @Test
    void aMessageThatWaitsTooLongExpiresIntoTheDeadLetterQueue() {
        client.createQueue("expired-letters");
        client.createQueue(
                "short-lived",
                QueueOptions.builder().messageTtl(Duration.ofMillis(600)).deadLetterQueue("expired-letters").build());
        client.produce("short-lived", 0, OutMessage.value("stale"));
        try (QueueConsumer dl = client.subscribe("expired-letters")) {
            Delivery d = take(dl);
            assertEquals("stale", d.value().toUtf8());
            assertEquals("expired", d.deadLetter().orElseThrow().reason());
            assertEquals("short-lived", d.deadLetter().orElseThrow().queue());
            dl.ack(d);
        }
    }
}

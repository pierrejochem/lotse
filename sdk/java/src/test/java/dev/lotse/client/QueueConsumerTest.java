package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.client.FakeNode.Conn;
import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Frames;
import dev.lotse.proto.LeaderHint;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.DeliveredOffset;
import dev.lotse.proto.Messages.Range;
import dev.lotse.proto.Messages.ResourceMetadata;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.ByteString;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A queue consumer against scripted brokers: what it sends, and what it does with what it is sent. */
class QueueConsumerTest {
    private static final long QUEUE_ID = 9;
    private static final Duration SHORT = Duration.ofMillis(300);

    private FakeNode a;
    private FakeNode b;
    private LotseClient client;
    private final AtomicLong nextSub = new AtomicLong(100);
    private volatile FakeNode leader;
    private volatile long term = 1;

    @BeforeEach
    void start() throws IOException {
        a = new FakeNode(1);
        b = new FakeNode(2);
        leader = a;
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        a.close();
        b.close();
    }

    // ---- scripts ----

    /** What a node does with a request, after the common answers. */
    interface Script {
        Response handle(FakeNode node, Conn conn, Request request) throws Exception;
    }

    private ResourceMetadata queueMetadata() {
        return new ResourceMetadata(
                ErrorCode.NONE,
                "jobs",
                "default",
                QUEUE_ID,
                Messages.ResourceKind.QUEUE,
                List.of(Fx.partition(0, leader.id, term, 1L, 2L)),
                null);
    }

    private void script(FakeNode node, Script script) {
        node.onConn((n, conn, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(List.of(a, b), queueMetadata());
            }
            return script.handle(n, conn, request);
        });
    }

    /** The answers of a healthy queue leader that does not push anything of its own. */
    private Response plain(FakeNode n, Conn conn, Request r) {
        if (r instanceof Request.Subscribe) {
            return new Response.Subscribe(ErrorCode.NONE, nextSub.incrementAndGet(), null);
        }
        if (r instanceof Request.Credit) {
            return null;
        }
        return new Response.Outcome(ErrorCode.NONE, null);
    }

    private void connect(FakeNode... nodes) {
        client = new LotseClient(Fx.config(nodes)
                .requestTimeout(Duration.ofSeconds(2))
                .retryFor(Duration.ofSeconds(3))
                .build());
    }

    private static Frames.DeliverPush push(long sub, long base, int count, long... delivered) {
        String[] values = new String[count];
        for (int i = 0; i < count; i++) {
            values[i] = "m-" + (base + i);
        }
        ByteString batch = FetchTest.data(base, 0, false, values);
        List<DeliveredOffset> d = new ArrayList<>();
        for (long o : delivered) {
            d.add(new DeliveredOffset(o, 1));
        }
        return new Frames.DeliverPush(sub, ErrorCode.NONE, batch, d, null);
    }

    private static Frames.DeliverPush pushAll(long sub, long base, int count) {
        long[] offsets = new long[count];
        for (int i = 0; i < count; i++) {
            offsets[i] = base + i;
        }
        return push(sub, base, count, offsets);
    }

    private static Frames.DeliverPush failure(long sub, ErrorCode code, LeaderHint hint) {
        return new Frames.DeliverPush(sub, code, ByteString.EMPTY, List.of(), hint);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting");
            }
            Thread.sleep(5);
        }
    }

    private static Delivery take(QueueConsumer c) {
        Delivery d = c.poll(Duration.ofSeconds(3));
        assertNotNull(d, "a message should have come");
        return d;
    }

    // ---- subscribing ----

    @Test
    void subscribeSendsTheQueueAndTheCredit() {
        script(a, this::plain);
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(8))) {
            assertNotNull(c);
            Request.Subscribe s = a.requests(Request.Subscribe.class).get(0);
            assertEquals(QUEUE_ID, s.queueId());
            assertEquals("", s.consumerTag());
            assertEquals(8, s.creditMsgs());
            assertEquals(0xFFFFFFFFL, s.creditBytes());
            assertEquals(false, s.exclusive());
        }
    }

    @Test
    void aCreditBelowOneIsOne() {
        assertEquals(1, SubscribeOptions.withCredit(0).credit());
        assertEquals(1, SubscribeOptions.withCredit(-5).credit());
    }

    @Test
    void anUnknownQueueIsReported() {
        script(a, this::plain);
        a.onConn((n, conn, r) -> r instanceof Request.Metadata ? Fx.metadata(List.of(a), Fx.unknown("jobs")) : null);
        connect(a);
        assertThrows(ClientException.UnknownStream.class, () -> client.subscribe("jobs"));
    }

    @Test
    void aRefusedSubscriptionFailsAtOnce() {
        script(a, (n, conn, r) -> new Response.Subscribe(ErrorCode.INVALID_REQUEST, 0, null));
        connect(a);
        ClientException.Broker e = assertThrows(ClientException.Broker.class, () -> client.subscribe("jobs"));
        assertEquals(ErrorCode.INVALID_REQUEST, e.code());
        assertEquals(1, a.count(Request.Subscribe.class), "not tried again");
    }

    @Test
    void aSubscriptionToANodeThatIsNotTheLeaderFollowsTheHint() {
        script(a, (n, conn, r) -> new Response.Subscribe(ErrorCode.NOT_LEADER, 0, new LeaderHint(2, 2)));
        script(b, this::plain);
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            assertNotNull(c);
            assertEquals(1, b.count(Request.Subscribe.class));
            assertEquals(1, a.count(Request.Subscribe.class));
        }
    }

    // ---- receiving ----

    @Test
    void pushesThatArriveBeforeTheAnswerToSubscribeAreNotLost() {
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                long id = nextSub.incrementAndGet();
                // Sent before the answer, so the client has no subscription to give them to yet.
                conn.push(pushAll(id, 10, 2));
                conn.push(pushAll(id, 12, 1));
                return new Response.Subscribe(ErrorCode.NONE, id, null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            assertEquals(10, take(c).offset());
            assertEquals(11, take(c).offset());
            assertEquals(12, take(c).offset());
            assertNull(c.poll(SHORT));
        }
    }

    @Test
    void onlyTheOffsetsInTheDeliveriesListAreHandedOut() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(10))) {
            where.get().push(push(sub.get(), 10, 4, 11, 13));
            Delivery first = take(c);
            Delivery second = take(c);
            assertEquals(11, first.offset());
            assertEquals(13, second.offset());
            assertEquals("m-11", first.value().toUtf8());
            assertEquals("m-13", second.value().toUtf8());
            assertNull(c.poll(SHORT), "the records 10 and 12 are somebody else's");
        }
    }

    @Test
    void theDeliveryCountComesFromTheDeliveriesList() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            ByteString batch = FetchTest.data(5, 0, false, "x");
            where.get().push(new Frames.DeliverPush(
                    sub.get(), ErrorCode.NONE, batch, List.of(new DeliveredOffset(5, 4)), null));
            Delivery d = take(c);
            assertEquals(4, d.deliveryCount());
            assertEquals(1_000, d.timestamp());
        }
    }

    @Test
    void pollReturnsNullWhenNothingComes() {
        script(a, this::plain);
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            long start = System.nanoTime();
            assertNull(c.poll(Duration.ofMillis(200)));
            assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() >= 150);
            assertEquals(1, a.count(Request.Subscribe.class), "waiting does not subscribe again");
        }
    }

    // ---- credit ----

    /** Credit {@code credit}: pushes {@code credit} messages in one batch and hands them out. */
    private List<Request.Credit> creditsAfter(int credit, int taken, long[] sizeOut) throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(credit));
        Frames.DeliverPush p = pushAll(sub.get(), 10, credit);
        sizeOut[0] = p.batch().size();
        where.get().push(p);
        for (int i = 0; i < taken; i++) {
            take(c);
        }
        Thread.sleep(150); // credit is fire-and-forget: give it time to arrive
        List<Request.Credit> out = a.requests(Request.Credit.class);
        sizeOut[1] = sub.get();
        c.close();
        return out;
    }

    @Test
    void creditIsAddedOnceHalfOfItIsUsed() throws Exception {
        long[] info = new long[2];
        // credit 5: half rounded up is 3
        assertEquals(0, creditsAfter(5, 2, info).size(), "2 of 5 used: not yet");
    }

    @Test
    void creditCarriesTheMessagesAndBytesUsed() throws Exception {
        long[] info = new long[2];
        List<Request.Credit> credits = creditsAfter(5, 3, info);
        assertEquals(1, credits.size(), "3 of 5 used: now");
        assertEquals(info[1], credits.get(0).subscriptionId());
        assertEquals(3, credits.get(0).addMsgs());
        assertEquals(info[0], credits.get(0).addBytes(), "the bytes of the batch that was received");
    }

    @Test
    void anEvenCreditRefillsAtExactlyHalf() throws Exception {
        long[] info = new long[2];
        assertEquals(0, creditsAfter(4, 1, info).size());
    }

    @Test
    void anEvenCreditRefillsAtHalf() throws Exception {
        long[] info = new long[2];
        List<Request.Credit> credits = creditsAfter(4, 2, info);
        assertEquals(1, credits.size());
        assertEquals(2, credits.get(0).addMsgs());
    }

    @Test
    void aCreditOfOneRefillsAfterEveryMessage() throws Exception {
        long[] info = new long[2];
        List<Request.Credit> credits = creditsAfter(1, 1, info);
        assertEquals(1, credits.size());
        assertEquals(1, credits.get(0).addMsgs());
        assertEquals(info[0], credits.get(0).addBytes());
    }

    @Test
    void usedCountsStartOverAfterARefill() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(4))) {
            Frames.DeliverPush p1 = pushAll(sub.get(), 10, 2);
            Frames.DeliverPush p2 = pushAll(sub.get(), 12, 2);
            where.get().push(p1);
            take(c);
            take(c);
            where.get().push(p2);
            take(c);
            take(c);
            await(() -> a.count(Request.Credit.class) >= 2);
            Thread.sleep(100);
            assertEquals(2, a.count(Request.Credit.class), "two refills for four messages");
            List<Request.Credit> credits = a.requests(Request.Credit.class);
            assertEquals(2, credits.get(0).addMsgs());
            assertEquals(p1.batch().size(), credits.get(0).addBytes());
            assertEquals(2, credits.get(1).addMsgs());
            assertEquals(p2.batch().size(), credits.get(1).addBytes(), "bytes of the first batch are not counted twice");
        }
    }

    @Test
    void creditGoesOnTheSubscriptionsConnection() throws Exception {
        long[] info = new long[2];
        creditsAfter(2, 1, info);
        assertEquals(1, a.requests(Request.Credit.class).size());
        Conn first = a.conns().get(0);
        assertEquals(1, first.requests(Request.Credit.class).size());
    }

    // ---- settling ----

    @Test
    void anAckNamesTheOffsetAsAHalfOpenRange() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(10))) {
            where.get().push(pushAll(sub.get(), 10, 3));
            Delivery d10 = take(c);
            take(c);
            Delivery d12 = take(c);
            c.ack(d10);
            Request.Ack ack = a.requests(Request.Ack.class).get(0);
            assertEquals(QUEUE_ID, ack.queueId());
            assertEquals(List.of(new Range(10, 11)), ack.ranges());
            assertNull(ack.txnId());

            c.ack(List.of(d12, d10, d12));
            assertEquals(List.of(new Range(10, 11), new Range(12, 13)), a.requests(Request.Ack.class).get(1).ranges());
        }
    }

    @Test
    void ackedNeighboursAreOneRange() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(10))) {
            where.get().push(pushAll(sub.get(), 10, 3));
            List<Delivery> all = List.of(take(c), take(c), take(c));
            c.ack(all);
            assertEquals(List.of(new Range(10, 13)), a.requests(Request.Ack.class).get(0).ranges());
        }
    }

    @Test
    void aStandbyIsToldWhenItsTurnComes() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null, true);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            assertTrue(c.isStandby());
            assertTrue(a.requests(Request.Subscribe.class).get(0).notifyActive(), "asks to be told");
            where.get().push(new Frames.DeliverPush(sub.get(), ErrorCode.NONE, ByteString.EMPTY, List.of(), null));
            assertNull(c.poll(SHORT), "the activation is no message");
            assertFalse(c.isStandby());
            where.get().push(pushAll(sub.get(), 10, 1));
            assertEquals(10, take(c).offset());
        }
    }

    @Test
    void aStandbyThatGetsAMessageIsActive() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null, true);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            where.get().push(pushAll(sub.get(), 10, 1));
            take(c);
            assertFalse(c.isStandby());
        }
    }

    @Test
    void aDelayedRequeueCarriesItsDelayOnTheSubscriptionsConnection() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            assertFalse(c.isStandby());
            where.get().push(pushAll(sub.get(), 10, 2));
            c.nackAfter(take(c), Duration.ofMillis(1500));
            Request.Nack nack = where.get().requests(Request.Nack.class).get(0);
            assertEquals(1500, nack.delayMs());
            assertEquals(true, nack.requeue());
            assertEquals(sub.get(), nack.subscriptionId(), "names its subscription (ADR 0106)");
            assertEquals(List.of(new Range(10, 11)), nack.ranges());
            c.nack(take(c), true);
            assertEquals(0, where.get().requests(Request.Nack.class).get(1).delayMs(), "a plain requeue waits not");
        }
    }

    @Test
    void anAckThatIsRefusedAsNotLeaderIsRetriedAtTheNewLeader() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            if (r instanceof Request.Ack) {
                return new Response.Outcome(ErrorCode.NOT_LEADER, new LeaderHint(2, 2));
            }
            return plain(n, conn, r);
        });
        script(b, this::plain);
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            where.get().push(pushAll(sub.get(), 10, 1));
            Delivery d = take(c);
            c.ack(d);
            assertEquals(1, a.count(Request.Ack.class));
            assertEquals(List.of(new Range(10, 11)), b.requests(Request.Ack.class).get(0).ranges());
        }
    }

    @Test
    void anAckWhoseAnswerIsLostIsSentAgain() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        AtomicBoolean dropped = new AtomicBoolean();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            if (r instanceof Request.Ack && !dropped.getAndSet(true)) {
                throw new FakeNode.Drop();
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            where.get().push(pushAll(sub.get(), 10, 1));
            Delivery d = take(c);
            c.ack(d);
            assertEquals(2, a.count(Request.Ack.class), "idempotent: sent again");
        }
    }

    @Test
    void aRefusedAckIsAnError() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            if (r instanceof Request.Ack) {
                return new Response.Outcome(ErrorCode.INVALID_REQUEST, null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            where.get().push(pushAll(sub.get(), 10, 1));
            Delivery d = take(c);
            ClientException.Broker e = assertThrows(ClientException.Broker.class, () -> c.ack(d));
            assertEquals(ErrorCode.INVALID_REQUEST, e.code());
        }
    }

    @Test
    void nackWithoutRequeueDiscardsAtTheLeader() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            where.get().push(pushAll(sub.get(), 10, 1));
            c.nack(take(c), false);
            Request.Nack nack = a.requests(Request.Nack.class).get(0);
            assertEquals(false, nack.requeue());
            assertEquals(0, nack.delayMs());
            assertEquals(-1L, nack.subscriptionId(), "a discard takes nothing back: no subscription named");
            assertEquals(QUEUE_ID, nack.queueId());
            assertEquals(List.of(new Range(10, 11)), nack.ranges());
        }
    }

    @Test
    void nackWithRequeueGoesOnTheSubscriptionsConnection() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            where.get().push(pushAll(sub.get(), 10, 2));
            Delivery d = take(c);
            c.nack(d, true);
            assertEquals(1, where.get().requests(Request.Nack.class).size(), "on the connection of the subscription");
            Request.Nack nack = where.get().requests(Request.Nack.class).get(0);
            assertEquals(true, nack.requeue());
            assertEquals(sub.get(), nack.subscriptionId(), "names its subscription (ADR 0106)");
            assertEquals(0, nack.delayMs());
            assertEquals(List.of(new Range(10, 11)), nack.ranges());
        }
    }

    @Test
    void aMessageOfAnEndedSubscriptionIsNotRequeuedOnTheNewOne() throws Exception {
        List<Conn> conns = new ArrayList<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                conns.add(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            conns.get(0).push(pushAll(sub.get(), 10, 1));
            Delivery old = take(c);
            conns.get(0).push(failure(sub.get(), ErrorCode.ACK_TIMED_OUT, null));
            assertNull(c.poll(SHORT)); // subscribes again
            assertEquals(2, a.count(Request.Subscribe.class));
            c.nack(old, true);
            assertEquals(0, a.count(Request.Nack.class), "the broker took it back when the subscription ended");
            // An ack still settles it: it names an offset.
            c.ack(old);
            assertEquals(1, a.count(Request.Ack.class));
        }
    }

    // ---- ending and subscribing again ----

    @Test
    void aPushWithNotLeaderMovesTheSubscriptionToTheHintedLeader() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        AtomicLong subB = new AtomicLong();
        script(b, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                long id = nextSub.incrementAndGet();
                subB.set(id);
                conn.push(pushAll(id, 30, 1));
                return new Response.Subscribe(ErrorCode.NONE, id, null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            where.get().push(failure(sub.get(), ErrorCode.NOT_LEADER, new LeaderHint(2, 2)));
            Delivery d = take(c);
            assertEquals(30, d.offset());
            assertEquals(1, b.count(Request.Subscribe.class));
            assertEquals(1, a.count(Request.Subscribe.class));
            assertEquals(0, a.count(Request.Unsubscribe.class), "the push ended it: nothing to unsubscribe");
        }
    }

    /** ADR 0109: the consumer names the id its first answer gave when it subscribes again, and keeps its messages. */
    @Test
    void aConsumerResumesWithItsIdAndKeepsWhatItReceivedAcrossAFailover() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                conn.push(pushAll(sub.get(), 10, 2));
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null, false, 77);
            }
            return plain(n, conn, r);
        });
        AtomicLong subB = new AtomicLong();
        script(b, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                subB.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, subB.get(), null, false, 77);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(2))) {
            Delivery first = take(c);
            assertEquals(10, first.offset());
            where.get().push(failure(sub.get(), ErrorCode.NOT_LEADER, new LeaderHint(2, 2)));
            Delivery second = take(c);
            assertEquals(11, second.offset(), "kept: the new subscription holds it still");
            assertNull(c.poll(SHORT), "subscribed again, nothing new");
            List<Request.Subscribe> again = b.requests(Request.Subscribe.class);
            assertEquals(1, again.size());
            assertEquals(77, again.get(0).resume(), "resumes as the consumer it was");
            assertEquals(0, a.requests(Request.Subscribe.class).get(0).resume(), "a new consumer first");
            // A requeue of a message from before goes on the new subscription, which took it back.
            c.nack(first, true);
            List<Request.Nack> nacks = b.requests(Request.Nack.class);
            assertEquals(1, nacks.size());
            assertEquals(subB.get(), nacks.get(0).subscriptionId());
            assertEquals(List.of(new Range(10, 11)), nacks.get(0).ranges());
        }
    }

    @Test
    void aPushWithUnknownResourceForgetsTheRoute() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            int before = a.count(Request.Metadata.class);
            where.get().push(failure(sub.get(), ErrorCode.UNKNOWN_RESOURCE, null));
            assertNull(c.poll(SHORT));
            assertTrue(a.count(Request.Metadata.class) > before, "the route was looked up again");
            assertEquals(2, a.count(Request.Subscribe.class));
        }
    }

    @Test
    void anAckTimeoutEndsTheSubscriptionAndTheNextCallSubscribesAgain() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                long id = nextSub.incrementAndGet();
                sub.set(id);
                if (nextSub.get() > 101) {
                    conn.push(pushAll(id, 50, 1));
                }
                return new Response.Subscribe(ErrorCode.NONE, id, null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            where.get().push(failure(sub.get(), ErrorCode.ACK_TIMED_OUT, null));
            Delivery d = take(c);
            assertEquals(50, d.offset(), "the message of the new subscription");
            assertEquals(2, a.count(Request.Subscribe.class));
            assertEquals(1, a.conns().size(), "the connection is still good: the same one is used");
        }
    }

    @Test
    void aBrokenConnectionEndsTheSubscriptionAndTheNextCallSubscribesAgain() throws Exception {
        List<Conn> conns = new ArrayList<>();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                conns.add(conn);
                long id = nextSub.incrementAndGet();
                if (conns.size() > 1) {
                    conn.push(pushAll(id, 50, 1));
                }
                return new Response.Subscribe(ErrorCode.NONE, id, null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs")) {
            conns.get(0).drop();
            Delivery d = take(c);
            assertEquals(50, d.offset());
            assertEquals(2, a.count(Request.Subscribe.class));
            assertEquals(2, a.conns().size(), "on a new connection");
        }
    }

    @Test
    void messagesAlreadyReceivedAreStillHandedOutWhenTheSubscriptionEnds() throws Exception {
        // The consumer reads its end only after it has handed out everything it holds; the broker gives those
        // messages to others (the delivery count then rises), so they may come twice but not zero times.
        List<Conn> conns = new ArrayList<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                conns.add(conn);
                long id = nextSub.incrementAndGet();
                sub.set(id);
                return new Response.Subscribe(ErrorCode.NONE, id, null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        try (QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(10))) {
            conns.get(0).push(pushAll(sub.get(), 10, 3));
            assertEquals(10, take(c).offset());
            conns.get(0).drop();
            Thread.sleep(100);
            assertEquals(11, take(c).offset());
            assertEquals(12, take(c).offset());
            assertEquals(1, a.count(Request.Subscribe.class), "not subscribed again while messages are held");
            assertNull(c.poll(SHORT));
            assertEquals(2, a.count(Request.Subscribe.class));
        }
    }

    // ---- closing ----

    @Test
    void closeUnsubscribesAndGivesBackWhatWasNotHandedOut() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(10));
        where.get().push(pushAll(sub.get(), 10, 4));
        take(c); // offset 10 is handed out and stays with the consumer
        c.close();
        await(() -> a.count(Request.Unsubscribe.class) == 1);
        Thread.sleep(150);
        assertEquals(1, where.get().requests(Request.Nack.class).size(), "the three not handed out go back");
        List<Request> sent = where.get().requests();
        Request.Unsubscribe un = where.get().requests(Request.Unsubscribe.class).get(0);
        assertEquals(sub.get(), un.subscriptionId());
        Request.Nack nack = where.get().requests(Request.Nack.class).get(0);
        assertEquals(true, nack.requeue());
        assertEquals(QUEUE_ID, nack.queueId());
        assertEquals(sub.get(), nack.subscriptionId(), "names the subscription it ends (ADR 0106)");
        assertEquals(List.of(new Range(11, 14)), nack.ranges(), "the three not handed out, as one range");
        assertTrue(sent.indexOf(un) < sent.indexOf(nack), "unsubscribe first");
        c.close(); // twice is harmless
        assertEquals(1, a.count(Request.Unsubscribe.class));
    }

    @Test
    void closeGivesBackWhatWasPushedButNotRead() throws Exception {
        AtomicReference<Conn> where = new AtomicReference<>();
        AtomicLong sub = new AtomicLong();
        script(a, (n, conn, r) -> {
            if (r instanceof Request.Subscribe) {
                where.set(conn);
                sub.set(nextSub.incrementAndGet());
                return new Response.Subscribe(ErrorCode.NONE, sub.get(), null);
            }
            return plain(n, conn, r);
        });
        connect(a);
        QueueConsumer c = client.subscribe("jobs", SubscribeOptions.withCredit(10));
        where.get().push(pushAll(sub.get(), 10, 2));
        assertEquals(10, take(c).offset()); // 11 is buffered; the push below is not read at all
        where.get().push(pushAll(sub.get(), 20, 2));
        Thread.sleep(150);
        c.close();
        await(() -> a.count(Request.Unsubscribe.class) == 1);
        Thread.sleep(150);
        List<Request.Nack> nacks = where.get().requests(Request.Nack.class);
        assertEquals(1, nacks.size());
        assertEquals(true, nacks.get(0).requeue());
        assertEquals(
                List.of(new Range(11, 12), new Range(20, 22)),
                nacks.get(0).ranges(),
                "the buffered message and both unread ones go back");
    }

    @Test
    void closeWithNothingBufferedOnlyUnsubscribes() throws Exception {
        script(a, this::plain);
        connect(a);
        QueueConsumer c = client.subscribe("jobs");
        c.close();
        await(() -> a.count(Request.Unsubscribe.class) == 1);
        Thread.sleep(100);
        assertEquals(0, a.count(Request.Nack.class));
    }

    @Test
    void aClosedConsumerRefusesFurtherReads() {
        script(a, this::plain);
        connect(a);
        QueueConsumer c = client.subscribe("jobs");
        c.close();
        assertThrows(IllegalStateException.class, () -> c.poll(SHORT));
        assertThrows(IllegalStateException.class, c::next);
    }

    @Test
    void closeWakesAThreadWaitingForAMessage() throws Exception {
        script(a, this::plain);
        connect(a);
        QueueConsumer c = client.subscribe("jobs");
        AtomicReference<Throwable> got = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                c.poll(Duration.ofSeconds(30));
            } catch (Throwable e) {
                got.set(e);
            }
        });
        t.start();
        Thread.sleep(150);
        long start = System.nanoTime();
        c.close();
        t.join(5_000);
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toSeconds() < 5);
        assertTrue(got.get() instanceof IllegalStateException, "was " + got.get());
    }
}

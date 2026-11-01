package dev.lotse.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Frames;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.proto.Writer;
import dev.lotse.record.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ConnectionTest {
    private static ConnectionOptions plain() {
        return ConnectionOptions.builder().plaintextOnLoopback().requestTimeout(Duration.ofSeconds(5)).build();
    }

    /** A request whose answer tells which one it answers. */
    private static Request.Unsubscribe ask(long n) {
        return new Request.Unsubscribe(n);
    }

    private static Response answer(long n) {
        return new Response.Outcome(new ErrorCode((int) n), null);
    }

    private static String text(Response r) {
        return "answer " + ((Response.Outcome) r).error().code();
    }

    private static Throwable failure(CompletableFuture<?> f) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
        return e.getCause();
    }

    private static Frames.DeliverPush push(long subscription, long offset) {
        return new Frames.DeliverPush(
                subscription, ErrorCode.NONE, ByteString.EMPTY, List.of(new Messages.DeliveredOffset(offset, 1)), null);
    }

    /** Collects what a subscription is told. */
    private static final class Inbox implements PushListener {
        final BlockingQueue<Frames.DeliverPush> pushes = new LinkedBlockingQueue<>();
        final BlockingQueue<ConnectionException> closed = new LinkedBlockingQueue<>();

        @Override
        public void onPush(Frames.DeliverPush push) {
            pushes.add(push);
        }

        @Override
        public void onClose(ConnectionException reason) {
            closed.add(reason);
        }

        long nextOffset() throws InterruptedException {
            Frames.DeliverPush p = pushes.poll(5, TimeUnit.SECONDS);
            assertTrue(p != null, "a push arrives");
            return p.deliveries().get(0).offset();
        }
    }

    @Test
    void requestsArePipelinedAndAnsweredInAnyOrder() throws Exception {
        List<Frames.DecodedRequest> held = new CopyOnWriteArrayList<>();
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    held.add(req);
                    if (held.size() == 3) {
                        // All three are on the wire before any answer: pipelined. Answer the last first.
                        for (int i = 2; i >= 0; i--) {
                            Frames.DecodedRequest r = held.get(i);
                            peer.reply(r.header().correlationId(), answer(((Request.Unsubscribe) r.request()).subscriptionId()));
                        }
                    }
                });
                Connection c = Connection.open(broker.address(), plain())) {
            CompletableFuture<Response> a = c.send(ask(10));
            CompletableFuture<Response> b = c.send(ask(20));
            CompletableFuture<Response> d = c.send(ask(30));
            assertEquals("answer 10", text(a.get(5, TimeUnit.SECONDS)));
            assertEquals("answer 20", text(b.get(5, TimeUnit.SECONDS)));
            assertEquals("answer 30", text(d.get(5, TimeUnit.SECONDS)));
            assertEquals(
                    List.of(1L, 2L, 3L),
                    held.stream().map(r -> r.header().correlationId()).toList(),
                    "correlation ids start at 1; 0 is for pushes");
        }
    }

    @Test
    void manyThreadsMayShareOneConnection() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) ->
                        peer.reply(req.header().correlationId(), answer(((Request.Unsubscribe) req.request()).subscriptionId())));
                Connection c = Connection.open(broker.address(), plain())) {
            List<Thread> threads = new java.util.ArrayList<>();
            java.util.concurrent.atomic.AtomicInteger wrong = new java.util.concurrent.atomic.AtomicInteger();
            for (int t = 0; t < 8; t++) {
                final int base = t * 1000;
                Thread th = new Thread(() -> {
                    for (int i = 0; i < 100; i++) {
                        if (!text(c.call(ask(base + i))).equals("answer " + (base + i))) {
                            wrong.incrementAndGet();
                        }
                    }
                });
                threads.add(th);
                th.start();
            }
            for (Thread th : threads) {
                th.join();
            }
            assertEquals(0, wrong.get());
        }
    }

    @Test
    void creditIsNeverAnsweredAndHasNoResponseSlot() throws Exception {
        List<Frames.DecodedRequest> seen = new CopyOnWriteArrayList<>();
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    seen.add(req);
                    if (!(req.request() instanceof Request.Credit)) {
                        peer.reply(req.header().correlationId(), answer(1));
                    }
                });
                Connection c = Connection.open(broker.address(), plain())) {
            assertThrows(IllegalArgumentException.class, () -> c.send(new Request.Credit(5, 1, 1)));
            c.sendOnly(new Request.Credit(5, 10, 1000));
            assertEquals("answer 1", text(c.call(ask(1))));
            assertEquals(2, seen.size());
            assertInstanceOf(Request.Credit.class, seen.get(0).request());
            assertEquals(1, seen.get(0).header().correlationId());
            assertEquals(2, seen.get(1).header().correlationId(), "Credit used an id, and nothing waits for it");
        }
    }

    @Test
    void aResponseNobodyWaitsForIsDropped() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    peer.reply(777, answer(0)); // no such request
                    peer.reply(req.header().correlationId(), answer(1));
                });
                Connection c = Connection.open(broker.address(), plain())) {
            assertEquals("answer 1", text(c.call(ask(1))));
            assertEquals("answer 1", text(c.call(ask(1))));
            assertTrue(c.isOpen());
        }
    }

    @Test
    void pushesBeforeTheSubscriptionIsRegisteredAreKeptAndDeliveredInOrder() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    long n = ((Request.Unsubscribe) req.request()).subscriptionId();
                    if (n == 1) {
                        // The subscription's pushes come before anyone registered for it.
                        peer.push(push(7, 100));
                        peer.push(push(7, 101));
                        peer.push(push(8, 200));
                        peer.push(push(7, 102));
                    }
                    if (n == 2) {
                        peer.push(push(7, 103));
                    }
                    peer.reply(req.header().correlationId(), answer(n));
                });
                Connection c = Connection.open(broker.address(), plain())) {
            c.call(ask(1)); // when this returns, the reader has routed the four pushes before it
            Inbox seven = new Inbox();
            Inbox eight = new Inbox();
            c.register(7, seven);
            assertEquals(100, seven.nextOffset());
            assertEquals(101, seven.nextOffset());
            assertEquals(102, seven.nextOffset());
            c.register(8, eight);
            assertEquals(200, eight.nextOffset());
            c.call(ask(2));
            assertEquals(103, seven.nextOffset(), "later pushes follow the kept ones");
            assertNull(seven.pushes.poll(50, TimeUnit.MILLISECONDS));
            c.unregister(7);
            c.register(7, new Inbox());
        }
    }

    @Test
    void onlySixtyFourSubscriptionsAreKeptWaiting() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    for (long sub = 1; sub <= 70; sub++) {
                        peer.push(push(sub, sub));
                    }
                    peer.reply(req.header().correlationId(), answer(1));
                });
                Connection c = Connection.open(broker.address(), plain())) {
            c.call(ask(1));
            Inbox evicted = new Inbox();
            Inbox oldestKept = new Inbox();
            Inbox newest = new Inbox();
            c.register(6, evicted);
            c.register(7, oldestKept);
            c.register(70, newest);
            assertNull(evicted.pushes.poll(50, TimeUnit.MILLISECONDS), "the lowest ids made room");
            assertEquals(7, oldestKept.nextOffset());
            assertEquals(70, newest.nextOffset());
        }
    }

    @Test
    void unregisteringForgetsTheKeptPushes() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    peer.push(push(5, 1));
                    peer.reply(req.header().correlationId(), answer(1));
                });
                Connection c = Connection.open(broker.address(), plain())) {
            c.call(ask(1));
            c.unregister(5);
            Inbox late = new Inbox();
            c.register(5, late);
            assertNull(late.pushes.poll(50, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void aResponseWithTheErrorFlagBecomesABrokerExceptionBeforeAnyTypedDecoding() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    if (req.request() instanceof Request.Metadata) {
                        // A body that is only an error code, for a request whose answer has a long typed body.
                        peer.reply(req.header().correlationId(), new Response.Error(ErrorCode.UNAUTHORIZED));
                    } else {
                        peer.reply(req.header().correlationId(), new Response.Refused(ErrorCode.INVALID_REQUEST, "no such thing"));
                    }
                });
                Connection c = Connection.open(broker.address(), plain())) {
            BrokerException plainError = assertInstanceOf(BrokerException.class, failure(c.send(new Request.Metadata(List.of("x")))));
            assertEquals(ErrorCode.UNAUTHORIZED, plainError.code());
            assertNull(plainError.reason());
            BrokerException withReason = assertInstanceOf(BrokerException.class, failure(c.send(ask(1))));
            assertEquals(ErrorCode.INVALID_REQUEST, withReason.code());
            assertEquals("no such thing", withReason.reason());
            assertTrue(withReason.getMessage().contains("no such thing"));
            assertTrue(c.isOpen(), "a refused request does not end the connection");
        }
    }

    @Test
    void aTimedOutRequestFailsAndTheLateAnswerChangesNothing() throws Exception {
        java.util.concurrent.atomic.AtomicReference<FakeBroker.Peer> peerRef = new java.util.concurrent.atomic.AtomicReference<>();
        List<Long> ids = new CopyOnWriteArrayList<>();
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    peerRef.set(peer);
                    ids.add(req.header().correlationId());
                    if (ids.size() > 1) {
                        peer.reply(req.header().correlationId(), answer(2));
                    }
                });
                Connection c = Connection.open(
                        broker.address(),
                        ConnectionOptions.builder().plaintextOnLoopback().requestTimeout(Duration.ofMillis(150)).build())) {
            long start = System.nanoTime();
            Throwable timeout = failure(c.send(ask(1)));
            assertInstanceOf(RequestTimeoutException.class, timeout);
            assertTrue(((ConnectionException) timeout).requestSent(), "the outcome of a timed out request is unknown");
            assertTrue(System.nanoTime() - start >= Duration.ofMillis(140).toNanos());
            peerRef.get().reply(ids.get(0), answer(1)); // too late
            assertEquals("answer 2", text(c.call(ask(2))));
            assertTrue(c.isOpen());
        }
    }

    @Test
    void closingFailsEveryPendingRequestAndNotifiesSubscriptions() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> {})) {
            Connection c = Connection.open(broker.address(), plain());
            Inbox inbox = new Inbox();
            c.register(3, inbox);
            CompletableFuture<Response> a = c.send(ask(1));
            CompletableFuture<Response> b = c.send(ask(2));
            c.close();
            for (CompletableFuture<Response> f : List.of(a, b)) {
                ConnectionException e = assertInstanceOf(ConnectionException.class, failure(f));
                assertTrue(e.requestSent());
            }
            assertTrue(inbox.closed.poll(5, TimeUnit.SECONDS) != null);
            assertFalse(c.isOpen());
            ConnectionException later = assertInstanceOf(ConnectionException.class, failure(c.send(ask(3))));
            assertFalse(later.requestSent(), "nothing was sent on a closed connection");
            assertThrows(ConnectionException.class, () -> c.register(4, new Inbox()));
            assertThrows(ConnectionException.class, () -> c.sendOnly(new Request.Credit(1, 1, 1)));
            c.close(); // closing twice is fine
        }
    }

    @Test
    void theBrokerClosingTheConnectionFailsPendingRequests() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> peer.closeSocket());
                Connection c = Connection.open(broker.address(), plain())) {
            Inbox inbox = new Inbox();
            c.register(3, inbox);
            ConnectionException e = assertInstanceOf(ConnectionException.class, failure(c.send(ask(1))));
            assertTrue(e.getMessage().contains("closed"), e.getMessage());
            assertTrue(inbox.closed.poll(5, TimeUnit.SECONDS) != null);
            Eventually.await("the connection to end", () -> !c.isOpen());
        }
    }

    @Test
    void aFrameOverTheLimitEndsTheConnection() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> peer.rawBytes(new byte[] {1, 0, 0, 1}));
                Connection c = Connection.open(broker.address(), plain())) {
            ConnectionException e = assertInstanceOf(ConnectionException.class, failure(c.send(ask(1))));
            assertTrue(e.getMessage().contains("limit"), e.getMessage());
            assertFalse(c.isOpen());
        }
    }

    @Test
    void aRequestOverTheLimitIsNotSentAndTheConnectionStaysUsable() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> peer.reply(req.header().correlationId(), answer(1)));
                Connection c = Connection.open(broker.address(), plain())) {
            byte[] big = new byte[Frames.MAX_FRAME + 1];
            ConnectionException e = assertInstanceOf(
                    ConnectionException.class, failure(c.send(new Request.SaslAuthenticate(ByteString.copyOf(big)))));
            assertFalse(e.requestSent());
            assertEquals("answer 1", text(c.call(ask(1))));
        }
    }

    @Test
    void anUndecodableResponseEndsTheConnection() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    Writer w = new Writer();
                    w.u32(req.header().correlationId()).u16(0).noTags();
                    w.u8(9); // far too short for a Metadata response
                    peer.raw(w.toByteArray());
                });
                Connection c = Connection.open(broker.address(), plain())) {
            ConnectionException e = assertInstanceOf(ConnectionException.class, failure(c.send(new Request.Metadata(List.of()))));
            assertTrue(e.getMessage().contains("undecodable"), e.getMessage());
            Eventually.await("the connection to end", () -> !c.isOpen());
        }
    }

    @Test
    void apiVersionsIsOptionalButAvailable() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    assertInstanceOf(Request.ApiVersions.class, req.request());
                    peer.reply(req.header().correlationId(), new Response.ApiVersions(
                            List.of(new dev.lotse.proto.ApiVersionRange(dev.lotse.proto.ApiKey.METADATA, 0, 0)), List.of("streams")));
                });
                Connection c = Connection.open(broker.address(), plain())) {
            Response.ApiVersions v = c.apiVersions().get(5, TimeUnit.SECONDS);
            assertEquals(List.of("streams"), v.features());
            assertEquals(1, v.versions().size());
        }
    }

    @Test
    void aWrongTypeOfResponseIsReported() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> peer.reply(req.header().correlationId(), answer(1)));
                Connection c = Connection.open(broker.address(), plain())) {
            assertThrows(ConnectionException.class, () -> c.call(ask(1), Response.Metadata.class));
        }
    }

    @Test
    void connectingToNothingFailsWithoutHanging() {
        ConnectionException e = assertThrows(
                ConnectionException.class,
                () -> Connection.open(new Address("127.0.0.1", 1), plain()));
        assertFalse(e.requestSent());
    }
}

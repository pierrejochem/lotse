package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Frames;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.BindingMetadata;
import dev.lotse.proto.Messages.DeliveredOffset;
import dev.lotse.proto.Messages.ExchangeMetadata;
import dev.lotse.proto.Messages.ResourceMetadata;
import dev.lotse.proto.Messages.TxnPartitionResult;
import dev.lotse.proto.Messages.TxnResourceResult;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.Batch;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Transactions against a scripted broker that is coordinator and leader at once, as the Rust client does them. */
class TransactionTest {
    private static final long STREAM_ID = 7;
    private static final long EXCHANGE_ID = 20;
    private static final long QUEUE_A = 21;
    private static final long QUEUE_B = 22;

    private FakeNode node;
    private LotseClient client;
    private final AtomicLong producerIds = new AtomicLong(100);
    private final AtomicLong offsets = new AtomicLong();
    /** What a produce to a resource id gets; absent: written at the next offset. */
    private final Map<Long, Function<Request.Produce, Response>> produce = new ConcurrentHashMap<>();
    private volatile Function<Request.EndTxn, Response> endTxn =
            r -> new Response.EndTxn(ErrorCode.NONE, r.producerId(), r.producerEpoch() + 1);
    private volatile ErrorCode offsetsError = ErrorCode.NONE;
    private volatile ErrorCode ackError = ErrorCode.NONE;

    @BeforeEach
    void start() throws IOException {
        node = new FakeNode(1);
        node.onConn((n, conn, request) -> answer(n, request));
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        node.close();
    }

    private static ResourceMetadata queue(String name, long id) {
        return new ResourceMetadata(
                ErrorCode.NONE, name, "default", id, Messages.ResourceKind.QUEUE, List.of(Fx.partition(0, 1, 1, 1L)), null);
    }

    private Response answer(FakeNode n, Request request) {
        if (request instanceof Request.Metadata) {
            // A topic exchange: "a.#" to queue a, "*.b" to queue b.
            ExchangeMetadata x = new ExchangeMetadata(
                    Exchange.TOPIC,
                    List.of(new BindingMetadata(QUEUE_A, "a.#", List.of(), true),
                            new BindingMetadata(QUEUE_B, "*.b", List.of(), true)));
            return Fx.metadata(
                    List.of(n),
                    Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 1, 1L), Fx.partition(1, 1, 1, 1L)),
                    new ResourceMetadata(
                            ErrorCode.NONE, "x", "default", EXCHANGE_ID, Messages.ResourceKind.EXCHANGE, List.of(), x),
                    queue("qa", QUEUE_A),
                    queue("qb", QUEUE_B));
        }
        if (request instanceof Request.FindCoordinator) {
            return Fx.coordinator(n);
        }
        if (request instanceof Request.InitProducer) {
            return Fx.producer(producerIds.getAndIncrement(), 4);
        }
        if (request instanceof Request.AddPartitionsToTxn a) {
            List<TxnResourceResult> results = new ArrayList<>();
            for (Messages.TxnResource r : a.resources()) {
                List<TxnPartitionResult> parts = new ArrayList<>();
                for (long p : r.partitions()) {
                    parts.add(new TxnPartitionResult(p, ErrorCode.NONE));
                }
                results.add(new TxnResourceResult(r.resourceId(), parts));
            }
            return new Response.AddPartitionsToTxn(results);
        }
        if (request instanceof Request.Produce p) {
            long id = p.resources().get(0).resourceId();
            Function<Request.Produce, Response> f = produce.get(id);
            if (f != null) {
                return f.apply(p);
            }
            return Fx.produced(id, (int) p.resources().get(0).partitions().get(0).partition(), offsets.getAndIncrement());
        }
        if (request instanceof Request.EndTxn e) {
            return endTxn.apply(e);
        }
        if (request instanceof Request.AddOffsetsToTxn a) {
            List<TxnResourceResult> results = new ArrayList<>();
            for (Messages.OffsetCommitResource r : a.resources()) {
                List<TxnPartitionResult> parts = new ArrayList<>();
                for (Messages.OffsetCommitPartition p : r.partitions()) {
                    parts.add(new TxnPartitionResult(p.partition(), offsetsError));
                }
                results.add(new TxnResourceResult(r.resourceId(), parts));
            }
            return new Response.CommitOffsets(results);
        }
        if (request instanceof Request.Subscribe) {
            return new Response.Subscribe(ErrorCode.NONE, 500, null);
        }
        if (request instanceof Request.Ack) {
            return new Response.Outcome(ackError, null);
        }
        if (request instanceof Request.Unsubscribe || request instanceof Request.Nack) {
            return new Response.Outcome(ErrorCode.NONE, null);
        }
        throw new AssertionError("unexpected " + request);
    }

    private LotseClient client() {
        client = new LotseClient(Fx.config(node)
                .transactionalId("t")
                .retryFor(Duration.ofMillis(400))
                .build());
        return client;
    }

    private List<Batch> produced(long resourceId) {
        List<Batch> out = new ArrayList<>();
        for (Request.Produce p : node.requests(Request.Produce.class)) {
            if (p.resources().get(0).resourceId() == resourceId) {
                out.add(Fx.batchOf(p));
            }
        }
        return out;
    }

    @Test
    void aClientWithoutATransactionalIdCannotBegin() {
        client = new LotseClient(Fx.config(node).build());
        assertThrows(ClientException.NotTransactional.class, client::beginTransaction);
    }

    @Test
    void batchesCarryTheTransactionAndEachPartitionIsAddedOnce() {
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            txn.produce("s", 0, List.of(OutMessage.value("a"), OutMessage.value("b")));
            txn.produce("s", 0, OutMessage.value("c"));
            txn.produce("s", 1, OutMessage.value("d"));
            txn.commit();
        }
        List<Request.AddPartitionsToTxn> adds = node.requests(Request.AddPartitionsToTxn.class);
        assertEquals(2, adds.size(), "each partition once");
        assertEquals("t", adds.get(0).txnId());
        List<Batch> batches = produced(STREAM_ID);
        assertEquals(3, batches.size());
        for (Batch b : batches) {
            assertTrue(b.attributes().transactional(), "transactional");
            assertEquals(100, b.producerId());
            assertEquals(4, b.producerEpoch());
        }
        assertEquals(0, batches.get(0).baseSequence());
        assertEquals(2, batches.get(1).baseSequence(), "counted by records");
        assertEquals(0, batches.get(2).baseSequence(), "per partition");
        for (Request.Produce p : node.requests(Request.Produce.class)) {
            assertEquals("t", p.txnId());
        }
        Request.EndTxn end = node.requests(Request.EndTxn.class).get(0);
        assertTrue(end.commit());
        assertEquals(100, end.producerId());
    }

    @Test
    void theNextTransactionGoesOnWithTheRaisedEpoch() {
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            txn.produce("s", 0, OutMessage.value("a"));
            txn.commit();
        }
        try (Transaction txn = lotse.beginTransaction()) {
            txn.produce("s", 0, OutMessage.value("b"));
            txn.abort();
        }
        assertEquals(1, node.requests(Request.InitProducer.class).size(), "the producer id is kept");
        List<Batch> batches = produced(STREAM_ID);
        assertEquals(5, batches.get(1).producerEpoch(), "the epoch EndTxn raised");
        assertEquals(0, batches.get(1).baseSequence(), "a new epoch starts at 0");
        assertFalse(node.requests(Request.EndTxn.class).get(1).commit());
    }

    @Test
    void aTransactionThatWroteNothingEndsWithoutTheCoordinator() {
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            txn.commit();
        }
        assertEquals(0, node.requests(Request.EndTxn.class).size());
        try (Transaction txn = lotse.beginTransaction()) {
            txn.abort();
        }
        assertEquals(1, node.requests(Request.InitProducer.class).size());
    }

    @Test
    void aTransactionLeftOpenIsAbandonedForAFreshProducerId() {
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            txn.produce("s", 0, OutMessage.value("a"));
        }
        try (Transaction txn = lotse.beginTransaction()) {
            txn.produce("s", 0, OutMessage.value("b"));
            txn.commit();
        }
        assertEquals(2, node.requests(Request.InitProducer.class).size(), "a fresh producer id aborts the first");
        assertEquals(101, produced(STREAM_ID).get(1).producerId());
    }

    @Test
    void anUnknownWriteLeavesOnlyAnAbort() {
        produce.put(STREAM_ID, p -> Fx.produced(STREAM_ID, 0, ErrorCode.REQUEST_TIMED_OUT, 0, null));
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            assertThrows(ClientException.Unknown.class, () -> txn.produce("s", 0, OutMessage.value("a")));
            assertThrows(ClientException.MustAbort.class, () -> txn.produce("s", 1, OutMessage.value("b")));
            assertThrows(ClientException.MustAbort.class, txn::commit);
            assertEquals(0, node.requests(Request.EndTxn.class).size(), "the coordinator was never asked to commit");
            assertTrue(txn.isOpen(), "open for the abort");
            txn.abort();
            assertFalse(txn.isOpen());
        }
        assertFalse(node.requests(Request.EndTxn.class).get(0).commit());
    }

    @Test
    void aProduceThatEndsWithoutAnOutcomeLeavesOnlyAnAbort() throws Exception {
        // The leader never answers; the client is closed while the produce waits, which ends it with an
        // IllegalStateException rather than a ClientException.
        produce.put(STREAM_ID, p -> null);
        LotseClient lotse = new LotseClient(Fx.config(node)
                .transactionalId("t")
                .requestTimeout(Duration.ofSeconds(5))
                .retryFor(Duration.ofSeconds(5))
                .build());
        client = lotse;
        Transaction txn = lotse.beginTransaction();
        java.util.concurrent.CompletableFuture<Void> closing = java.util.concurrent.CompletableFuture.runAsync(() -> {
            try {
                Eventually.await("the produce", () -> !node.requests(Request.Produce.class).isEmpty());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            lotse.close();
        });
        assertThrows(RuntimeException.class, () -> txn.produce("s", 0, OutMessage.value("a")));
        closing.get(5, java.util.concurrent.TimeUnit.SECONDS);
        assertThrows(ClientException.MustAbort.class, () -> txn.produce("s", 0, OutMessage.value("b")));
        assertThrows(ClientException.MustAbort.class, txn::commit);
    }

    @Test
    void aRefusedWriteLeavesOnlyAnAbort() {
        produce.put(STREAM_ID, p -> Fx.produced(STREAM_ID, 0, ErrorCode.INVALID_REQUEST, 0, null));
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            assertThrows(ClientException.Broker.class, () -> txn.produce("s", 0, OutMessage.value("a")));
            assertThrows(ClientException.MustAbort.class, txn::commit);
            txn.abort();
        }
    }

    @Test
    void aWriteThatCertainlyFailedLeavesTheTransactionOpen() {
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            assertThrows(ClientException.UnknownStream.class, () -> txn.produce("nope", 0, OutMessage.value("a")));
            txn.produce("s", 0, OutMessage.value("b"));
            txn.commit();
        }
        assertTrue(node.requests(Request.EndTxn.class).get(0).commit());
    }

    @Test
    void aLostEndIsAskedAgainAndCountsAsDecided() {
        java.util.concurrent.atomic.AtomicInteger ends = new java.util.concurrent.atomic.AtomicInteger();
        endTxn = r -> ends.incrementAndGet() == 1
                ? new Response.EndTxn(ErrorCode.COORDINATOR_NOT_AVAILABLE, 0, 0)
                : new Response.EndTxn(ErrorCode.NONE, r.producerId(), r.producerEpoch() + 1);
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            txn.produce("s", 0, OutMessage.value("a"));
            txn.commit();
        }
        assertEquals(2, ends.get());
    }

    @Test
    void aFailedEndStartsTheNextTransactionWithAFreshProducerId() {
        endTxn = r -> new Response.EndTxn(ErrorCode.PRODUCER_FENCED, 0, 0);
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            txn.produce("s", 0, OutMessage.value("a"));
            assertThrows(ClientException.Broker.class, txn::commit);
        }
        try (Transaction txn = lotse.beginTransaction()) {
            txn.abort();
        }
        assertEquals(2, node.requests(Request.InitProducer.class).size());
    }

    @Test
    void oneTransactionAtATime() throws Exception {
        LotseClient lotse = client();
        Transaction first = lotse.beginTransaction();
        java.util.concurrent.CompletableFuture<Transaction> second =
                java.util.concurrent.CompletableFuture.supplyAsync(lotse::beginTransaction);
        Thread.sleep(100);
        assertFalse(second.isDone(), "waits for the first");
        first.abort();
        second.get(5, java.util.concurrent.TimeUnit.SECONDS).abort();
    }

    // ---- publishing: routed by this client ----

    @Test
    void aPublishIsRoutedHereAndWrittenToEveryQueueInTheTransaction() {
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            Published p = txn.publish("x", "a.b", OutMessage.value("m"), false);
            assertEquals(List.of("qa", "qb"), p.routed().stream().map(Published.Routed::queue).toList());
            assertEquals(List.of("qa"), txn.publish("x", "a.c", OutMessage.value("n"), true).routed().stream()
                    .map(Published.Routed::queue).toList());
            assertEquals(List.of(), txn.publish("x", "c", OutMessage.value("o"), false).routed());
            assertThrows(ClientException.Unroutable.class, () -> txn.publish("x", "c", OutMessage.value("o"), true));
            txn.commit();
        }
        assertEquals(2, produced(QUEUE_A).size());
        assertEquals(1, produced(QUEUE_B).size());
        assertTrue(produced(QUEUE_B).get(0).attributes().transactional());
        assertEquals(0, node.requests(Request.Publish.class).size(), "not routed by a node");
    }

    @Test
    void aPublishThatReachedSomeQueuesOnlyLeavesOnlyAnAbort() {
        produce.put(QUEUE_B, p -> Fx.produced(QUEUE_B, 0, ErrorCode.NOT_LEADER, 0, null));
        LotseClient lotse = client();
        try (Transaction txn = lotse.beginTransaction()) {
            assertThrows(ClientException.GaveUp.class, () -> txn.publish("x", "a.b", OutMessage.value("m"), false));
            assertThrows(ClientException.MustAbort.class, txn::commit);
            txn.abort();
        }
        assertEquals(1, produced(QUEUE_A).size());
    }

    // ---- queue acks in a transaction (ADR 0075) ----

    @Test
    void anAckInATransactionCarriesItOnTheSubscriptionsConnection() throws Exception {
        LotseClient lotse = client();
        try (QueueConsumer c = lotse.subscribe("qa"); Transaction txn = lotse.beginTransaction()) {
            FakeNode.Conn on = node.conns().get(node.conns().size() - 1);
            on.push(new Frames.DeliverPush(
                    500, ErrorCode.NONE, FetchTest.data(3, 0, false, "m"), List.of(new DeliveredOffset(3, 1)), null));
            Delivery d = c.poll(Duration.ofSeconds(3));
            c.ackIn(txn, d);
            txn.commit();
            List<Request.Ack> acks = on.requests(Request.Ack.class);
            assertEquals(1, acks.size(), "on the subscription's connection");
            Request.Ack ack = acks.get(0);
            assertEquals("t", ack.txnId());
            assertEquals(100, ack.producerId());
            assertEquals(4, ack.producerEpoch());
            assertEquals(List.of(new Messages.Range(3, 4)), ack.ranges());
        }
        Request.AddPartitionsToTxn add = node.requests(Request.AddPartitionsToTxn.class).get(0);
        assertEquals(QUEUE_A, add.resources().get(0).resourceId(), "the queue joins the transaction first");
    }

    @Test
    void anAckTheLeaderRefusesLeavesOnlyAnAbort() throws Exception {
        ackError = ErrorCode.MESSAGE_NOT_HELD;
        LotseClient lotse = client();
        try (QueueConsumer c = lotse.subscribe("qa"); Transaction txn = lotse.beginTransaction()) {
            FakeNode.Conn on = node.conns().get(node.conns().size() - 1);
            on.push(new Frames.DeliverPush(
                    500, ErrorCode.NONE, FetchTest.data(3, 0, false, "m"), List.of(new DeliveredOffset(3, 1)), null));
            Delivery d = c.poll(Duration.ofSeconds(3));
            ClientException.Broker e = assertThrows(ClientException.Broker.class, () -> c.ackIn(txn, d));
            assertEquals(ErrorCode.MESSAGE_NOT_HELD, e.code());
            assertThrows(ClientException.MustAbort.class, txn::commit);
            txn.abort();
        }
    }

}

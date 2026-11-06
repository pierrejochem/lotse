package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.Batch;
import dev.lotse.record.ByteString;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Idempotent and plain produce against a scripted broker. */
class ProducerTest {
    private static final long STREAM_ID = 7;

    private FakeNode node;
    private LotseClient client;
    private final AtomicLong nextProducerId = new AtomicLong(100);
    private final AtomicLong offsets = new AtomicLong();

    @BeforeEach
    void start() throws IOException {
        node = new FakeNode(1);
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        node.close();
    }

    /** The broker serves metadata (partitions 0 and 1, both led by node 1), a coordinator, and {@code produce}. */
    private void serve(IntFunction<Response> produce) {
        AtomicInteger produces = new AtomicInteger();
        node.on((n, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(
                        List.of(n),
                        Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 1, 1L), Fx.partition(1, 1, 1, 1L)));
            }
            if (request instanceof Request.FindCoordinator) {
                return Fx.coordinator(n);
            }
            if (request instanceof Request.InitProducer) {
                return Fx.producer(nextProducerId.getAndIncrement(), 3);
            }
            if (request instanceof Request.Produce) {
                return produce.apply(produces.incrementAndGet());
            }
            throw new AssertionError("unexpected " + request);
        });
    }

    private Response ok() {
        return Fx.produced(STREAM_ID, 0, offsets.getAndAdd(1));
    }

    private LotseClient client(boolean idempotent) {
        client = new LotseClient(Fx.config(node).idempotent(idempotent).retryFor(Duration.ofMillis(400)).build());
        return client;
    }

    private static List<OutMessage> messages(int n) {
        return java.util.stream.IntStream.range(0, n).mapToObj(i -> OutMessage.value("m" + i)).toList();
    }

    @Test
    void anIdempotentProducerAsksForItsIdOnceAndCountsRecordsPerPartition() {
        serve(n -> ok());
        LotseClient lotse = client(true);
        lotse.produce("s", 0, messages(2));
        lotse.produce("s", 0, messages(1));
        lotse.produce("s", 0, messages(3));
        lotse.produce("s", 1, messages(1));

        Request.FindCoordinator find = node.requests(Request.FindCoordinator.class).get(0);
        assertEquals(Messages.CoordinatorType.PRODUCER, find.keyType());
        assertEquals("", find.key());
        assertEquals(1, node.count(Request.FindCoordinator.class));
        List<Request.InitProducer> inits = node.requests(Request.InitProducer.class);
        assertEquals(1, inits.size());
        assertNull(inits.get(0).txnId(), "an idempotent producer has no transactional id");

        List<Request.Produce> sent = node.requests(Request.Produce.class);
        assertEquals(List.of(0, 2, 3), List.of(0, 1, 2).stream().map(i -> Fx.batchOf(sent.get(i)).baseSequence()).toList());
        assertEquals(0, Fx.batchOf(sent.get(3)).baseSequence(), "another partition has its own sequence");
        for (Request.Produce p : sent) {
            Batch b = Fx.batchOf(p);
            assertEquals(100, b.producerId());
            assertEquals(3, b.producerEpoch());
            assertFalse(b.attributes().transactional());
        }
        assertEquals(5000, sent.get(0).timeoutMs(), "produce_timeout goes to the leader");
    }

    @Test
    void aBatchWhoseOutcomeIsUnknownIsSentAgainWithTheSameSequence() {
        // The first answer is lost with the connection, the second says the quorum timed out, the third works.
        serve(n -> switch (n) {
            case 1 -> throw new FakeNode.Drop();
            case 2 -> Fx.produced(STREAM_ID, 0, ErrorCode.REQUEST_TIMED_OUT, 0, null);
            default -> ok();
        });
        LotseClient lotse = client(true);
        lotse.produce("s", 0, messages(2));
        lotse.produce("s", 0, messages(1));

        List<Request.Produce> sent = node.requests(Request.Produce.class);
        assertEquals(4, sent.size());
        assertEquals(Fx.bytesOf(sent.get(0)), Fx.bytesOf(sent.get(1)), "the same bytes again");
        assertEquals(Fx.bytesOf(sent.get(0)), Fx.bytesOf(sent.get(2)), "the same bytes again");
        assertEquals(0, Fx.batchOf(sent.get(2)).baseSequence());
        assertEquals(2, Fx.batchOf(sent.get(3)).baseSequence(), "advanced once, by the two records");
        assertEquals(1, node.count(Request.InitProducer.class), "the producer id stayed");
    }

    @Test
    void anOutcomeStillUnknownWhenTimeRunsOutIsReportedAndTheNextProduceGetsANewProducerId() {
        AtomicInteger phase = new AtomicInteger();
        serve(n -> phase.get() == 0 ? Fx.produced(STREAM_ID, 0, ErrorCode.REQUEST_TIMED_OUT, 0, null) : ok());
        LotseClient lotse = client(true);

        ClientException.Unknown unknown =
                assertThrows(ClientException.Unknown.class, () -> lotse.produce("s", 0, messages(2)));
        assertTrue(unknown.getMessage().contains("still unknown"), unknown.getMessage());
        List<Request.Produce> sent = node.requests(Request.Produce.class);
        assertTrue(sent.size() >= 2, "it retried while it could");
        for (Request.Produce p : sent) {
            assertEquals(Fx.bytesOf(sent.get(0)), Fx.bytesOf(p), "always the same sequence");
        }
        assertEquals(0, lotse.producerId(), "the session was discarded");

        phase.set(1);
        lotse.produce("s", 0, messages(1));
        assertEquals(2, node.count(Request.InitProducer.class), "a new producer id");
        Batch next = Fx.batchOf(node.requests(Request.Produce.class).get(sent.size()));
        assertEquals(101, next.producerId());
        assertEquals(0, next.baseSequence(), "the new id starts at 0");
    }

    @Test
    void anOutOfOrderSequenceOrAFencedProducerIsReportedAndStartsOver() {
        for (ErrorCode code : new ErrorCode[] {ErrorCode.OUT_OF_ORDER_SEQUENCE, ErrorCode.PRODUCER_FENCED}) {
            AtomicInteger phase = new AtomicInteger();
            serve(n -> phase.get() == 0 ? Fx.produced(STREAM_ID, 0, code, 0, null) : ok());
            try (LotseClient lotse = new LotseClient(Fx.config(node).retryFor(Duration.ofMillis(400)).build())) {
                ClientException.Broker e =
                        assertThrows(ClientException.Broker.class, () -> lotse.produce("s", 0, messages(1)));
                assertEquals(code, e.code());
                assertEquals(0, lotse.producerId(), "discarded after " + code);
                phase.set(1);
                long before = nextProducerId.get();
                lotse.produce("s", 0, messages(1));
                assertEquals(before + 1, nextProducerId.get(), "a new producer id after " + code);
            }
        }
    }

    @Test
    void aNamedProducerAsksUnderItsNameAndStaysFencedOnceANewerInstanceTookOver() {
        AtomicInteger phase = new AtomicInteger();
        serve(n -> phase.get() == 0 ? ok() : Fx.produced(STREAM_ID, 0, ErrorCode.PRODUCER_FENCED, 0, null));
        try (LotseClient lotse = new LotseClient(
                Fx.config(node).producerName("orders-writer").retryFor(Duration.ofMillis(400)).build())) {
            lotse.produce("s", 0, messages(1));
            assertEquals("orders-writer", node.requests(Request.InitProducer.class).get(0).txnId(), "asked by name");
            assertEquals("orders-writer", node.requests(Request.FindCoordinator.class).get(0).key());
            phase.set(1);
            for (int i = 0; i < 2; i++) {
                ClientException.Broker e =
                        assertThrows(ClientException.Broker.class, () -> lotse.produce("s", 0, messages(1)));
                assertEquals(ErrorCode.PRODUCER_FENCED, e.code());
            }
            assertEquals(1, node.count(Request.InitProducer.class), "no new epoch that would fence the newer one");
        }
        assertThrows(IllegalArgumentException.class, () -> Fx.config(node).producerName("a\0b"));
    }

    @Test
    void aDefiniteFailureKeepsTheSessionAndTheSequence() {
        AtomicInteger phase = new AtomicInteger();
        serve(n -> phase.get() == 0 ? Fx.produced(STREAM_ID, 0, ErrorCode.QUOTA_EXCEEDED, 0, null) : ok());
        LotseClient lotse = client(true);
        ClientException.Broker e = assertThrows(ClientException.Broker.class, () -> lotse.produce("s", 0, messages(2)));
        assertEquals(ErrorCode.QUOTA_EXCEEDED, e.code());
        assertEquals(100, lotse.producerId(), "the producer id is kept");
        phase.set(1);
        lotse.produce("s", 0, messages(1));
        assertEquals(1, node.count(Request.InitProducer.class));
        List<Request.Produce> sent = node.requests(Request.Produce.class);
        assertEquals(0, Fx.batchOf(sent.get(1)).baseSequence(), "the sequence of a batch that was not written is free");
    }

    @Test
    void produceToTheSamePartitionIsSerialisedAndToOthersIsNot() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlightOnPartition0 = new AtomicInteger();
        node.on((n, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(List.of(n), Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 1, 1L), Fx.partition(1, 1, 1, 1L)));
            }
            if (request instanceof Request.FindCoordinator) {
                return Fx.coordinator(n);
            }
            if (request instanceof Request.InitProducer) {
                return Fx.producer(100, 1);
            }
            Request.Produce p = (Request.Produce) request;
            long partition = p.resources().get(0).partitions().get(0).partition();
            if (partition == 0) {
                int now = inFlight.incrementAndGet();
                maxInFlightOnPartition0.accumulateAndGet(now, Math::max);
                release.await(10, TimeUnit.SECONDS);
                inFlight.decrementAndGet();
            }
            return Fx.produced(STREAM_ID, (int) partition, 0);
        });
        LotseClient lotse = client(true);
        lotse.metadata("s"); // route and producer id are settled before the race
        lotse.produce("s", 1, messages(1));
        int sentBefore = node.count(Request.Produce.class);

        CompletableFuture<ProduceResult> first = lotse.produceAsync("s", 0, messages(1));
        Eventually.await("the first produce to arrive", () -> node.count(Request.Produce.class) == sentBefore + 1);
        CompletableFuture<ProduceResult> second = lotse.produceAsync("s", 0, messages(1));
        // Another partition is not held up by the first.
        lotse.produce("s", 1, messages(1));
        Thread.sleep(300);
        assertEquals(sentBefore + 2, node.count(Request.Produce.class), "the second produce to partition 0 waits its turn");

        release.countDown();
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(1, maxInFlightOnPartition0.get(), "never two in flight to one partition");
        List<Request.Produce> sent = node.requests(Request.Produce.class);
        List<Integer> seqs = sent.stream()
                .filter(p -> p.resources().get(0).partitions().get(0).partition() == 0)
                .map(p -> Fx.batchOf(p).baseSequence())
                .toList();
        assertEquals(List.of(0, 1), seqs, "sequences reach the broker in order");
    }

    @Test
    void withoutIdempotenceThereIsNoProducerIdAndAnUnknownOutcomeIsNeverRetried() {
        serve(n -> Fx.produced(STREAM_ID, 0, ErrorCode.REQUEST_TIMED_OUT, 0, null));
        LotseClient lotse = client(false);
        assertThrows(ClientException.Unknown.class, () -> lotse.produce("s", 0, messages(1)));
        assertEquals(1, node.count(Request.Produce.class), "not sent again");
        assertEquals(0, node.count(Request.FindCoordinator.class) + node.count(Request.InitProducer.class));
        assertEquals(0, Fx.batchOf(node.requests(Request.Produce.class).get(0)).producerId(), "not stamped");
    }

    @Test
    void withoutIdempotenceALostConnectionAfterSendingIsNeverRetried() {
        serve(n -> {
            throw new FakeNode.Drop();
        });
        LotseClient lotse = client(false);
        assertThrows(ClientException.Unknown.class, () -> lotse.produce("s", 0, messages(1)));
        assertEquals(1, node.count(Request.Produce.class));
    }

    @Test
    void withoutIdempotenceALeaderErrorStillIsRetriedBecauseNothingWasWritten() {
        serve(n -> n == 1 ? Fx.produced(STREAM_ID, 0, ErrorCode.NOT_LEADER, 0, null) : ok());
        client(false).produce("s", 0, messages(1));
        assertEquals(2, node.count(Request.Produce.class));
    }

    @Test
    void aConnectionThatCouldNotBeOpenedIsRetriedEvenWithoutIdempotence() throws Exception {
        FakeNode dead = new FakeNode(2);
        dead.kill();
        // The metadata names a leader that is down for the first attempts: nothing is sent, so a retry is safe.
        AtomicInteger asked = new AtomicInteger();
        node.on((n, request) -> {
            if (request instanceof Request.Metadata) {
                boolean first = asked.incrementAndGet() == 1;
                return Fx.metadata(
                        List.of(n, dead), Fx.stream("s", STREAM_ID, Fx.partition(0, first ? 2 : 1, 1, 1L, 2L)));
            }
            return Fx.produced(STREAM_ID, 0, 11);
        });
        assertEquals(11, client(false).produce("s", 0, messages(1)).baseOffset());
        assertEquals(1, node.count(Request.Produce.class));
        dead.close();
    }

    @Test
    void anAuthenticationFailureAfterALostAttemptIsReportedAsUnknown() {
        AtomicInteger n = new AtomicInteger();
        node.on((self, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(List.of(self), Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 1, 1L)));
            }
            if (request instanceof Request.FindCoordinator) {
                return Fx.coordinator(self);
            }
            if (request instanceof Request.InitProducer) {
                return Fx.producer(100, 1);
            }
            if (n.incrementAndGet() == 1) {
                throw new FakeNode.Drop();
            }
            return new Response.Error(ErrorCode.UNAUTHORIZED);
        });
        LotseClient lotse = client(true);
        ClientException.Unknown e = assertThrows(ClientException.Unknown.class, () -> lotse.produce("s", 0, messages(1)));
        assertTrue(e.getMessage().contains("may have been carried out"), e.getMessage());
    }

    @Test
    void theTimestampDefaultsToNowAndAnExplicitOneIsKept() {
        serve(n -> ok());
        LotseClient lotse = client(true);
        long before = System.currentTimeMillis();
        lotse.produce("s", 0, List.of(OutMessage.value("a"), OutMessage.value("b").withTimestamp(1234)));
        long after = System.currentTimeMillis();
        Batch b = Fx.batchOf(node.requests(Request.Produce.class).get(0));
        var records = b.records();
        assertTrue(records.get(0).timestamp() >= before && records.get(0).timestamp() <= after, "now, not 0");
        assertEquals(1234, records.get(1).timestamp());
    }

    @Test
    void keysValuesAndHeadersArriveInTheBatch() {
        serve(n -> ok());
        client(true).produce("s", 0, OutMessage.value("v").withKey("k").withHeader("h", "x").withHeader("n", (byte[]) null));
        var rec = Fx.batchOf(node.requests(Request.Produce.class).get(0)).records().get(0);
        assertEquals(ByteString.utf8("k"), rec.key());
        assertEquals(ByteString.utf8("v"), rec.value());
        assertEquals(2, rec.headers().size());
        assertEquals(ByteString.utf8("x"), rec.headers().get(0).value());
        assertNull(rec.headers().get(1).value());
    }

    @Test
    void anEmptyListIsRefused() {
        serve(n -> ok());
        LotseClient lotse = client(true);
        assertThrows(IllegalArgumentException.class, () -> lotse.produce("s", 0, List.<OutMessage>of()));
    }

    @Test
    void aFailedFutureCarriesTheClientException() throws Exception {
        serve(n -> Fx.produced(STREAM_ID, 0, ErrorCode.QUOTA_EXCEEDED, 0, null));
        LotseClient lotse = client(true);
        ExecutionException e = assertThrows(
                ExecutionException.class, () -> lotse.produceAsync("s", 0, messages(1)).get(5, TimeUnit.SECONDS));
        assertInstanceOf(ClientException.Broker.class, e.getCause());
    }
}

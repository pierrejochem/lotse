package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.LeaderHint;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.AbortedTxn;
import dev.lotse.proto.Request;
import dev.lotse.record.Batch;
import dev.lotse.record.BatchBuilder;
import dev.lotse.record.ByteString;
import dev.lotse.record.ControlBatches;
import dev.lotse.record.ControlType;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Fetch requests, retries, and what a read_committed reader sees. */
class FetchTest {
    private static final long STREAM_ID = 7;

    private FakeNode a;
    private FakeNode b;
    private LotseClient client;

    @BeforeEach
    void start() throws IOException {
        a = new FakeNode(1);
        b = new FakeNode(2);
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        a.close();
        b.close();
    }

    // ---- batches as the broker stores them ----

    /** A batch of data records at {@code baseOffset}; {@code producer} 0 is not idempotent. */
    static ByteString data(long baseOffset, long producer, boolean transactional, String... values) {
        BatchBuilder builder = new BatchBuilder();
        for (String v : values) {
            builder.push(1_000, null, v.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        byte[] bytes = builder.finish().toByteArray();
        if (producer != 0) {
            Batch.setProducer(bytes, producer, 1, 0);
        }
        if (transactional) {
            Batch.setTransactional(bytes, true);
        }
        Batch.stamp(bytes, baseOffset, 1);
        return ByteString.view(bytes, 0, bytes.length);
    }

    static ByteString marker(long baseOffset, long producer, ControlType type) {
        byte[] bytes = ControlBatches.marker(producer, 1, type, 1, 1_000).toByteArray();
        Batch.stamp(bytes, baseOffset, 1);
        return ByteString.view(bytes, 0, bytes.length);
    }

    static ByteString concat(ByteString... parts) {
        int n = 0;
        for (ByteString p : parts) {
            n += p.size();
        }
        byte[] all = new byte[n];
        int at = 0;
        for (ByteString p : parts) {
            p.copyTo(all, at);
            at += p.size();
        }
        return ByteString.view(all, 0, n);
    }

    private static Fetched fetched(ByteString records, List<AbortedTxn> aborted) {
        return new Fetched(records, 100, 100, 0, aborted);
    }

    private static List<String> values(Fetched f) {
        return f.committedMessages().stream().map(r -> r.value().toUtf8()).toList();
    }

    // ---- committed_batches ----

    @Test
    void aReadCommittedReaderSkipsAbortedTransactionsAndMarkers() {
        ByteString records = concat(
                data(0, 0, false, "plain"),
                data(1, 7, true, "aborted-1"),
                data(2, 8, true, "committed"),
                data(3, 7, true, "aborted-2"),
                marker(4, 8, ControlType.COMMIT),
                marker(5, 7, ControlType.ABORT),
                data(6, 7, true, "later transaction of the same producer"),
                marker(7, 7, ControlType.COMMIT));
        Fetched f = fetched(records, List.of(new AbortedTxn(7, 1)));
        assertEquals(List.of("plain", "committed", "later transaction of the same producer"), values(f));
    }

    @Test
    void theAbortedListNeedNotArriveInOrder() {
        // The broker lists aborted transactions by producer; the reader must take them by first offset.
        ByteString records = concat(
                data(1, 7, true, "aborted-7"),
                marker(2, 7, ControlType.ABORT),
                data(10, 9, true, "aborted-9"),
                marker(11, 9, ControlType.ABORT),
                data(12, 9, true, "kept"));
        Fetched f = fetched(records, List.of(new AbortedTxn(9, 10), new AbortedTxn(7, 1)));
        assertEquals(List.of("kept"), values(f));
    }

    @Test
    void anAbortedProducerIsSkippedOnlyFromItsFirstOffset() {
        // The same producer wrote a committed transaction before the aborted one began.
        ByteString records = concat(
                data(0, 7, true, "earlier, committed"),
                marker(1, 7, ControlType.COMMIT),
                data(2, 7, true, "aborted"),
                marker(3, 7, ControlType.ABORT));
        Fetched f = fetched(records, List.of(new AbortedTxn(7, 2)));
        assertEquals(List.of("earlier, committed"), values(f));
    }

    @Test
    void onlyTransactionalBatchesOfAnAbortedProducerAreSkipped() {
        ByteString records = concat(
                data(1, 7, true, "aborted"),
                data(2, 7, false, "not transactional"),
                marker(3, 7, ControlType.ABORT));
        Fetched f = fetched(records, List.of(new AbortedTxn(7, 1)));
        assertEquals(List.of("not transactional"), values(f));
    }

    @Test
    void anAbortOfOneProducerDoesNotEndTheSkippingOfAnother() {
        ByteString records = concat(
                data(1, 7, true, "a7"),
                data(2, 8, true, "a8"),
                marker(3, 7, ControlType.ABORT),
                data(4, 8, true, "a8 again"),
                marker(5, 8, ControlType.ABORT));
        Fetched f = fetched(records, List.of(new AbortedTxn(7, 1), new AbortedTxn(8, 2)));
        assertEquals(List.of(), values(f));
    }

    @Test
    void aCommitMarkerDoesNotEndTheSkipping() {
        // Cannot happen for one producer in one transaction, but the rule is "up to its ABORT marker".
        ByteString records = concat(
                data(1, 7, true, "a"),
                marker(2, 7, ControlType.COMMIT),
                data(3, 7, true, "b"),
                marker(4, 7, ControlType.ABORT));
        Fetched f = fetched(records, List.of(new AbortedTxn(7, 1)));
        assertEquals(List.of(), values(f));
    }

    @Test
    void withoutAbortedTransactionsEveryDataBatchIsSeen() {
        ByteString records = concat(data(0, 8, true, "x", "y"), marker(2, 8, ControlType.COMMIT), data(3, 0, false, "z"));
        assertEquals(List.of("x", "y", "z"), values(fetched(records, List.of())));
        assertEquals(2, fetched(records, List.of()).committedBatches().size(), "the marker is not a batch of data");
    }

    @Test
    void aBatchOfARaftControlKindIsSkipped() {
        // A control batch that is not a transaction marker (Raft's own entry) is skipped, not an error.
        BatchBuilder builder = new BatchBuilder(new dev.lotse.record.BatchOptions(
                new dev.lotse.record.Attributes(
                        dev.lotse.record.Compression.NONE, false, true, dev.lotse.record.BatchKind.DATA),
                0,
                0,
                -1));
        builder.push(1, new byte[] {0, 0, 2, 0}, new byte[] {1, 2, 3});
        byte[] raft = builder.finish().toByteArray();
        Batch.stamp(raft, 0, 1);
        ByteString records = concat(ByteString.view(raft, 0, raft.length), data(1, 0, false, "v"));
        assertEquals(List.of("v"), values(fetched(records, List.of())));
    }

    @Test
    void recordsThatDoNotParseAreACorruptBatch() {
        byte[] bytes = data(0, 0, false, "x").toByteArray();
        bytes[bytes.length - 1] ^= 1;
        ClientException.Broker e = assertThrows(
                ClientException.Broker.class,
                () -> fetched(ByteString.view(bytes, 0, bytes.length), List.of()).committedBatches());
        assertEquals(ErrorCode.CORRUPT_BATCH, e.code());
    }

    @Test
    void theNextOffsetFollowsTheLastBatch() {
        ByteString records = concat(data(0, 0, false, "a", "b", "c"), data(3, 0, false, "d"));
        Fetched f = fetched(records, List.of());
        assertEquals(4, f.nextOffset(0));
        assertEquals(9, fetched(ByteString.EMPTY, List.of()).nextOffset(9), "nothing read: where it was");
    }

    // ---- the request and the retries ----

    @Test
    void theFetchRequestAsksForCommittedDataFromTheRoutesLeaderTerm() {
        a.on((node, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(List.of(node), Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L)));
            }
            return Fx.fetched(STREAM_ID, 0, ErrorCode.NONE, data(10, 0, false, "v"), 12, 11, 3, List.of(new AbortedTxn(4, 5)), null);
        });
        client = new LotseClient(Fx.config(a).build());
        Fetched f = client.fetch("s", 0, 10, Duration.ofMillis(250));

        Request.Fetch sent = a.requests(Request.Fetch.class).get(0);
        assertEquals(250, sent.maxWaitMs());
        assertEquals(1, sent.minBytes());
        assertEquals(4L << 20, sent.maxBytes());
        assertEquals(Messages.Isolation.COMMITTED, sent.isolation());
        assertEquals(STREAM_ID, sent.resources().get(0).resourceId());
        var p = sent.resources().get(0).partitions().get(0);
        assertEquals(0, p.partition());
        assertEquals(10, p.fetchOffset());
        assertEquals(5, p.leaderTerm(), "the term of the route");
        assertEquals(1L << 20, p.maxBytes());

        assertEquals(12, f.highWatermark());
        assertEquals(11, f.lastStableOffset());
        assertEquals(3, f.logStartOffset());
        assertEquals(List.of(new AbortedTxn(4, 5)), f.aborted());
        assertEquals(List.of("v"), values(f));
    }

    @Test
    void fetchUncommittedAsksForTheHighWatermark() {
        a.on((node, request) -> request instanceof Request.Metadata
                ? Fx.metadata(List.of(node), Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L)))
                : Fx.fetched(STREAM_ID, 0, ErrorCode.NONE, ByteString.EMPTY, 0, 0, 0, List.of(), null));
        client = new LotseClient(Fx.config(a).build());
        client.fetchUncommitted("s", 0, 0, Duration.ZERO.plusMillis(1));
        assertEquals(Messages.Isolation.UNCOMMITTED, a.requests(Request.Fetch.class).get(0).isolation());
    }

    @Test
    void anOffsetOutOfRangeSaysWhereThePartitionStarts() {
        a.on((node, request) -> request instanceof Request.Metadata
                ? Fx.metadata(List.of(node), Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L)))
                : Fx.fetched(STREAM_ID, 0, ErrorCode.OFFSET_OUT_OF_RANGE, ByteString.EMPTY, 50, 50, 40, List.of(), null));
        client = new LotseClient(Fx.config(a).build());
        ClientException.OffsetOutOfRange e = assertThrows(
                ClientException.OffsetOutOfRange.class, () -> client.fetch("s", 0, 3, Duration.ofMillis(1)));
        assertEquals(40, e.logStart());
        assertEquals(3, e.offset());
        assertEquals("s", e.stream());
        assertEquals(1, a.count(Request.Fetch.class), "not retried");
    }

    @Test
    void aRetriableFetchErrorFollowsTheLeaderHint() {
        List<FakeNode> both = List.of(a, b);
        a.on((node, request) -> request instanceof Request.Metadata
                ? Fx.metadata(both, Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L, 2L)))
                : Fx.fetched(STREAM_ID, 0, ErrorCode.NOT_LEADER, ByteString.EMPTY, 0, 0, 0, List.of(), new LeaderHint(2, 6)));
        b.on((node, request) -> Fx.fetched(STREAM_ID, 0, ErrorCode.NONE, data(0, 0, false, "from b"), 1, 1, 0, List.of(), null));
        client = new LotseClient(Fx.config(a).build());
        assertEquals(List.of("from b"), values(client.fetch("s", 0, 0, Duration.ofMillis(1))));
        assertEquals(1, a.count(Request.Metadata.class), "the hint was enough");
        assertEquals(6, b.requests(Request.Fetch.class).get(0).resources().get(0).partitions().get(0).leaderTerm());
    }

    @Test
    void aFetchThatNeverGetsALeaderGivesUp() {
        a.on((node, request) -> request instanceof Request.Metadata
                ? Fx.metadata(List.of(node), Fx.stream("s", STREAM_ID, Fx.partition(0, 0, 0)))
                : null);
        client = new LotseClient(Fx.config(a).retryFor(Duration.ofMillis(150)).build());
        assertThrows(ClientException.GaveUp.class, () -> client.fetch("s", 0, 0, Duration.ofMillis(1)));
    }

    @Test
    void theFetchedRecordsAreAvailableWithoutFiltering() {
        ByteString records = concat(data(0, 7, true, "t"), marker(1, 7, ControlType.ABORT));
        Fetched f = fetched(records, List.of(new AbortedTxn(7, 0)));
        assertEquals(records, f.records());
        assertTrue(f.committedBatches().isEmpty());
        List<Batch> raw = new ArrayList<>(Batch.parseAll(f.records()));
        assertEquals(2, raw.size());
    }

    // ---- batches the Rust side wrote ----

    private static ByteString vector(String label) {
        for (String[] v : dev.lotse.record.Vectors.lines("batches.hex")) {
            if (v[0].equals(label)) {
                return ByteString.copyOf(dev.lotse.record.Vectors.hex(v[1]));
            }
        }
        throw new AssertionError("no vector " + label);
    }

    /**
     * The transaction batches and markers in {@code batches.hex} were written by the Rust code. This is the closest
     * the tests come to a read_committed fetch of a Rust-written transaction without a node: a real transaction
     * needs a Rust client against a running cluster, which this repository's tests do not start for the SDK.
     */
    @Test
    void controlBatchesWrittenByRustAreSkippedAndItsTransactionalDataIsKept() {
        ByteString records = concat(
                vector("transactional"), // producer 17, offset 200
                vector("raft-noop"),
                vector("raft-config"),
                vector("control-commit")); // producer 17
        Fetched committed = fetched(records, List.of());
        assertEquals(1, committed.committedBatches().size());
        assertEquals(200, committed.committedBatches().get(0).baseOffset());
    }

    @Test
    void aTransactionOfRustsProducerStaysHiddenUntilItsOwnAbortMarker() {
        // The abort marker in the vectors belongs to producer 18, so producer 17's transaction is still open.
        ByteString records = concat(vector("transactional"), vector("control-abort"));
        assertTrue(fetched(records, List.of(new AbortedTxn(17, 200))).committedBatches().isEmpty());
        assertEquals(1, fetched(records, List.of()).committedBatches().size());
    }
}

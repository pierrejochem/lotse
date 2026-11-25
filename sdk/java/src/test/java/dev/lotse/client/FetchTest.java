package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.GroupIndex;
import dev.lotse.proto.LeaderHint;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.AbortedTxn;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.Batch;
import dev.lotse.record.BatchBuilder;
import dev.lotse.record.ByteString;
import dev.lotse.record.ControlBatches;
import dev.lotse.record.ControlType;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Fetch requests, retries, and what a read_committed reader sees. */
class FetchTest {
    private static final long STREAM_ID = 7;

    private FakeNode a;
    private FakeNode b;
    private FakeNode c;
    private LotseClient client;

    @BeforeEach
    void start() throws IOException {
        a = new FakeNode(1);
        b = new FakeNode(2);
        c = new FakeNode(3);
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        a.close();
        b.close();
        c.close();
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

    /** Node 1 leads in zone a, node 2 follows in zone b; node 2 answers with {@code followerCode}. */
    private void zones(ErrorCode followerCode) {
        List<FakeNode> both = List.of(a, b);
        FakeNode.Handler h = (node, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadataInZones(both, List.of("a", "b"), Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L, 2L)));
            }
            ErrorCode code = node.id == 2 ? followerCode : ErrorCode.NONE;
            if (request instanceof Request.ListOffsets) {
                return new dev.lotse.proto.Response.ListOffsets(List.of(new Messages.ListOffsetsResourceResponse(
                        STREAM_ID, List.of(new Messages.ListOffsetsPartitionResponse(0, code, node.id * 10L, -1, null)))));
            }
            ByteString records = code.isOk() ? data(0, 0, false, "from " + node.id) : ByteString.EMPTY;
            return Fx.fetched(STREAM_ID, 0, code, records, 1, 1, 0, List.of(), null);
        };
        a.on(h);
        b.on(h);
    }

    @Test
    void aClientInAZoneFetchesFromTheReplicaThere() {
        zones(ErrorCode.NONE);
        client = new LotseClient(Fx.config(a).zone("b").build());
        assertEquals(List.of("from 2"), values(client.fetch("s", 0, 0, Duration.ofMillis(1))));
        assertEquals(List.of("from 2"), values(client.fetch("s", 0, 0, Duration.ofMillis(1))));
        assertEquals(0, a.count(Request.Fetch.class), "the leader is in another zone");
        assertTrue(b.requests(Request.Fetch.class).get(0).allowFollower(), "the follower is asked for what it has");
        client.close();
        // In the leader's zone, or in one without a replica, the leader serves, as without a zone.
        for (String zone : new String[] {"a", "c", null}) {
            client = new LotseClient(Fx.config(a).zone(zone).build());
            assertEquals(List.of("from 1"), values(client.fetch("s", 0, 0, Duration.ofMillis(1))), "zone " + zone);
            client.close();
        }
        assertTrue(a.requests(Request.Fetch.class).stream().noneMatch(Request.Fetch::allowFollower));
        client = null;
    }

    @Test
    void aClientInAZoneListsOffsetsAtTheReplicaThereAndLeavesOneThatLags() {
        zones(ErrorCode.NONE);
        client = new LotseClient(Fx.config(a).zone("b").build());
        assertEquals(20, client.listOffset("s", 0, OffsetAt.LATEST), "the follower in zone b answers");
        assertEquals(0, a.count(Request.ListOffsets.class));
        assertTrue(b.requests(Request.ListOffsets.class).get(0).allowFollower());
        client.close();
        // A follower that lags too far answers NOT_LEADER (ADR 0116): asked once, then left for the leader.
        zones(ErrorCode.NOT_LEADER);
        int before = b.count(Request.ListOffsets.class);
        client = new LotseClient(Fx.config(a).zone("b").build());
        assertEquals(10, client.listOffset("s", 0, OffsetAt.LATEST));
        assertEquals(10, client.listOffset("s", 0, OffsetAt.LATEST));
        assertEquals(before + 1, b.count(Request.ListOffsets.class));
        assertTrue(a.requests(Request.ListOffsets.class).stream().noneMatch(Request.ListOffsets::allowFollower));
        client.close();
        client = null;
    }

    @Test
    void aReplicaThatFailsIsLeftForTheLeader() {
        // An older node answers NOT_LEADER; one whose log starts later, OFFSET_OUT_OF_RANGE.
        for (ErrorCode code : List.of(ErrorCode.NOT_LEADER, ErrorCode.OFFSET_OUT_OF_RANGE)) {
            zones(code);
            int before = b.count(Request.Fetch.class);
            client = new LotseClient(Fx.config(a).zone("b").build());
            assertEquals(List.of("from 1"), values(client.fetch("s", 0, 0, Duration.ofMillis(1))), code.toString());
            assertEquals(List.of("from 1"), values(client.fetch("s", 0, 0, Duration.ofMillis(1))), code.toString());
            assertEquals(before + 1, b.count(Request.Fetch.class), "asked once, then left: " + code);
            client.close();
        }
        client = null;
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

    // ---- zone reads: offsets and admin state in the client's zone, and back to the zone ----
    //
    // Node 1 (a) is in zone x and leads everything; nodes 2 (b) and 3 (c) are in zone y. Stream s (id 7) lives on
    // node 1 alone, stream t (id 8) on nodes 1 and 2. Node 1 coordinates, with all three voting.

    private static final LeaderHint HINT = new LeaderHint(1, 1);

    /** What every node heard, in order across the nodes. */
    private final List<FakeNode.Heard> heard = new CopyOnWriteArrayList<>();

    /** Indexes from pairs of group and index. */
    private static List<GroupIndex> at(long... pairs) {
        List<GroupIndex> out = new ArrayList<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.add(new GroupIndex(pairs[i], pairs[i + 1]));
        }
        return out;
    }

    /** The metadata of the zone tests, listing the nodes {@code listed}. */
    private static Response.Metadata zoneMetadata(FakeNode... listed) {
        return Fx.metadataInZones(
                List.of(listed),
                List.of("x", "y", "y"),
                Fx.stream("s", 7, Fx.partition(0, 1, 1, 1L)),
                Fx.stream("t", 8, Fx.partition(0, 1, 1, 1L, 2L)));
    }

    private static Response.Fetch zoneFetched(ErrorCode error, Long preferredReplica) {
        return new Response.Fetch(
                0,
                List.of(new Messages.FetchResourceResponse(
                        0,
                        List.of(new Messages.FetchPartitionResponse(
                                0, error, 0, 0, 0, List.of(), ByteString.EMPTY, null, preferredReplica)))));
    }

    /** The answer to a zone read, with {@code error} (and then the leader's hint) or without. */
    private static Response read(Request req, ErrorCode error) {
        LeaderHint hint = error.isOk() ? null : HINT;
        if (req instanceof Request.DescribeConfigs d) {
            List<Messages.ResourceConfigs> out = new ArrayList<>();
            for (long id : d.resourceIds()) {
                out.add(new Messages.ResourceConfigs(id, ErrorCode.NONE, List.of()));
            }
            return new Response.DescribeConfigs(error, hint, out);
        } else if (req instanceof Request.DescribeNodes) {
            return new Response.DescribeNodes(error, hint, List.of());
        } else if (req instanceof Request.DescribeUsers) {
            return new Response.DescribeUsers(error, hint, List.of());
        } else if (req instanceof Request.DescribeTokens) {
            return new Response.DescribeTokens(error, hint, List.of());
        } else if (req instanceof Request.DescribeCertificates) {
            return new Response.DescribeCertificates(error, hint, 0, List.of());
        } else if (req instanceof Request.DescribeCertificateRequests) {
            return new Response.DescribeCertificateRequests(error, hint, 0, List.of());
        } else if (req instanceof Request.DescribeUpgrade) {
            return new Response.DescribeUpgrade(error, hint, null, false, null, List.of());
        } else if (req instanceof Request.DescribeTenants) {
            return new Response.DescribeTenants(error, hint, List.of());
        } else if (req instanceof Request.DescribeRoleBindings) {
            return new Response.DescribeRoleBindings(error, hint, List.of());
        } else if (req instanceof Request.OffsetFetch o) {
            List<Messages.OffsetFetchResource> out = new ArrayList<>();
            for (Messages.TxnResource r : o.resources()) {
                List<Messages.OffsetFetchPartition> parts = new ArrayList<>();
                for (long partition : r.partitions()) {
                    parts.add(new Messages.OffsetFetchPartition(partition, error, 0, 1, null));
                }
                out.add(new Messages.OffsetFetchResource(r.resourceId(), parts));
            }
            return new Response.OffsetFetch(out);
        }
        throw new AssertionError("not a zone read: " + req);
    }

    /** What every node answers, unless a test says otherwise. */
    private Response common(Request req) {
        if (req instanceof Request.Metadata) {
            return zoneMetadata(a, b, c);
        } else if (req instanceof Request.FindCoordinator) {
            return new Response.FindCoordinator(ErrorCode.NONE, 1, "127.0.0.1", a.address().port(), null, List.of(1L, 2L, 3L));
        } else if (req instanceof Request.GroupHeartbeat) {
            return new Response.GroupHeartbeat(
                    0, ErrorCode.NONE, "m", 1, 1_000, List.of(new Messages.TxnResource(7, List.of(0L))));
        } else if (req instanceof Request.Fetch) {
            return zoneFetched(ErrorCode.NONE, null);
        } else if (req instanceof Request.ListOffsets) {
            return new Response.ListOffsets(List.of(new Messages.ListOffsetsResourceResponse(
                    0, List.of(new Messages.ListOffsetsPartitionResponse(0, ErrorCode.NONE, 0, -1, null)))));
        } else if (req instanceof Request.CommitOffsets co) {
            List<Messages.TxnResourceResult> out = new ArrayList<>();
            for (Messages.OffsetCommitResource r : co.resources()) {
                List<Messages.TxnPartitionResult> parts = new ArrayList<>();
                for (Messages.OffsetCommitPartition p : r.partitions()) {
                    parts.add(new Messages.TxnPartitionResult(p.partition(), ErrorCode.NONE));
                }
                out.add(new Messages.TxnResourceResult(r.resourceId(), parts));
            }
            return new Response.CommitOffsets(out);
        }
        return read(req, ErrorCode.NONE);
    }

    /** Every node answers {@link #common} and records what it hears in {@link #heard}. */
    private void zoneNodes() {
        for (FakeNode n : List.of(a, b, c)) {
            n.heardIn(heard).on((node, req) -> common(req));
        }
    }

    private LotseClient zoneClient(String zone) {
        client = new LotseClient(Fx.config(a).zone(zone).build());
        return client;
    }

    /** The requests of {@code type} that were heard, as (node, read_at_least). */
    private List<Map.Entry<Long, List<GroupIndex>>> heardOf(Class<? extends Request> type) {
        List<Map.Entry<Long, List<GroupIndex>>> out = new ArrayList<>();
        for (FakeNode.Heard h : heard) {
            if (type.isInstance(h.request())) {
                out.add(Map.entry(h.node(), h.readAtLeast()));
            }
        }
        return out;
    }

    /** The nodes that heard the requests of {@code type}, in order. */
    private List<Long> nodesOf(Class<? extends Request> type) {
        return heardOf(type).stream().map(Map.Entry::getKey).toList();
    }

    /** Indexes only rise, and every connection's answers raise the one set. */
    @Test
    void anIndexOnlyRises() {
        Zone.Indexes i = new Zone.Indexes();
        assertEquals(at(1, 0, 2, 0), i.snapshot());
        i.raise(at(1, 5, 2, 3));
        i.raise(at(1, 2));
        assertEquals(at(1, 5, 2, 3), i.snapshot());
        i.raise(at(2, -1));
        assertEquals(at(1, 5, 2, -1), i.snapshot(), "indexes are u64");
    }

    @Test
    void theZoneNodeIsTheLowestIdInTheZone() {
        Map<Long, String> zones = Map.of(1L, "x", 2L, "y", 3L, "y");
        assertEquals(2L, Zone.zoneNode("y", List.of(3L, 2L, 1L), zones));
        assertEquals(null, Zone.zoneNode("x", List.of(2L, 3L), zones));
        assertEquals(null, Zone.zoneNode("z", zones.keySet(), zones));
    }

    /**
     * Every {@code applied} the client hears raises its index of that group; a lower one (from a follower further
     * behind) leaves it as it is.
     */
    @Test
    void appliedRaisesTheIndexAndNeverLowersIt() {
        zoneNodes();
        a.applied(r -> r instanceof Request.Metadata ? at(1, 5, 2, 3) : List.of());
        List<List<GroupIndex>> answers = new ArrayList<>(List.of(at(1, 2), at(1, 9)));
        b.applied(r -> answers.isEmpty() ? List.of() : answers.remove(0));
        zoneClient("y").metadata();
        for (int i = 0; i < 3; i++) {
            client.describeUsers();
        }
        assertEquals(
                List.of(Map.entry(2L, at(1, 5, 2, 3)), Map.entry(2L, at(1, 5, 2, 3)), Map.entry(2L, at(1, 9, 2, 3))),
                heardOf(Request.DescribeUsers.class),
                "2 is below 5 and leaves it; 9 raises it");
    }

    /** Each of the nine admin reads goes to the lowest-id node in the client's zone with both groups' indexes. */
    @Test
    void anAdminReadGoesToTheZoneWithReadAtLeast() {
        zoneNodes();
        a.applied(r -> r instanceof Request.Metadata ? at(1, 4) : List.of());
        zoneClient("y").metadata();
        client.describeConfigs("s");
        client.describeNodes();
        client.describeUsers();
        client.describeTokens(null);
        client.describeCertificates();
        client.describeCertificateRequests();
        client.describeUpgrade();
        client.describeTenants();
        client.describeRoleBindings();
        List<FakeNode.Heard> reads =
                heard.stream().filter(h -> !(h.request() instanceof Request.Metadata)).toList();
        assertEquals(9, reads.size(), reads.toString());
        for (FakeNode.Heard h : reads) {
            assertEquals(Map.entry(2L, at(1, 4, 2, 0)), Map.entry(h.node(), h.readAtLeast()), h.request().toString());
        }
    }

    /**
     * A zone node that has not applied what the client saw refuses with NOT_LEADER: that read goes to the leader with
     * the same tag, which a leader deposed without knowing it refuses (ADR 0141), and the next read tries the zone
     * again.
     */
    @Test
    void aRefusedZoneReadGoesToTheLeaderAndTheNextTriesTheZoneAgain() {
        zoneNodes();
        a.applied(r -> at(1, 6));
        AtomicBoolean refused = new AtomicBoolean();
        b.on((node, r) -> r instanceof Request.DescribeTenants && !refused.getAndSet(true)
                ? read(r, ErrorCode.NOT_LEADER)
                : common(r));
        zoneClient("y").metadata();
        client.describeTenants();
        client.describeTenants();
        List<GroupIndex> tagged = at(1, 6, 2, 0);
        assertEquals(
                List.of(Map.entry(2L, tagged), Map.entry(1L, tagged), Map.entry(2L, tagged)),
                heardOf(Request.DescribeTenants.class));
    }

    /**
     * A node of an older release ignores the tag and refuses the leader-only reads with NOT_LEADER: every such read
     * ends at the leader.
     */
    @Test
    void anOlderZoneNodeSendsTheReadToTheLeader() {
        zoneNodes();
        b.on((node, r) -> r instanceof Request.DescribeUpgrade ? read(r, ErrorCode.NOT_LEADER) : common(r));
        zoneClient("y").metadata();
        client.describeUpgrade();
        client.describeUpgrade();
        assertEquals(List.of(2L, 1L, 2L, 1L), nodesOf(Request.DescribeUpgrade.class));
    }

    /** A group member reads its committed offsets from the lowest-id coordinator voter in its zone. */
    @Test
    void offsetFetchGoesToACoordinatorVoterInTheZone() {
        zoneNodes();
        a.applied(r -> r instanceof Request.Metadata
                ? at(1, 4)
                : r instanceof Request.FindCoordinator || r instanceof Request.GroupHeartbeat ? at(2, 6) : List.of());
        GroupConsumer group = zoneClient("y").joinGroup("g", List.of("s"), GroupConfig.defaults());
        group.poll(Duration.ofMillis(10));
        assertEquals(Map.of("s", Map.of(0, 0L)), group.positions(), "the committed offset");
        assertEquals(List.of(Map.entry(2L, at(1, 4, 2, 6))), heardOf(Request.OffsetFetch.class));
    }

    /** A fetch that goes to the leader names the client's zone, so the leader can point back to it; one to the zone does not. */
    @Test
    void aFetchToTheLeaderNamesTheZone() {
        zoneNodes();
        zoneClient("y").fetch("s", 0, 0, Duration.ZERO);
        client.fetch("t", 0, 0, Duration.ZERO);
        List<String> fetches = new ArrayList<>();
        for (FakeNode.Heard h : heard) {
            if (h.request() instanceof Request.Fetch f) {
                fetches.add(h.node() + " " + f.zone() + " " + f.allowFollower());
            }
        }
        assertEquals(List.of("1 y false", "2 null true"), fetches);
    }

    /** The reads of {@code t}, as "node kind allow_follower". */
    private List<String> streamReads() {
        List<String> out = new ArrayList<>();
        for (FakeNode.Heard h : heard) {
            if (h.request() instanceof Request.Fetch f) {
                out.add(h.node() + " fetch " + f.allowFollower());
            } else if (h.request() instanceof Request.ListOffsets l) {
                out.add(h.node() + " list " + l.allowFollower());
            }
        }
        return out;
    }

    /**
     * After a replica in the zone failed, the leader serves; once it names that replica as current again, the next
     * fetch and offset lookup go back there, without asking for metadata.
     */
    @Test
    void preferredReplicaSendsTheNextFetchBackToTheZone() {
        zoneNodes();
        a.on((node, r) -> r instanceof Request.Fetch ? zoneFetched(ErrorCode.NONE, 2L) : common(r));
        AtomicBoolean refused = new AtomicBoolean();
        b.on((node, r) -> r instanceof Request.Fetch && !refused.getAndSet(true)
                ? zoneFetched(ErrorCode.NOT_LEADER, null)
                : common(r));
        zoneClient("y").fetch("t", 0, 0, Duration.ZERO);
        client.fetch("t", 0, 0, Duration.ZERO);
        client.listOffset("t", 0, OffsetAt.LATEST);
        assertEquals(List.of("2 fetch true", "1 fetch false", "2 fetch true", "2 list true"), streamReads());
        assertEquals(1, nodesOf(Request.Metadata.class).size(), "metadata only for the first route");
    }

    /** A client without a zone sends neither tag, and its reads go to the leaders. */
    @Test
    void noZoneNoTags() {
        zoneNodes();
        a.applied(r -> at(1, 4, 2, 6));
        for (FakeNode n : List.of(b, c)) {
            n.on((node, r) -> {
                if (r instanceof Request.Metadata || r instanceof Request.FindCoordinator) {
                    return common(r);
                }
                throw new AssertionError("only the leader is asked, not for " + r);
            });
        }
        zoneClient(null).describeUpgrade();
        client.describeUsers();
        client.fetch("t", 0, 0, Duration.ZERO);
        client.listOffset("t", 0, OffsetAt.LATEST);
        client.joinGroup("g", List.of("s"), GroupConfig.defaults()).poll(Duration.ofMillis(10));
        assertTrue(heard.stream().anyMatch(h -> h.request() instanceof Request.OffsetFetch));
        for (FakeNode.Heard h : heard) {
            assertEquals(List.of(), h.readAtLeast(), h.request().toString());
            if (h.request() instanceof Request.Fetch f) {
                assertEquals(null, f.zone());
                assertTrue(!f.allowFollower());
            }
            if (!(h.request() instanceof Request.Metadata || h.request() instanceof Request.FindCoordinator)) {
                assertEquals(1L, h.node(), h.request().toString());
            }
        }
    }

    /**
     * A node the latest metadata no longer lists is no zone node, for admin reads and offset reads alike: here node 2
     * left, and the reads go to node 3 without trying node 2 first.
     */
    @Test
    void zoneReadsSkipANodeTheLatestMetadataLeftOut() {
        zoneNodes();
        AtomicInteger answers = new AtomicInteger();
        a.on((node, r) -> r instanceof Request.Metadata
                ? (answers.incrementAndGet() == 1 ? zoneMetadata(a, b, c) : zoneMetadata(a, c))
                : common(r));
        zoneClient("y").metadata();
        client.metadata();
        client.describeUsers();
        client.joinGroup("g", List.of("s"), GroupConfig.defaults()).poll(Duration.ofMillis(10));
        assertEquals(List.of(3L), nodesOf(Request.DescribeUsers.class));
        assertEquals(List.of(3L), nodesOf(Request.OffsetFetch.class));
    }

    /** Node 2 refuses the first fetch of t, then serves; node 1 answers its n-th fetch with what {@code leader} gives. */
    private List<String> afterAFallback(IntFunction<Response> leader) {
        zoneNodes();
        AtomicInteger count = new AtomicInteger();
        a.on((node, r) -> r instanceof Request.Fetch ? leader.apply(count.incrementAndGet()) : common(r));
        AtomicBoolean refused = new AtomicBoolean();
        b.on((node, r) -> r instanceof Request.Fetch && !refused.getAndSet(true)
                ? zoneFetched(ErrorCode.NOT_LEADER, null)
                : common(r));
        zoneClient("y");
        try {
            client.fetch("t", 0, 0, Duration.ZERO);
        } catch (ClientException e) {
            // the leader's answer to it is the test's to choose
        }
        client.fetch("t", 0, 0, Duration.ZERO);
        return streamReads();
    }

    /** A leader answer with an error says nothing about the replicas: its preferred replica leaves the next fetch with the leader. */
    @Test
    void preferredReplicaOnAnErrorIsIgnored() {
        List<String> seen = afterAFallback(n -> n == 1
                ? zoneFetched(ErrorCode.OFFSET_OUT_OF_RANGE, 2L)
                : zoneFetched(ErrorCode.NONE, null));
        assertEquals(List.of("2 fetch true", "1 fetch false", "1 fetch false"), seen);
    }

    /**
     * A replica the client has no address for cannot be asked: the fallback stays, rather than sending the next fetch
     * to the replica that failed.
     */
    @Test
    void preferredReplicaWithoutAnAddressLeavesTheFetchOnTheLeader() {
        List<String> seen = afterAFallback(n -> zoneFetched(ErrorCode.NONE, n == 1 ? 4L : null));
        assertEquals(List.of("2 fetch true", "1 fetch false", "1 fetch false"), seen);
    }

    /**
     * The zone voter refuses an {@code OffsetFetch}, or the call to it fails: that read goes to the coordinator with
     * the same tag, and the next tries the zone again.
     */
    @Test
    void aRefusedOffsetFetchGoesToTheCoordinatorAndTheNextTriesTheZoneAgain() {
        for (boolean failedCall : new boolean[] {false, true}) {
            heard.clear();
            zoneNodes();
            a.applied(r -> r instanceof Request.Metadata
                    ? at(1, 4)
                    : r instanceof Request.FindCoordinator ? at(2, 6) : List.of());
            AtomicBoolean refused = new AtomicBoolean();
            b.on((node, r) -> {
                if (r instanceof Request.OffsetFetch && !refused.getAndSet(true)) {
                    return failedCall ? new Response.Error(ErrorCode.INVALID_REQUEST) : read(r, ErrorCode.NOT_LEADER);
                }
                return common(r);
            });
            zoneClient("y").metadata();
            Request req = new Request.OffsetFetch(
                    "g", true, List.of(new Messages.TxnResource(7, List.of(0L))), Messages.DEFAULT_TENANT);
            for (int i = 0; i < 2; i++) {
                Response r = client.callCoordinatorRead("g", req);
                assertTrue(r instanceof Response.OffsetFetch && !Zone.refused(r), r.toString());
            }
            List<GroupIndex> tagged = at(1, 4, 2, 6);
            assertEquals(
                    List.of(Map.entry(2L, tagged), Map.entry(1L, tagged), Map.entry(2L, tagged)),
                    heardOf(Request.OffsetFetch.class),
                    "failed call: " + failedCall);
            client.close();
        }
        client = null;
    }
}

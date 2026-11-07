package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.TxnPartitionResult;
import dev.lotse.proto.Messages.TxnResource;
import dev.lotse.proto.Messages.TxnResourceResult;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.ByteString;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A consumer group member against a scripted coordinator and leader, as the Rust client's member behaves. */
class GroupConsumerTest {
    private static final long STREAM_ID = 7;
    private static final Duration WAIT = Duration.ofMillis(20);

    private FakeNode node;
    private LotseClient client;
    /** The answer to the n-th heartbeat, counted from 1. */
    private volatile Function<Integer, Response> heartbeat = n -> assign(0);
    private final AtomicInteger heartbeats = new AtomicInteger();
    /** Committed offsets per partition; absent: none. */
    private final Map<Long, Long> committed = new ConcurrentHashMap<>();
    /** Fetch answers per partition; absent: two records from the offset asked for. */
    private final Map<Long, Function<Long, Response>> fetch = new ConcurrentHashMap<>();
    private volatile ErrorCode commitError = ErrorCode.NONE;

    @BeforeEach
    void start() throws IOException {
        node = new FakeNode(1);
        node.on(this::answer);
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        node.close();
    }

    private static Response assign(long... partitions) {
        List<Long> parts = new ArrayList<>();
        for (long p : partitions) {
            parts.add(p);
        }
        // An interval of 0: every poll heartbeats.
        return new Response.GroupHeartbeat(
                0, ErrorCode.NONE, "m1", 3, 0, List.of(new TxnResource(STREAM_ID, parts)));
    }

    /** {@link #assign} with the next heartbeat due only after {@code ms}. */
    private static Response assignEvery(long ms, long... partitions) {
        Response.GroupHeartbeat r = (Response.GroupHeartbeat) assign(partitions);
        return new Response.GroupHeartbeat(0, ErrorCode.NONE, r.memberId(), r.memberEpoch(), ms, r.assignment());
    }

    private static Response failed(ErrorCode code) {
        return new Response.GroupHeartbeat(0, code, "", 0, 0, null);
    }

    private Response answer(FakeNode n, Request request) {
        if (request instanceof Request.Metadata) {
            return Fx.metadata(
                    List.of(n),
                    Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 1, 1L), Fx.partition(1, 1, 1, 1L)));
        }
        if (request instanceof Request.FindCoordinator) {
            return Fx.coordinator(n);
        }
        if (request instanceof Request.GroupHeartbeat h) {
            if (h.memberEpoch() == -1) {
                return failed(ErrorCode.NONE);
            }
            return heartbeat.apply(heartbeats.incrementAndGet());
        }
        if (request instanceof Request.OffsetFetch f) {
            List<Messages.OffsetFetchResource> out = new ArrayList<>();
            for (TxnResource r : f.resources()) {
                List<Messages.OffsetFetchPartition> parts = new ArrayList<>();
                for (long p : r.partitions()) {
                    parts.add(new Messages.OffsetFetchPartition(
                            p, ErrorCode.NONE, committed.getOrDefault(p, Messages.NO_OFFSET), 0, null));
                }
                out.add(new Messages.OffsetFetchResource(r.resourceId(), parts));
            }
            return new Response.OffsetFetch(out);
        }
        if (request instanceof Request.Fetch f) {
            Messages.FetchPartition p = f.resources().get(0).partitions().get(0);
            Function<Long, Response> script = fetch.get(p.partition());
            if (script != null) {
                return script.apply(p.fetchOffset());
            }
            ByteString records = FetchTest.data(p.fetchOffset(), 0, false, "r" + p.fetchOffset(), "r" + (p.fetchOffset() + 1));
            return Fx.fetched(STREAM_ID, (int) p.partition(), ErrorCode.NONE, records, 100, 100, 0, List.of(), null);
        }
        if (request instanceof Request.CommitOffsets c) {
            return commitAnswer(c.resources());
        }
        if (request instanceof Request.AddOffsetsToTxn a) {
            return commitAnswer(a.resources());
        }
        if (request instanceof Request.ListOffsets l) {
            Messages.ListOffsetsPartition p = l.resources().get(0).partitions().get(0);
            return new Response.ListOffsets(List.of(new Messages.ListOffsetsResourceResponse(
                    STREAM_ID,
                    List.of(new Messages.ListOffsetsPartitionResponse(
                            p.partition(), ErrorCode.NONE, p.timestamp() == -1 ? 42 : 30, -1, null)))));
        }
        if (request instanceof Request.InitProducer) {
            return Fx.producer(100, 1);
        }
        if (request instanceof Request.AddPartitionsToTxn a) {
            return new Response.AddPartitionsToTxn(List.of(new TxnResourceResult(
                    a.resources().get(0).resourceId(), List.of(new TxnPartitionResult(0, ErrorCode.NONE)))));
        }
        if (request instanceof Request.Produce p) {
            return Fx.produced(p.resources().get(0).resourceId(), 0, 0);
        }
        if (request instanceof Request.EndTxn e) {
            return new Response.EndTxn(ErrorCode.NONE, e.producerId(), e.producerEpoch() + 1);
        }
        throw new AssertionError("unexpected " + request);
    }

    private Response commitAnswer(List<Messages.OffsetCommitResource> resources) {
        List<TxnResourceResult> out = new ArrayList<>();
        for (Messages.OffsetCommitResource r : resources) {
            List<TxnPartitionResult> parts = new ArrayList<>();
            for (Messages.OffsetCommitPartition p : r.partitions()) {
                parts.add(new TxnPartitionResult(p.partition(), commitError));
            }
            out.add(new TxnResourceResult(r.resourceId(), parts));
        }
        return new Response.CommitOffsets(out);
    }

    private LotseClient client() {
        client = new LotseClient(Fx.config(node).transactionalId("t").retryFor(Duration.ofMillis(400)).build());
        return client;
    }

    private List<Request.GroupHeartbeat> heartbeatsSent() {
        return node.requests(Request.GroupHeartbeat.class);
    }

    private static List<String> values(List<GroupRecord> records) {
        return records.stream().map(r -> r.record().value().toUtf8()).toList();
    }

    @Test
    void aMemberJoinsReadsFromTheCommittedOffsetAndCommitsWhereItGot() {
        committed.put(0L, 5L);
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.builder().autoCommit(false).build());
        assertEquals(List.of("r5", "r6"), values(g.poll(WAIT)));
        Request.GroupHeartbeat join = heartbeatsSent().get(0);
        assertEquals(0, join.memberEpoch());
        assertEquals("", join.memberId());
        assertEquals(List.of(STREAM_ID), join.subscribed());
        assertNull(join.subscribedPattern());
        assertEquals("m1", g.memberId());
        assertEquals(3, g.memberEpoch());
        assertEquals(Map.of("s", Map.of(0, 7L)), g.positions());
        g.commit();
        Request.CommitOffsets c = node.requests(Request.CommitOffsets.class).get(0);
        assertEquals("m1", c.memberId());
        assertEquals(3, c.memberEpoch());
        assertEquals(7, c.resources().get(0).partitions().get(0).offset());
        assertTrue(node.requests(Request.OffsetFetch.class).get(0).requireStable(), "waits for open transactions");
    }

    @Test
    void aMemberHeartbeatsAgainOnlyWhenDueAndReportsWhatItOwns() throws Exception {
        heartbeat = n -> new Response.GroupHeartbeat(
                0, ErrorCode.NONE, "m1", 3, 200, List.of(new TxnResource(STREAM_ID, List.of(0L))));
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.defaults());
        g.poll(WAIT);
        g.poll(WAIT);
        assertEquals(1, heartbeatsSent().size(), "not due yet");
        Thread.sleep(250);
        g.poll(WAIT);
        assertEquals(2, heartbeatsSent().size());
        Request.GroupHeartbeat second = heartbeatsSent().get(1);
        assertEquals(3, second.memberEpoch());
        assertEquals("m1", second.memberId());
        assertNull(second.subscribed(), "the subscription goes with the join only");
        assertEquals(List.of(new TxnResource(STREAM_ID, List.of(0L))), second.owned());
    }

    @Test
    void aRevokedPartitionIsCommittedThenReportedReleasedAtOnce() {
        // The revoking answer schedules the next heartbeat 10 s later; the release must not wait for it.
        heartbeat = n -> n == 1 ? assign(0, 1) : assignEvery(10_000, 1);
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.defaults());
        g.poll(WAIT);
        g.poll(WAIT);
        assertEquals(Map.of("s", new TreeSet<>(List.of(1))), g.assignment());
        g.poll(WAIT);
        assertEquals(3, heartbeatsSent().size(), "released with the next poll");
        List<Request> all = node.requests(Request.class);
        int commit = -1;
        int release = -1;
        for (int i = 0; i < all.size(); i++) {
            if (all.get(i) instanceof Request.CommitOffsets c && commit < 0) {
                assertEquals(0, c.resources().get(0).partitions().get(0).partition());
                commit = i;
            }
            if (all.get(i) instanceof Request.GroupHeartbeat h && h.owned() != null
                    && h.owned().equals(List.of(new TxnResource(STREAM_ID, List.of(1L))))) {
                release = release < 0 ? i : release;
            }
        }
        assertTrue(commit >= 0, "the revoked partition's position is committed");
        assertTrue(release > commit, "and only then reported released");
    }

    @Test
    void withoutAutoCommitARevokedPartitionIsReleasedWithoutACommit() {
        heartbeat = n -> n == 1 ? assign(0, 1) : assign(1);
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.builder().autoCommit(false).build());
        g.poll(WAIT);
        g.poll(WAIT);
        g.poll(WAIT);
        assertEquals(0, node.requests(Request.CommitOffsets.class).size());
    }

    @Test
    void aMemberWithoutAHeartbeatForTheSessionTimeoutDropsEverythingAndJoinsAgain() throws Exception {
        heartbeat = n -> n == 1 ? assign(0) : n < 100 ? failed(ErrorCode.COORDINATOR_NOT_AVAILABLE) : assign(0);
        GroupConsumer g = client().joinGroup(
                "g", List.of("s"), GroupConfig.builder().sessionTimeout(Duration.ofMillis(300)).build());
        g.poll(WAIT);
        assertEquals(Map.of("s", new TreeSet<>(List.of(0))), g.assignment());
        Thread.sleep(350);
        g.poll(WAIT);
        assertEquals(Map.of(), g.assignment(), "the coordinator may have given it away");
        assertEquals("", g.memberId());
        Request.GroupHeartbeat last = heartbeatsSent().get(heartbeatsSent().size() - 1);
        assertEquals(0, last.memberEpoch(), "joins again as a new member");
        assertEquals(0, node.requests(Request.CommitOffsets.class).size(), "no commit for what was lost");
    }

    @Test
    void aFetchThatOutlastsTheSessionHandsOutNothing() {
        // The fetch answer comes after the session timeout: the coordinator may have given the partition away.
        fetch.put(0L, offset -> {
            try {
                Thread.sleep(400);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Fx.fetched(
                    STREAM_ID, 0, ErrorCode.NONE, FetchTest.data(offset, 0, false, "late"), 100, 100, 0, List.of(), null);
        });
        GroupConsumer g = client().joinGroup(
                "g", List.of("s"), GroupConfig.builder().sessionTimeout(Duration.ofMillis(300)).build());
        assertEquals(List.of(), g.poll(Duration.ofMillis(50)));
        assertEquals(Map.of(), g.assignment());
        assertEquals("", g.memberId());
    }

    @Test
    void anUnknownMemberJoinsAgain() {
        heartbeat = n -> n == 2 ? failed(ErrorCode.UNKNOWN_MEMBER_ID) : assign(0);
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.defaults());
        g.poll(WAIT);
        g.poll(WAIT);
        assertEquals(Map.of(), g.assignment());
        g.poll(WAIT);
        Request.GroupHeartbeat third = heartbeatsSent().get(2);
        assertEquals(0, third.memberEpoch());
        assertEquals("", third.memberId());
        assertEquals(List.of(STREAM_ID), third.subscribed());
    }

    @Test
    void aFencedInstanceStaysOut() {
        heartbeat = n -> failed(ErrorCode.FENCED_INSTANCE_ID);
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.builder().instanceId("i1").build());
        ClientException.Broker e = assertThrows(ClientException.Broker.class, () -> g.poll(WAIT));
        assertEquals(ErrorCode.FENCED_INSTANCE_ID, e.code());
        assertEquals("i1", heartbeatsSent().get(0).instanceId());
        assertThrows(ClientException.Broker.class, () -> g.poll(WAIT));
        assertEquals(1, heartbeatsSent().size(), "it does not join again");
    }

    @Test
    void aPartitionWithoutACommittedOffsetStartsWhereTheConfigSays() {
        GroupConsumer latest = client().joinGroup(
                "g", List.of("s"), GroupConfig.builder().startFrom(OffsetAt.LATEST).build());
        latest.poll(WAIT);
        assertEquals(Map.of("s", Map.of(0, 44L)), latest.positions(), "42, then the two records read");
        Messages.ListOffsetsPartition asked =
                node.requests(Request.ListOffsets.class).get(0).resources().get(0).partitions().get(0);
        assertEquals(-1, asked.timestamp());
        assertEquals(Messages.Isolation.COMMITTED, node.requests(Request.ListOffsets.class).get(0).isolation());
        GroupConsumer earliest = client.joinGroup("g2", List.of("s"), GroupConfig.defaults());
        earliest.poll(WAIT);
        assertEquals(Map.of("s", Map.of(0, 2L)), earliest.positions(), "from 0, without asking");
        assertEquals(1, node.requests(Request.ListOffsets.class).size());
    }

    @Test
    void aPositionRetentionDeletedGoesOnFromTheLogStart() {
        committed.put(0L, 3L);
        fetch.put(0L, offset -> offset < 10
                ? Fx.fetched(STREAM_ID, 0, ErrorCode.OFFSET_OUT_OF_RANGE, ByteString.EMPTY, 100, 100, 10, List.of(), null)
                : Fx.fetched(STREAM_ID, 0, ErrorCode.NONE, FetchTest.data(offset, 0, false, "x"), 100, 100, 10, List.of(), null));
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.defaults());
        assertEquals(List.of(), g.poll(WAIT));
        assertEquals(Map.of("s", Map.of(0, 10L)), g.positions());
        assertEquals(List.of("x"), values(g.poll(WAIT)));
    }

    @Test
    void rewindForgetsThePositions() {
        committed.put(0L, 5L);
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.builder().autoCommit(false).build());
        g.poll(WAIT);
        g.rewind();
        assertEquals(Map.of(), g.positions());
        assertEquals(List.of("r5", "r6"), values(g.poll(WAIT)), "from the committed offset again");
    }

    @Test
    void closingCommitsAndLeaves() {
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.defaults());
        g.poll(WAIT);
        g.close();
        assertEquals(2, node.requests(Request.CommitOffsets.class).get(0).resources().get(0).partitions().get(0).offset());
        Request.GroupHeartbeat leave = heartbeatsSent().get(heartbeatsSent().size() - 1);
        assertEquals(-1, leave.memberEpoch());
        assertEquals("m1", leave.memberId());
    }

    @Test
    void aPatternGoesWithTheJoinInsteadOfStreams() {
        GroupConsumer g = client().joinGroupMatching("g", "s.*", GroupConfig.defaults());
        g.poll(WAIT);
        Request.GroupHeartbeat join = heartbeatsSent().get(0);
        assertEquals("s.*", join.subscribedPattern());
        assertNull(join.subscribed());
        assertEquals(Map.of("s", new TreeSet<>(List.of(0))), g.assignment());
    }

    @Test
    void offsetsSentWithATransactionCarryTheMember() {
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.builder().autoCommit(false).build());
        g.poll(WAIT);
        try (Transaction txn = client.beginTransaction()) {
            txn.produce("s", 1, OutMessage.value("out"));
            txn.sendOffsets(g);
            txn.commit();
        }
        Request.AddOffsetsToTxn a = node.requests(Request.AddOffsetsToTxn.class).get(0);
        assertEquals("g", a.groupId());
        assertEquals("m1", a.memberId());
        assertEquals(3, a.memberEpoch());
        assertEquals("t", a.txnId());
        assertEquals(2, a.resources().get(0).partitions().get(0).offset());
        assertEquals(0, node.requests(Request.CommitOffsets.class).size(), "not committed on their own");
    }

    @Test
    void refusedOffsetsLeaveTheTransactionOnlyAnAbort() {
        GroupConsumer g = client().joinGroup("g", List.of("s"), GroupConfig.builder().autoCommit(false).build());
        g.poll(WAIT);
        commitError = ErrorCode.FENCED_MEMBER_EPOCH;
        try (Transaction txn = client.beginTransaction()) {
            txn.produce("s", 1, OutMessage.value("out"));
            assertThrows(ClientException.Broker.class, () -> txn.sendOffsets(g));
            assertThrows(ClientException.MustAbort.class, txn::commit);
            txn.abort();
        }
    }
}

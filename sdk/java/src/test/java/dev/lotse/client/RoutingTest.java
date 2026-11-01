package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.LeaderHint;
import dev.lotse.proto.Messages.ResourceMetadata;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** How the client finds a partition's leader and what it does with leader hints. */
class RoutingTest {
    private static final long STREAM_ID = 7;

    private FakeNode a;
    private FakeNode b;
    private FakeNode c;
    private List<FakeNode> all;
    private LotseClient client;

    @BeforeEach
    void start() throws IOException {
        a = new FakeNode(1);
        b = new FakeNode(2);
        c = new FakeNode(3);
        all = List.of(a, b, c);
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        all.forEach(FakeNode::close);
    }

    private LotseClient client(FakeNode... bootstrap) {
        client = new LotseClient(Fx.config(bootstrap).idempotent(false).build());
        return client;
    }

    /** A node that serves metadata from {@code view} and acknowledges every produce at {@code offset}. */
    private static FakeNode.Handler serving(List<FakeNode> nodes, ResourceMetadata view, long offset) {
        return (node, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(nodes, view);
            }
            if (request instanceof Request.Produce) {
                return Fx.produced(STREAM_ID, 0, offset);
            }
            throw new AssertionError("unexpected " + request);
        };
    }

    @Test
    void aRouteIsUsedUntilSomethingFails() {
        ResourceMetadata view = Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L, 2L, 3L));
        a.on(serving(all, view, 10));
        LotseClient lotse = client(a);
        for (int i = 0; i < 3; i++) {
            assertEquals(10, lotse.produce("s", 0, OutMessage.value("x")).baseOffset());
        }
        assertEquals(1, a.count(Request.Metadata.class), "metadata once, then the route");
        assertEquals(3, a.count(Request.Produce.class));
    }

    @Test
    void aHintWithTheSameOrANewerTermAndAnotherLeaderIsFollowedWithoutNewMetadata() {
        ResourceMetadata view = Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L, 2L, 3L));
        a.on((node, request) -> request instanceof Request.Metadata
                ? Fx.metadata(all, view)
                : Fx.produced(STREAM_ID, 0, ErrorCode.NOT_LEADER, 0, new LeaderHint(2, 5)));
        b.on(serving(all, view, 42));
        LotseClient lotse = client(a);

        assertEquals(42, lotse.produce("s", 0, OutMessage.value("x")).baseOffset());
        assertEquals(1, a.count(Request.Produce.class));
        assertEquals(1, b.count(Request.Produce.class));
        assertEquals(1, a.count(Request.Metadata.class) + b.count(Request.Metadata.class), "no new metadata");
        assertEquals(new LotseClient.Route(STREAM_ID, 2, 5), lotse.routeOf("s", 0));
    }

    @ParameterizedTest
    @ValueSource(longs = {6, -1L})
    void aNewerTermIsFollowed(long hintTerm) {
        // -1 is u64::MAX: newer than any term, not older than 5 (the term is unsigned)
        ResourceMetadata view = Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L, 2L, 3L));
        a.on((node, request) -> request instanceof Request.Metadata
                ? Fx.metadata(all, view)
                : Fx.produced(STREAM_ID, 0, ErrorCode.NOT_LEADER, 0, new LeaderHint(2, hintTerm)));
        b.on(serving(all, view, 1));
        LotseClient lotse = client(a);
        lotse.produce("s", 0, OutMessage.value("x"));
        assertEquals(1, b.count(Request.Produce.class));
        assertEquals(1, a.count(Request.Metadata.class));
        assertEquals(hintTerm, lotse.routeOf("s", 0).term());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void bothLeaderErrorsAreHandledAlike(int code) {
        ResourceMetadata view = Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L, 2L, 3L));
        a.on((node, request) -> request instanceof Request.Metadata
                ? Fx.metadata(all, view)
                : Fx.produced(STREAM_ID, 0, new ErrorCode(code), 0, new LeaderHint(2, 5)));
        b.on(serving(all, view, 1));
        client(a).produce("s", 0, OutMessage.value("x"));
        assertEquals(1, b.count(Request.Produce.class));
        assertEquals(1, a.count(Request.Metadata.class));
    }

    @Test
    void aHintWithAnOlderTermIsNotFollowedAndMetadataIsAskedAgain() {
        ResourceMetadata stale = Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L, 2L, 3L));
        ResourceMetadata fresh = Fx.stream("s", STREAM_ID, Fx.partition(0, 3, 9, 1L, 2L, 3L));
        a.on((node, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(all, node.count(Request.Metadata.class) == 1 ? stale : fresh);
            }
            // Node 2 claims to lead in term 4, which is older than the route's 5: it is lagging.
            return Fx.produced(STREAM_ID, 0, ErrorCode.NOT_LEADER, 0, new LeaderHint(2, 4));
        });
        c.on(serving(all, fresh, 77));
        LotseClient lotse = client(a);

        assertEquals(77, lotse.produce("s", 0, OutMessage.value("x")).baseOffset());
        assertEquals(0, b.count(Request.Produce.class), "the old hint was not followed");
        assertEquals(2, a.count(Request.Metadata.class), "the route was dropped and metadata asked again");
    }

    @ParameterizedTest
    @ValueSource(strings = {"same leader, same term", "same leader, newer term", "no hint"})
    void aHintNamingTheSameLeaderOrNoHintDropsTheRoute(String kind) {
        LeaderHint hint = switch (kind) {
            case "same leader, same term" -> new LeaderHint(1, 5);
            case "same leader, newer term" -> new LeaderHint(1, 99);
            default -> null;
        };
        ResourceMetadata view = Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 5, 1L, 2L, 3L));
        a.on((node, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(all, view);
            }
            // The first produce is refused; the second works.
            return node.count(Request.Produce.class) == 1
                    ? Fx.produced(STREAM_ID, 0, ErrorCode.NOT_LEADER, 0, hint)
                    : Fx.produced(STREAM_ID, 0, 3);
        });
        assertEquals(3, client(a).produce("s", 0, OutMessage.value("x")).baseOffset());
        assertEquals(2, a.count(Request.Metadata.class), "the route was dropped and metadata asked again");
        assertEquals(2, a.count(Request.Produce.class));
    }

    @Test
    void aHintFromNoRouteIsIgnoredAndTheRouteDropped() {
        // redirect() with no route held: nothing to switch, nothing to keep
        LotseClient lotse = client(a);
        lotse.redirect("s", 0, new LeaderHint(2, 5));
        assertNull(lotse.routeOf("s", 0));
    }

    @Test
    void leaderZeroAsksThePartitionsVotersDirectly() {
        ResourceMetadata none = Fx.stream("s", STREAM_ID, Fx.partition(0, 0, 0, 2L, 3L));
        ResourceMetadata knows = Fx.stream("s", STREAM_ID, Fx.partition(0, 3, 8, 2L, 3L));
        a.on((node, request) -> Fx.metadata(all, none));
        // The first voter does not know either, the second does.
        b.on((node, request) -> Fx.metadata(all, none));
        c.on(serving(all, knows, 5));
        LotseClient lotse = client(a);

        assertEquals(5, lotse.produce("s", 0, OutMessage.value("x")).baseOffset());
        assertEquals(1, b.count(Request.Metadata.class), "voter 2 was asked");
        assertEquals(1, c.count(Request.Metadata.class), "voter 3 was asked once");
        assertEquals(1, c.count(Request.Produce.class));
        assertEquals(new LotseClient.Route(STREAM_ID, 3, 8), lotse.routeOf("s", 0));
    }

    @Test
    void leaderZeroEverywhereIsRetriedAndThenGivenUp() {
        ResourceMetadata none = Fx.stream("s", STREAM_ID, Fx.partition(0, 0, 0, 2L, 3L));
        for (FakeNode n : all) {
            n.on((node, request) -> Fx.metadata(all, none));
        }
        client = new LotseClient(Fx.config(a).idempotent(false).retryFor(Duration.ofMillis(200)).build());
        ClientException.GaveUp e =
                assertThrows(ClientException.GaveUp.class, () -> client.produce("s", 0, OutMessage.value("x")));
        assertTrue(e.getMessage().contains("no leader"), e.getMessage());
        assertEquals(0, a.count(Request.Produce.class) + b.count(Request.Produce.class) + c.count(Request.Produce.class));
    }

    @Test
    void aNameOneBrokerDoesNotKnowIsAskedOfAllAndTheHighestResourceIdWins() {
        // Node 1 has not heard of the stream; 2 knows an old incarnation, 3 the newest.
        ResourceMetadata old = Fx.stream("s", 10, Fx.partition(0, 2, 1, 2L));
        ResourceMetadata newest = Fx.stream("s", 20, Fx.partition(0, 3, 1, 3L));
        a.on((node, request) -> Fx.metadata(all, Fx.unknown("s")));
        b.on((node, request) -> Fx.metadata(all, old));
        c.on((node, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(all, newest);
            }
            return Fx.produced(20, 0, 9);
        });
        LotseClient lotse = client(a);

        assertEquals(9, lotse.produce("s", 0, OutMessage.value("x")).baseOffset());
        Request.Produce sent = c.requests(Request.Produce.class).get(0);
        assertEquals(20, sent.resources().get(0).resourceId(), "the newest incarnation");
        assertEquals(0, b.count(Request.Produce.class));
    }

    @Test
    void aNameNobodyKnowsIsAnUnknownStreamAndIsNotRetried() {
        for (FakeNode n : all) {
            n.on((node, request) -> Fx.metadata(all, Fx.unknown("s")));
        }
        LotseClient lotse = client(a);
        long start = System.nanoTime();
        ClientException.UnknownStream e =
                assertThrows(ClientException.UnknownStream.class, () -> lotse.produce("s", 0, OutMessage.value("x")));
        assertEquals("s", e.name());
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofMillis(1500)) < 0, "not retried");
    }

    @Test
    void aPartitionThatDoesNotExistIsAnUnknownStream() {
        a.on((node, request) -> Fx.metadata(all, Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 1, 1L))));
        LotseClient lotse = client(a);
        ClientException.UnknownStream e =
                assertThrows(ClientException.UnknownStream.class, () -> lotse.produce("s", 5, OutMessage.value("x")));
        assertEquals(5, e.partition());
    }

    @Test
    void aLeaderWhoseAddressIsUnknownIsRetriedWithFreshMetadata() {
        // The first answer names node 9, which no broker list contains; the second names node 1.
        ResourceMetadata ghost = Fx.stream("s", STREAM_ID, Fx.partition(0, 9, 1, 9L));
        ResourceMetadata real = Fx.stream("s", STREAM_ID, Fx.partition(0, 1, 2, 1L));
        a.on((node, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(all, node.count(Request.Metadata.class) == 1 ? ghost : real);
            }
            return Fx.produced(STREAM_ID, 0, 4);
        });
        assertEquals(4, client(a).produce("s", 0, OutMessage.value("x")).baseOffset());
        assertEquals(2, a.count(Request.Metadata.class));
    }

    // ---- merge_newest ----

    private static ResourceMetadata resource(String name, long id) {
        return Fx.stream(name, id);
    }

    @Test
    void theNewestResourceOfANameWinsInEveryOrder() {
        long[][] orders = {{100, 200}, {200, 100}};
        for (long[] order : orders) {
            List<ResourceMetadata> merged = new ArrayList<>();
            for (long id : order) {
                LotseClient.mergeNewest(merged, List.of(resource("s", id), resource("only" + id, id)));
            }
            assertEquals(3, merged.size());
            assertEquals(1, merged.stream().filter(r -> r.name().equals("s")).count());
            assertEquals(200, merged.stream().filter(r -> r.name().equals("s")).findFirst().orElseThrow().resourceId());
        }
    }

    @Test
    void anErrorNamesNothing() {
        List<ResourceMetadata> merged = new ArrayList<>(List.of(resource("s", 200)));
        LotseClient.mergeNewest(merged, List.of(new ResourceMetadata(
                ErrorCode.UNKNOWN_RESOURCE, "s", "", 300, 0, List.of(), null)));
        assertEquals(200, merged.get(0).resourceId());
        LotseClient.mergeNewest(merged, List.of(new ResourceMetadata(
                ErrorCode.UNKNOWN_RESOURCE, "t", "", 300, 0, List.of(), null)));
        assertEquals(1, merged.size());
    }

    @Test
    void resourceIdsAreComparedAsUnsigned() {
        List<ResourceMetadata> merged = new ArrayList<>(List.of(resource("s", 5)));
        LotseClient.mergeNewest(merged, List.of(resource("s", Long.MIN_VALUE)));
        assertEquals(Long.MIN_VALUE, merged.get(0).resourceId(), "2^63 is newer than 5");
    }

    @Test
    void metadataGivesUpWhenNoBrokerAnswers() {
        a.kill();
        client = new LotseClient(Fx.config(a).retryFor(Duration.ofMillis(100)).build());
        assertThrows(ClientException.GaveUp.class, () -> client.metadata("s"));
    }

    @Test
    void aBrokerThatRefusesLoginFailsAtOnceAsAuthentication() {
        a.on((node, request) -> new Response.Error(ErrorCode.UNAUTHORIZED));
        client = new LotseClient(Fx.config(a).retryFor(Duration.ofSeconds(30)).build());
        long start = System.nanoTime();
        assertThrows(ClientException.Authentication.class, () -> client.produce("s", 0, OutMessage.value("x")));
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(5)) < 0);
    }
}

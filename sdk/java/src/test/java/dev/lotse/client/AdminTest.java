package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.auth.ScramCredential;
import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.LeaderHint;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.BindingMetadata;
import dev.lotse.proto.Messages.ExchangeMetadata;
import dev.lotse.proto.Messages.ResourceMetadata;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.ByteString;
import dev.lotse.record.Header;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Admin calls against scripted brokers: node 2 leads the metadata group, node 1 points to it. */
class AdminTest {
    private static final long EXCHANGE_ID = 20;
    private static final long QUEUE_ID = 21;
    private static final long STREAM_ID = 7;

    private FakeNode a;
    private FakeNode b;
    private LotseClient client;
    private final AtomicInteger calls = new AtomicInteger();

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

    private Response metadata() {
        ExchangeMetadata x = new ExchangeMetadata(
                Messages.ExchangeType.DIRECT, List.of(new BindingMetadata(QUEUE_ID, "k", List.of(), true)));
        return Fx.metadata(
                List.of(a, b),
                Fx.stream("s", STREAM_ID, Fx.partition(0, 2, 1, 2L)),
                new ResourceMetadata(
                        ErrorCode.NONE, "x", "default", EXCHANGE_ID, Messages.ResourceKind.EXCHANGE, List.of(), x),
                new ResourceMetadata(
                        ErrorCode.NONE, "q", "default", QUEUE_ID, Messages.ResourceKind.QUEUE,
                        List.of(Fx.partition(0, 2, 1, 2L)), null));
    }

    /** Node 1 answers metadata and sends admin requests to node 2; node 2 answers with {@code admin}. */
    private void serve(FakeNode.Handler admin) {
        a.on((n, r) -> r instanceof Request.Metadata ? metadata() : pointToB(r));
        b.on((n, r) -> r instanceof Request.Metadata ? metadata() : admin.handle(n, r));
        client = new LotseClient(Fx.config(a).retryFor(Duration.ofSeconds(1)).build());
    }

    private static Response pointToB(Request r) {
        LeaderHint hint = new LeaderHint(2, 1);
        if (r instanceof Request.CreateStream || r instanceof Request.CreateQueue || r instanceof Request.DeclareExchange) {
            return new Response.Create(ErrorCode.NOT_LEADER, 0, null, hint);
        }
        if (r instanceof Request.Bind || r instanceof Request.Unbind) {
            return new Response.Outcome(ErrorCode.NOT_LEADER, hint);
        }
        if (r instanceof Request.DescribeConfigs) {
            return new Response.DescribeConfigs(ErrorCode.NOT_LEADER, hint, List.of());
        }
        return new Response.Admin(ErrorCode.NOT_LEADER, null, hint);
    }

    private static Response.Admin ok() {
        return new Response.Admin(ErrorCode.NONE, null, null);
    }

    @Test
    void aCreateFollowsTheHintToTheMetadataLeader() {
        serve((n, r) -> new Response.Create(ErrorCode.NONE, EXCHANGE_ID, null, null));
        assertEquals(EXCHANGE_ID, client.declareExchange("x", Messages.ExchangeType.TOPIC));
        Request.DeclareExchange d = b.requests(Request.DeclareExchange.class).get(0);
        assertEquals(Messages.ExchangeType.TOPIC, d.exchangeType());
        assertEquals("default", d.tenant());
        client.createStream("t", 1, 3);
        assertEquals(1, a.requests(Request.CreateStream.class).size() + a.requests(Request.DeclareExchange.class).size(),
                "the leader, once known, is asked directly");
        assertThrows(IllegalArgumentException.class, () -> client.declareExchange("y", 4));
    }

    @Test
    void aCreateWhoseAnswerWasLostFindsItAndSaysSo() {
        serve((n, r) -> {
            if (calls.incrementAndGet() == 1) {
                throw new FakeNode.Drop();
            }
            return new Response.Create(ErrorCode.RESOURCE_EXISTS, 9, null, null);
        });
        ClientException.ResourceExists e =
                assertThrows(ClientException.ResourceExists.class, () -> client.declareExchange("x", 0));
        assertEquals(9, e.resourceId());
    }

    @Test
    void aDeleteIsDoneOnceTheResourceIsGone() {
        serve((n, r) -> {
            Request.DeleteResource d = (Request.DeleteResource) r;
            if (d.ifEmpty()) {
                return new Response.Admin(ErrorCode.RESOURCE_NOT_EMPTY, "2 messages", null);
            }
            return new Response.Admin(ErrorCode.UNKNOWN_RESOURCE, null, null);
        });
        assertEquals(QUEUE_ID, client.delete("q"), "deleted by someone else: done");
        assertEquals(QUEUE_ID, b.requests(Request.DeleteResource.class).get(0).resourceId());
        ClientException.Broker e = assertThrows(ClientException.Broker.class, () -> client.deleteIfEmpty("q"));
        assertEquals(ErrorCode.RESOURCE_NOT_EMPTY, e.code());
        assertEquals("2 messages", e.brokerMessage());
        assertThrows(ClientException.UnknownStream.class, () -> client.delete("nothing"));
    }

    @Test
    void bindingsNameBothResourcesAndAPublishInATransactionSeesThem() {
        serve((n, r) -> new Response.Outcome(ErrorCode.NONE, null));
        client.exchange("x");
        int before = a.requests(Request.Metadata.class).size() + b.requests(Request.Metadata.class).size();
        client.exchange("x");
        assertEquals(before, a.requests(Request.Metadata.class).size() + b.requests(Request.Metadata.class).size(),
                "kept for metadataMaxAge");
        client.bind("x", "q", "k2");
        Request.Bind bind = b.requests(Request.Bind.class).get(0);
        assertEquals(EXCHANGE_ID, bind.exchangeId());
        assertEquals(QUEUE_ID, bind.queueId());
        assertEquals("k2", bind.bindingKey());
        int afterBind = a.requests(Request.Metadata.class).size() + b.requests(Request.Metadata.class).size();
        client.exchange("x");
        assertTrue(a.requests(Request.Metadata.class).size() + b.requests(Request.Metadata.class).size() > afterBind,
                "asked again after a bind");
        client.bindHeaders("x", "q", List.of(new Header(ByteString.utf8("region"), ByteString.utf8("eu")),
                new Header(ByteString.utf8("flag"), null)), false);
        Request.Bind headers = b.requests(Request.Bind.class).get(1);
        assertEquals(List.of(new Messages.HeaderMatch(ByteString.utf8("region"), ByteString.utf8("eu")),
                new Messages.HeaderMatch(ByteString.utf8("flag"), null)), headers.headerMatch());
        assertEquals(false, headers.matchAll());
        client.unbind("x", "q", "k2");
        assertEquals("k2", b.requests(Request.Unbind.class).get(0).bindingKey());
    }

    @Test
    void purgeGoesOnUntilTheLeaderSaysThereIsNoMore() {
        a.on((n, r) -> metadata());
        b.on((n, r) -> {
            if (r instanceof Request.Metadata) {
                return metadata();
            }
            Request.QueueAction q = (Request.QueueAction) r;
            return q.upTo() == 0
                    ? new Response.QueueAction(ErrorCode.NONE, null, 3, 10, true, null)
                    : new Response.QueueAction(ErrorCode.NONE, null, 2, 12, false, null);
        });
        client = new LotseClient(Fx.config(a).build());
        assertEquals(5, client.purge("q"));
        List<Request.QueueAction> sent = b.requests(Request.QueueAction.class);
        assertEquals(2, sent.size());
        assertEquals(10, sent.get(1).upTo(), "the next round starts where the last ended");
        assertEquals(false, sent.get(0).requeue());
        assertEquals(QUEUE_ID, sent.get(0).queueId());
        client.requeue("q");
        assertEquals(true, b.requests(Request.QueueAction.class).get(2).requeue());
    }

    @Test
    void settingsAreChangedAndRead() {
        serve((n, r) -> {
            if (r instanceof Request.DescribeConfigs) {
                return new Response.DescribeConfigs(ErrorCode.NONE, null, List.of(new Messages.ResourceConfigs(
                        QUEUE_ID, ErrorCode.NONE,
                        List.of(new Messages.ConfigEntry("ack.timeout.ms", "500", Messages.ConfigSource.DYNAMIC)))));
            }
            return ok();
        });
        client.alterConfigs("q", Map.of("ack.timeout.ms", "500"), List.of("max.deliveries"));
        Request.AlterConfigs alter = b.requests(Request.AlterConfigs.class).get(0);
        assertEquals(QUEUE_ID, alter.resourceId());
        assertEquals(List.of(new Messages.Config("ack.timeout.ms", "500")), alter.set());
        assertEquals(List.of("max.deliveries"), alter.delete());
        assertEquals("500", client.describeConfigs("q").get(0).value());
    }

    @Test
    void aPasswordLeavesTheClientOnlyAsASaltedCredential() {
        serve((n, r) -> ok());
        client.setUserPassword("alice", "s3cret");
        Request.AlterUser u = b.requests(Request.AlterUser.class).get(0);
        assertEquals("alice", u.user());
        Messages.ScramCredentialData c = u.credential();
        assertEquals(16, c.salt().size());
        assertTrue(c.iterations() >= 4096);
        ScramCredential expected = ScramCredential.of("s3cret", c.salt().toByteArray(), (int) c.iterations());
        assertArrayEquals(expected.storedKey(), c.storedKey().toByteArray());
        assertArrayEquals(expected.serverKey(), c.serverKey().toByteArray());
        client.setUserPassword("alice", "s3cret");
        Messages.ScramCredentialData again = b.requests(Request.AlterUser.class).get(1).credential();
        assertTrue(!again.salt().equals(c.salt()), "a new salt each time");
        client.deleteUser("alice");
        assertNull(b.requests(Request.AlterUser.class).get(2).credential());
    }

    @Test
    void aRetriableAnswerIsRetriedAndARefusalKeepsItsReason() {
        serve((n, r) -> calls.incrementAndGet() == 1
                ? new Response.Admin(ErrorCode.NOT_ENOUGH_REPLICAS, null, new LeaderHint(2, 1))
                : new Response.Admin(ErrorCode.INVALID_REQUEST, "no such role", null));
        ClientException.Broker e = assertThrows(ClientException.Broker.class, () -> client.createTenant("t"));
        assertEquals(ErrorCode.INVALID_REQUEST, e.code());
        assertEquals("no such role", e.brokerMessage());
        assertEquals(2, b.requests(Request.CreateTenant.class).size());
    }

    @Test
    void clusterCallsCarryWhatTheyName() {
        serve((n, r) -> ok());
        client.moveLeader("s", 0, 3L);
        client.moveLeader("s", 0, null);
        assertEquals(3, b.requests(Request.MoveLeader.class).get(0).nodeId());
        assertEquals(0, b.requests(Request.MoveLeader.class).get(1).nodeId(), "unpinned");
        assertEquals(STREAM_ID, b.requests(Request.MoveLeader.class).get(0).resourceId());
        client.revokeSessions("ops", true);
        assertEquals(Messages.SubjectKind.GROUP, b.requests(Request.RevokeSessions.class).get(0).subjectKind());
        client.revokeSessions("bob", false);
        assertEquals(Messages.SubjectKind.USER, b.requests(Request.RevokeSessions.class).get(1).subjectKind());
        client.createPartitions("s", 4);
        assertEquals(4, b.requests(Request.CreatePartitions.class).get(0).count());
        client.addNode(4, "n4:9100", "n4:9000");
        assertEquals("n4:9000", b.requests(Request.AddNode.class).get(0).client());
        client.removeNode(4);
        assertEquals(4, b.requests(Request.RemoveNode.class).get(0).nodeId());
        client.deleteTenant("t");
        assertEquals("t", b.requests(Request.DeleteTenant.class).get(0).name());
    }

    @Test
    void everyNodeIsAskedForItsMetricsAndOneThatIsDownIsReported() {
        Response.DescribeMetrics m = new Response.DescribeMetrics(
                ErrorCode.NONE, 1, "0.0.1", 10, 1, 2, 0, 0, 0, 0, 0, List.of(),
                new Messages.HostMetrics(null, null, null, null, null), List.of(), List.of(), List.of());
        a.on((n, r) -> r instanceof Request.Metadata ? metadata() : m);
        client = new LotseClient(Fx.config(a).build());
        client.metadata();
        b.close();
        List<NodeMetrics> got = client.nodeMetrics();
        assertEquals(2, got.size());
        assertEquals(1, got.get(0).nodeId());
        assertEquals(m, got.get(0).metrics());
        assertNull(got.get(1).metrics());
        assertTrue(got.get(1).error() instanceof ClientException.NoAnswer);
    }
}

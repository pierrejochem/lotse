package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.ResourceMetadata;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Publishing through an exchange that a node routes (ADR 0079), against a scripted broker. */
class PublishTest {
    private FakeNode a;
    private LotseClient client;

    @BeforeEach
    void start() throws IOException {
        a = new FakeNode(1);
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        a.close();
    }

    private static ResourceMetadata resource(String name, long id, int kind) {
        return new ResourceMetadata(ErrorCode.NONE, name, "default", id, kind, List.of(), null);
    }

    /** The exchange {@code x} (20) with the queues {@code a} (7) and {@code b} (8) bound to it. */
    private void answer(java.util.function.Function<Request.Publish, Response> publish) {
        a.onConn((n, conn, request) -> {
            if (request instanceof Request.Metadata) {
                return Fx.metadata(
                        List.of(a),
                        resource("x", 20, Messages.ResourceKind.EXCHANGE),
                        resource("a", 7, Messages.ResourceKind.QUEUE),
                        resource("b", 8, Messages.ResourceKind.QUEUE));
            }
            if (request instanceof Request.Publish p) {
                return publish.apply(p);
            }
            throw new AssertionError("unexpected " + request);
        });
        client = new LotseClient(Fx.config(a)
                .requestTimeout(Duration.ofMillis(500))
                .retryFor(Duration.ofSeconds(1))
                .build());
    }

    private static Response routed(Messages.RoutedMessage... queues) {
        return new Response.Publish(0, List.of(new Messages.PublishResult(ErrorCode.NONE, List.of(queues))));
    }

    @Test
    void aRoutedMessageNamesItsQueuesAndOffsets() {
        AtomicReference<Request.Publish> sent = new AtomicReference<>();
        answer(p -> {
            sent.set(p);
            // 9 was bound and deleted between this client's lookups: only the node can name it. An empty
            // name falls back to the name this client knows.
            return routed(
                    new Messages.RoutedMessage(8, "b", ErrorCode.NONE, 3),
                    new Messages.RoutedMessage(9, "gone", ErrorCode.NONE, 5),
                    new Messages.RoutedMessage(7, "", ErrorCode.NONE, 41));
        });
        Published got = client.publish("x", "k", OutMessage.value("hello"), true);
        assertEquals(
                List.of(new Published.Routed("a", 41), new Published.Routed("b", 3), new Published.Routed("gone", 5)),
                got.routed());
        Messages.PublishEntry e = sent.get().entries().get(0);
        assertEquals(20, e.exchangeId());
        assertEquals("k", e.routingKey());
        assertEquals(true, e.mandatory());
        assertEquals(null, sent.get().txnId());
    }

    @Test
    void someQueuesOnlyOrNoneAreReportedAsSuch() {
        answer(p -> routed(
                new Messages.RoutedMessage(7, "a", ErrorCode.NONE, 41),
                new Messages.RoutedMessage(8, "b", ErrorCode.REQUEST_TIMED_OUT, 0)));
        ClientException.NotAllWritten e = assertThrows(
                ClientException.NotAllWritten.class, () -> client.publish("x", "k", OutMessage.value("m"), false));
        assertEquals(List.of("a"), e.written());
        assertEquals(ClientException.Unknown.class, e.failures().get(0).getClass());
        client.close();
        answer(p -> new Response.Publish(
                0, List.of(new Messages.PublishResult(ErrorCode.UNROUTABLE, List.of()))));
        assertThrows(ClientException.Unroutable.class, () -> client.publish("x", "nobody", OutMessage.value("m"), true));
    }

    @Test
    void aLostAnswerIsNotSentAgain() {
        answer(p -> {
            throw new FakeNode.Drop();
        });
        assertThrows(ClientException.Unknown.class, () -> client.publish("x", "k", OutMessage.value("m"), false));
        long publishes = a.requests(Request.Publish.class).size();
        assertEquals(1, publishes, "the node may have written it");
    }
}

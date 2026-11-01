package dev.lotse.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Plain TCP only on loopback and only when asked for (the rule of AGENTS.md, rule 7). */
class TransportTest {
    @Test
    void nothingIsConnectedUntilTheTransportIsChosen() throws Exception {
        try (FakeBroker broker = new FakeBroker((peer, req) -> {})) {
            ConnectionOptions none = ConnectionOptions.builder().build();
            assertThrows(InsecureTransportException.class, () -> Connection.open(broker.address(), none));
            assertThrows(InsecureTransportException.class, () -> new ConnectionPool(none).get(broker.address()));
            Thread.sleep(100);
            assertEquals(0, broker.accepted.get(), "not even a TCP connection was made");
        }
    }

    @Test
    void plainTcpIsRefusedOffLoopbackBeforeAnythingIsSent() {
        ConnectionOptions plain = ConnectionOptions.builder().plaintextOnLoopback().connectTimeout(Duration.ofSeconds(30)).build();
        long start = System.nanoTime();
        for (String host : new String[] {"192.0.2.1", "10.1.2.3", "0.0.0.0", "8.8.8.8", "2001:db8::1"}) {
            InsecureTransportException e =
                    assertThrows(InsecureTransportException.class, () -> Connection.open(new Address(host, 9092), plain), host);
            assertTrue(e.getMessage().contains("loopback"), e.getMessage());
            assertTrue(!e.requestSent());
        }
        assertTrue(System.nanoTime() - start < Duration.ofSeconds(5).toNanos(), "refused without trying to connect");
    }

    @Test
    void plainTcpToLoopbackNamesWorks() throws Exception {
        ConnectionOptions plain = ConnectionOptions.builder().plaintextOnLoopback().build();
        try (FakeBroker broker = new FakeBroker((peer, req) ->
                        peer.reply(req.header().correlationId(), new Response.Outcome(ErrorCode.NONE, null)));
                Connection c = Connection.open(new Address("localhost", broker.address().port()), plain)) {
            assertTrue(c.call(new Request.Unsubscribe(1)) instanceof Response.Outcome);
            assertTrue(!c.isSecure());
        }
    }

    @Test
    void addressesParse() {
        assertEquals(new Address("broker-1.example", 9092), Address.parse("broker-1.example:9092"));
        assertEquals(new Address("127.0.0.1", 7), Address.parse("127.0.0.1:7"));
        assertEquals(new Address("::1", 9092), Address.parse("[::1]:9092"));
        assertEquals("[::1]:9092", Address.parse("[::1]:9092").toString());
        for (String bad : new String[] {"nohost", "::1:9092", "host:", "host:abc", "host:0", "host:70000", ":9092"}) {
            assertThrows(IllegalArgumentException.class, () -> Address.parse(bad), bad);
        }
    }
}

package dev.lotse.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.auth.AuthenticationException;
import dev.lotse.auth.Credentials;
import dev.lotse.net.Connection.SessionState;
import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.ByteString;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Logging in, renewing a session at four fifths of its lifetime, and replacing the connection when it ends. */
class SessionTest {
    private static final long SECOND = 1_000_000_000L;

    private static FakeBroker.Handler serving(FakeSasl sasl) {
        return (peer, req) -> {
            if (!sasl.handle(peer, req)) {
                peer.reply(req.header().correlationId(), new Response.Outcome(ErrorCode.NONE, null));
            }
        };
    }

    private static ConnectionOptions options(AtomicLong clock, Credentials credentials) {
        return ConnectionOptions.builder()
                .plaintextOnLoopback()
                .credentials(credentials)
                .ticker(clock::get)
                .requestTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Test
    void aSessionIsRenewedOnTheSameConnectionAtFourFifthsAndReplacedAtTheEnd() throws Exception {
        AtomicLong clock = new AtomicLong(1_000_000 * SECOND); // an arbitrary origin
        long origin = clock.get();
        FakeSasl sasl = new FakeSasl(10_000);
        try (FakeBroker broker = new FakeBroker(serving(sasl));
                ConnectionPool pool = new ConnectionPool(options(clock, Credentials.scram("alice", "pw")))) {
            Connection first = pool.get(broker.address());
            assertEquals(1, sasl.handshakes.get());
            assertEquals(SessionState.ACTIVE, first.sessionState());

            clock.set(origin + 8 * SECOND - 1);
            assertSame(first, pool.get(broker.address()), "not yet");
            assertEquals(SessionState.ACTIVE, first.sessionState());
            assertEquals(1, sasl.handshakes.get());

            clock.set(origin + 8 * SECOND);
            assertEquals(SessionState.RENEW_DUE, first.sessionState());
            assertSame(first, pool.get(broker.address()), "renewed on the same connection");
            assertEquals(2, sasl.handshakes.get(), "handshake and authenticate again");
            assertEquals(2, sasl.finals.get());
            assertEquals(1, broker.accepted.get());
            assertEquals(SessionState.ACTIVE, first.sessionState(), "a new session of 10 s from 8 s");

            clock.set(origin + 18 * SECOND - 1);
            assertTrue(first.sessionState() != SessionState.ENDED);
            clock.set(origin + 18 * SECOND);
            assertEquals(SessionState.ENDED, first.sessionState());
            Connection second = pool.get(broker.address());
            assertNotSame(first, second, "an ended session is not renewed: the node refuses it");
            assertEquals(2, broker.accepted.get());
            assertFalse(first.isOpen());
            assertEquals(3, sasl.handshakes.get());
            assertEquals(SessionState.ACTIVE, second.sessionState());
        }
    }

    @Test
    void theLifetimeComesFromTheLastResponse() throws Exception {
        AtomicLong clock = new AtomicLong(5 * SECOND);
        long origin = clock.get();
        FakeSasl sasl = new FakeSasl(10_000);
        try (FakeBroker broker = new FakeBroker(serving(sasl));
                ConnectionPool pool = new ConnectionPool(options(clock, Credentials.scram("alice", "pw")))) {
            Connection c = pool.get(broker.address());
            sasl.lifetimeMs.set(4_000); // the node grants less on renewal
            clock.set(origin + 8 * SECOND);
            assertSame(c, pool.get(broker.address()));
            clock.set(origin + 8 * SECOND + 3_199_999_999L);
            assertEquals(SessionState.ACTIVE, c.sessionState(), "renewal at 3.2 s of 4 s");
            clock.set(origin + 8 * SECOND + 3_200_000_000L);
            assertEquals(SessionState.RENEW_DUE, c.sessionState());
            clock.set(origin + 8 * SECOND + 4 * SECOND);
            assertEquals(SessionState.ENDED, c.sessionState());
        }
    }

    @Test
    void theLifetimeCountsFromBeforeTheHandshake() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        FakeSasl sasl = new FakeSasl(10_000);
        // The log in takes a second: the node's clock started when it received the first message.
        sasl.onHandshake = n -> clock.addAndGet(SECOND);
        try (FakeBroker broker = new FakeBroker(serving(sasl));
                ConnectionPool pool = new ConnectionPool(options(clock, Credentials.scram("alice", "pw")))) {
            Connection c = pool.get(broker.address());
            assertEquals(1 * SECOND, clock.get());
            clock.set(8 * SECOND - 1);
            assertEquals(SessionState.ACTIVE, c.sessionState());
            clock.set(8 * SECOND);
            assertEquals(SessionState.RENEW_DUE, c.sessionState(), "4/5 of 10 s from time 0, not from time 1 s");
            clock.set(10 * SECOND - 1);
            assertEquals(SessionState.RENEW_DUE, c.sessionState());
            clock.set(10 * SECOND);
            assertEquals(SessionState.ENDED, c.sessionState());
        }
    }

    @Test
    void aSessionWithoutALifetimeLastsTheConnection() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        FakeSasl sasl = new FakeSasl(0);
        try (FakeBroker broker = new FakeBroker(serving(sasl));
                ConnectionPool pool = new ConnectionPool(options(clock, Credentials.scram("alice", "pw")))) {
            Connection c = pool.get(broker.address());
            assertEquals(SessionState.UNLIMITED, c.sessionState());
            clock.set(1_000_000 * SECOND);
            assertSame(c, pool.get(broker.address()));
            assertEquals(1, sasl.handshakes.get());
        }
    }

    @Test
    void oauthBearerAsksForTheTokenOnEveryAuthentication() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        FakeSasl sasl = new FakeSasl(10_000);
        java.util.concurrent.atomic.AtomicInteger issued = new java.util.concurrent.atomic.AtomicInteger();
        try (FakeBroker broker = new FakeBroker(serving(sasl));
                ConnectionPool pool = new ConnectionPool(options(clock, Credentials.oauthBearer(() -> "token-" + issued.incrementAndGet())))) {
            Connection c = pool.get(broker.address());
            assertEquals(java.util.List.of("token-1"), sasl.tokens);
            clock.set(8 * SECOND);
            assertSame(c, pool.get(broker.address()));
            assertEquals(java.util.List.of("token-1", "token-2"), sasl.tokens, "a fresh token to renew");
        }
    }

    @Test
    void aWrongPasswordFailsTheLoginAndLeavesNothingInThePool() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        FakeSasl sasl = new FakeSasl(10_000);
        try (FakeBroker broker = new FakeBroker(serving(sasl));
                ConnectionPool pool = new ConnectionPool(options(clock, Credentials.scram("alice", "wrong")))) {
            AuthenticationException e = assertThrows(AuthenticationException.class, () -> pool.get(broker.address()));
            assertEquals(ErrorCode.AUTHENTICATION_FAILED, e.code());
            assertEquals("authentication failed", e.getMessage());
            assertThrows(AuthenticationException.class, () -> pool.get(broker.address()));
            assertEquals(2, broker.accepted.get(), "each try is a new connection; the failed one was closed");
        }
    }

    @Test
    void aMechanismTheNodeDoesNotOfferNamesTheOnesItDoes() throws Exception {
        FakeSasl sasl = new FakeSasl(10_000);
        sasl.mechanisms = java.util.List.of("OAUTHBEARER");
        try (FakeBroker broker = new FakeBroker(serving(sasl))) {
            AuthenticationException e = assertThrows(
                    AuthenticationException.class,
                    () -> Connection.open(broker.address(), options(new AtomicLong(), Credentials.scram("alice", "pw"))));
            assertEquals(ErrorCode.UNSUPPORTED_SASL_MECHANISM, e.code());
            assertTrue(e.getMessage().contains("SCRAM-SHA-512 is not enabled"), e.getMessage());
            assertTrue(e.getMessage().contains("OAUTHBEARER"), e.getMessage());
        }
    }

    @Test
    void aServerThatCannotProveItselfIsRefused() throws Exception {
        // A broker that accepts any proof and answers with a made up signature.
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
            if (req.request() instanceof Request.SaslHandshake) {
                peer.reply(req.header().correlationId(), new Response.SaslHandshake(ErrorCode.NONE, java.util.List.of("SCRAM-SHA-512")));
            } else {
                String first = new String(((Request.SaslAuthenticate) req.request()).authBytes().toByteArray());
                byte[] out;
                if (first.startsWith("n,,")) {
                    String nonce = first.substring(first.indexOf("r=") + 2);
                    out = ("r=" + nonce + "xyz,s=MDEyMzQ1Njc4OWFiY2RlZg==,i=4096").getBytes();
                } else {
                    out = ("v=" + java.util.Base64.getEncoder().encodeToString(new byte[64])).getBytes();
                }
                peer.reply(req.header().correlationId(), new Response.SaslAuthenticate(ErrorCode.NONE, null, ByteString.copyOf(out), 10_000));
            }
        })) {
            AuthenticationException e = assertThrows(
                    AuthenticationException.class,
                    () -> Connection.open(broker.address(), options(new AtomicLong(), Credentials.scram("alice", "pw"))));
            assertTrue(e.getMessage().contains("could not prove"), e.getMessage());
        }
    }

    @Test
    void aConnectionThatBreaksDuringRenewalIsReplacedWithAFreshLogin() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        FakeSasl sasl = new FakeSasl(10_000);
        AtomicReference<Boolean> breakNext = new AtomicReference<>(false);
        sasl.onHandshake = n -> {
            if (breakNext.get() && n == 2) {
                throw new IllegalStateException("closes the connection"); // the fake's peer thread ends and closes
            }
        };
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    try {
                        serving(sasl).onRequest(peer, req);
                    } catch (IllegalStateException e) {
                        peer.closeSocket();
                    }
                });
                ConnectionPool pool = new ConnectionPool(options(clock, Credentials.scram("alice", "pw")))) {
            Connection first = pool.get(broker.address());
            breakNext.set(true);
            clock.set(8 * SECOND);
            Connection second = pool.get(broker.address());
            assertNotSame(first, second);
            assertEquals(2, broker.accepted.get());
            assertFalse(first.isOpen());
            assertEquals(SessionState.ACTIVE, second.sessionState());
        }
    }

    @Test
    void aRefusedRenewalIsReportedAndTheConnectionDropped() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        FakeSasl sasl = new FakeSasl(10_000);
        AtomicReference<FakeSasl> holder = new AtomicReference<>(sasl);
        try (FakeBroker broker = new FakeBroker((peer, req) -> {
                    if (req.request() instanceof Request.SaslHandshake && holder.get().handshakes.get() >= 1) {
                        // On renewal the node has forgotten the user.
                        if (holder.get().handshakes.incrementAndGet() > 1) {
                            peer.reply(req.header().correlationId(), new Response.SaslHandshake(ErrorCode.UNSUPPORTED_SASL_MECHANISM, java.util.List.of()));
                            return;
                        }
                    }
                    serving(holder.get()).onRequest(peer, req);
                });
                ConnectionPool pool = new ConnectionPool(options(clock, Credentials.scram("alice", "pw")))) {
            Connection first = pool.get(broker.address());
            clock.set(8 * SECOND);
            assertThrows(AuthenticationException.class, () -> pool.get(broker.address()));
            assertFalse(first.isOpen());
        }
    }

    @Test
    void aClosedPoolRefuses() {
        ConnectionPool pool = new ConnectionPool(ConnectionOptions.builder().plaintextOnLoopback().build());
        pool.close();
        assertInstanceOf(ConnectionException.class, assertThrows(ConnectionException.class, () -> pool.get(new Address("127.0.0.1", 9))));
    }
}

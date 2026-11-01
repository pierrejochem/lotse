package dev.lotse.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.auth.AuthenticationException;
import dev.lotse.auth.Credentials;
import dev.lotse.auth.ScramCredential;
import dev.lotse.net.Connection.SessionState;
import dev.lotse.proto.ApiKey;
import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;

/**
 * One real {@code lotse} node, started from a generated cluster file with TLS, {@code alice} as cluster admin and
 * sessions limited to 4 s. The admin connects with a client certificate and creates a SCRAM user; the user then
 * logs in over SCRAM, renews its session, and loses it when it does not. Skipped when no executable is found; see
 * {@link LotseBinary}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RealNodeTest {
    private static final long SESSION_MS = 4_000;

    private static Certs certs;
    private static Process node;
    private static Address address;
    private static TlsConfig adminTls;
    private static TlsConfig anonymousTls;

    @BeforeAll
    static void startNode() throws Exception {
        Path bin = LotseBinary.require();
        certs = new Certs(bin, "max_session_s = " + SESSION_MS / 1000 + "\n");
        address = new Address("127.0.0.1", certs.clientPort);
        adminTls = TlsConfig.fromFiles(certs.ca, certs.clientCert, certs.clientKey);
        anonymousTls = TlsConfig.fromFiles(certs.ca);
        node = new ProcessBuilder(bin.toString(), "run", "--config", certs.config.toString(), "--node", "1")
                .redirectErrorStream(true)
                .redirectOutput(certs.dir.resolve("node.log").toFile())
                .start();
        // Ready when the node answers Metadata and has a leader for its groups: AlterUser is the proof.
        retry("the node to start", () -> {
            try (Connection c = Connection.open(address, options(adminTls, null))) {
                return c.call(new Request.Metadata(List.of()), Response.Metadata.class);
            }
        });
    }

    @AfterAll
    static void stopNode() {
        if (node != null) {
            node.destroy();
            try {
                node.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            node.destroyForcibly();
        }
    }

    private static ConnectionOptions options(TlsConfig tls, Credentials credentials) {
        return ConnectionOptions.builder()
                .tls(tls)
                .credentials(credentials)
                .requestTimeout(Duration.ofSeconds(10))
                .build();
    }

    private static <T> T retry(String what, Supplier<T> action) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        RuntimeException last = null;
        while (System.nanoTime() - deadline < 0) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                last = e;
                Thread.sleep(200);
            }
        }
        String log = Files.exists(certs.dir.resolve("node.log")) ? Files.readString(certs.dir.resolve("node.log")) : "";
        throw new AssertionError("timed out waiting for " + what + ": " + last + "\nnode log:\n" + log, last);
    }

    @Test
    @Order(1)
    void aNodeAnswersApiVersionsAndMetadataOverTls() throws Exception {
        try (Connection c = Connection.open(address, options(adminTls, null))) {
            assertTrue(c.isSecure());
            Response.ApiVersions versions = c.apiVersions().get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue(versions.versions().stream().anyMatch(v -> v.apiKey().equals(ApiKey.METADATA) && v.min() == 0), versions.toString());
            assertTrue(versions.versions().stream().anyMatch(v -> v.apiKey().equals(ApiKey.SASL_AUTHENTICATE)));
            Response.Metadata metadata = c.call(new Request.Metadata(List.of()), Response.Metadata.class);
            assertEquals(1, metadata.brokers().size());
            assertEquals(1, metadata.brokers().get(0).nodeId());
            assertEquals(certs.clientPort, metadata.brokers().get(0).port());
            Response.WhoAmI who = c.call(new Request.WhoAmI(), Response.WhoAmI.class);
            assertEquals("alice", who.name(), "the principal comes from the certificate's common name");
            assertEquals(SessionState.UNLIMITED, c.sessionState(), "a certificate's session lasts the connection");
        }
    }

    @Test
    @Order(2)
    void aNodeThatRequiresAuthenticationRefusesAnAnonymousClientWithTheErrorFlag() throws Exception {
        try (Connection c = Connection.open(address, options(anonymousTls, null))) {
            BrokerException e = assertThrows(BrokerException.class, () -> c.call(new Request.Metadata(List.of())));
            assertEquals(null, e.reason(), "an unauthenticated connection is told no more than the code");
            assertEquals(ErrorCode.UNAUTHORIZED, e.code());
            assertTrue(c.isOpen(), "a refused request does not end the connection");
            // ApiVersions is answered before anyone has authenticated: a client may look before it logs in.
            assertTrue(c.apiVersions().get(10, java.util.concurrent.TimeUnit.SECONDS).versions().size() > 10);
        }
    }

    @Test
    @Order(3)
    void theAdminCreatesAScramUserAndTheUserLogsIn() throws Exception {
        ScramCredential credential = ScramCredential.forPassword("correct horse", new SecureRandom());
        retry("AlterUser to be accepted", () -> {
            try (Connection admin = Connection.open(address, options(adminTls, null))) {
                Response.Admin r = admin.call(credential.alterUser("bob"), Response.Admin.class);
                if (!r.error().isOk()) {
                    throw new IllegalStateException("AlterUser answered " + r.error() + " " + r.errorMsg());
                }
                return r;
            }
        });

        try (ConnectionPool pool = new ConnectionPool(options(anonymousTls, Credentials.scram("bob", "correct horse")))) {
            Connection bob = pool.get(address);
            Response.WhoAmI who = bob.call(new Request.WhoAmI(), Response.WhoAmI.class);
            assertEquals("bob", who.name());
            assertEquals(SessionState.ACTIVE, bob.sessionState());
            assertSame(bob, pool.get(address));
        }
    }

    @Test
    @Order(4)
    void aWrongPasswordAndAnUnknownUserAreRefusedTheSameWay() throws Exception {
        AuthenticationException wrong = assertThrows(
                AuthenticationException.class,
                () -> Connection.open(address, options(anonymousTls, Credentials.scram("bob", "wrong"))));
        AuthenticationException unknown = assertThrows(
                AuthenticationException.class,
                () -> Connection.open(address, options(anonymousTls, Credentials.scram("nobody", "wrong"))));
        assertEquals(ErrorCode.AUTHENTICATION_FAILED, wrong.code());
        assertEquals(wrong.getMessage(), unknown.getMessage(), "the node does not say which was wrong");
        assertEquals(ErrorCode.AUTHENTICATION_FAILED, unknown.code());
    }

    @Test
    @Order(5)
    void aMechanismTheNodeDoesNotOfferIsNamedWithTheOnesItDoes() {
        AuthenticationException e = assertThrows(
                AuthenticationException.class,
                () -> Connection.open(address, options(anonymousTls, Credentials.oauthBearer(() -> "token"))));
        assertEquals(ErrorCode.UNSUPPORTED_SASL_MECHANISM, e.code());
        assertTrue(e.getMessage().contains("OAUTHBEARER is not enabled"), e.getMessage());
        assertTrue(e.getMessage().contains("SCRAM-SHA-512"), e.getMessage());
    }

    @Test
    @Order(6)
    void theSessionIsRenewedOnTheSameConnectionAndEndsWhenItIsNot() throws Exception {
        Credentials bob = Credentials.scram("bob", "correct horse");
        try (ConnectionPool pool = new ConnectionPool(options(anonymousTls, bob))) {
            Connection c = pool.get(address);
            long granted = SESSION_MS; // the node grants at most its maximum
            Thread.sleep(granted * 4 / 5 + 200);
            assertEquals(SessionState.RENEW_DUE, c.sessionState());
            assertSame(c, pool.get(address), "renewed, same connection");
            assertEquals(SessionState.ACTIVE, c.sessionState());
            // Past the first session's end, the renewed one carries the connection.
            Thread.sleep(granted / 5 + 300);
            assertEquals("bob", c.call(new Request.WhoAmI(), Response.WhoAmI.class).name());
        }

        // Without renewal the node ends the session and refuses what follows.
        try (Connection c = Connection.open(address, options(anonymousTls, bob))) {
            assertEquals("bob", c.call(new Request.WhoAmI(), Response.WhoAmI.class).name());
            Thread.sleep(SESSION_MS + 300);
            assertEquals(SessionState.ENDED, c.sessionState());
            BrokerException e = assertThrows(BrokerException.class, () -> c.call(new Request.WhoAmI()));
            assertEquals(ErrorCode.UNAUTHORIZED, e.code());
        }
    }

    @Test
    @Order(7)
    void aPoolReplacesAConnectionWhoseSessionHasEnded() throws Exception {
        try (ConnectionPool pool = new ConnectionPool(options(anonymousTls, Credentials.scram("bob", "correct horse")))) {
            Connection first = pool.get(address);
            first.close(); // as if the node had dropped it
            Connection second = pool.get(address);
            assertNotEquals(first, second);
            assertEquals("bob", second.call(new Request.WhoAmI(), Response.WhoAmI.class).name());
            assertInstanceOf(Response.WhoAmI.class, second.call(new Request.WhoAmI()));
        }
    }
}

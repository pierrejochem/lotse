package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.auth.Credentials;
import dev.lotse.auth.TokenLifetime;
import dev.lotse.net.LotseBinary;
import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Messages;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Access tokens (ADR 0133) against one real node with TLS: the admin, signed in with a certificate, makes a token; a
 * client with no certificate signs in with it as the admin, the node lists the session with the token's id, and once
 * the token is revoked the open session ends and the token signs no one in.
 */
class RealTokenTest {
    /** Cluster version 0.1, which tokens need, as {@code major << 16 | minor}. */
    private static final long TOKENS_SINCE = 1;

    private static RealCluster cluster;

    @BeforeAll
    static void startNode() throws Exception {
        Path bin = LotseBinary.require();
        cluster = new RealCluster(bin, 1, true);
        cluster.startAll();
    }

    @AfterAll
    static void stopNode() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private static ClientConfig.Builder withToken(String text) throws Exception {
        return ClientConfig.builder(cluster.bootstrap())
                .tls(cluster.anonymousTls())
                .credentials(Credentials.token(text))
                .retryFor(Duration.ofSeconds(10));
    }

    /** A fresh cluster takes the version its nodes run once each has reported it (ADR 0127). */
    private static void awaitTokenVersion(LotseClient admin) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            Long v = admin.describeUpgrade().clusterVersion();
            if (v != null && v >= TOKENS_SINCE) {
                return;
            }
            assertTrue(System.nanoTime() - deadline < 0, "the cluster never took version 0.1: " + v);
            Thread.sleep(100);
        }
    }

    private static boolean signedOut(ClientException e) {
        return e instanceof ClientException.Authentication
                || (e instanceof ClientException.Broker b && b.code().equals(ErrorCode.UNAUTHORIZED))
                // The revocation closed the connection while a call was on it: the client reports the refusal
                // that followed as unknown, as it does after any lost attempt.
                || (e instanceof ClientException.Unknown && e.getMessage().endsWith("authentication failed"));
    }

    @Test
    void aTokenSignsInAsItsOwnerUntilItIsRevoked() throws Exception {
        try (LotseClient admin = new LotseClient(ClientConfig.builder(cluster.bootstrap())
                .tls(cluster.adminTls())
                .retryFor(Duration.ofSeconds(60))
                .build())) {
            awaitTokenVersion(admin);
            CreatedToken created = admin.createToken("laptop", null, TokenLifetime.WEEK);
            assertTrue(created.id() != 0 && created.text().startsWith("lotse_tok_"), created.toString());
            long days = (created.expiresAtMs() - System.currentTimeMillis()) / 86_400_000L;
            assertEquals(6, days, "7 days from now");

            try (LotseClient viaToken = new LotseClient(withToken(created.text()).build())) {
                assertEquals("alice", viaToken.whoAmI().name());
                List<Messages.TokenEntry> listed = viaToken.describeTokens(null);
                assertEquals(1, listed.size());
                assertEquals(created.id(), listed.get(0).id());
                assertEquals("laptop", listed.get(0).name());
                assertEquals("alice", listed.get(0).owner());
                // A session signed in with a token makes no tokens.
                ClientException.Broker refused = assertThrows(
                        ClientException.Broker.class, () -> viaToken.createToken("more", null, TokenLifetime.WEEK));
                assertEquals(ErrorCode.UNAUTHORIZED, refused.code());

                // The node lists the session by its token, never the secret.
                boolean seen = false;
                for (NodeSessions n : admin.nodeSessions()) {
                    assertTrue(n.sessions() != null, "node " + n.nodeId() + ": " + n.error());
                    for (Messages.SessionEntry s : n.sessions().sessions()) {
                        if (s.mechanism() == Messages.SessionMechanism.TOKEN) {
                            assertEquals(Long.valueOf(created.id()), s.tokenId());
                            assertEquals("alice", s.name());
                            seen = true;
                        }
                    }
                }
                assertTrue(seen, "the token's session is listed");

                // The same id with another secret is not the token.
                String text = created.text();
                String forged = text.substring(0, text.length() - 1) + (text.endsWith("A") ? "Q" : "A");
                try (LotseClient forger = new LotseClient(withToken(forged).build())) {
                    assertThrows(ClientException.Authentication.class, forger::whoAmI);
                }

                // Revoked, it ends the open session and signs no one in.
                admin.revokeToken(created.id());
                ClientException.Broker again =
                        assertThrows(ClientException.Broker.class, () -> admin.revokeToken(created.id()));
                assertEquals(ErrorCode.UNKNOWN_RESOURCE, again.code());
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                boolean ended = false;
                while (!ended) {
                    assertTrue(System.nanoTime() - deadline < 0, "the revoked token's session goes on");
                    try {
                        viaToken.whoAmI();
                        Thread.sleep(100);
                    } catch (ClientException e) {
                        assertTrue(signedOut(e), e.toString());
                        ended = true;
                    }
                }
            }
            try (LotseClient late = new LotseClient(withToken(created.text()).build())) {
                assertThrows(ClientException.Authentication.class, late::whoAmI);
            }
            assertTrue(admin.describeTokens(null).isEmpty(), "a revoked token is not listed");
        }
    }
}

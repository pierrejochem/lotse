package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.net.LotseBinary;
import dev.lotse.net.Pem;
import dev.lotse.net.TlsConfig;
import dev.lotse.proto.ErrorCode;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Renewing a client certificate over a connection made with it (ADR 0123), against one real node with a renewal CA:
 * the call writes the new files, a {@link TlsConfig} built from them presents the new certificate at its next
 * connection, which is outside its window and so cannot be renewed again; and a client configured to renew by itself
 * does so once its certificate is in its window.
 */
class RealRenewalTest {
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

    private static ClientConfig.Builder config(TlsConfig tls) {
        return ClientConfig.builder(cluster.bootstrap()).tls(tls).retryFor(Duration.ofSeconds(60));
    }

    private static X509Certificate leaf(Path file) throws Exception {
        return Pem.certificatesFile(file).get(0);
    }

    @Test
    void aCertificateInItsWindowIsRenewedAndTheNewFilesAreUsedAtTheNextConnection() throws Exception {
        Path cert = cluster.certFile("carol");
        Path key = cluster.keyFile("carol");
        TlsConfig tls = cluster.userTls("carol");
        X509Certificate before = leaf(cert);
        try (LotseClient carol = new LotseClient(config(tls).build())) {
            RenewedCertificate renewed = carol.renewCertificate(cert, key);
            List<X509Certificate> chain = Pem.certificatesFile(cert);
            assertEquals(2, chain.size(), "the new certificate and the renewal CA's");
            assertNotEquals(before.getSerialNumber(), chain.get(0).getSerialNumber());
            assertEquals("CN=carol", chain.get(0).getSubjectX500Principal().getName());
            assertEquals(renewed.notAfterMs() / 1000, chain.get(0).getNotAfter().getTime() / 1000);
            long days = (renewed.notAfterMs() - System.currentTimeMillis()) / 86_400_000L;
            assertEquals(89, days, "90 days from now");
        }
        // The same TlsConfig, on a new client's connections, shows the renewed certificate: 90 days long, it is not
        // in its window, so renewing again is refused.
        try (LotseClient again = new LotseClient(config(tls).build())) {
            assertEquals("carol", again.whoAmI().name());
            ClientException.Broker refused = assertThrows(ClientException.Broker.class, again::renewCertificate);
            assertEquals(ErrorCode.INVALID_REQUEST, refused.code());
            assertTrue(refused.getMessage().contains("two thirds"), refused.getMessage());
        }
    }

    @Test
    void aClientConfiguredToRenewByItselfRenewsInTheWindow() throws Exception {
        Path cert = cluster.certFile("dave");
        Path key = cluster.keyFile("dave");
        java.math.BigInteger before = leaf(cert).getSerialNumber();
        try (LotseClient dave = new LotseClient(config(cluster.userTls("dave"))
                .renewCertificate(cert, key)
                .renewCheck(Duration.ofMillis(200))
                .build())) {
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (leaf(cert).getSerialNumber().equals(before)) {
                assertTrue(System.nanoTime() - deadline < 0, "never renewed: " + dave.renewalFailure());
                Thread.sleep(100);
            }
            assertEquals(2, Pem.certificatesFile(cert).size());
            // Renewed, it is out of its window: the next looks change nothing.
            java.math.BigInteger renewed = leaf(cert).getSerialNumber();
            Thread.sleep(1_000);
            assertEquals(renewed, leaf(cert).getSerialNumber());
            assertNull(dave.renewalFailure());
        }
    }
}

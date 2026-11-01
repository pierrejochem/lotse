package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.net.LotseBinary;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * One real node with TLS 1.3 and a required login: the admin's certificate lets the client create and use a stream,
 * and a client without a certificate is refused as {@link ClientException.Authentication} at once, not retried until
 * the time runs out.
 */
class RealTlsNodeTest {
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

    @Test
    void anAdminWithACertificateCreatesWritesAndReads() throws Exception {
        try (LotseClient admin = new LotseClient(ClientConfig.builder(cluster.bootstrap())
                .tls(cluster.adminTls())
                .retryFor(Duration.ofSeconds(60))
                .build())) {
            admin.createStream("secure", 1, 1);
            admin.produce("secure", 0, OutMessage.value("over tls"));
            // The log starts with entries of the broker's own, which a read returns without any message in them.
            java.util.List<dev.lotse.record.BatchRecord> got = new java.util.ArrayList<>();
            long offset = 0;
            for (int i = 0; i < 50 && got.isEmpty(); i++) {
                Fetched f = admin.fetch("secure", 0, offset, Duration.ofMillis(100));
                got.addAll(f.committedMessages());
                offset = f.nextOffset(offset);
            }
            assertEquals("over tls", got.get(0).value().toUtf8());
        }
    }

    @Test
    void aClientWithoutACertificateIsRefusedAtOnce() throws Exception {
        try (LotseClient stranger = new LotseClient(ClientConfig.builder(cluster.bootstrap())
                .tls(cluster.anonymousTls())
                .retryFor(Duration.ofSeconds(60))
                .build())) {
            long start = System.nanoTime();
            assertThrows(ClientException.Authentication.class, () -> stranger.produce("secure", 0, OutMessage.value("x")));
            assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(20)) < 0, "not retried for a minute");
        }
    }

    @Test
    void aClientThatAsksForNoTransportIsRefusedBeforeAnythingIsSent() {
        try (LotseClient bare = new LotseClient(ClientConfig.builder(cluster.bootstrap()).build())) {
            assertThrows(dev.lotse.net.InsecureTransportException.class, () -> bare.metadata("secure"));
        }
    }
}

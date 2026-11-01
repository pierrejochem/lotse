package dev.lotse.net;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * TLS 1.3 against a Java server with certificates made by the real {@code lotse certs} (a CA, a node certificate
 * with the IP entry 127.0.0.1 and a client certificate for "alice"). Skipped when no {@code lotse} executable is
 * found; see {@link LotseBinary}.
 */
class TlsTest {
    private static final String[] TLS13 = {"TLSv1.3"};

    private static Certs certs;
    private static Certs otherCa;

    @BeforeAll
    static void makeCertificates() throws Exception {
        Path bin = LotseBinary.require();
        certs = new Certs(bin, "");
        otherCa = new Certs(bin, "");
    }

    @AfterAll
    static void forget() {
        // the temporary directories hold throwaway certificates only
    }

    private static ConnectionOptions.Builder options(TlsConfig tls) {
        return ConnectionOptions.builder().tls(tls).requestTimeout(Duration.ofSeconds(5));
    }

    private static Address at(String host, TlsTestServer server) {
        return new Address(host, server.port());
    }

    private static Response.ApiVersions ping(Connection c) throws Exception {
        return c.apiVersions().get(5, TimeUnit.SECONDS);
    }

    @Test
    void theKeyThatLotseCertsWritesLoadsAndMatchesItsCertificate() throws Exception {
        for (Path[] pair : new Path[][] {{certs.nodeCert, certs.nodeKey}, {certs.clientCert, certs.clientKey}}) {
            X509Certificate cert = Pem.certificatesFile(pair[0]).get(0);
            PrivateKey key = Pem.privateKeyFile(pair[1]);
            assertEquals("EC", key.getAlgorithm(), "rcgen's default is ECDSA P-256");
            Signature sign = Signature.getInstance("SHA256withECDSA");
            sign.initSign(key);
            sign.update(new byte[] {1, 2, 3});
            byte[] signature = sign.sign();
            PublicKey pub = cert.getPublicKey();
            Signature verify = Signature.getInstance("SHA256withECDSA");
            verify.initVerify(pub);
            verify.update(new byte[] {1, 2, 3});
            assertTrue(verify.verify(signature), "the key belongs to the certificate");
        }
        assertThrows(IllegalArgumentException.class, () -> Pem.privateKey("-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----\n"));
        assertThrows(IllegalArgumentException.class, () -> Pem.privateKey("nothing"));
        assertThrows(IllegalArgumentException.class, () -> Pem.certificates("nothing"));
    }

    @Test
    void aNodeWithACertificateFromTheClustersCaIsReachedAtItsIpAddress() throws Exception {
        try (TlsTestServer server = new TlsTestServer(certs, TLS13, false);
                Connection c = Connection.open(at("127.0.0.1", server), options(TlsConfig.fromFiles(certs.ca)).build())) {
            assertTrue(c.isSecure());
            assertEquals(List.of("tls-test"), ping(c).features());
        }
    }

    @Test
    void theNameMustBeOneTheCertificateIsValidFor() throws Exception {
        // The certificate lists the IP address 127.0.0.1 and the node's URI, but not the name "localhost".
        try (TlsTestServer server = new TlsTestServer(certs, TLS13, false)) {
            ConnectionException e = assertThrows(
                    ConnectionException.class,
                    () -> Connection.open(at("localhost", server), options(TlsConfig.fromFiles(certs.ca)).build()));
            assertFalse(e.requestSent());
            assertTrue(String.valueOf(e.getMessage()).contains("localhost") || e.getCause() != null, e.getMessage());
        }
    }

    @Test
    void aCertificateFromAnotherCaIsRefused() throws Exception {
        try (TlsTestServer server = new TlsTestServer(certs, TLS13, false)) {
            ConnectionException e = assertThrows(
                    ConnectionException.class,
                    () -> Connection.open(at("127.0.0.1", server), options(TlsConfig.fromFiles(otherCa.ca)).build()));
            assertFalse(e.requestSent());
            assertInstanceOf(javax.net.ssl.SSLException.class, e.getCause());
        }
    }

    @Test
    void theJdkRootsAreNotTrusted() throws Exception {
        // Only the CA that was given counts: a CA the JDK trusts would not be asked.
        try (TlsTestServer server = new TlsTestServer(certs, TLS13, false)) {
            TlsConfig onlyOther = TlsConfig.builder().ca(otherCa.ca).build();
            assertThrows(ConnectionException.class, () -> Connection.open(at("127.0.0.1", server), options(onlyOther).build()));
        }
    }

    @Test
    void tls12IsNotSpoken() throws Exception {
        try (TlsTestServer server = new TlsTestServer(certs, new String[] {"TLSv1.2"}, false)) {
            ConnectionException e = assertThrows(
                    ConnectionException.class,
                    () -> Connection.open(at("127.0.0.1", server), options(TlsConfig.fromFiles(certs.ca)).build()));
            assertInstanceOf(javax.net.ssl.SSLException.class, e.getCause());
        }
    }

    @Test
    void aClientCertificateIsPresentedWhenTheNodeAsksForOne() throws Exception {
        try (TlsTestServer server = new TlsTestServer(certs, TLS13, true)) {
            try (Connection c = Connection.open(
                    at("127.0.0.1", server), options(TlsConfig.fromFiles(certs.ca, certs.clientCert, certs.clientKey)).build())) {
                assertEquals(List.of("tls-test"), ping(c).features());
            }
            assertEquals(List.of("CN=alice"), server.clientSubjects);
        }
    }

    @Test
    void withoutAClientCertificateANodeThatRequiresOneRefuses() throws Exception {
        try (TlsTestServer server = new TlsTestServer(certs, TLS13, true)) {
            // In TLS 1.3 the client finishes its handshake before the server has checked the client's
            // certificate, so the refusal can arrive with the first request instead of the handshake.
            ConnectionException e = assertThrows(ConnectionException.class, () -> {
                try (Connection c = Connection.open(at("127.0.0.1", server), options(TlsConfig.fromFiles(certs.ca)).build())) {
                    try {
                        ping(c);
                    } catch (ExecutionException x) {
                        throw (RuntimeException) x.getCause();
                    }
                }
            });
            assertTrue(e.getMessage() != null);
        }
    }

    @Test
    void aClientCertificateFromAnotherCaIsRefused() throws Exception {
        try (TlsTestServer server = new TlsTestServer(certs, TLS13, true)) {
            TlsConfig wrong = TlsConfig.fromFiles(certs.ca, otherCa.clientCert, otherCa.clientKey);
            assertThrows(ConnectionException.class, () -> {
                try (Connection c = Connection.open(at("127.0.0.1", server), options(wrong).build())) {
                    try {
                        ping(c);
                    } catch (ExecutionException x) {
                        throw (RuntimeException) x.getCause();
                    }
                }
            });
        }
    }

    @Test
    void aTlsConfigNeedsACa() {
        assertThrows(IllegalStateException.class, () -> TlsConfig.builder().build());
        assertArrayEquals(new String[] {"TLSv1.3"}, new String[] {TlsConfig.PROTOCOL});
    }

    @Test
    void theTlsRequestsGoThroughTheSameFraming() throws Exception {
        try (TlsTestServer server = new TlsTestServer(certs, TLS13, false);
                Connection c = Connection.open(at("127.0.0.1", server), options(TlsConfig.fromFiles(certs.ca)).build())) {
            for (int i = 0; i < 20; i++) {
                assertInstanceOf(Response.ApiVersions.class, c.call(new Request.ApiVersions()));
            }
        }
    }
}

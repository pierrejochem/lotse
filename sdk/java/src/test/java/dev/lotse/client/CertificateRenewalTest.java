package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.net.Pem;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The renewal window, new keys and writing the files (ADR 0123), without a node. */
class CertificateRenewalTest {
    @Test
    void theWindowOpensInTheLast30DaysOrAfterTwoThirds() {
        long day = 86_400_000L;
        // A year: two thirds (day 243 and a third) come before the last 30 days.
        assertEquals(365 * day / 3 * 2, CertificateRenewal.windowOpensMs(0, 365 * day));
        // 60 days: the last 30 days come first.
        assertEquals(30 * day, CertificateRenewal.windowOpensMs(0, 60 * day));
        assertEquals(0, CertificateRenewal.windowOpensMs(100, 103));
        assertTrue(CertificateRenewal.mayRenew(0, 60 * day, 30 * day));
        assertFalse(CertificateRenewal.mayRenew(0, 60 * day, 30 * day - 1));
        assertFalse(CertificateRenewal.mayRenew(0, 60 * day, 60 * day), "not once it expired");
    }

    @Test
    void aNewKeyIsAP256KeyWithASigningRequest() {
        CertificateRenewal.NewKey k = CertificateRenewal.newKey();
        assertEquals("EC", Pem.privateKey(k.keyPem()).getAlgorithm());
        assertEquals(0x30, k.csr()[0] & 0xFF, "a DER sequence");
        assertTrue(k.csr().length > 150 && k.csr().length < 4096, "about 200 bytes: " + k.csr().length);
    }

    @Test
    void theKeyIsWrittenThenTheChainEachWhole() throws Exception {
        Path dir = Files.createTempDirectory("lotse-renewal");
        Path cert = dir.resolve("c.pem");
        Path key = dir.resolve("c.key");
        Files.writeString(cert, "old cert");
        Files.writeString(key, "old key");
        CertificateRenewal.write(new RenewedCertificate("new key", "new chain", 1), cert, key);
        assertEquals("new chain", Files.readString(cert));
        assertEquals("new key", Files.readString(key));
        assertFalse(Files.exists(dir.resolve("c.pem.new")) || Files.exists(dir.resolve("c.key.new")));
    }
}

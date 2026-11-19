package dev.lotse.client;

import dev.lotse.net.Pem;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import java.util.List;

/**
 * Renewing a client certificate over a connection made with it (ADR 0123): when a certificate may be renewed, a new
 * key with its signing request, and writing the files a {@link dev.lotse.net.TlsConfig} reads.
 *
 * <p>A certificate may be renewed in its last 30 days or once two thirds of its validity have passed, whichever comes
 * first, until it expires. The new certificate is valid for 90 days.
 */
public final class CertificateRenewal {
    /** The last stretch of a certificate's validity in which it may always be renewed: 30 days. */
    public static final long WINDOW_MS = 30L * 86_400_000L;

    private CertificateRenewal() {}

    /**
     * The millisecond since the Unix epoch from which a certificate valid from {@code notBeforeMs} until
     * {@code notAfterMs} may be renewed: the earlier of its last 30 days and two thirds of its validity.
     */
    public static long windowOpensMs(long notBeforeMs, long notAfterMs) {
        long span = Math.max(0, notAfterMs - notBeforeMs);
        long twoThirds = notBeforeMs + span / 3 * 2 + span % 3 * 2 / 3;
        return Math.min(Math.max(0, notAfterMs - WINDOW_MS), twoThirds);
    }

    /** Whether such a certificate may be renewed at {@code nowMs}: its window is open and it has not expired. */
    public static boolean mayRenew(long notBeforeMs, long notAfterMs, long nowMs) {
        return nowMs >= windowOpensMs(notBeforeMs, notAfterMs) && nowMs < notAfterMs;
    }

    /**
     * Whether the certificate in a PEM file (its first block, the leaf) may be renewed at {@code nowMs}.
     *
     * @throws IOException if the file cannot be read
     */
    public static boolean due(Path certPem, long nowMs) throws IOException {
        X509Certificate leaf = Pem.certificatesFile(certPem).get(0);
        return mayRenew(leaf.getNotBefore().getTime(), leaf.getNotAfter().getTime(), nowMs);
    }

    /**
     * A new key and a signing request for it.
     *
     * @param keyPem the private key, PKCS#8 PEM
     * @param csr the signing request, PKCS#10 DER, signed with the key
     */
    public record NewKey(String keyPem, byte[] csr) {}

    /** A new ECDSA P-256 key and a PKCS#10 request signed with it; the node ignores the request's subject. */
    public static NewKey newKey() {
        try {
            KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");
            gen.initialize(new ECGenParameterSpec("secp256r1"));
            KeyPair pair = gen.generateKeyPair();
            byte[] cn = "renewal".getBytes(StandardCharsets.UTF_8);
            byte[] name = tlv(0x30, tlv(0x31, tlv(0x30, COMMON_NAME, tlv(0x0C, cn))));
            byte[] info = tlv(0x30, new byte[] {0x02, 0x01, 0x00}, name, pair.getPublic().getEncoded(), new byte[] {
                (byte) 0xA0, 0x00
            });
            Signature sign = Signature.getInstance("SHA256withECDSA");
            sign.initSign(pair.getPrivate());
            sign.update(info);
            byte[] sig = sign.sign();
            byte[] bits = new byte[sig.length + 1];
            System.arraycopy(sig, 0, bits, 1, sig.length);
            byte[] csr = tlv(0x30, info, tlv(0x30, ECDSA_WITH_SHA256), tlv(0x03, bits));
            return new NewKey(pem("PRIVATE KEY", pair.getPrivate().getEncoded()), csr);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("this JDK makes no P-256 keys: " + e.getMessage(), e);
        }
    }

    /** PEM text of one DER block. */
    public static String pem(String label, byte[] der) {
        String body = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(der);
        return "-----BEGIN " + label + "-----\n" + body + "\n-----END " + label + "-----\n";
    }

    /**
     * Writes the renewed key and chain over the files: the key first, each through a file beside it that is then
     * moved over the old one, so a reader sees one file or the other, never half of one. A
     * {@link dev.lotse.net.TlsConfig} that reads them takes up the pair once both are written.
     */
    public static void write(RenewedCertificate renewed, Path certPem, Path keyPem) throws IOException {
        replace(keyPem, renewed.keyPem(), true);
        replace(certPem, renewed.chainPem(), false);
    }

    private static void replace(Path path, String text, boolean secret) throws IOException {
        Path dir = path.toAbsolutePath().getParent();
        Path tmp = dir.resolve(path.getFileName() + ".new");
        Files.writeString(tmp, text, StandardCharsets.US_ASCII);
        if (secret) {
            try {
                Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rw-------"));
            } catch (UnsupportedOperationException e) {
                // not a POSIX file system: the directory's permissions decide
            }
        }
        try {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** The PEM chain of DER certificates, leaf first. */
    static String chainPem(List<byte[]> chain) {
        StringBuilder b = new StringBuilder();
        for (byte[] c : chain) {
            b.append(pem("CERTIFICATE", c));
        }
        return b.toString();
    }

    /** OID 2.5.4.3, commonName. */
    private static final byte[] COMMON_NAME = {0x06, 0x03, 0x55, 0x04, 0x03};
    /** OID 1.2.840.10045.4.3.2, ecdsa-with-SHA256. */
    private static final byte[] ECDSA_WITH_SHA256 = {
        0x06, 0x08, 0x2A, (byte) 0x86, 0x48, (byte) 0xCE, 0x3D, 0x04, 0x03, 0x02
    };

    /** A DER value: tag, definite length, then the parts. */
    private static byte[] tlv(int tag, byte[]... parts) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            body.writeBytes(p);
        }
        int n = body.size();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(tag);
        if (n < 0x80) {
            out.write(n);
        } else if (n < 0x100) {
            out.write(0x81);
            out.write(n);
        } else {
            out.write(0x82);
            out.write(n >> 8);
            out.write(n & 0xFF);
        }
        out.writeBytes(body.toByteArray());
        return out.toByteArray();
    }
}

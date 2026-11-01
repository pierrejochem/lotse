package dev.lotse.net;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A small PEM reader for certificates and unencrypted PKCS#8 private keys, which is what {@code lotse certs} writes
 * (ECDSA P-256 keys in {@code BEGIN PRIVATE KEY} blocks). RSA, EC and Ed25519 PKCS#8 keys load; the traditional
 * {@code BEGIN RSA PRIVATE KEY} and {@code BEGIN EC PRIVATE KEY} forms and encrypted keys do not, and the error says
 * how to convert them.
 */
public final class Pem {
    private static final Pattern BLOCK =
            Pattern.compile("-----BEGIN ([A-Z0-9 ]+)-----([^-]*)-----END \\1-----");

    private Pem() {}

    /** The bodies of the blocks of the given label, decoded. */
    private static List<byte[]> blocks(String pem, String label) {
        List<byte[]> out = new ArrayList<>();
        Matcher m = BLOCK.matcher(pem);
        while (m.find()) {
            if (m.group(1).equals(label)) {
                try {
                    out.add(Base64.getMimeDecoder().decode(m.group(2)));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("a " + label + " block is not valid base64");
                }
            }
        }
        return out;
    }

    /**
     * The certificates in {@code pem}, in file order.
     *
     * @throws IllegalArgumentException if there are none or one cannot be parsed
     */
    public static List<X509Certificate> certificates(String pem) {
        List<X509Certificate> out = new ArrayList<>();
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            for (byte[] der : blocks(pem, "CERTIFICATE")) {
                out.add((X509Certificate) factory.generateCertificate(new ByteArrayInputStream(der)));
            }
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("not a certificate: " + e.getMessage(), e);
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("no -----BEGIN CERTIFICATE----- block");
        }
        return out;
    }

    /** The certificates in a file. */
    public static List<X509Certificate> certificatesFile(Path file) throws IOException {
        try {
            return certificates(Files.readString(file, StandardCharsets.US_ASCII));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(file + ": " + e.getMessage(), e);
        }
    }

    /**
     * The private key in {@code pem}: an unencrypted PKCS#8 key ({@code BEGIN PRIVATE KEY}).
     *
     * @throws IllegalArgumentException if there is no such key
     */
    public static PrivateKey privateKey(String pem) {
        if (pem.contains("-----BEGIN ENCRYPTED PRIVATE KEY-----")) {
            throw new IllegalArgumentException(
                    "the key is encrypted; decrypt it first: openssl pkcs8 -in key.pem -out plain.pem -nocrypt");
        }
        if (pem.contains("-----BEGIN RSA PRIVATE KEY-----") || pem.contains("-----BEGIN EC PRIVATE KEY-----")) {
            throw new IllegalArgumentException(
                    "the key is in the traditional format; convert it to PKCS#8: "
                            + "openssl pkcs8 -topk8 -nocrypt -in key.pem -out pkcs8.pem");
        }
        List<byte[]> keys = blocks(pem, "PRIVATE KEY");
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("no -----BEGIN PRIVATE KEY----- block");
        }
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keys.get(0));
        // The algorithm is inside the encoding; trying the ones a certificate can use is the short way to find it.
        for (String algorithm : List.of("EC", "RSA", "Ed25519")) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec);
            } catch (GeneralSecurityException e) {
                // not this one
            }
        }
        throw new IllegalArgumentException("the PKCS#8 key is neither EC, RSA nor Ed25519");
    }

    /** The private key in a file. */
    public static PrivateKey privateKeyFile(Path file) throws IOException {
        try {
            return privateKey(Files.readString(file, StandardCharsets.US_ASCII));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(file + ": " + e.getMessage(), e);
        }
    }
}

package dev.lotse.auth;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** The primitives of SCRAM-SHA-512 on raw bytes. */
final class Scram {
    static final byte[] CLIENT_KEY = "Client Key".getBytes(StandardCharsets.US_ASCII);
    static final byte[] SERVER_KEY = "Server Key".getBytes(StandardCharsets.US_ASCII);
    private static final String HMAC = "HmacSHA512";
    private static final int HMAC_BLOCK = 128;

    private Scram() {}

    private static Mac mac(byte[] key) {
        try {
            Mac mac = Mac.getInstance(HMAC);
            // HMAC zero-pads a short key to the block size, so an empty key and 128 zero bytes are the same key;
            // the JDK refuses to build a key from no bytes.
            mac.init(new SecretKeySpec(key.length == 0 ? new byte[HMAC_BLOCK] : key, HMAC));
            return mac;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("the JDK has no HmacSHA512", e);
        }
    }

    static byte[] hmac(byte[] key, byte[] data) {
        return mac(key).doFinal(data);
    }

    static byte[] sha512(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-512").digest(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("the JDK has no SHA-512", e);
        }
    }

    /** PBKDF2-HMAC-SHA512 with a 64-byte output: one block, so no block counter loop. */
    static byte[] saltedPassword(byte[] password, byte[] salt, int iterations) {
        Mac mac = mac(password);
        mac.update(salt);
        mac.update(new byte[] {0, 0, 0, 1});
        byte[] u = mac.doFinal();
        byte[] t = u.clone();
        for (int i = 1; i < iterations; i++) {
            u = mac.doFinal(u);
            for (int j = 0; j < t.length; j++) {
                t[j] ^= u[j];
            }
        }
        return t;
    }

    static byte[] xor(byte[] a, byte[] b) {
        byte[] out = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            out[i] = (byte) (a[i] ^ b[i]);
        }
        return out;
    }

    static boolean constantTimeEquals(byte[] a, byte[] b) {
        return MessageDigest.isEqual(a, b);
    }
}

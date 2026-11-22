package dev.lotse.auth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;

/**
 * An access token (ADR 0133): its id and its secret, as {@code lotse_tok_<id>_<secret>}. The id is 16 lower-case hex
 * digits and names the token in the cluster's state; the secret is 32 random bytes in unpadded base64url (43
 * characters). The cluster keeps only the SHA-256 of the secret bytes, which is all a client sends when it creates a
 * token. Nothing here draws randomness: the caller passes the secret in.
 */
public final class AccessToken {
    private static final String PREFIX = "lotse_tok_";
    private static final int SECRET_BYTES = 32;
    private static final int SECRET_CHARS = 43;
    private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();

    private final long id;
    private final byte[] secret;

    /**
     * A token from an id and a secret the caller drew.
     *
     * @param id the token's id, a {@code u64}
     * @param secret the 32 secret bytes; copied
     */
    public AccessToken(long id, byte[] secret) {
        if (secret.length != SECRET_BYTES) {
            throw new IllegalArgumentException("a token's secret is 32 bytes, got " + secret.length);
        }
        this.id = id;
        this.secret = secret.clone();
    }

    /** The token's id, a {@code u64}. */
    public long id() {
        return id;
    }

    /** A copy of the 32 secret bytes. */
    public byte[] secret() {
        return secret.clone();
    }

    /** The text the user holds: {@code lotse_tok_<id>_<secret>}. */
    public String text() {
        String hex = Long.toHexString(id);
        return PREFIX + "0".repeat(16 - hex.length()) + hex + "_" + B64.encodeToString(secret);
    }

    /** The SHA-256 of the secret bytes: all the cluster keeps, and all a client sends to create the token. */
    public byte[] hash() {
        try {
            return MessageDigest.getInstance("SHA-256").digest(secret);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("every Java platform has SHA-256", e);
        }
    }

    /**
     * Reads a token's text, ignoring ASCII whitespace around it (a pasted token often carries a newline), as the
     * node does. Anything that is not exactly a token's canonical text is refused: upper-case hex, a sign, another
     * length, padding, or a last character whose spare bits are not zero, so one secret has one text.
     *
     * @return the token, or empty if {@code text} is not one
     */
    public static Optional<AccessToken> parse(String text) {
        String t = trimAscii(text);
        if (!t.startsWith(PREFIX)) {
            return Optional.empty();
        }
        String rest = t.substring(PREFIX.length());
        int sep = rest.indexOf('_');
        if (sep < 0) {
            return Optional.empty();
        }
        String id = rest.substring(0, sep);
        String secret = rest.substring(sep + 1);
        if (id.length() != 16) {
            return Optional.empty();
        }
        for (int i = 0; i < id.length(); i++) {
            char c = id.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return Optional.empty();
            }
        }
        if (secret.length() != SECRET_CHARS) {
            return Optional.empty();
        }
        for (int i = 0; i < secret.length(); i++) {
            char c = secret.charAt(i);
            boolean b64 = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
                    || c == '_';
            if (!b64) {
                return Optional.empty();
            }
        }
        byte[] bytes = Base64.getUrlDecoder().decode(secret);
        // The JDK's decoder ignores the 2 spare bits of the last character; only the canonical text is a token.
        if (bytes.length != SECRET_BYTES || !B64.encodeToString(bytes).equals(secret)) {
            return Optional.empty();
        }
        return Optional.of(new AccessToken(Long.parseUnsignedLong(id, 16), bytes));
    }

    /** Strips what Rust's {@code trim_ascii} strips: space, tab, line feed, form feed and carriage return. */
    static String trimAscii(String s) {
        int start = 0;
        int end = s.length();
        while (start < end && isAsciiWhitespace(s.charAt(start))) {
            start++;
        }
        while (end > start && isAsciiWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(start, end);
    }

    private static boolean isAsciiWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r';
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof AccessToken other && id == other.id && MessageDigest.isEqual(secret, other.secret);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id) * 31 + Arrays.hashCode(secret);
    }

    /** Names the token by its id only: the secret stays out of logs. */
    @Override
    public String toString() {
        String hex = Long.toHexString(id);
        return "AccessToken[id=" + "0".repeat(16 - hex.length()) + hex + "]";
    }
}

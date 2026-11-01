package dev.lotse.auth;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * A client's end of one SCRAM-SHA-512 exchange (RFC 5802, RFC 7677), without I/O.
 *
 * <p>Three calls in this order: {@link #first}, {@link #respond} with the server's first message, {@link #verify}
 * with the server's final message. A call out of order fails. Channel binding is not used ({@code n,,}): TLS
 * ties the session to the server's certificate. The password is the PBKDF2 input as its UTF-8 bytes, with no
 * SASLprep; a password that SASLprep would change gives a different key than other SCRAM clients compute.
 *
 * <p>The client refuses a server whose nonce does not strictly extend its own, and an iteration count outside
 * {@value #MIN_ITERATIONS} to {@value #MAX_ITERATIONS}, which would let a hostile server make it spin. Not
 * thread-safe.
 */
public final class ScramClient {
    /** Fewest iterations a credential may have (RFC 7677 asks for at least 4096). */
    public static final int MIN_ITERATIONS = 4096;

    /** Most iterations a credential may have: a bound on the work one login costs either side. */
    public static final int MAX_ITERATIONS = 1 << 20;

    private enum State {
        START,
        SENT_FIRST,
        SENT_FINAL,
        DONE
    }

    private final String user;
    private final byte[] password;
    private final String nonce;
    private State state = State.START;
    private String bare;
    private byte[] serverSignature;

    /**
     * An exchange for {@code user} with {@code password}.
     *
     * @param nonce fresh and unpredictable: printable ASCII without a comma. {@link Authenticator} uses 24 random
     *     bytes in hex.
     */
    public ScramClient(String user, String password, String nonce) {
        this.user = user;
        this.password = password.getBytes(StandardCharsets.UTF_8);
        this.nonce = nonce;
    }

    /** {@code saslname}: {@code =} and {@code ,} escaped as {@code =3D} and {@code =2C}. */
    static String escape(String name) {
        return name.replace("=", "=3D").replace(",", "=2C");
    }

    /** The client's first message: {@code n,,n=<user>,r=<nonce>}. */
    public byte[] first() {
        bare = "n=" + escape(user) + ",r=" + nonce;
        state = State.SENT_FIRST;
        return ("n,," + bare).getBytes(StandardCharsets.UTF_8);
    }

    private static String attribute(String[] parts, int index, char key) {
        if (index >= parts.length) {
            throw new ScramException(ScramException.Kind.MALFORMED, "malformed SCRAM message: missing attribute " + key);
        }
        String part = parts[index];
        if (part.length() < 2 || part.charAt(0) != key || part.charAt(1) != '=') {
            throw new ScramException(ScramException.Kind.MALFORMED, "malformed SCRAM message: expected attribute " + key);
        }
        return part.substring(2);
    }

    private static boolean validNonce(String n) {
        if (n.isEmpty()) {
            return false;
        }
        for (int i = 0; i < n.length(); i++) {
            char c = n.charAt(i);
            if (c < 0x21 || c > 0x7e || c == ',') {
                return false;
            }
        }
        return true;
    }

    private static String utf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new ScramException(ScramException.Kind.MALFORMED, "malformed SCRAM message: not UTF-8");
        }
    }

    /**
     * Answers the server's first message with the client's final one.
     *
     * @throws ScramException if the message is malformed, its nonce does not extend ours, or its iteration count
     *     is out of bounds
     */
    public byte[] respond(byte[] serverFirstBytes) {
        if (state != State.SENT_FIRST) {
            throw new ScramException(ScramException.Kind.OUT_OF_ORDER, "unexpected SCRAM step");
        }
        state = State.DONE;
        String serverFirst = utf8(serverFirstBytes);
        String[] parts = serverFirst.split(",", -1);
        String serverNonce = attribute(parts, 0, 'r');
        byte[] salt;
        try {
            salt = Base64.getDecoder().decode(attribute(parts, 1, 's'));
        } catch (IllegalArgumentException e) {
            throw new ScramException(ScramException.Kind.MALFORMED, "malformed SCRAM message: salt is not base64");
        }
        String count = attribute(parts, 2, 'i');
        long iterations;
        try {
            if (count.isEmpty() || !count.chars().allMatch(c -> c >= '0' && c <= '9') || count.length() > 10) {
                throw new NumberFormatException();
            }
            iterations = Long.parseLong(count);
        } catch (NumberFormatException e) {
            throw new ScramException(ScramException.Kind.MALFORMED, "malformed SCRAM message: bad iterations");
        }
        // The server's nonce extends ours; a bound on the iterations keeps a hostile server from making the client spin.
        if (!serverNonce.startsWith(nonce)
                || serverNonce.length() == nonce.length()
                || !validNonce(serverNonce)
                || iterations < MIN_ITERATIONS
                || iterations > MAX_ITERATIONS) {
            throw new ScramException(
                    ScramException.Kind.MALFORMED, "malformed SCRAM message: server nonce or iteration count not acceptable");
        }
        byte[] salted = Scram.saltedPassword(password, salt, (int) iterations);
        byte[] clientKey = Scram.hmac(salted, Scram.CLIENT_KEY);
        byte[] storedKey = Scram.sha512(clientKey);
        String withoutProof = "c=biws,r=" + serverNonce;
        byte[] authMessage = (bare + "," + serverFirst + "," + withoutProof).getBytes(StandardCharsets.UTF_8);
        byte[] clientSignature = Scram.hmac(storedKey, authMessage);
        byte[] proof = Scram.xor(clientKey, clientSignature);
        byte[] serverKey = Scram.hmac(salted, Scram.SERVER_KEY);
        serverSignature = Scram.hmac(serverKey, authMessage);
        state = State.SENT_FINAL;
        return (withoutProof + ",p=" + Base64.getEncoder().encodeToString(proof)).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Checks the server's final message: the server proves it knew the credential.
     *
     * @throws ScramException of kind {@code SERVER_NOT_VERIFIED} if the signature is missing or wrong
     */
    public void verify(byte[] serverFinal) {
        if (state != State.SENT_FINAL) {
            throw new ScramException(ScramException.Kind.OUT_OF_ORDER, "unexpected SCRAM step");
        }
        state = State.DONE;
        String msg = utf8(serverFinal);
        if (!msg.startsWith("v=")) {
            throw notVerified();
        }
        byte[] signature;
        try {
            signature = Base64.getDecoder().decode(msg.substring(2));
        } catch (IllegalArgumentException e) {
            throw notVerified();
        }
        if (!Scram.constantTimeEquals(signature, serverSignature)) {
            throw notVerified();
        }
    }

    private static ScramException notVerified() {
        return new ScramException(
                ScramException.Kind.SERVER_NOT_VERIFIED, "the server could not prove it knows the credential");
    }
}

package dev.lotse.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Function;

/**
 * The broker's end of a SCRAM-SHA-512 exchange, written for tests from RFC 5802 and checked against the Rust
 * implementation through the vectors. It lets the fake broker of the connection tests accept or refuse a login.
 */
public final class TestScramServer {
    /** The login failed; the real broker answers {@code AUTHENTICATION_FAILED} and closes the connection. */
    public static final class Failed extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failed(String why) {
            super(why);
        }
    }

    /** What a finished exchange gives. */
    public record Result(String user, byte[] serverFinal) {}

    private final Function<String, ScramCredential> lookup;
    private final String serverNonce;
    private String user;
    private ScramCredential credential;
    private String nonce;
    private String authPrefix;

    public TestScramServer(Function<String, ScramCredential> lookup, String serverNonce) {
        this.lookup = lookup;
        this.serverNonce = serverNonce;
    }

    private static String unescape(String name) {
        return name.replace("=2C", ",").replace("=3D", "=");
    }

    public byte[] first(byte[] message) {
        String msg = new String(message, StandardCharsets.UTF_8);
        if (!msg.startsWith("n,,")) {
            throw new Failed("only n,, is supported");
        }
        String bare = msg.substring(3);
        String[] parts = bare.split(",");
        user = unescape(parts[0].substring(2));
        String clientNonce = parts[1].substring(2);
        credential = lookup.apply(user);
        if (credential == null) {
            throw new Failed("unknown user");
        }
        nonce = clientNonce + serverNonce;
        String serverFirst = "r=" + nonce + ",s=" + Base64.getEncoder().encodeToString(credential.salt()) + ",i="
                + credential.iterations();
        authPrefix = bare + "," + serverFirst;
        return serverFirst.getBytes(StandardCharsets.UTF_8);
    }

    public Result last(byte[] message) {
        String msg = new String(message, StandardCharsets.UTF_8);
        int at = msg.lastIndexOf(",p=");
        String withoutProof = msg.substring(0, at);
        byte[] proof = Base64.getDecoder().decode(msg.substring(at + 3));
        if (!withoutProof.equals("c=biws,r=" + nonce)) {
            throw new Failed("channel binding or nonce does not match");
        }
        byte[] authMessage = (authPrefix + "," + withoutProof).getBytes(StandardCharsets.UTF_8);
        byte[] clientSignature = Scram.hmac(credential.storedKey(), authMessage);
        byte[] clientKey = Scram.xor(proof, clientSignature);
        if (!java.security.MessageDigest.isEqual(Scram.sha512(clientKey), credential.storedKey())) {
            throw new Failed("wrong password");
        }
        byte[] serverSignature = Scram.hmac(credential.serverKey(), authMessage);
        return new Result(user, ("v=" + Base64.getEncoder().encodeToString(serverSignature)).getBytes(StandardCharsets.UTF_8));
    }
}

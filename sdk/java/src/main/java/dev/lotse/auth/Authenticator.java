package dev.lotse.auth;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.ByteString;
import java.security.SecureRandom;
import java.util.function.Function;

/**
 * Runs the SASL exchange of a connection: {@code SaslHandshake} with the mechanism, then {@code SaslAuthenticate}
 * rounds. Calling it again on a connection that is already authenticated renews the session; the node then insists
 * on the same principal.
 */
public final class Authenticator {
    private Authenticator() {}

    /**
     * Authenticates over {@code exchange}, which sends one request and returns its response, or throws if there is
     * none. Blocks for the round trips.
     *
     * @param exchange sends a request and waits for the answer; may throw whatever the transport throws
     * @param credentials who to log in as
     * @param random the source of the SCRAM client nonce
     * @return the session's lifetime in milliseconds as the node granted it with its last answer; {@code 0} if the
     *     session lasts as long as the connection. Take it from the last response: a renewal can grant another
     *     lifetime than the first login.
     * @throws AuthenticationException if the node refuses, does not offer the mechanism (the message names the
     *     mechanisms it offers), or a SCRAM server fails the checks of {@link ScramClient}
     */
    public static long authenticate(Function<Request, Response> exchange, Credentials credentials, SecureRandom random) {
        String mechanism = credentials.mechanism();
        Response handshake = exchange.apply(new Request.SaslHandshake(mechanism));
        if (!(handshake instanceof Response.SaslHandshake h)) {
            throw unexpected(handshake);
        }
        if (!h.error().isOk()) {
            throw new AuthenticationException(
                    mechanism + " is not enabled on this node; it offers "
                            + (h.mechanisms().isEmpty() ? "none" : String.join(", ", h.mechanisms())),
                    h.error());
        }
        if (credentials instanceof Credentials.Scram scram) {
            ScramClient client = new ScramClient(scram.user(), scram.password(), nonce(random));
            Response.SaslAuthenticate first = step(exchange, client.first());
            byte[] last = client.respond(first.authBytes().toByteArray());
            Response.SaslAuthenticate fin = step(exchange, last);
            client.verify(fin.authBytes().toByteArray());
            return fin.sessionLifetimeMs();
        }
        Credentials.OAuthBearer bearer = (Credentials.OAuthBearer) credentials;
        return step(exchange, Credentials.OAuthBearer.initialResponse(bearer.token().get())).sessionLifetimeMs();
    }

    private static Response.SaslAuthenticate step(Function<Request, Response> exchange, byte[] bytes) {
        Response r = exchange.apply(new Request.SaslAuthenticate(ByteString.copyOf(bytes)));
        if (!(r instanceof Response.SaslAuthenticate a)) {
            throw unexpected(r);
        }
        if (!a.error().isOk()) {
            ErrorCode code = a.error();
            throw new AuthenticationException(a.errorMsg() != null ? a.errorMsg() : code.toString(), code);
        }
        return a;
    }

    private static AuthenticationException unexpected(Response r) {
        return new AuthenticationException("unexpected response " + r.getClass().getSimpleName() + " during authentication", null);
    }

    /** A fresh client nonce: 24 random bytes in hex. */
    static String nonce(SecureRandom random) {
        byte[] b = new byte[24];
        random.nextBytes(b);
        StringBuilder s = new StringBuilder(48);
        for (byte x : b) {
            s.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
        }
        return s.toString();
    }
}

package dev.lotse.auth;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * How a client proves who it is to a node. A client certificate needs no entry here: it is part of the TLS
 * configuration.
 */
public sealed interface Credentials {
    /** The mechanism's name in {@code SaslHandshake}. */
    String mechanism();

    /** SASL SCRAM-SHA-512 with a user name and a password. The password never leaves the client. */
    static Credentials scram(String user, String password) {
        return new Scram(user, password);
    }

    /**
     * SASL OAUTHBEARER. {@code token} is called for <em>every</em> authentication, on connecting and before each
     * session ends, and returns the current access token; keep it cheap and let it refresh the token when needed.
     */
    static Credentials oauthBearer(Supplier<String> token) {
        return new OAuthBearer(token);
    }

    /** See {@link Credentials#scram}. */
    record Scram(String user, String password) implements Credentials {
        /** The mechanism's name. */
        public static final String MECHANISM = "SCRAM-SHA-512";

        /** Creates the credentials. */
        public Scram {
            Objects.requireNonNull(user);
            Objects.requireNonNull(password);
            if (user.isEmpty()) {
                throw new IllegalArgumentException("the user name is empty");
            }
        }

        @Override
        public String mechanism() {
            return MECHANISM;
        }

        @Override
        public String toString() {
            return "Scram[user=" + user + "]";
        }
    }

    /** See {@link Credentials#oauthBearer}. */
    record OAuthBearer(Supplier<String> token) implements Credentials {
        /** The mechanism's name. */
        public static final String MECHANISM = "OAUTHBEARER";

        /** Creates the credentials. */
        public OAuthBearer {
            Objects.requireNonNull(token);
        }

        @Override
        public String mechanism() {
            return MECHANISM;
        }

        /** The initial client response for {@code token}: {@code n,,\u0001auth=Bearer <token>\u0001\u0001} (RFC 7628). */
        public static byte[] initialResponse(String token) {
            return ("n,,\u0001auth=Bearer " + token + "\u0001\u0001").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {
            return "OAuthBearer";
        }
    }
}

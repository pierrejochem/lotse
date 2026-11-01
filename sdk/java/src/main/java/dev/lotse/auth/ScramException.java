package dev.lotse.auth;

/** A SCRAM exchange that cannot go on: a message that breaks the grammar, or a server that fails its checks. */
public final class ScramException extends AuthenticationException {
    private static final long serialVersionUID = 1L;

    /** Why the exchange stopped. */
    public enum Kind {
        /** A message does not follow the mechanism's grammar, or carries a nonce or iteration count that is not acceptable. */
        MALFORMED,
        /** The server's signature is missing or wrong: it does not know the user's credential. */
        SERVER_NOT_VERIFIED,
        /** The step does not fit the exchange's state. */
        OUT_OF_ORDER
    }

    private final Kind kind;

    ScramException(Kind kind, String message) {
        super(message, null);
        this.kind = kind;
    }

    /** Why the exchange stopped. */
    public Kind kind() {
        return kind;
    }
}

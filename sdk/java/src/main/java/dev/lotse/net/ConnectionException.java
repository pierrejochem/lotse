package dev.lotse.net;

/**
 * A request got no answer because the connection was not there, broke, was closed, or the node did not reply in time.
 *
 * <p>{@link #requestSent()} says whether the request may have reached the node. When it is {@code false} the
 * request was certainly not carried out and may be sent again. When it is {@code true} the outcome is unknown:
 * resend only what is idempotent.
 */
public class ConnectionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final boolean requestSent;

    /** Creates the exception. */
    public ConnectionException(String message, boolean requestSent, Throwable cause) {
        super(message, cause);
        this.requestSent = requestSent;
    }

    /** Creates the exception without a cause. */
    public ConnectionException(String message, boolean requestSent) {
        this(message, requestSent, null);
    }

    /** Whether the request may have reached the node. */
    public boolean requestSent() {
        return requestSent;
    }
}

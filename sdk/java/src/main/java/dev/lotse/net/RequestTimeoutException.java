package dev.lotse.net;

import java.time.Duration;

/** The node did not answer within the request timeout. The request may still be carried out: its outcome is unknown. */
public final class RequestTimeoutException extends ConnectionException {
    private static final long serialVersionUID = 1L;

    /** Creates the exception. */
    public RequestTimeoutException(Duration timeout) {
        super("no response within " + timeout, true);
    }
}

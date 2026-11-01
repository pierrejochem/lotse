package dev.lotse.net;

/**
 * The connection was refused before any byte was sent because it would be unprotected: no transport security was
 * configured, or plain TCP was asked for an address that is not loopback.
 */
public final class InsecureTransportException extends ConnectionException {
    private static final long serialVersionUID = 1L;

    /** Creates the exception. */
    public InsecureTransportException(String message) {
        super(message, false);
    }
}

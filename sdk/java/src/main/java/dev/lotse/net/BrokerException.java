package dev.lotse.net;

import dev.lotse.proto.ErrorCode;

/**
 * The node refused a request as a whole: the response had the {@code ERROR} flag, so it carries only an error code
 * and perhaps a reason, whatever the request was (an unsupported version, a connection that is not authenticated,
 * a request the principal may not make). The request was not carried out.
 *
 * <p>Errors inside a typed response, such as a partition's error in {@code Produce}, are not exceptions; they stay
 * in the response.
 */
public final class BrokerException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final transient ErrorCode code;
    private final String reason;

    /**
     * Creates the exception.
     *
     * @param code the error code
     * @param reason the reason the node gave (tagged field 0), or {@code null}
     */
    public BrokerException(ErrorCode code, String reason) {
        super(reason == null ? "refused: " + code : "refused: " + code + ": " + reason);
        this.code = code;
        this.reason = reason;
    }

    /** The error code. */
    public ErrorCode code() {
        return code;
    }

    /** The reason the node gave, or {@code null}. */
    public String reason() {
        return reason;
    }
}

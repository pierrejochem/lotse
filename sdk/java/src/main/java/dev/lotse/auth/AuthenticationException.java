package dev.lotse.auth;

import dev.lotse.proto.ErrorCode;

/**
 * The node did not accept the credentials, or the node could not prove it knows the user's credential. The
 * connection must not be used afterwards: the node closes it after a failed attempt.
 *
 * <p>A node says no more than "authentication failed" when the user or the password is wrong; the two cannot be
 * told apart, by design.
 */
public class AuthenticationException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final transient ErrorCode code;

    /**
     * Creates the exception.
     *
     * @param message what happened and, where useful, what to do
     * @param code the error code the node answered with, or {@code null} if the failure was found on this side
     */
    public AuthenticationException(String message, ErrorCode code) {
        super(message);
        this.code = code;
    }

    /** The error code the node answered with ({@code AUTHENTICATION_FAILED}, {@code UNSUPPORTED_SASL_MECHANISM}), or {@code null}. */
    public ErrorCode code() {
        return code;
    }
}

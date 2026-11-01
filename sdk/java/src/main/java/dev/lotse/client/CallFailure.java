package dev.lotse.client;

import dev.lotse.proto.ErrorCode;

/** Why a request got no response. */
final class CallFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    enum Kind {
        /** It never left: retrying cannot duplicate anything. */
        NOT_SENT,
        /** It may have reached the broker. */
        LOST,
        /** The connection could not authenticate: the request never left, and trying again will not help. */
        DENIED,
        /** The node refused the request (a role is missing): not carried out, and trying again will not help. */
        REFUSED
    }

    final Kind kind;
    final transient ErrorCode code;

    private CallFailure(Kind kind, String why, ErrorCode code) {
        super(why, null, false, false);
        this.kind = kind;
        this.code = code;
    }

    static CallFailure notSent(String why) {
        return new CallFailure(Kind.NOT_SENT, why, null);
    }

    static CallFailure lost(String why) {
        return new CallFailure(Kind.LOST, why, null);
    }

    static CallFailure denied(String why) {
        return new CallFailure(Kind.DENIED, why, null);
    }

    static CallFailure refused(ErrorCode code, String why) {
        return new CallFailure(Kind.REFUSED, why, code);
    }

    /** A connection that could not authenticate, or a refusal, fails at once; anything else is tried again. */
    Attempt toAttempt() {
        return switch (kind) {
            case DENIED -> Attempt.fail(new ClientException.Authentication(getMessage()));
            case REFUSED -> Attempt.fail(new ClientException.Broker(code, getMessage()));
            case NOT_SENT, LOST -> Attempt.retry(describe());
        };
    }

    String describe() {
        return kind == Kind.REFUSED ? code + ": " + getMessage() : getMessage();
    }
}

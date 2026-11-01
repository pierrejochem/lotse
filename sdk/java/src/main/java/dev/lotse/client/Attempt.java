package dev.lotse.client;

/**
 * Why one attempt failed, thrown inside the retry loop only. {@code RETRY} may be tried again at once, after
 * metadata is refreshed; {@code RETRY_MAYBE_DONE} may be tried again, but the request may have been carried out,
 * so only an idempotent produce does; {@code FAIL} ends the call with an error.
 */
final class Attempt extends RuntimeException {
    private static final long serialVersionUID = 1L;

    enum Kind {
        RETRY,
        RETRY_MAYBE_DONE,
        FAIL
    }

    final Kind kind;
    final transient ClientException error;

    private Attempt(Kind kind, String why, ClientException error) {
        super(why, null, false, false);
        this.kind = kind;
        this.error = error;
    }

    static Attempt retry(String why) {
        return new Attempt(Kind.RETRY, why, null);
    }

    static Attempt retryMaybeDone(String why) {
        return new Attempt(Kind.RETRY_MAYBE_DONE, why, null);
    }

    static Attempt fail(ClientException error) {
        return new Attempt(Kind.FAIL, error.getMessage(), error);
    }
}

package dev.lotse.proto;

/** Frame flags, a {@code u16} bit set in every header after the correlation id. */
public final class Flags {
    /** A push frame (delivery to a queue consumer), not a response. */
    public static final int PUSH = 1;
    /** A response whose body is only an error code: the request could not be served at all. */
    public static final int ERROR = 2;

    private Flags() {}
}

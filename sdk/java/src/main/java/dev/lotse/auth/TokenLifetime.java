package dev.lotse.auth;

import java.time.Duration;

/**
 * How long an access token lives (ADR 0133): one of five steps, counted from when the metadata leader records it.
 * The code is what travels in {@code CreateToken}.
 */
public enum TokenLifetime {
    /** 7 days. */
    WEEK(1, 7),
    /** 30 days. */
    MONTH(2, 30),
    /** 90 days. */
    QUARTER(3, 90),
    /** 180 days. */
    HALF_YEAR(4, 180),
    /** 365 days. */
    YEAR(5, 365);

    private final int code;
    private final int days;

    TokenLifetime(int code, int days) {
        this.code = code;
        this.days = days;
    }

    /** The code on the wire, 1 to 5. */
    public int code() {
        return code;
    }

    /** The lifetime. */
    public Duration duration() {
        return Duration.ofDays(days);
    }

    /**
     * The lifetime a code names.
     *
     * @throws IllegalArgumentException for anything but 1 to 5
     */
    public static TokenLifetime fromCode(int code) {
        for (TokenLifetime l : values()) {
            if (l.code == code) {
                return l;
            }
        }
        throw new IllegalArgumentException("a token lifetime's code is 1 to 5, got " + code);
    }
}

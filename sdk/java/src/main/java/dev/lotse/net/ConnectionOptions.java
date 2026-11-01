package dev.lotse.net;

import dev.lotse.auth.Credentials;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Objects;

/**
 * How connections are made: transport security, credentials, timeouts. Immutable; make one with {@link #builder()}.
 *
 * <p>There is no default for the transport. A connection is refused until {@link Builder#tls} or
 * {@link Builder#plaintextOnLoopback} says which, so that nothing is unencrypted by accident.
 */
public final class ConnectionOptions {
    private final TlsConfig tls;
    private final boolean plaintextOnLoopback;
    private final Credentials credentials;
    private final Duration requestTimeout;
    private final Duration connectTimeout;
    private final Ticker ticker;
    private final SecureRandom random;

    private ConnectionOptions(Builder b) {
        this.tls = b.tls;
        this.plaintextOnLoopback = b.plaintextOnLoopback;
        this.credentials = b.credentials;
        this.requestTimeout = b.requestTimeout;
        this.connectTimeout = b.connectTimeout;
        this.ticker = b.ticker;
        this.random = b.random;
    }

    /** A builder with the defaults: 10 s request timeout, 10 s connect timeout, the system clock. */
    public static Builder builder() {
        return new Builder();
    }

    TlsConfig tls() {
        return tls;
    }

    boolean plaintextOnLoopback() {
        return plaintextOnLoopback;
    }

    /** The credentials every connection authenticates with, or {@code null}. */
    public Credentials credentials() {
        return credentials;
    }

    /** How long a request waits for its response. */
    public Duration requestTimeout() {
        return requestTimeout;
    }

    /** How long connecting a socket may take. */
    public Duration connectTimeout() {
        return connectTimeout;
    }

    Ticker ticker() {
        return ticker;
    }

    SecureRandom random() {
        return random;
    }

    /** Builds {@link ConnectionOptions}. */
    public static final class Builder {
        private TlsConfig tls;
        private boolean plaintextOnLoopback;
        private Credentials credentials;
        private Duration requestTimeout = Duration.ofSeconds(10);
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Ticker ticker = Ticker.SYSTEM;
        private SecureRandom random = new SecureRandom();

        private Builder() {}

        /** Connects with TLS 1.3 as {@code config} says. */
        public Builder tls(TlsConfig config) {
            this.tls = Objects.requireNonNull(config);
            this.plaintextOnLoopback = false;
            return this;
        }

        /**
         * Allows plain TCP to loopback addresses, and only to those: nothing is encrypted or authenticated, so this
         * is for development and tests. A connection to any other address is refused.
         */
        public Builder plaintextOnLoopback() {
            this.plaintextOnLoopback = true;
            this.tls = null;
            return this;
        }

        /** Authenticates every connection with SASL as {@code credentials} says. */
        public Builder credentials(Credentials credentials) {
            this.credentials = credentials;
            return this;
        }

        /** How long a request waits for its response before failing with {@link RequestTimeoutException}. */
        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = positive(timeout);
            return this;
        }

        /** How long connecting a socket may take. */
        public Builder connectTimeout(Duration timeout) {
            this.connectTimeout = positive(timeout);
            return this;
        }

        /** The clock session times follow; for tests. */
        public Builder ticker(Ticker ticker) {
            this.ticker = Objects.requireNonNull(ticker);
            return this;
        }

        /** The source of SCRAM nonces; for tests. */
        public Builder random(SecureRandom random) {
            this.random = Objects.requireNonNull(random);
            return this;
        }

        private static Duration positive(Duration d) {
            if (d.isZero() || d.isNegative()) {
                throw new IllegalArgumentException("a timeout must be positive");
            }
            return d;
        }

        /** Builds the options. */
        public ConnectionOptions build() {
            return new ConnectionOptions(this);
        }
    }
}

package dev.lotse.client;

import dev.lotse.auth.Credentials;
import dev.lotse.net.Address;
import dev.lotse.net.ConnectionOptions;
import dev.lotse.net.TlsConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * How a {@link LotseClient} behaves. Immutable; make one with {@link #builder}.
 *
 * <p>The defaults are the Rust client's: 10 s per request, 30 s of retrying, 50 ms between attempts, 5 s for the
 * leader to wait for the quorum on a produce, idempotent produce, tenant {@code default}. There is no default for
 * the transport: call {@link Builder#tls} or {@link Builder#plaintextOnLoopback}, or the client refuses to connect.
 */
public final class ClientConfig {
    /** The tenant every cluster has. */
    public static final String DEFAULT_TENANT = "default";

    private final List<Address> bootstrap;
    private final Duration requestTimeout;
    private final Duration retryFor;
    private final Duration backoff;
    private final Duration produceTimeout;
    private final boolean idempotent;
    private final Duration txnTimeout;
    private final String producerName;
    private final Duration metadataMaxAge;
    private final String tenant;
    private final Credentials credentials;
    private final TlsConfig tls;
    private final boolean plaintextOnLoopback;

    private ClientConfig(Builder b) {
        this.bootstrap = List.copyOf(b.bootstrap);
        this.requestTimeout = b.requestTimeout;
        this.retryFor = b.retryFor;
        this.backoff = b.backoff;
        this.produceTimeout = b.produceTimeout;
        this.idempotent = b.idempotent;
        this.txnTimeout = b.txnTimeout;
        this.producerName = b.producerName;
        this.metadataMaxAge = b.metadataMaxAge;
        this.tenant = b.tenant;
        this.credentials = b.credentials;
        this.tls = b.tls;
        this.plaintextOnLoopback = b.plaintextOnLoopback;
    }

    /**
     * Starts a configuration.
     *
     * @param bootstrap brokers to ask for metadata first, each {@code host:port}; at least one
     */
    public static Builder builder(String... bootstrap) {
        return builder(List.of(bootstrap));
    }

    /** Starts a configuration; see {@link #builder(String...)}. */
    public static Builder builder(List<String> bootstrap) {
        return new Builder(bootstrap);
    }

    /** Brokers to ask for metadata first. */
    public List<Address> bootstrap() {
        return bootstrap;
    }

    /** How long one request may take before it counts as lost. */
    public Duration requestTimeout() {
        return requestTimeout;
    }

    /** How long retriable failures (no leader, broken connections) are retried before {@link ClientException.GaveUp}. */
    public Duration retryFor() {
        return retryFor;
    }

    /** The pause between retries. */
    public Duration backoff() {
        return backoff;
    }

    /** How long the leader waits for the quorum on a produce. */
    public Duration produceTimeout() {
        return produceTimeout;
    }

    /** Whether produce is idempotent. */
    public boolean idempotent() {
        return idempotent;
    }

    /** How long a transaction may stay open before the coordinator aborts it. */
    public Duration txnTimeout() {
        return txnTimeout;
    }

    /** The idempotent producer's stable name, or {@code null} (ADR 0071). */
    public String producerName() {
        return producerName;
    }

    /**
     * How long exchange bindings learnt from metadata are used before they are asked for again. Routes of streams
     * do not expire by age: they are used until a request fails, as in the Rust client.
     */
    public Duration metadataMaxAge() {
        return metadataMaxAge;
    }

    /**
     * The tenant resources created by this client go to, and the one its transactional and group ids live in (ADR
     * 0062).
     */
    public String tenant() {
        return tenant;
    }

    /** The SASL credentials, or {@code null}. */
    public Credentials credentials() {
        return credentials;
    }

    ConnectionOptions connectionOptions() {
        ConnectionOptions.Builder b = ConnectionOptions.builder().requestTimeout(requestTimeout).credentials(credentials);
        if (tls != null) {
            b.tls(tls);
        } else if (plaintextOnLoopback) {
            b.plaintextOnLoopback();
        }
        return b.build();
    }

    /** Builds a {@link ClientConfig}. */
    public static final class Builder {
        private final List<Address> bootstrap = new ArrayList<>();
        private Duration requestTimeout = Duration.ofSeconds(10);
        private Duration retryFor = Duration.ofSeconds(30);
        private Duration backoff = Duration.ofMillis(50);
        private Duration produceTimeout = Duration.ofSeconds(5);
        private boolean idempotent = true;
        private Duration txnTimeout = Duration.ofSeconds(60);
        private String producerName;
        private Duration metadataMaxAge = Duration.ofSeconds(30);
        private String tenant = DEFAULT_TENANT;
        private Credentials credentials;
        private TlsConfig tls;
        private boolean plaintextOnLoopback;

        private Builder(List<String> bootstrap) {
            for (String b : bootstrap) {
                this.bootstrap.add(Address.parse(b));
            }
            if (this.bootstrap.isEmpty()) {
                throw new IllegalArgumentException("a client needs at least one bootstrap address");
            }
        }

        /** How long one request may take before it counts as lost. Default 10 s. */
        public Builder requestTimeout(Duration d) {
            this.requestTimeout = positive(d);
            return this;
        }

        /** How long retriable failures are retried. Default 30 s. */
        public Builder retryFor(Duration d) {
            this.retryFor = positive(d);
            return this;
        }

        /** The pause between retries. Default 50 ms. */
        public Builder backoff(Duration d) {
            this.backoff = positive(d);
            return this;
        }

        /** How long the leader waits for the quorum on a produce. Default 5 s. */
        public Builder produceTimeout(Duration d) {
            this.produceTimeout = positive(d);
            return this;
        }

        /**
         * Produce idempotently (the default). Needs the coordinator group, which every cluster started from a
         * cluster file has. Without it an unknown outcome is never retried, since a retry could write twice.
         */
        public Builder idempotent(boolean on) {
            this.idempotent = on;
            return this;
        }

        /** How long a transaction may stay open. Default 60 s. */
        public Builder txnTimeout(Duration d) {
            this.txnTimeout = positive(d);
            return this;
        }

        /**
         * A stable name for the idempotent producer (ADR 0071): the producer id comes from the coordinator under the
         * name, and every start raises its epoch, so a restarted client fences its earlier instance. Names share one
         * namespace with transactional ids; one client instance per name. Default none: a fresh id per client.
         */
        public Builder producerName(String name) {
            if (name != null && (name.isEmpty() || name.indexOf('\0') >= 0)) {
                throw new IllegalArgumentException("a producer name is not empty and has no NUL");
            }
            this.producerName = name;
            return this;
        }

        /** How long exchange bindings are used before they are asked for again. Default 30 s. */
        public Builder metadataMaxAge(Duration d) {
            this.metadataMaxAge = positive(d);
            return this;
        }

        /**
         * The tenant resources created by this client go to, and its transactional and group ids live in. Default
         * {@code default}.
         */
        public Builder tenant(String tenant) {
            this.tenant = Objects.requireNonNull(tenant);
            return this;
        }

        /** Logs in on every connection. Without it only a client certificate, if any, identifies the client. */
        public Builder credentials(Credentials credentials) {
            this.credentials = credentials;
            return this;
        }

        /** Connects with TLS 1.3 as {@code tls} says. */
        public Builder tls(TlsConfig tls) {
            this.tls = Objects.requireNonNull(tls);
            this.plaintextOnLoopback = false;
            return this;
        }

        /** Allows plain TCP to loopback addresses only; for development and tests. */
        public Builder plaintextOnLoopback() {
            this.plaintextOnLoopback = true;
            this.tls = null;
            return this;
        }

        /** Builds the configuration. */
        public ClientConfig build() {
            return new ClientConfig(this);
        }

        private static Duration positive(Duration d) {
            if (d.isZero() || d.isNegative()) {
                throw new IllegalArgumentException("a duration must be positive");
            }
            return d;
        }
    }
}

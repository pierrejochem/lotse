package dev.lotse.client;

import java.time.Duration;
import java.util.Objects;

/** How a consumer group member behaves; see {@link LotseClient#joinGroup}. */
public final class GroupConfig {
    private final Duration sessionTimeout;
    private final Duration rebalanceTimeout;
    private final boolean autoCommit;
    private final OffsetAt startFrom;
    private final String instanceId;

    private GroupConfig(Builder b) {
        this.sessionTimeout = b.sessionTimeout;
        this.rebalanceTimeout = b.rebalanceTimeout;
        this.autoCommit = b.autoCommit;
        this.startFrom = b.startFrom;
        this.instanceId = b.instanceId;
    }

    /** The defaults: 45 s session, 60 s to release a partition, auto-commit, from the log start, no instance id. */
    public static GroupConfig defaults() {
        return builder().build();
    }

    /** Starts a configuration with the defaults. */
    public static Builder builder() {
        return new Builder();
    }

    /** After this long without a successful heartbeat the member drops its partitions. */
    public Duration sessionTimeout() {
        return sessionTimeout;
    }

    /** How long the member may take to release a revoked partition. */
    public Duration rebalanceTimeout() {
        return rebalanceTimeout;
    }

    /** Whether a revoked partition's position is committed before it is released. */
    public boolean autoCommit() {
        return autoCommit;
    }

    /** Where a partition the group has committed no offset for is read from. */
    public OffsetAt startFrom() {
        return startFrom;
    }

    /** The member's stable id across restarts, or {@code null}. */
    public String instanceId() {
        return instanceId;
    }

    /** Builds a {@link GroupConfig}. */
    public static final class Builder {
        private Duration sessionTimeout = Duration.ofSeconds(45);
        private Duration rebalanceTimeout = Duration.ofSeconds(60);
        private boolean autoCommit = true;
        private OffsetAt startFrom = OffsetAt.EARLIEST;
        private String instanceId;

        private Builder() {}

        /**
         * After this long without a successful heartbeat, counted from when the last successful one was sent, the
         * member drops every partition and joins again as a new member. It must not exceed the brokers' group
         * session timeout. The member heartbeats only from {@link GroupConsumer#poll}: call it more often than this.
         * Default 45 s.
         */
        public Builder sessionTimeout(Duration d) {
            this.sessionTimeout = positive(d);
            return this;
        }

        /** How long the member may take to release a revoked partition. Default 60 s. */
        public Builder rebalanceTimeout(Duration d) {
            this.rebalanceTimeout = positive(d);
            return this;
        }

        /**
         * Commit a revoked partition's position before releasing it, and every position on {@link
         * GroupConsumer#close}. Turn it off for exactly-once processing, where offsets go with each transaction
         * ({@link Transaction#sendOffsets}). Default on.
         */
        public Builder autoCommit(boolean on) {
            this.autoCommit = on;
            return this;
        }

        /** Where to start a partition the group has committed no offset for (ADR 0059). Default the log start. */
        public Builder startFrom(OffsetAt at) {
            this.startFrom = Objects.requireNonNull(at);
            return this;
        }

        /**
         * A stable id for this member across restarts (ADR 0072). A member that comes back with it within the session
         * timeout takes over its partitions without a rebalance; a second instance with the same id takes over too,
         * and the first then gets {@code FENCED_INSTANCE_ID} and stops. Leaving keeps the partitions for the session
         * timeout. Default none.
         */
        public Builder instanceId(String id) {
            if (id != null && id.isEmpty()) {
                throw new IllegalArgumentException("an instance id is not empty");
            }
            this.instanceId = id;
            return this;
        }

        /** Builds the configuration. */
        public GroupConfig build() {
            return new GroupConfig(this);
        }

        private static Duration positive(Duration d) {
            if (d.isZero() || d.isNegative()) {
                throw new IllegalArgumentException("a duration must be positive");
            }
            return d;
        }
    }
}

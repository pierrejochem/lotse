package dev.lotse.client;

import dev.lotse.proto.Messages.Config;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Settings of a queue for {@link LotseClient#createQueue}.
 *
 * <p>Priorities and single active consumer are not offered: the broker supports one priority level and refuses
 * single active so far.
 */
public final class QueueOptions {
    private final int replication;
    private final Duration ackTimeout;
    private final Long maxDeliveries;
    private final Duration messageTtl;
    private final String deadLetterQueue;

    private QueueOptions(Builder b) {
        this.replication = b.replication;
        this.ackTimeout = b.ackTimeout;
        this.maxDeliveries = b.maxDeliveries;
        this.messageTtl = b.messageTtl;
        this.deadLetterQueue = b.deadLetterQueue;
    }

    /** A builder; the queue is replicated on 3 nodes unless told otherwise. */
    public static Builder builder() {
        return new Builder();
    }

    /** The default settings, on 3 nodes. */
    public static QueueOptions defaults() {
        return new Builder().build();
    }

    /** The number of nodes holding the queue. */
    public int replication() {
        return replication;
    }

    /** The name of the queue that receives retired messages, or {@code null}. */
    public String deadLetterQueue() {
        return deadLetterQueue;
    }

    List<Config> configs() {
        List<Config> out = new ArrayList<>();
        if (ackTimeout != null) {
            out.add(new Config("ack.timeout.ms", Long.toString(ackTimeout.toMillis())));
        }
        if (maxDeliveries != null) {
            out.add(new Config("max.deliveries", Long.toString(maxDeliveries)));
        }
        if (messageTtl != null) {
            out.add(new Config("message.ttl.ms", Long.toString(messageTtl.toMillis())));
        }
        return out;
    }

    /** Builds {@link QueueOptions}. */
    public static final class Builder {
        private int replication = 3;
        private Duration ackTimeout;
        private Long maxDeliveries;
        private Duration messageTtl;
        private String deadLetterQueue;

        private Builder() {}

        /** The number of nodes holding the queue (1 to 255). */
        public Builder replication(int nodes) {
            if (nodes < 1 || nodes > 255) {
                throw new IllegalArgumentException("replication must be between 1 and 255");
            }
            this.replication = nodes;
            return this;
        }

        /** How long a consumer may hold a message unacknowledged before it goes to others ({@code ack.timeout.ms}). */
        public Builder ackTimeout(Duration timeout) {
            this.ackTimeout = timeout;
            return this;
        }

        /** How often a message is delivered before the queue retires it ({@code max.deliveries}); 0 is no limit. */
        public Builder maxDeliveries(long deliveries) {
            this.maxDeliveries = deliveries;
            return this;
        }

        /** How long a message may wait in the queue before it is retired ({@code message.ttl.ms}). */
        public Builder messageTtl(Duration ttl) {
            this.messageTtl = ttl;
            return this;
        }

        /** A queue of the same tenant that receives the messages the limits retire; without one they are dropped. */
        public Builder deadLetterQueue(String name) {
            this.deadLetterQueue = name;
            return this;
        }

        /** Builds the options. */
        public QueueOptions build() {
            return new QueueOptions(this);
        }
    }
}

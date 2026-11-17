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
    private final Long maxLength;
    private final Long maxLengthBytes;
    private final String overflow;

    private QueueOptions(Builder b) {
        this.replication = b.replication;
        this.ackTimeout = b.ackTimeout;
        this.maxDeliveries = b.maxDeliveries;
        this.messageTtl = b.messageTtl;
        this.deadLetterQueue = b.deadLetterQueue;
        this.maxLength = b.maxLength;
        this.maxLengthBytes = b.maxLengthBytes;
        this.overflow = b.overflow;
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
        if (maxLength != null) {
            out.add(new Config("max.length", Long.toString(maxLength)));
        }
        if (maxLengthBytes != null) {
            out.add(new Config("max.length.bytes", Long.toString(maxLengthBytes)));
        }
        if (overflow != null) {
            out.add(new Config("overflow", overflow));
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
        private Long maxLength;
        private Long maxLengthBytes;
        private String overflow;

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

        /**
         * How many messages may wait for delivery ({@code max.length}, ADR 0118); messages out with a consumer do not
         * count.
         */
        public Builder maxLength(long messages) {
            if (messages < 1) {
                throw new IllegalArgumentException("max.length is a number of messages from 1");
            }
            this.maxLength = messages;
            return this;
        }

        /** How many bytes of keys, values and headers may wait for delivery ({@code max.length.bytes}, ADR 0118). */
        public Builder maxLengthBytes(long bytes) {
            if (bytes < 1) {
                throw new IllegalArgumentException("max.length.bytes is a number of bytes from 1");
            }
            this.maxLengthBytes = bytes;
            return this;
        }

        /**
         * What a full queue does ({@code overflow}, ADR 0118): {@code reject-publish} (the default) refuses with
         * QUEUE_FULL, {@code drop-head} retires the oldest waiting messages, into the dead-letter queue if there is one.
         */
        public Builder overflow(String overflow) {
            if (!"reject-publish".equals(overflow) && !"drop-head".equals(overflow)) {
                throw new IllegalArgumentException("overflow is reject-publish or drop-head, not " + overflow);
            }
            this.overflow = overflow;
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

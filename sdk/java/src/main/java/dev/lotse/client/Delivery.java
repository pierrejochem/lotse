package dev.lotse.client;

import dev.lotse.record.BatchRecord;
import dev.lotse.record.ByteString;
import dev.lotse.record.Header;
import java.util.List;
import java.util.Optional;

/**
 * One message from a queue, from {@link QueueConsumer#next}. Settle it with {@link QueueConsumer#ack} or
 * {@link QueueConsumer#nack}.
 */
public final class Delivery {
    private final BatchRecord record;
    private final int deliveryCount;
    final Object subscription;

    Delivery(BatchRecord record, int deliveryCount, Object subscription) {
        this.record = record;
        this.deliveryCount = deliveryCount;
        this.subscription = subscription;
    }

    /** The message's offset in the queue, a {@code u64}; it identifies the message for acks. */
    public long offset() {
        return record.offset();
    }

    /** How often the message has been delivered, this time included. More than 1 means an earlier delivery was not acknowledged. */
    public int deliveryCount() {
        return deliveryCount;
    }

    /** The producer's timestamp, in milliseconds since the epoch. */
    public long timestamp() {
        return record.timestamp();
    }

    /** The key, or {@code null}. */
    public ByteString key() {
        return record.key();
    }

    /** The value, or {@code null}. */
    public ByteString value() {
        return record.value();
    }

    /** The headers, in the order the producer set them. */
    public List<Header> headers() {
        return record.headers();
    }

    /** The record as stored. */
    public BatchRecord record() {
        return record;
    }

    /**
     * Where this message came from, if it was moved to this queue by a dead-letter move: present when the
     * {@code x-lotse-dead-letter-reason} header is. A missing or unreadable number reads as 0, a missing name as
     * the empty string.
     */
    public Optional<DeadLetter> deadLetter() {
        String reason = header(DeadLetter.REASON);
        if (reason == null) {
            return Optional.empty();
        }
        String queue = header(DeadLetter.QUEUE);
        return Optional.of(new DeadLetter(
                reason,
                queue == null ? "" : queue,
                number(DeadLetter.QUEUE_ID),
                number(DeadLetter.OFFSET),
                (int) number(DeadLetter.DELIVERIES),
                number(DeadLetter.FLOOR)));
    }

    private String header(String name) {
        // The broker replaces the headers of an earlier move, so each appears once; the last wins if not.
        String found = null;
        for (Header h : record.headers()) {
            if (h.value() != null && h.key().toUtf8().equals(name)) {
                found = h.value().toUtf8();
            }
        }
        return found;
    }

    private long number(String name) {
        String v = header(name);
        if (v == null) {
            return 0;
        }
        try {
            return Long.parseUnsignedLong(v);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "Delivery{offset=" + Long.toUnsignedString(offset()) + ", deliveryCount=" + deliveryCount + "}";
    }
}

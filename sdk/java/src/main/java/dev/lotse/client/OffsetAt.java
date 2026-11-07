package dev.lotse.client;

/**
 * A place in a partition, as a read_committed reader sees it (ADR 0059): {@link #EARLIEST} the first offset still
 * stored, {@link #LATEST} the offset after the last one it may read, and {@link #time} the first offset of the first
 * batch with a record at or after a time, or the latest offset if there is none yet.
 */
public final class OffsetAt {
    /** The first offset still stored. */
    public static final OffsetAt EARLIEST = new OffsetAt(-2);

    /** The offset after the last one a read_committed reader may see. */
    public static final OffsetAt LATEST = new OffsetAt(-1);

    private final long timestamp;

    private OffsetAt(long timestamp) {
        this.timestamp = timestamp;
    }

    /** The first batch with a record at or after {@code epochMillis}; a negative time counts as 0. */
    public static OffsetAt time(long epochMillis) {
        return new OffsetAt(Math.max(0, epochMillis));
    }

    /** What {@code ListOffsets} asks for: {@code -2}, {@code -1} or milliseconds since the epoch. */
    long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof OffsetAt other && other.timestamp == timestamp;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(timestamp);
    }

    @Override
    public String toString() {
        return timestamp == -2 ? "EARLIEST" : timestamp == -1 ? "LATEST" : "time(" + timestamp + ")";
    }
}

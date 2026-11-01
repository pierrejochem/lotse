package dev.lotse.record;

import java.util.Objects;

/**
 * Header fields chosen by the producer.
 *
 * @param attributes attributes; compression must be {@link Compression#NONE}
 * @param producerId producer id, a {@code u64}; {@code 0} for producers that are not idempotent
 * @param producerEpoch producer epoch, a {@code u16}
 * @param baseSequence sequence number of the first record; {@code -1} when there is none
 */
public record BatchOptions(Attributes attributes, long producerId, int producerEpoch, int baseSequence) {
    /** Plain data from a producer that is not idempotent. */
    public static final BatchOptions DEFAULT = new BatchOptions(Attributes.DATA, 0, 0, -1);

    /** Creates options. */
    public BatchOptions {
        Objects.requireNonNull(attributes);
        if (producerEpoch < 0 || producerEpoch > Unsigned.U16_MAX) {
            throw new IllegalArgumentException("producer epoch is not a u16: " + producerEpoch);
        }
    }
}

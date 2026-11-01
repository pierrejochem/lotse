package dev.lotse.client;

/**
 * Where a produced batch went.
 *
 * @param baseOffset the offset of the batch's first record, a {@code u64}; the batch is committed
 * @param appendTimestamp the leader's append time in milliseconds since the epoch, or {@code -1}
 */
public record ProduceResult(long baseOffset, long appendTimestamp) {}

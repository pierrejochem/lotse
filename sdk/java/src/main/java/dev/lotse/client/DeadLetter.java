package dev.lotse.client;

/**
 * Where a dead-lettered message came from, read from the {@code x-lotse-dead-letter-*} headers the broker adds
 * when it moves a message to a dead-letter queue. They describe the latest move.
 *
 * @param reason why: {@code max-deliveries} or {@code expired}
 * @param queue the name of the queue the message came from
 * @param queueId that queue's resource id, a {@code u64}
 * @param offset the message's offset there, a {@code u64}
 * @param deliveries how often it had been delivered there
 * @param floor that queue's ack floor when the message was sent, a {@code u64}: every offset below it is settled
 */
public record DeadLetter(String reason, String queue, long queueId, long offset, int deliveries, long floor) {
    static final String REASON = "x-lotse-dead-letter-reason";
    static final String QUEUE = "x-lotse-dead-letter-queue";
    static final String QUEUE_ID = "x-lotse-dead-letter-queue-id";
    static final String OFFSET = "x-lotse-dead-letter-offset";
    static final String DELIVERIES = "x-lotse-dead-letter-deliveries";
    static final String FLOOR = "x-lotse-dead-letter-floor";
}

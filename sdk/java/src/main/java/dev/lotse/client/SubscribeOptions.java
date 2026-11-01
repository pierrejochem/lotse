package dev.lotse.client;

/**
 * How a queue consumer is subscribed.
 *
 * <p>Credit is how many messages the queue's leader may have pushed to the consumer and not yet counted as used.
 * At most that many messages wait unprocessed in the client. A consumer that works through a message slowly wants
 * a small credit, so that the messages it is not working on can go to others.
 *
 * <p>Single active consumer and exclusive subscriptions are not offered: the broker refuses them so far.
 */
public final class SubscribeOptions {
    /** The credit used when none is given: 64 messages. */
    public static final int DEFAULT_CREDIT = 64;

    private final int credit;

    private SubscribeOptions(int credit) {
        this.credit = credit;
    }

    /** The defaults: {@value #DEFAULT_CREDIT} messages of credit. */
    public static SubscribeOptions defaults() {
        return new SubscribeOptions(DEFAULT_CREDIT);
    }

    /** Options with room for {@code credit} messages at a time; a value below 1 is taken as 1. */
    public static SubscribeOptions withCredit(int credit) {
        return new SubscribeOptions(Math.max(1, credit));
    }

    /** The credit in messages, at least 1. */
    public int credit() {
        return credit;
    }
}

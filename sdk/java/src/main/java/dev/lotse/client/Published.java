package dev.lotse.client;

import java.util.List;

/**
 * Where a published message went: each queue it was routed to and the message's offset there, in queue name order.
 *
 * @param routed the queues and offsets
 */
public record Published(List<Routed> routed) {
    /** Creates the result. */
    public Published {
        routed = List.copyOf(routed);
    }

    /**
     * One queue that holds the message.
     *
     * @param queue the queue's name
     * @param offset the message's offset there, a {@code u64}
     */
    public record Routed(String queue, long offset) {}
}

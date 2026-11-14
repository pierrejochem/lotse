package dev.lotse.client;

import dev.lotse.net.Address;
import dev.lotse.net.Connection;
import dev.lotse.net.ConnectionException;
import dev.lotse.net.PushListener;
import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Frames;
import dev.lotse.proto.Messages.DeliveredOffset;
import dev.lotse.proto.Messages.Range;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.Batch;
import dev.lotse.record.BatchRecord;
import dev.lotse.record.RecordFormatException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A subscription to a queue, from {@link LotseClient#subscribe}. Read messages with {@link #next} or
 * {@link #poll} and settle each with {@link #ack} or {@link #nack}.
 *
 * <p>A subscription lives on one connection to the queue's leader. The leader pushes batches within the
 * consumer's credit; the consumer hands the messages out one at a time and adds credit once half of it is used up,
 * so at most {@code credit} messages wait unprocessed on this side.
 *
 * <p>When the subscription ends the consumer subscribes again on its next call. The leader gave it a consumer id
 * with its first subscription (ADR 0109), which it names when it subscribes again: after a failover or a broken
 * connection the messages it holds are still its own at the new subscription, so it keeps the ones it has received,
 * and acknowledges them, within a transaction too, through the new one. Only after the ack timeout, or when the
 * consumer did not come back within the queue's resume window, did the leader give them to others; after the ack
 * timeout the ones received are dropped. A message can so arrive more than once ({@link Delivery#deliveryCount()}
 * then exceeds 1); it is not lost. Acknowledging a message that went to someone else still settles it: an ack names
 * an offset, not a delivery.
 *
 * <p>{@link #next} and {@link #poll} are for one thread at a time. {@link #ack}, {@link #nack} and {@link #close}
 * may be called from any thread, also while another thread waits in {@code next}.
 */
public final class QueueConsumer implements AutoCloseable {
    private static final long U32_MAX = 0xFFFFFFFFL;

    /** Marks a closed consumer in a {@link Sub}'s events, so that a thread waiting in {@code next} wakes. */
    private static final Object CLOSED = new Object();

    /** One subscription on one connection; its events are pushes and the end of the connection. */
    private static final class Sub implements PushListener {
        final Connection conn;
        final long id;
        /** Waits as a standby of the queue's single active consumer (ADR 0076). */
        volatile boolean standby;
        final BlockingQueue<Object> events = new LinkedBlockingQueue<>();

        Sub(Connection conn, long id) {
            this.conn = conn;
            this.id = id;
        }

        @Override
        public void onPush(Frames.DeliverPush push) {
            events.add(push);
        }

        @Override
        public void onClose(ConnectionException reason) {
            events.add(reason);
        }
    }

    private final LotseClient client;
    private final String queue;
    private final int credit;

    private final Object mu = new Object();
    private Sub sub; // guarded by mu
    private long resourceId; // guarded by mu
    private final ArrayDeque<Delivery> buffered = new ArrayDeque<>(); // guarded by mu
    private long usedMsgs; // guarded by mu
    private long usedBytes; // guarded by mu
    private boolean closed; // guarded by mu
    /** The id the leader gave this consumer, which it resumes with (ADR 0109); 0 until one is given. */
    private long consumerId; // guarded by mu
    /** Requeues that missed a subscription because the leader moved or the connection broke: offset, delay ms. */
    private final List<long[]> unsent = new ArrayList<>(); // guarded by mu

    QueueConsumer(LotseClient client, String queue, int credit) {
        this.client = client;
        this.queue = queue;
        this.credit = credit;
    }

    /** Subscribes for the first time, so that a queue that cannot be reached fails {@code subscribe}. */
    void start() {
        ensureSubscribed();
    }

    // ---- subscribing ----

    private void ensureSubscribed() {
        synchronized (mu) {
            checkOpen();
            if (sub != null) {
                return;
            }
        }
        Subscribed made = client.retrying(this::subscribeOnce);
        boolean abandon;
        List<long[]> missed = List.of();
        synchronized (mu) {
            abandon = closed;
            if (!abandon) {
                sub = made.sub;
                resourceId = made.resourceId;
                if (made.consumerId != 0) {
                    consumerId = made.consumerId;
                }
                usedMsgs = 0;
                usedBytes = 0;
                missed = new ArrayList<>(unsent);
                unsent.clear();
            }
        }
        // Requeues that missed the old subscription go on the new one, which took their messages back (ADR 0109).
        for (long[] m : missed) {
            sendQuietly(made.sub.conn, new Request.Nack(made.resourceId, OffsetRanges.of(new long[] {m[0]}), true, m[1], made.sub.id));
        }
        if (abandon) {
            // Closed while subscribing: the broker has the subscription, so end it.
            made.sub.conn.unregister(made.sub.id);
            sendQuietly(made.sub.conn, new Request.Unsubscribe(made.sub.id));
            throw new IllegalStateException("the consumer is closed");
        }
    }

    private record Subscribed(Sub sub, long resourceId, long consumerId) {}

    private Subscribed subscribeOnce() {
        LotseClient.Routed routed = client.route(queue, 0);
        Connection conn;
        try {
            conn = client.connect(routed.address());
        } catch (CallFailure e) {
            throw e.toAttempt();
        }
        long resume;
        synchronized (mu) {
            resume = consumerId;
        }
        // Asks to be told when it becomes the single active consumer, should the queue have one (ADR 0076), and
        // resumes as the consumer it was (ADR 0109).
        Request req = new Request.Subscribe(routed.route().resourceId(), "", credit, U32_MAX, false, true, resume);
        Response resp;
        try {
            resp = client.callOn(conn, req);
        } catch (CallFailure e) {
            // A subscription the broker may have made ends with the connection, which callOn has closed.
            client.forget(queue, 0);
            throw e.toAttempt();
        }
        if (!(resp instanceof Response.Subscribe s)) {
            throw Attempt.retry("unexpected response " + resp);
        }
        ErrorCode code = s.error();
        if (code.isOk()) {
            Sub made = new Sub(conn, s.subscriptionId());
            made.standby = s.standby();
            try {
                conn.register(made.id, made);
            } catch (ConnectionException e) {
                throw Attempt.retry("the connection broke");
            }
            return new Subscribed(made, routed.route().resourceId(), s.consumerId());
        }
        if (code.retriable()) {
            client.redirect(queue, 0, s.leaderHint());
            throw Attempt.retry(code.toString());
        }
        if (code.equals(ErrorCode.UNKNOWN_RESOURCE)) {
            // Deleted: the name may come back as a new queue.
            client.forget(queue, 0);
        }
        throw Attempt.fail(new ClientException.Broker(code, null));
    }

    // ---- receiving ----

    /**
     * The next message, waiting for one as long as it takes. Subscribes again first if the subscription has ended.
     *
     * @throws IllegalStateException if the consumer is closed
     * @throws ClientException.GaveUp if the queue's leader cannot be reached within the client's {@code retryFor}
     */
    public Delivery next() {
        while (true) {
            Delivery d = poll(Duration.ofSeconds(1));
            if (d != null) {
                return d;
            }
        }
    }

    /**
     * The next message, waiting up to {@code wait} for one; {@code null} if none came. Subscribes again first if
     * the subscription has ended.
     *
     * @throws IllegalStateException if the consumer is closed
     */
    public Delivery poll(Duration wait) {
        long deadline = System.nanoTime() + wait.toNanos();
        while (true) {
            Delivery out = null;
            Request creditRequest = null;
            Sub on = null;
            synchronized (mu) {
                checkOpen();
                out = buffered.pollFirst();
                if (out != null) {
                    usedMsgs++;
                    creditRequest = refill();
                    on = sub;
                }
            }
            if (out != null) {
                if (creditRequest != null) {
                    // A lost credit is noticed as a broken connection by the next wait.
                    sendQuietly(on.conn, creditRequest);
                }
                return out;
            }
            ensureSubscribed();
            long left = deadline - System.nanoTime();
            if (left <= 0) {
                return null;
            }
            Sub waitingOn;
            synchronized (mu) {
                checkOpen();
                waitingOn = sub;
            }
            if (waitingOn == null) {
                continue;
            }
            Object event;
            try {
                event = waitingOn.events.poll(left, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            if (event == null) {
                return null;
            }
            handle(waitingOn, event);
        }
    }

    private void handle(Sub from, Object event) {
        if (event == CLOSED) {
            synchronized (mu) {
                checkOpen();
            }
            return;
        }
        if (event instanceof ConnectionException) {
            ended(from, true);
            return;
        }
        Frames.DeliverPush push = (Frames.DeliverPush) event;
        if (!push.error().isOk()) {
            if (push.error().equals(ErrorCode.NOT_LEADER)) {
                client.redirect(queue, 0, push.leaderHint());
            } else if (push.error().equals(ErrorCode.UNKNOWN_RESOURCE)) {
                client.forget(queue, 0);
            }
            // A failover leaves this consumer its messages; the ack timeout, or a queue that is gone, does not.
            ended(from, push.error().equals(ErrorCode.NOT_LEADER));
            return;
        }
        // Its turn as the single active consumer, or messages, which come to the active consumer only.
        from.standby = false;
        if (push.isActivation()) {
            return;
        }
        List<Delivery> received = deliveries(from, push);
        boolean closedMeanwhile = false;
        synchronized (mu) {
            if (sub != from) {
                // The subscription ended meanwhile. Closed: this push was taken off the events before close could
                // give it back with the rest, and the broker still holds it for the subscription.
                closedMeanwhile = closed;
            } else {
                buffered.addAll(received);
                usedBytes = Math.min(U32_MAX, usedBytes + push.batch().size());
            }
        }
        if (closedMeanwhile) {
            List<Long> offsets = new ArrayList<>();
            for (DeliveredOffset d : push.deliveries()) {
                offsets.add(d.offset());
            }
            giveBack(from, offsets);
        }
    }

    /** The messages of a delivery push that are {@code from}'s. */
    private static List<Delivery> deliveries(Sub from, Frames.DeliverPush push) {
        List<BatchRecord> records;
        try {
            records = Batch.parse(push.batch()).batch().records();
        } catch (RecordFormatException e) {
            // A codec library missing here is no damage: say which one to add (ADR 0061).
            if (e.kind() == RecordFormatException.Kind.UNSUPPORTED_COMPRESSION) {
                throw e;
            }
            throw new ClientException.Broker(ErrorCode.CORRUPT_BATCH, e.getMessage());
        }
        // The batch is as stored: it can hold messages that are not for this subscription.
        Map<Long, Integer> mine = new HashMap<>();
        for (DeliveredOffset d : push.deliveries()) {
            mine.put(d.offset(), d.deliveryCount());
        }
        List<Delivery> out = new ArrayList<>();
        for (BatchRecord r : records) {
            Integer count = mine.get(r.offset());
            if (count != null) {
                out.add(new Delivery(r, count, from));
            }
        }
        return out;
    }

    /** Requeues messages the broker holds for a subscription this consumer no longer reads, over its connection. */
    private void giveBack(Sub from, List<Long> offsets) {
        if (!offsets.isEmpty()) {
            long queueId;
            synchronized (mu) {
                queueId = resourceId;
            }
            // Named (ADR 0106): a newer subscription on the connection keeps what it received since.
            sendQuietly(from.conn, new Request.Nack(queueId, OffsetRanges.of(offsets.stream().mapToLong(Long::longValue).toArray()), true, 0, from.id));
        }
    }

    /**
     * The subscription is over. If the leader gave this consumer an id and it ended for a failover or a broken
     * connection ({@code resumable}), its messages are still its own when it subscribes again (ADR 0109): those
     * received stay, with those pushed and not read yet. Otherwise they went to others and are dropped.
     */
    private void ended(Sub which, boolean resumable) {
        synchronized (mu) {
            if (sub != which) {
                return;
            }
            sub = null;
            if (!resumable || consumerId == 0) {
                buffered.clear();
            } else {
                List<Object> unread = new ArrayList<>();
                which.events.drainTo(unread);
                for (Object e : unread) {
                    if (e instanceof Frames.DeliverPush push && push.error().isOk() && !push.isActivation()) {
                        buffered.addAll(deliveries(which, push));
                    }
                }
            }
        }
        which.conn.unregister(which.id);
    }

    /**
     * The subscription to send a nack or a transactional ack of {@code d} on: its own, or, once the leader gave this
     * consumer an id, the current one, which took back what earlier ones held (ADR 0109); called with {@code mu} held.
     */
    private Sub subFor(Delivery d) {
        return sub != null && (d.subscription == sub || consumerId != 0) ? sub : null;
    }

    /** The request that adds credit once half of it is used up, or {@code null}; called with {@code mu} held. */
    private Request refill() {
        if (usedMsgs < (credit + 1) / 2 || sub == null) {
            return null;
        }
        Request req = new Request.Credit(sub.id, usedMsgs, usedBytes);
        usedMsgs = 0;
        usedBytes = 0;
        return req;
    }

    /**
     * Whether the subscription waits as a standby of its queue's single active consumer (ADR 0076): subscribed, and
     * not its turn yet. The broker tells a standby when it becomes active; this is false from the next {@link #poll}
     * or {@link #next} on that sees it.
     */
    public boolean isStandby() {
        synchronized (mu) {
            return sub != null && sub.standby;
        }
    }

    // ---- settling ----

    /** Acknowledges a message: once this returns, it is never delivered again. */
    public void ack(Delivery delivery) {
        settle(OffsetRanges.of(new long[] {delivery.offset()}), null);
    }

    /**
     * Acknowledges several messages in as few ranges as their offsets allow. The outcome of an ack that was lost is
     * retried: settling twice is harmless.
     */
    public void ack(Collection<Delivery> deliveries) {
        if (!deliveries.isEmpty()) {
            settle(OffsetRanges.of(offsets(deliveries)), null);
        }
    }

    /**
     * Acknowledges a message within a transaction (ADR 0075): the transaction holds it until it ends, the commit
     * settles it, and an abort gives it back to be delivered again; until then no one else receives it. Adds the
     * queue to the transaction first. The ack goes on the subscription's own connection, since only a message that
     * connection holds can be acknowledged so; any error leaves the transaction to be aborted. If the leader changed
     * or the connection broke, the consumer subscribes again, naming its id, and sends it on the new subscription,
     * which holds the message still (ADR 0109). {@code MESSAGE_NOT_HELD} means the message has gone to someone else,
     * or another transaction holds it.
     */
    public void ackIn(Transaction txn, Delivery delivery) {
        txn.add(queue, 0);
        try {
            ackInResuming(txn, delivery);
        } catch (ClientException e) {
            txn.mustAbort(e.getMessage());
            throw e;
        }
    }

    /** Thrown by {@link #ackInOnce} when the leader moved or the connection broke: worth sending on a new subscription. */
    private static final class Resend extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Resend(String why) {
            super(why, null, false, false);
        }
    }

    private void ackInResuming(Transaction txn, Delivery delivery) {
        long deadline = System.nanoTime() + client.config().retryFor().toNanos();
        while (true) {
            boolean resubscribe;
            synchronized (mu) {
                resubscribe = sub == null && consumerId != 0;
            }
            if (resubscribe) {
                ensureSubscribed();
            }
            Sub on;
            long queueId;
            boolean resumable;
            synchronized (mu) {
                // Without an id the message is held only on its subscription's connection, gone if this is another.
                on = subFor(delivery);
                queueId = resourceId;
                resumable = consumerId != 0;
            }
            if (on == null) {
                throw new ClientException.Broker(ErrorCode.MESSAGE_NOT_HELD, "the subscription ended; the message went back");
            }
            try {
                ackInOnce(on, queueId, txn, delivery);
                return;
            } catch (Resend e) {
                if (!resumable || System.nanoTime() >= deadline) {
                    throw new ClientException.Broker(ErrorCode.MESSAGE_NOT_HELD, e.getMessage());
                }
                // Subscribe again, as this consumer, and send it there (the connection is shared: only the
                // subscription goes).
                client.forget(queue, 0);
                ended(on, true);
            }
        }
    }

    private void ackInOnce(Sub on, long queueId, Transaction txn, Delivery delivery) {
        Request req = new Request.Ack(
                queueId,
                txn.txnId(),
                OffsetRanges.of(new long[] {delivery.offset()}),
                client.config().tenant(),
                txn.producerId(),
                txn.epoch());
        int lost = 0;
        while (true) {
            Response resp;
            try {
                resp = client.callOn(on.conn, req);
            } catch (CallFailure e) {
                // Sent again on the same connection first: the leader takes a retry for what the transaction holds.
                if (e.kind == CallFailure.Kind.LOST && lost < 2) {
                    lost++;
                    continue;
                }
                throw new Resend(e.describe());
            }
            if (resp instanceof Response.Outcome o) {
                if (o.error().isOk()) {
                    return;
                }
                if (o.error().retriable()) {
                    throw new Resend(o.error().toString());
                }
                throw new ClientException.Broker(o.error(), null);
            }
            throw new Resend("unexpected response " + resp);
        }
    }

    /**
     * Rejects a message. With {@code requeue} it is delivered again, to any consumer; without it, it is discarded
     * like an acknowledged one (or dead-lettered, if the queue has a dead-letter queue and the limits say so).
     */
    public void nack(Delivery delivery, boolean requeue) {
        nack(List.of(delivery), requeue);
    }

    /** {@link #nack(Delivery, boolean)} for several messages. */
    public void nack(Collection<Delivery> deliveries, boolean requeue) {
        if (deliveries.isEmpty()) {
            return;
        }
        List<Range> ranges = OffsetRanges.of(offsets(deliveries));
        if (!requeue) {
            settle(ranges, Boolean.FALSE);
            return;
        }
        requeue(deliveries, 0);
    }

    /**
     * Gives a message back to be delivered again, to any consumer, once {@code delay} has passed (ADR 0077): a retry
     * with backoff. Rounded down to whole milliseconds, at most about 49 days; a delay of 0 is {@link #nack(Delivery,
     * boolean)} with requeue. If the subscription has ended, the message went back with it, without a delay; or it
     * waits at the new leader for this consumer, which gives it back once it has subscribed again (ADR 0109).
     */
    public void nackAfter(Delivery delivery, Duration delay) {
        long ms = Math.min(Math.max(delay.toMillis(), 0), U32_MAX);
        requeue(List.of(delivery), ms);
    }

    private void requeue(Collection<Delivery> deliveries, long delayMs) {
        // Only the subscription's connection can give a message back. Without an id a message of an earlier
        // subscription went back with it; with one, the current subscription took it back (ADR 0109).
        Sub on;
        long queueId;
        List<Delivery> held = new ArrayList<>();
        boolean resumable;
        synchronized (mu) {
            on = sub;
            queueId = resourceId;
            resumable = consumerId != 0;
            for (Delivery d : deliveries) {
                if (subFor(d) != null) {
                    held.add(d);
                }
            }
            if (on == null && resumable) {
                // Between subscriptions: the next one gives them back.
                for (Delivery d : deliveries) {
                    unsent.add(new long[] {d.offset(), delayMs});
                }
                return;
            }
        }
        if (on == null || held.isEmpty()) {
            return;
        }
        Request req = new Request.Nack(queueId, OffsetRanges.of(offsets(held)), true, delayMs, on.id);
        Response resp;
        try {
            resp = client.callOn(on.conn, req);
        } catch (CallFailure e) {
            resp = null;
        }
        if (resp instanceof Response.Outcome o && o.error().isOk()) {
            return;
        }
        if (resp instanceof Response.Outcome o && !o.error().retriable()) {
            throw new ClientException.Broker(o.error(), null);
        }
        // Not done (no leader, broken connection): the subscription ends with it. Without an id the message goes
        // back with it; with one it stays this consumer's, and the next subscription gives it back.
        if (resumable) {
            synchronized (mu) {
                for (Delivery d : held) {
                    unsent.add(new long[] {d.offset(), delayMs});
                }
            }
        }
    }

    private static long[] offsets(Collection<Delivery> deliveries) {
        long[] out = new long[deliveries.size()];
        int i = 0;
        for (Delivery d : deliveries) {
            out[i++] = d.offset();
        }
        return out;
    }

    /** An {@code Ack} ({@code nack == null}) or a discarding {@code Nack} to the queue's leader, retried. */
    private void settle(List<Range> ranges, Boolean nack) {
        client.retrying(() -> {
            LotseClient.Routed routed = client.route(queue, 0);
            long queueId = routed.route().resourceId();
            Request req = nack == null ? new Request.Ack(queueId, ranges) : new Request.Nack(queueId, ranges, false, 0);
            Response resp;
            try {
                resp = client.call(routed.address(), req);
            } catch (CallFailure e) {
                client.forget(queue, 0);
                if (e.kind == CallFailure.Kind.DENIED || e.kind == CallFailure.Kind.REFUSED) {
                    throw e.toAttempt();
                }
                throw Attempt.retryMaybeDone(e.describe());
            }
            if (!(resp instanceof Response.Outcome o)) {
                throw Attempt.retry("unexpected response " + resp);
            }
            if (o.error().isOk()) {
                return null;
            }
            if (o.error().retriable()) {
                client.redirect(queue, 0, o.leaderHint());
                throw Attempt.retryMaybeDone(o.error().toString());
            }
            throw Attempt.fail(new ClientException.Broker(o.error(), null));
        });
    }

    // ---- closing ----

    /**
     * Ends the subscription. Messages handed out stay with this consumer until acknowledged (or until this client's
     * connection closes); the ones not handed out yet, also those pushed and not read, go back to the queue. A thread waiting in {@link #next} gets
     * an {@link IllegalStateException}.
     */
    @Override
    public void close() {
        Sub ending;
        long queueId;
        List<Long> back = new ArrayList<>();
        synchronized (mu) {
            if (closed) {
                return;
            }
            closed = true;
            ending = sub;
            sub = null;
            queueId = resourceId;
            for (long o : offsets(buffered)) {
                back.add(o);
            }
            buffered.clear();
        }
        if (ending == null) {
            return;
        }
        // Unsubscribed, and the answer waited for: pushes and answers come in order on the connection, so every push
        // the leader sent before it took the unsubscribe has arrived by then (the leader writes a checkout before it
        // pushes, ADR 0109, so one may still be on its way). The broker holds their messages for this subscription.
        try {
            client.callOn(ending.conn, new Request.Unsubscribe(ending.id));
        } catch (CallFailure e) {
            // the subscription ends with the connection
        }
        ending.conn.unregister(ending.id);
        List<Object> unread = new ArrayList<>();
        ending.events.drainTo(unread);
        for (Object e : unread) {
            if (e instanceof Frames.DeliverPush push && push.error().isOk()) {
                for (DeliveredOffset d : push.deliveries()) {
                    back.add(d.offset());
                }
            }
        }
        ending.events.add(CLOSED);
        if (!back.isEmpty()) {
            // Named (ADR 0106): sent late, it must not take what a newer subscription on the connection holds.
            sendQuietly(ending.conn, new Request.Nack(queueId, OffsetRanges.of(back.stream().mapToLong(Long::longValue).toArray()), true, 0, ending.id));
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("the consumer is closed");
        }
    }

    /** Sends without waiting for an answer; the connection breaking is noticed elsewhere. */
    private static void sendQuietly(Connection conn, Request request) {
        try {
            conn.sendOnly(request);
        } catch (ConnectionException e) {
            // the subscription ends with the connection
        }
    }

    /** The address this consumer's subscription is on, or {@code null}; for tests. */
    Address subscribedAddress() {
        synchronized (mu) {
            return sub == null ? null : sub.conn.address();
        }
    }
}

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
 * <p>When the subscription ends (the leader changed, the connection broke, or the consumer held a message longer
 * than the queue's ack timeout) the leader gives its unacknowledged messages to other consumers, and the consumer
 * subscribes again on its next call once it has handed out the messages it already holds. Those may meanwhile have
 * gone to someone else, so a message can arrive more than once ({@link Delivery#deliveryCount()} then exceeds 1);
 * it is not lost. Acknowledging a message that went to someone else still settles it: an ack names an offset, not a
 * delivery.
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
        synchronized (mu) {
            abandon = closed;
            if (!abandon) {
                sub = made.sub;
                resourceId = made.resourceId;
                usedMsgs = 0;
                usedBytes = 0;
            }
        }
        if (abandon) {
            // Closed while subscribing: the broker has the subscription, so end it.
            made.sub.conn.unregister(made.sub.id);
            sendQuietly(made.sub.conn, new Request.Unsubscribe(made.sub.id));
            throw new IllegalStateException("the consumer is closed");
        }
    }

    private record Subscribed(Sub sub, long resourceId) {}

    private Subscribed subscribeOnce() {
        LotseClient.Routed routed = client.route(queue, 0);
        Connection conn;
        try {
            conn = client.connect(routed.address());
        } catch (CallFailure e) {
            throw e.toAttempt();
        }
        Request req = new Request.Subscribe(routed.route().resourceId(), "", credit, U32_MAX, false);
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
            try {
                conn.register(made.id, made);
            } catch (ConnectionException e) {
                throw Attempt.retry("the connection broke");
            }
            return new Subscribed(made, routed.route().resourceId());
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
            ended(from);
            return;
        }
        Frames.DeliverPush push = (Frames.DeliverPush) event;
        if (!push.error().isOk()) {
            if (push.error().equals(ErrorCode.NOT_LEADER)) {
                client.redirect(queue, 0, push.leaderHint());
            } else if (push.error().equals(ErrorCode.UNKNOWN_RESOURCE)) {
                client.forget(queue, 0);
            }
            ended(from);
            return;
        }
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
        synchronized (mu) {
            if (sub != from) {
                return; // the subscription ended meanwhile (closed)
            }
            for (BatchRecord r : records) {
                Integer count = mine.get(r.offset());
                if (count != null) {
                    buffered.addLast(new Delivery(r, count, from));
                }
            }
            usedBytes = Math.min(U32_MAX, usedBytes + push.batch().size());
        }
    }

    /**
     * The subscription is over. Called only when nothing is buffered (the buffer is read before the events), so the
     * clearing is for a consumer closed meanwhile.
     */
    private void ended(Sub which) {
        synchronized (mu) {
            if (sub != which) {
                return;
            }
            sub = null;
            buffered.clear();
        }
        which.conn.unregister(which.id);
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
     * connection holds can be acknowledged so; any error leaves the transaction to be aborted. {@code MESSAGE_NOT_HELD}
     * means the message has gone back, or another transaction holds it.
     */
    public void ackIn(Transaction txn, Delivery delivery) {
        txn.add(queue, 0);
        try {
            ackInOnce(txn, delivery);
        } catch (ClientException e) {
            txn.mustAbort(e.getMessage());
            throw e;
        }
    }

    private void ackInOnce(Transaction txn, Delivery delivery) {
        Sub on;
        long queueId;
        synchronized (mu) {
            on = sub;
            queueId = resourceId;
        }
        // The messages are the subscription's connection's: on another one they are not held.
        if (on == null) {
            throw new ClientException.Broker(
                    ErrorCode.MESSAGE_NOT_HELD, "the subscription ended; the message went back");
        }
        Request req = new Request.Ack(
                queueId,
                txn.txnId(),
                OffsetRanges.of(new long[] {delivery.offset()}),
                client.config().tenant(),
                txn.producerId(),
                txn.epoch());
        client.retrying(() -> {
            Response resp;
            try {
                resp = client.callOn(on.conn, req);
            } catch (CallFailure e) {
                switch (e.kind) {
                    // Sent again on the same connection: the leader takes a retry for what the transaction holds.
                    case LOST -> throw Attempt.retryMaybeDone(e.getMessage());
                    case NOT_SENT -> throw Attempt.fail(new ClientException.Broker(
                            ErrorCode.MESSAGE_NOT_HELD, "the connection broke; the message went back"));
                    default -> throw e.toAttempt();
                }
            }
            if (resp instanceof Response.Outcome o) {
                if (o.error().isOk()) {
                    return null;
                }
                throw Attempt.fail(new ClientException.Broker(o.error(), null));
            }
            throw Attempt.retryMaybeDone("unexpected response " + resp);
        });
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
        // Only the subscription's connection can give a message back; if it is gone, so is the message (it went
        // back with the subscription). A message of an earlier subscription is gone with it.
        Sub on;
        long queueId;
        synchronized (mu) {
            on = sub;
            queueId = resourceId;
            if (on == null) {
                return;
            }
        }
        List<Delivery> held = new ArrayList<>();
        for (Delivery d : deliveries) {
            if (d.subscription == on) {
                held.add(d);
            }
        }
        if (held.isEmpty()) {
            return;
        }
        Request req = new Request.Nack(queueId, OffsetRanges.of(offsets(held)), true, 0);
        Response resp;
        try {
            resp = client.callOn(on.conn, req);
        } catch (CallFailure e) {
            // Not done (no leader, broken connection): the subscription ends with it and the message goes back anyway.
            return;
        }
        if (resp instanceof Response.Outcome o && !o.error().isOk() && !o.error().retriable()) {
            throw new ClientException.Broker(o.error(), null);
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
     * connection closes); the ones not handed out yet go back to the queue. A thread waiting in {@link #next} gets
     * an {@link IllegalStateException}.
     */
    @Override
    public void close() {
        Sub ending;
        List<Request> requests = new ArrayList<>();
        synchronized (mu) {
            if (closed) {
                return;
            }
            closed = true;
            ending = sub;
            sub = null;
            if (ending != null) {
                requests.add(new Request.Unsubscribe(ending.id));
                if (!buffered.isEmpty()) {
                    List<Delivery> back = new ArrayList<>(buffered);
                    requests.add(new Request.Nack(resourceId, OffsetRanges.of(offsets(back)), true, 0));
                }
            }
            buffered.clear();
        }
        if (ending != null) {
            ending.conn.unregister(ending.id);
            ending.events.add(CLOSED);
            for (Request r : requests) {
                sendQuietly(ending.conn, r);
            }
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

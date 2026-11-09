package dev.lotse.client;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.Batch;
import dev.lotse.record.ByteString;
import dev.lotse.record.RecordFormatException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An open transaction, from {@link LotseClient#beginTransaction} (ADR 0029). Everything written in it becomes
 * visible to read_committed readers together when it commits, and never if it aborts. It follows the Rust client's
 * {@code Transaction} rule for rule:
 *
 * <ul>
 *   <li>a partition (or queue) is added to the transaction with {@code AddPartitionsToTxn} before the first batch is
 *       written to it;
 *   <li>batches carry the transaction's producer id and epoch, a sequence per partition, and the transactional flag;
 *       a batch whose outcome is unknown is retried with the same bytes;
 *   <li>after an error that may have left something written, or a write that never got its outcome, only {@link
 *       #abort} is left: every other call throws {@link ClientException.MustAbort}. Errors that prove nothing was
 *       written ({@link ClientException.GaveUp}, {@link ClientException.UnknownStream}) leave the transaction open;
 *   <li>{@link #commit} returns once the decision is durable. A transaction that wrote nothing ends without asking
 *       the coordinator.
 * </ul>
 *
 * <p>Not for use by several threads at once. {@link #close} without {@link #commit} or {@link #abort} leaves the
 * transaction to the coordinator, which aborts it after the transaction timeout, or at once when this client starts
 * the next one with a fresh producer id.
 */
public final class Transaction implements AutoCloseable {
    private record Part(String name, int partition) {}

    private final LotseClient client;
    private final String txnId;
    private final long producerId;
    private final int epoch;
    private final Map<Part, Integer> next = new HashMap<>();
    private final Set<Part> added = new HashSet<>();
    /**
     * Offsets were sent: the coordinator may hold them although no partition was added. The transaction must then be
     * ended there, or the offsets stay pending until it times out and block the group.
     */
    private boolean coordinatorMayHold;
    /** Why only an abort is left, or {@code null}. */
    private String poisoned;
    private boolean ended;

    Transaction(LotseClient client, String txnId, long producerId, int epoch) {
        this.client = client;
        this.txnId = txnId;
        this.producerId = producerId;
        this.epoch = epoch;
    }

    String txnId() {
        return txnId;
    }

    /**
     * Whether the transaction may still be committed or aborted: neither has been called, and {@link #close} has not
     * left it to the coordinator. A commit refused with {@link ClientException.MustAbort} leaves it open for an abort.
     */
    public boolean isOpen() {
        return !ended;
    }

    long producerId() {
        return producerId;
    }

    int epoch() {
        return epoch;
    }

    private void checkUsable() {
        if (ended) {
            throw new IllegalStateException("the transaction has ended");
        }
        if (poisoned != null) {
            throw new ClientException.MustAbort(poisoned);
        }
    }

    /** Appends messages to a partition as one batch within the transaction; see {@link #produceBatch}. */
    public ProduceResult produce(String stream, int partition, List<OutMessage> messages) {
        return produceBatch(stream, partition, LotseClient.build(messages));
    }

    /** Appends one message to a partition within the transaction; see {@link #produceBatch}. */
    public ProduceResult produce(String stream, int partition, OutMessage message) {
        return produce(stream, partition, List.of(message));
    }

    /**
     * Appends a batch to a partition within the transaction and returns the offset of its first record.
     * Read_committed readers see it only once the transaction commits.
     *
     * @throws ClientException.MustAbort an earlier call left the transaction to be aborted
     */
    public ProduceResult produceBatch(String stream, int partition, ByteString batch) {
        checkUsable();
        add(stream, partition);
        long records;
        try {
            records = Batch.parse(batch).batch().recordCount();
        } catch (RecordFormatException e) {
            throw new ClientException.Broker(ErrorCode.CORRUPT_BATCH, e.getMessage());
        }
        Part key = new Part(stream, partition);
        int seq = next.getOrDefault(key, 0);
        byte[] bytes = batch.toByteArray();
        Batch.setProducer(bytes, producerId, epoch, seq);
        Batch.setTransactional(bytes, true);
        ByteString stamped = ByteString.view(bytes, 0, bytes.length);
        // Until the outcome is known the sequence is in doubt: a call that ends some other way (the client closed,
        // an error that is not a ClientException) leaves the transaction to be aborted, rather than let the next
        // batch take the same sequence and be taken for this one.
        poisoned = "a produce ended before its outcome was known";
        ProduceResult result;
        try {
            result = client.retrying(() -> client.produceOnce(stream, partition, stamped, true, txnId));
        } catch (ClientException e) {
            poisoned = null;
            poison(e);
            throw e;
        }
        poisoned = null;
        next.put(key, seq + (int) records);
        return result;
    }

    /**
     * Publishes a message within the transaction (ADR 0032): this client routes it by the exchange's bindings, as
     * metadata gave them within {@link ClientConfig#metadataMaxAge}, and produces it to every queue it goes to, so it
     * reaches them all if the transaction commits and none if it aborts. A message no queue takes is dropped, or
     * refused with {@link ClientException.Unroutable} when {@code mandatory}. If some queues hold it already when a
     * write fails, only an abort is left.
     */
    public Published publish(String exchange, String routingKey, OutMessage message, boolean mandatory) {
        checkUsable();
        List<String> queues = client.routeMessage(exchange, routingKey, message, mandatory);
        ByteString batch = LotseClient.build(List.of(message));
        List<Published.Routed> routed = new ArrayList<>();
        for (String q : queues) {
            try {
                routed.add(new Published.Routed(q, produceBatch(q, 0, batch).baseOffset()));
            } catch (ClientException e) {
                // Some queues may hold it already: committing would deliver it to those only.
                if (!routed.isEmpty() && poisoned == null) {
                    poisoned = "published to " + routed + " only: " + e.getMessage();
                }
                throw e;
            }
        }
        return new Published(routed);
    }

    /** Adds a partition (a queue: partition 0) to the transaction once. */
    void add(String name, int partition) {
        checkUsable();
        Part key = new Part(name, partition);
        if (added.contains(key)) {
            return;
        }
        try {
            client.retrying(() -> addPartitionOnce(name, partition));
        } catch (ClientException e) {
            poison(e);
            throw e;
        }
        added.add(key);
    }

    /** After an error only an abort is left, unless nothing was written and nothing else went wrong. */
    void poison(ClientException e) {
        if (!(e instanceof ClientException.GaveUp || e instanceof ClientException.UnknownStream)) {
            poisoned = e.getMessage();
        }
    }

    /** Leaves only an abort, for {@code why}. */
    void mustAbort(String why) {
        poisoned = why;
    }

    private Void addPartitionOnce(String name, int partition) {
        LotseClient.Routed routed = client.route(name, partition);
        Request req = new Request.AddPartitionsToTxn(
                txnId,
                producerId,
                epoch,
                List.of(new Messages.TxnResource(routed.route().resourceId(), List.of((long) partition))),
                client.config().tenant());
        if (!(client.callCoordinator(txnId, req) instanceof Response.AddPartitionsToTxn r)) {
            throw Attempt.retry("unexpected response");
        }
        ErrorCode error = ErrorCode.NONE;
        if (!r.resources().isEmpty() && !r.resources().get(0).partitions().isEmpty()) {
            error = r.resources().get(0).partitions().get(0).error();
        }
        if (error.isOk()) {
            return null;
        }
        if (error.retriable()) {
            client.forgetCoordinator();
            throw Attempt.retry(error.toString());
        }
        throw Attempt.fail(new ClientException.Broker(error, null));
    }

    /**
     * Commits the consumer's read positions with this transaction (exactly-once consume-transform-produce): they
     * become the group's committed offsets if it commits, and nothing changes if it aborts. A consumer that no
     * longer owns a partition is fenced, and the transaction can only be aborted.
     */
    public void sendOffsets(GroupConsumer consumer) {
        checkUsable();
        Request req = new Request.AddOffsetsToTxn(
                txnId,
                producerId,
                epoch,
                consumer.group(),
                consumer.memberId(),
                consumer.memberEpoch(),
                GroupConsumer.resources(client, consumer.positionMap()),
                client.config().tenant());
        // From here on the coordinator may hold the offsets, also if the answer is lost: the transaction has to be
        // ended there even when no partition was added.
        coordinatorMayHold = true;
        try {
            client.retrying(() -> GroupConsumer.commitOnce(client, txnId, req));
        } catch (ClientException e) {
            // Unknown or refused: committing could commit offsets nobody knows, or a zombie's. Only an abort is left.
            poisoned = "offsets not added: " + e.getMessage();
            throw e;
        }
    }

    /**
     * Commits: once this returns, the decision is durable and every partition will show the transaction to
     * read_committed readers.
     *
     * @throws ClientException.MustAbort an earlier call left the transaction to be aborted; it is still open
     */
    public void commit() {
        checkUsable();
        end(true);
    }

    /** Aborts: none of the transaction's batches will be visible to read_committed readers. */
    public void abort() {
        if (ended) {
            throw new IllegalStateException("the transaction has ended");
        }
        end(false);
    }

    private void end(boolean commit) {
        ended = true;
        if (added.isEmpty() && !coordinatorMayHold) {
            client.transactionEnded(new LotseClient.ProducerIds(producerId, epoch));
            return;
        }
        Request req = new Request.EndTxn(txnId, producerId, epoch, commit, client.config().tenant());
        LotseClient.ProducerIds next = null;
        try {
            next = client.retrying(() -> {
                Response resp;
                try {
                    resp = client.callCoordinator(txnId, req);
                } catch (Attempt a) {
                    // A lost request may have been decided: retry as maybe done.
                    if (a.kind == Attempt.Kind.RETRY) {
                        throw Attempt.retryMaybeDone(a.getMessage());
                    }
                    throw a;
                }
                if (!(resp instanceof Response.EndTxn r)) {
                    throw Attempt.retry("unexpected response " + resp);
                }
                if (r.error().isOk()) {
                    return new LotseClient.ProducerIds(r.producerId(), r.producerEpoch());
                }
                if (r.error().retriable()) {
                    client.forgetCoordinator();
                    // The coordinator may have decided before the answer was lost.
                    throw Attempt.retryMaybeDone(r.error().toString());
                }
                throw Attempt.fail(new ClientException.Broker(r.error(), null));
            });
        } finally {
            // The next transaction uses the raised epoch; after a failure, a fresh producer id (which also aborts
            // whatever is left open).
            client.transactionEnded(next);
        }
    }

    /**
     * Ends this handle without deciding: if neither {@link #commit} nor {@link #abort} was called, the next
     * transaction of this client starts with a fresh producer id, and the coordinator aborts this one.
     */
    @Override
    public void close() {
        if (!ended) {
            ended = true;
            client.transactionEnded(null);
        }
    }
}

package dev.lotse.kotlin

import dev.lotse.client.Delivery
import dev.lotse.client.GroupConsumer
import dev.lotse.client.GroupRecord
import dev.lotse.client.OutMessage
import dev.lotse.client.ProduceResult
import dev.lotse.client.Published
import dev.lotse.client.QueueConsumer
import dev.lotse.client.Transaction
import dev.lotse.record.ByteString
import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * An open transaction; see the Java [Transaction] for its rules. Not for several coroutines at once.
 *
 * @property java the Java transaction underneath
 */
public class LotseTransaction internal constructor(public val java: Transaction) : AutoCloseable {
    /** Appends one message to a partition within the transaction. */
    public suspend fun produce(stream: String, partition: Int, message: OutMessage): ProduceResult =
        io { java.produce(stream, partition, message) }

    /** Appends messages to a partition as one batch within the transaction. */
    public suspend fun produce(stream: String, partition: Int, messages: List<OutMessage>): ProduceResult =
        io { java.produce(stream, partition, messages) }

    /** Appends a batch within the transaction. */
    public suspend fun produceBatch(stream: String, partition: Int, batch: ByteString): ProduceResult =
        io { java.produceBatch(stream, partition, batch) }

    /** Publishes within the transaction, routed by this client (ADR 0032). */
    public suspend fun publish(
        exchange: String,
        routingKey: String,
        message: OutMessage,
        mandatory: Boolean = false,
    ): Published = io { java.publish(exchange, routingKey, message, mandatory) }

    /** Commits the group member's positions with the transaction. */
    public suspend fun sendOffsets(group: LotseGroup): Unit = io { java.sendOffsets(group.java) }

    /** Acknowledges a queue message within the transaction (ADR 0075). */
    public suspend fun ack(queue: LotseQueue, delivery: Delivery): Unit = io { queue.java.ackIn(java, delivery) }

    /** Commits. */
    public suspend fun commit(): Unit = io { java.commit() }

    /** Aborts. */
    public suspend fun abort(): Unit = io { java.abort() }

    /** Whether it may still be committed or aborted. */
    public val isOpen: Boolean
        get() = java.isOpen

    /** Leaves an undecided transaction to the coordinator, which aborts it. */
    override fun close(): Unit = java.close()
}

/**
 * A queue subscription; see the Java [QueueConsumer] for its rules. [deliveries] is for one collector at a time;
 * acknowledging may happen from anywhere.
 *
 * @property java the Java consumer underneath
 */
public class LotseQueue internal constructor(public val java: QueueConsumer) : AutoCloseable {
    /** The next message, waiting as long as it takes. */
    public suspend fun next(): Delivery {
        while (true) {
            poll(Duration.ofSeconds(1))?.let { return it }
        }
    }

    /** The next message within [wait], or `null`. */
    public suspend fun poll(wait: Duration): Delivery? = io { java.poll(wait) }

    /**
     * The messages as they come, for as long as the flow is collected. Each one stays with this consumer until it is
     * acknowledged, given back, or the subscription ends; collecting does not acknowledge.
     */
    public fun deliveries(): Flow<Delivery> = flow {
        while (true) {
            emit(next())
        }
    }

    /** Acknowledges a message. */
    public suspend fun ack(delivery: Delivery): Unit = io { java.ack(delivery) }

    /** Acknowledges several messages. */
    public suspend fun ack(deliveries: Collection<Delivery>): Unit = io { java.ack(deliveries) }

    /** Rejects a message: delivered again with [requeue], otherwise discarded or dead-lettered. */
    public suspend fun nack(delivery: Delivery, requeue: Boolean = true): Unit = io { java.nack(delivery, requeue) }

    /** Gives a message back to be delivered again after [delay] (ADR 0077). */
    public suspend fun nackAfter(delivery: Delivery, delay: Duration): Unit = io { java.nackAfter(delivery, delay) }

    /** Whether this subscription waits as a standby of a single active consumer (ADR 0076). */
    public val isStandby: Boolean
        get() = java.isStandby

    /** Ends the subscription; messages not handed out go back. */
    override fun close(): Unit = java.close()
}

/**
 * A consumer group member; see the Java [GroupConsumer] for its rules. **It heartbeats only from [poll]**, so
 * [records] heartbeats only while it is collected and between the records it hands out: a collector that takes
 * longer than the session timeout for the records of one poll loses the member's partitions. Not for several
 * coroutines at once.
 *
 * @property java the Java member underneath
 */
public class LotseGroup internal constructor(public val java: GroupConsumer) : AutoCloseable {
    /** Heartbeats when due, then reads one owned partition, waiting up to [wait]. */
    public suspend fun poll(wait: Duration): List<GroupRecord> = io { java.poll(wait) }

    /** The records as they come, polling with [wait], for as long as the flow is collected. */
    public fun records(wait: Duration = Duration.ofMillis(500)): Flow<GroupRecord> = flow {
        while (true) {
            for (record in poll(wait)) {
                emit(record)
            }
        }
    }

    /** Commits every owned partition's position. */
    public suspend fun commit(): Unit = io { java.commit() }

    /** Forgets the read positions, as after an aborted transaction. */
    public fun rewind(): Unit = java.rewind()

    /** The member id, empty until joined. */
    public val memberId: String
        get() = java.memberId()

    /** The partitions owned now, by stream. */
    public val assignment: Map<String, Set<Int>>
        get() = java.assignment()

    /** The next offset per owned partition whose position is known, by stream and partition. */
    public val positions: Map<String, Map<Int, Long>>
        get() = java.positions()

    /** Leaves the group, committing first with auto-commit. Blocks while it does. */
    override fun close(): Unit = java.close()
}

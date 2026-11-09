package dev.lotse.kotlin

import dev.lotse.client.ClientConfig
import dev.lotse.client.Fetched
import dev.lotse.client.GroupConfig
import dev.lotse.client.LotseClient
import dev.lotse.client.NodeMetrics
import dev.lotse.client.OffsetAt
import dev.lotse.client.OutMessage
import dev.lotse.client.ProduceResult
import dev.lotse.client.Published
import dev.lotse.client.QueueOptions
import dev.lotse.client.SubscribeOptions
import dev.lotse.proto.Messages
import dev.lotse.proto.Response
import dev.lotse.record.BatchRecord
import dev.lotse.record.ByteString
import dev.lotse.record.Header
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext

/**
 * Runs a blocking call of the Java core on [Dispatchers.IO] (ADR 0080). A coroutine cancelled meanwhile lets the call
 * finish, so its outcome is as the Java client decided it, and then drops the result.
 */
internal suspend inline fun <T> io(crossinline call: () -> T): T = withContext(Dispatchers.IO) { call() }

/**
 * A Lotse client for coroutines: every call of the Java [LotseClient] as a `suspend` function, reads as [Flow]s, and
 * [transaction] for a transaction that commits when its block returns. The rules, retries and errors are the Java
 * client's, which follow the Rust client's; see its documentation. Safe to share between coroutines, as the Java
 * client is between threads.
 *
 * @property java the Java client underneath, for anything this layer does not wrap
 */
public class Lotse(public val java: LotseClient) : AutoCloseable {
    /** A client with [config]; it connects on first use. */
    public constructor(config: ClientConfig) : this(LotseClient(config))

    /** Closes every connection. Calls in progress fail. */
    override fun close(): Unit = java.close()

    // ---- streams ----

    /** Metadata for [names], or for everything this client may see without names. */
    public suspend fun metadata(vararg names: String): Response.Metadata = io { java.metadata(*names) }

    /** Appends one message to a partition; see [LotseClient.produce]. */
    public suspend fun produce(stream: String, partition: Int, message: OutMessage): ProduceResult =
        io { java.produce(stream, partition, message) }

    /** Appends messages to a partition as one batch; see [LotseClient.produce]. */
    public suspend fun produce(stream: String, partition: Int, messages: List<OutMessage>): ProduceResult =
        io { java.produce(stream, partition, messages) }

    /** Appends a batch built with `BatchBuilder`; see [LotseClient.produceBatch]. */
    public suspend fun produceBatch(stream: String, partition: Int, batch: ByteString): ProduceResult =
        io { java.produceBatch(stream, partition, batch) }

    /** One read_committed fetch; see [LotseClient.fetch]. */
    public suspend fun fetch(stream: String, partition: Int, offset: Long, maxWait: Duration): Fetched =
        io { java.fetch(stream, partition, offset, maxWait) }

    /**
     * The records of a partition from [from] on, as a read_committed reader sees them, for as long as it is collected.
     * Each fetch waits up to [maxWait] for records; control and aborted records are skipped but move the offset.
     */
    public fun records(
        stream: String,
        partition: Int,
        from: Long,
        maxWait: Duration = Duration.ofMillis(500),
    ): Flow<BatchRecord> = flow {
        var offset = from
        while (true) {
            val fetched = io { java.fetch(stream, partition, offset, maxWait) }
            for (record in fetched.committedMessages()) {
                emit(record)
            }
            offset = fetched.nextOffset(offset)
        }
    }

    /** Where [at] falls in a partition (ADR 0059); see [LotseClient.listOffset]. */
    public suspend fun listOffset(stream: String, partition: Int, at: OffsetAt): Long =
        io { java.listOffset(stream, partition, at) }

    // ---- exchanges, transactions, queues, groups ----

    /** Publishes through an exchange, routed by a node (ADR 0079); see [LotseClient.publish]. */
    public suspend fun publish(
        exchange: String,
        routingKey: String,
        message: OutMessage,
        mandatory: Boolean = false,
    ): Published = io { java.publish(exchange, routingKey, message, mandatory) }

    /**
     * Begins a transaction; one at a time per client, so this waits while another is open. Prefer [transaction],
     * which commits or aborts for you.
     */
    public suspend fun beginTransaction(): LotseTransaction = LotseTransaction(io { java.beginTransaction() })

    /**
     * Runs [block] in a transaction and commits it when the block returns, returning the block's result. If the
     * block or the commit throws, or the coroutine is cancelled, the transaction is aborted (as far as that is still
     * possible) and the exception passed on. A commit that throws [dev.lotse.client.ClientException.Unknown] may have
     * committed: the abort that follows then fails quietly, and the outcome stays unknown.
     */
    public suspend fun <T> transaction(block: suspend LotseTransaction.() -> T): T {
        val txn = beginTransaction()
        try {
            val result = txn.block()
            txn.commit()
            return result
        } catch (e: Throwable) {
            withContext(NonCancellable) {
                io {
                    try {
                        if (txn.java.isOpen) {
                            txn.java.abort()
                        }
                    } catch (abort: RuntimeException) {
                        e.addSuppressed(abort)
                    } finally {
                        txn.java.close()
                    }
                }
            }
            throw e
        }
    }

    /** Subscribes to a queue; see [LotseClient.subscribe]. */
    public suspend fun subscribe(queue: String, options: SubscribeOptions = SubscribeOptions.defaults()): LotseQueue =
        LotseQueue(io { java.subscribe(queue, options) })

    /** Joins a consumer group to read [streams]; see [LotseClient.joinGroup]. */
    public suspend fun joinGroup(group: String, streams: List<String>, config: GroupConfig = GroupConfig.defaults()): LotseGroup =
        LotseGroup(io { java.joinGroup(group, streams, config) })

    /** Joins a consumer group to read the streams matching [pattern] (ADR 0073); see [LotseClient.joinGroupMatching]. */
    public suspend fun joinGroupMatching(group: String, pattern: String, config: GroupConfig = GroupConfig.defaults()): LotseGroup =
        LotseGroup(io { java.joinGroupMatching(group, pattern, config) })

    // ---- admin ----

    /** Creates a stream in the configured tenant and returns its id; see [LotseClient.createStream]. */
    public suspend fun createStream(
        name: String,
        partitions: Int,
        replication: Int,
        configs: Map<String, String> = emptyMap(),
    ): Long = io { java.createStream(name, partitions, replication, configs) }

    /** Creates a queue and returns its id; see [LotseClient.createQueue]. */
    public suspend fun createQueue(name: String, options: QueueOptions = QueueOptions.defaults()): Long =
        io { java.createQueue(name, options) }

    /** Declares an exchange of a type from [Messages.ExchangeType] and returns its id. */
    public suspend fun declareExchange(name: String, type: Int): Long = io { java.declareExchange(name, type) }

    /** Binds a queue to an exchange with a key. */
    public suspend fun bind(exchange: String, queue: String, key: String): Unit = io { java.bind(exchange, queue, key) }

    /** Binds a queue to a headers exchange. */
    public suspend fun bindHeaders(exchange: String, queue: String, headers: List<Header>, matchAll: Boolean): Unit =
        io { java.bindHeaders(exchange, queue, headers, matchAll) }

    /** Removes a binding made by [bind]. */
    public suspend fun unbind(exchange: String, queue: String, key: String): Unit =
        io { java.unbind(exchange, queue, key) }

    /** Deletes a stream, queue or exchange created at runtime; returns its id. */
    public suspend fun delete(name: String): Long = io { java.delete(name) }

    /** Deletes it only if it holds nothing (ADR 0063); returns its id. */
    public suspend fun deleteIfEmpty(name: String): Long = io { java.deleteIfEmpty(name) }

    /** Settles a queue's ready messages (ADR 0064); returns how many. */
    public suspend fun purge(queue: String): Long = io { java.purge(queue) }

    /** Moves a dead-letter queue's ready messages back where they came from (ADR 0064); returns how many. */
    public suspend fun requeue(queue: String): Long = io { java.requeue(queue) }

    /** Changes settings: [set] gives keys values, [reset] returns keys to their default. */
    public suspend fun alterConfigs(name: String, set: Map<String, String>, reset: List<String> = emptyList()): Unit =
        io { java.alterConfigs(name, set, reset) }

    /** A resource's settings with where each value comes from. */
    public suspend fun describeConfigs(name: String): List<Messages.ConfigEntry> = io { java.describeConfigs(name) }

    /** Sets a user's SCRAM-SHA-512 password; only a salted credential leaves the client. */
    public suspend fun setUserPassword(user: String, password: String): Unit =
        io { java.setUserPassword(user, password) }

    /** Removes a user's SCRAM credential. */
    public suspend fun deleteUser(user: String): Unit = io { java.deleteUser(user) }

    /** The users with a SCRAM credential. */
    public suspend fun describeUsers(): List<Messages.UserEntry> = io { java.describeUsers() }

    /** Creates a tenant. */
    public suspend fun createTenant(name: String): Unit = io { java.createTenant(name) }

    /** Deletes a tenant that holds nothing. */
    public suspend fun deleteTenant(name: String): Unit = io { java.deleteTenant(name) }

    /** The tenants this client has a role in. */
    public suspend fun describeTenants(): List<Messages.TenantEntry> = io { java.describeTenants() }

    /** Adds and removes role bindings; removals first. */
    public suspend fun alterRoleBindings(
        add: List<Messages.RoleBinding>,
        remove: List<Messages.RoleBinding> = emptyList(),
    ): Unit = io { java.alterRoleBindings(add, remove) }

    /** Every role binding with where it comes from. */
    public suspend fun describeRoleBindings(): List<Messages.SourcedBinding> = io { java.describeRoleBindings() }

    /** Who this client acts as, and the bindings that apply. */
    public suspend fun whoAmI(): Response.WhoAmI = io { java.whoAmI() }

    /** Revokes a user's or an IdP group's sessions (ADR 0070). */
    public suspend fun revokeSessions(subject: String, group: Boolean = false): Unit =
        io { java.revokeSessions(subject, group) }

    /** Adds a certificate to the cluster's revocation list. */
    public suspend fun revokeCertificate(
        issuer: ByteArray,
        serial: ByteArray,
        notAfterMs: Long,
        reason: String = "",
    ): Unit =
        io { java.revokeCertificate(issuer, serial, notAfterMs, reason) }

    /** The client certificates the cluster has seen or had revoked, by expiry (cluster admin, ADR 0091). */
    public suspend fun describeCertificates(): LotseClient.CertificateInventory = io { java.describeCertificates() }

    /** Adds a node to the cluster. */
    public suspend fun addNode(nodeId: Long, peer: String, client: String): Unit =
        io { java.addNode(nodeId, peer, client) }

    /** Starts draining a node. */
    public suspend fun removeNode(nodeId: Long): Unit = io { java.removeNode(nodeId) }

    /** The cluster's nodes. */
    public suspend fun describeNodes(): List<Messages.NodeEntry> = io { java.describeNodes() }

    /** Pins a partition's leader to a node, or unpins it with `null` (ADR 0065). */
    public suspend fun moveLeader(name: String, partition: Int, node: Long?): Unit =
        io { java.moveLeader(name, partition, node) }

    /** Raises a stream's partition count (ADR 0074). */
    public suspend fun createPartitions(name: String, count: Int): Unit = io { java.createPartitions(name, count) }

    /** Every node's metrics, or why a node gave none. */
    public suspend fun nodeMetrics(): List<NodeMetrics> = io { java.nodeMetrics() }
}

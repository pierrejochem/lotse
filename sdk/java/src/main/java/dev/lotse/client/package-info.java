/**
 * The Lotse client: streams and queues today, transactions, consumer groups, exchanges and admin in later phases.
 *
 * <p>{@link dev.lotse.client.LotseClient} follows the Rust client's behaviour: it finds each partition's leader
 * from metadata, follows leader hints, retries what is safe to retry, and produces idempotently by default. {@link dev.lotse.client.QueueConsumer} consumes a queue: the leader pushes
 * messages within the consumer's credit, and the consumer acknowledges or rejects each.
 */
package dev.lotse.client;

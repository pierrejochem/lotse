/**
 * The Lotse client: streams today, queues, transactions, consumer groups, exchanges and admin in later phases.
 *
 * <p>{@link dev.lotse.client.LotseClient} follows the Rust client's behaviour: it finds each partition's leader
 * from metadata, follows leader hints, retries what is safe to retry, and produces idempotently by default.
 */
package dev.lotse.client;

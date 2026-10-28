# 31. Queues: the first cut

- Status: Accepted
- Date: 2026-10-28

## Context

The design document describes queues on the log: an ack index next to the messages, credit-based push delivery, redelivery of what is not acknowledged, delivery counts for dead-lettering, transactional acks, exchanges, TTL and delay, priorities and single active consumer. It describes the ack index as Roaring bitmaps and leaves open what a consumer's subscription is bound to, what happens to unacknowledged messages when a consumer hangs, how a delivery reaches a consumer on the wire, and how a new leader avoids delivering what its predecessor saw acknowledged. Streams, idempotent producers and transactions exist (ADRs 0026, 0028, 0029); queues are next.

## Decision

**Scope.** A queue is one Raft group (partition 0), fed by `Produce` to that partition, including transactional produce. The protocol gets `Subscribe` (10), `Deliver` (11, a push), `Credit` (12, never answered), `Ack` (13), `Nack` (14, requeue or discard) and `Unsubscribe` (15). Deferred: `Publish` and exchanges, dead-letter queues, TTL and delay, priorities, single active consumer (`exclusive` is refused), transactional acks (`txn_id` is refused).

**The ack index is in the log.** Acks and persisted delivery counts are batches of their own kinds (`Ack`, `QueueState`) in the queue's log, proposed by the leader, replicated and recovered like messages. Every replica applies them into a `QueueLog`: the set of **settled** offsets (acknowledged or discarded messages, aborted transactional messages, and every batch that is not a message) as merged ranges, plus delivery counts for unsettled offsets. It is saved in the Raft snapshot as a fourth section next to producer, transaction and coordinator state (snapshot format 4). Ranges replace Roaring: acknowledged offsets form a few long runs, which ranges hold in a few bytes, and they need no dependency. `Produce` refuses every batch kind but data, so no client can write an ack.

**Delivery is the leader's, and volatile.** The leader keeps a dispatcher per term: subscriptions with credit in messages and bytes, messages in flight, messages released for delivery again (before new ones, lowest offset first). It is not replicated: a new leader starts empty, ends nothing it does not know, and delivers every unsettled message again. It delivers only after it has applied an entry of its own term, because then every ack committed before is applied and an acknowledged message is never delivered again; and only below the last stable offset, so messages of an open transaction wait and those of an aborted one never go out.

**Acks are writes.** `Ack` and a discarding `Nack` propose an `Ack` batch and are answered under the commit rule of ADR 0026. An ack names offsets, not deliveries: any connection may settle any offset, and settling twice is harmless, so the client retries unknown outcomes. Offsets beyond the leader's log end are refused with the new error code 21 `INVALID_REQUEST`; any offset in the log may be acknowledged, since the ack's entry follows it and commits only if it does.

**Requeue and the ack timeout persist the delivery count first.** A `Nack` with requeue takes back the messages this connection holds, writes their delivery counts as a `QueueState` batch and releases them once it is applied, then answers. A subscription that holds a message unacknowledged longer than the queue's ack timeout (30 minutes by default, RabbitMQ's consumer timeout) ends with a push carrying the new error code 23 `ACK_TIMED_OUT`; its messages are held back the same way and released once their counts are written. After a disconnect the count is raised in memory only; after a failover it may start over. Counts are therefore a lower bound.

**Unsubscribe is RabbitMQ's cancel.** No new deliveries; the messages in flight stay with the consumer until acknowledged or until its connection closes. A closed connection releases everything its subscriptions held.

**Push frames.** A push uses the response layout with correlation id 0 and flag `PUSH`, followed by its API key and version (`Deliver`, 0), then the body: subscription id, an error code, one batch exactly as stored, and the offsets in it that are this subscription's with their delivery counts. A non-zero error code means the subscription has ended (`NOT_LEADER` with a leader hint after a leader change, `ACK_TIMED_OUT`); the client subscribes again on its own. Subscription ids are per node; `Credit` and `Unsubscribe` name them and are honoured only on the connection that owns them (the new error code 22 `UNKNOWN_SUBSCRIPTION` otherwise).

**Client.** `Client::subscribe(queue, credit)` returns a `Consumer`: `next` hands out messages one at a time and adds credit when half is used, `ack` and `nack` settle them, `close` (or drop) unsubscribes and gives back what was delivered but not handed out. When a subscription ends, the messages waiting in the consumer are dropped, since the leader gives them to others.

## Consequences

- Messages are delivered at least once; a confirmed ack is final across leader changes, crashes and restarts. The queue simulation checks that nothing is lost, nothing settled comes back, and transactions stay atomic for consumers. It runs in a CI job of its own, like the client simulation (ADR 0030): 2,000 seeds on pushes, 5,000 nightly and on manual runs; it takes about 8 minutes at 2,000.
- The dispatcher is per leader term, so a failover redelivers everything in flight; with large credit windows that can be many messages.
- Settled offsets of a long-lived queue with scattered acks fragment into many ranges, all held in memory and in every snapshot, until log retention and a compaction of the index exist.
- Delivery counts are a lower bound, which is enough for the dead-letter threshold that follows but not an exact history.
- A queue's log is read through subscriptions only: `Fetch` on a queue is `UNKNOWN_RESOURCE`, since its log holds acks and queue state too.

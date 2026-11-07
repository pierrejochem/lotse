# 77. Delayed delivery

- Status: Accepted
- Date: 2026-11-07

## Context

Queues need two kinds of waiting. A producer schedules a message for later, for example a reminder or a retry after a known time. A consumer gives a message back to be tried again after a pause, a retry with backoff, instead of at once. `Nack` already had a `delay_ms` field, but it was refused. The design planned TIMER records with a due time in the queue's log, and `BatchKind::Timer` was reserved for them. The maintainer chose to build both kinds behind one rule, that a message is not delivered before its due time. The alternatives were only the nack's delay, or only a header on publish.

## Decision

**A publish delay** is the record header `x-lotse-delay-ms`, decimal ms from 0 to 4,294,967,295 (about 49 days). Anything else is no delay. It counts from the batch's base timestamp, which the queue's leader sets to its own clock when it appends the batch. Until now that happened only on queues with a message TTL (ADR 0055); it now happens on every queue, so neither the age nor the delay of a message depends on its producer's clock. Stream batches keep their producers' timestamps. Only data batches honour the header: a dead-lettered or requeued copy (ADR 0055, 0064) does not wait a second time. Nothing about a publish delay is written: every leader reads it from the batch again.

**A nack delay** is `delay_ms` on a `Nack` with `requeue`; without `requeue` it is `INVALID_REQUEST`. The leader writes a TIMER batch instead of a QUEUE_STATE batch. It holds one record with, per message, the offset, the delivery count and the due time: the leader's clock plus the delay. Every replica keeps the due time in the ack index until the message is settled, and a later nack's time replaces an earlier one. The time is in a trailing section of the queue's snapshot, written only when there is one. So a new leader waits too, and older snapshots read as before. `Consumer::nack_after(message, delay)` sends it. If the subscription has ended, the message has gone back with it, without a delay.

**At the leader**, a message's due time is the later of the two. A message that is not due yet is deferred: the dispatcher leaves it out until its time, then delivers it before anything new. Other messages keep flowing meanwhile, so a delayed message never blocks the queue. Due times are compared in whole ms of the leader's clock.

## Consequences

- Scheduled messages and retries with backoff work across failovers. A publish delay comes from the log itself, and a nack delay is in the log.
- A delayed message is unsettled while it waits, so the ack floor stays below it and retention waits for it. A queue full of long delays keeps its log.
- A message TTL still counts from the append time. A message delayed past its TTL expires before it is delivered.
- On a queue with a single active consumer (ADR 0076), a deferred message is not out with anyone, so a handover does not wait for it. It goes out after newer messages, which is what a delay asks for.
- Queue messages now carry the leader's append time as their timestamp, not the producer's.
- Leaders compare in ms of their own clocks, so clocks that disagree between leaders move a due time across a failover by that difference.
- The Java SDK can set the header on a message. Its consumer has no `nackAfter` yet.

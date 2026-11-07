# 75. Queue acks in transactions

- Status: Accepted
- Date: 2026-11-07

## Context

A consumer that reads a queue and writes what it makes of each message to a stream or another queue wants the two to happen together. Either its output is visible and the message is settled, or neither happens. Groups do this for streams by committing offsets in the transaction (ADR 0033). Queues had no equivalent: `Ack` carried a transactional id, but it was refused. The maintainer chose to write the acks to the queue's own log, decided by the transaction's markers. The alternatives were to keep them on the coordinator, or to ack after the commit and deduplicate.

## Decision

**The request.** `Ack` with a transactional id carries the tenant (tag 1) and the transaction's producer id and epoch (tag 2).
- The client first adds the queue, as partition 0, to the transaction with `AddPartitionsToTxn`.
- It then sends the ack on the connection the messages were delivered on.
- It needs the consumer role on the queue and the producer role on the transactional id.
- `Consumer::ack_in(&mut Transaction, &Message)` does all of this.

**What the leader accepts.** It accepts a message only if it is in flight with one of the connection's subscriptions, or if it is already taken for the same producer, so a retry goes through. Anything else is `MESSAGE_NOT_HELD` (error 31), and the client must abort. The leader then takes the messages out of flight. From then on they are not delivered again, the ack timeout does not apply to them, and a disconnect does not release them. The leader asks the coordinator first, with `VerifyTxn` (ADR 0057), when the producer has no transaction open in the queue yet. A batch at an older epoch than one the queue has seen for the producer is `PRODUCER_FENCED`.

**The batch.** The leader writes an ack batch with the transactional bit and the producer's id and epoch, but no sequence. Every replica applies it into the ack index (`QueueLog`):
- A message that is unsettled and held by no other transaction is now held by this producer.
- A commit marker settles what the producer holds.
- An abort marker releases it, and the leader delivers it again.
- A marker counts only for acks of an older epoch, as for data.

The leader answers once the batch is applied: `NONE` if the producer holds every message it named, `MESSAGE_NOT_HELD` otherwise. Of two transactions that ack the same message, the log's order picks one, and the other's client aborts.

**What a held message is not.** It is not ready:
- A purge leaves it.
- The limits of ADR 0055 neither count it as expired nor dead-letter it.
- A claim at apply time skips it.

A plain ack still settles it, because an ack names an offset (ADR 0031).

**The last stable offset.** A transaction that has written only acks in a partition does not hold the last stable offset back, because acks hide nothing from readers. Messages published after the ack are delivered while the transaction is open. Its first data batch there, if one comes, does hold it back, as before. An aborted transaction without data leaves no aborted range. The producer state ignores ack batches, so a producer that publishes to and acks the same queue keeps its sequences.

**Snapshots.** The queue state gets a trailing section of held messages: per producer, the epoch and the offsets. It is written only when something is held, so older snapshots still read.

## Consequences

- Consume-transform-produce from a queue is exactly once. The output and the ack commit or abort together, and while the transaction is open no one else receives the message.
- A message stays with its transaction until the transaction ends. The coordinator's transaction timeout bounds this.
- A failover keeps what the log holds. A new leader learns what is held from the ack index, and delivers again what an abort releases.
- An ack that went to the old leader but is not in the new leader's log is lost. So is the subscription: the client sees `MESSAGE_NOT_HELD`, and the message is delivered again.
- Holding works per message. A transactional ack of a large range costs as much as acking each message in it, and the leader refuses ranges of more than 10,000 messages.
- The Java SDK codes the new tags. Its queue consumer does not send transactional acks yet, because the SDK has no transactions so far.

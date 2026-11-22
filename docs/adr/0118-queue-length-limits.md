# 118. Queue length limits

- Status: Accepted
- Date: 2026-11-17

## Context

A queue could grow without bound. `max.deliveries`, `message.ttl.ms` and dead-lettering (ADR 0055) take out messages that are delivered too often or wait too long. Nothing stopped a queue whose consumers are gone from filling its nodes' disks. The README listed this as "queue length limits cannot change after creation", but queues had no length limit at all.

RabbitMQ limits a queue with `x-max-length` (messages) and `x-max-length-bytes` (body bytes). Only ready messages count; messages delivered and not yet acknowledged do not. `x-overflow` says what a full queue does:

- `drop-head`, the default, drops or dead-letters the oldest messages;
- `reject-publish` makes the broker refuse new ones with a publisher nack.

Quorum queues support both. Kafka has no per-topic length limit beyond retention.

## Decision

**A queue may limit how many messages, and how many bytes of them, wait for delivery. A full queue refuses more or drops its oldest, as the queue says** (maintainer's choices: both overflow modes with `reject-publish` as the default; waiting messages counted as in RabbitMQ; count and bytes).

- **Settings.** Three queue settings, set at creation or changed at any time with `AlterConfigs`; `DescribeConfigs` reports them.
  - `max.length`: messages, 0 for none.
  - `max.length.bytes`: bytes of keys, values and headers, 0 for none.
  - `overflow`: `reject-publish` (the default) or `drop-head`.

  They live in the metadata group like the other queue settings:
  - a `CreateQueue` record's flags gain bit 3;
  - an `AlterQueue` record's mask gains bit 7, followed by a second mask for the three;
  - the snapshot gains section 15.

  A queue without a limit writes what earlier nodes read.
- **What counts.** Every unsettled message that a consumer could still get, committed or not, in an open transaction or not, counts. Messages out with a consumer do not: in flight, held by a transactional ack, or waiting for a consumer to resume (ADR 0109). A consumer that holds many messages can therefore let the queue hold more than its limit, as in RabbitMQ.
- **Who counts.** The queue's leader, for its term, in `lotse-queue`'s `Lengths`:
  - It reads the log for each message's size, 4 MiB a round, and forgets what the queue settles.
  - A write it takes counts at once, before the log is read again, so writes arriving in one round do not pass the limit together.
  - A new leader counts what it has not read yet at its largest: every unread offset as a message, every byte of an unread segment as a message byte. It may refuse while it reads the backlog, but never takes more than the limit.
- **`reject-publish`.** A produce, a node-routed publish, a transaction's batch or a dead letter that would pass the limit is refused:
  - The first three fail with the new error code `QUEUE_FULL` (34), not retriable; nothing is written.
  - A dead letter stays owed in its source queue, which sends it again later (ADR 0055); nothing is lost.
  - The queue takes a forwarded part whole or not at all, so after a refusal the source sends half as many next time, down to one, and twice as many again after one is taken: a queue with room for fewer messages than are owed still fills to its limit.
  - The replica asks the limit only about a batch that is new to it (`Replica::propose_admitted`). A retry of a batch it already took is answered as written, never refused: otherwise its producer would take the batch's sequence as free.
- **A refusal after a lost answer.** An earlier attempt whose answer was lost may still commit at a leader that has it in its log, and a refusal says nothing about that attempt. So a client that gets `QUEUE_FULL` after such an attempt reports the outcome as unknown and starts a new producer, as it does for an authentication failure then. The Rust client and the Java, C# and Node.js SDKs all do this.
- **`drop-head`.** The queue takes every write. Each round its leader retires the oldest waiting messages until the queue fits, through the RETIRE path of ADR 0055, with the new reason `max-length`:
  - with a dead-letter queue, into it, once across failovers;
  - without one, they are dropped.

  Messages a claim already names no longer count, so the leader does not claim more than the excess. It retires only once it has read the whole log: it never drops on a bound.
- **Clients.** The Rust client sets the limits as plain settings. The Java, C# and Node.js SDKs offer `maxLength`, `maxLengthBytes` and `overflow` options, and know `QUEUE_FULL` and the reason `max-length`.

Rejected options:

- **`drop-head` as the default, like RabbitMQ.** A full queue would lose messages silently unless a dead-letter queue is set, which does not fit a broker that promises exactly-once delivery.
- **Counting everything unsettled.** That bounds what the queue holds even with slow consumers. But a full queue would refuse producers while consumers merely hold messages, and it differs from RabbitMQ.
- **Count only.** A queue of large messages could still grow without bound.
- **`reject-publish-dlx`.** RabbitMQ classic queues dead-letter refused messages. Quorum queues do not offer it, and the producer still learns of the refusal.
- **Keeping the count in every replica's applied state.** The size of every waiting message would be in every snapshot. Only the leader decides, as for priorities (ADR 0078) and delayed delivery (ADR 0077).

## Consequences

- A queue nobody reads stops at its limit instead of filling a disk. Producers learn of it at once (`QUEUE_FULL`), or the oldest messages make room, as the queue says.
- The leader keeps the size of each waiting message in memory, about 40 bytes each. A new leader reads the backlog again, as it does for priority levels.
- Limits apply as soon as the change is applied. A queue already over a new limit refuses new writes, or drops down to it.
- The protocol gains one error code. The golden vectors gain it, and each SDK knows its name.
- Tests:
  - `lotse-queue` unit tests cover what waits (settled messages, consumers, open transactions, writes taken) and what `drop-head` takes (passing over consumers' messages and claims, by count and by bytes).
  - A metadata test covers the records and the snapshot.
  - A replica test shows that a retry of a taken batch is never refused.
  - A client unit test shows that writes in one round count each other, a dead letter included.
  - A server unit test checks the bound a new leader gives what it has not read.
- Queue simulations:
  - A full queue refuses, two messages held by a consumer do not count, it is still full after a failover, a transactional batch is refused, and a byte limit holds.
  - A `drop-head` queue keeps its newest messages across a failover and dead-letters the rest with `max-length`.
  - Four producers arriving together share one free place.
  - A full dead-letter queue takes no more, and its source sends the rest once there is room.
  - Under faults (`queues_at_their_length_limit_refuse_or_drop_and_lose_nothing_under_faults`):
    - nothing refused is ever delivered;
    - nothing taken is lost, except what `drop-head` moved to its dead-letter queue;
    - an emptied queue takes exactly its limit again.
- The Rust client and the SDKs have tests for `QUEUE_FULL` with and without an earlier lost answer.
- Mutation-checked:
  - asking the limit about a duplicate;
  - counting messages out with a consumer;
  - not counting writes taken in the round, produced or dead-lettered;
  - a new leader without the bound on what it has not read;
  - not forgetting settled messages;
  - `drop-head` counting messages already claimed, or retiring nothing, or `drop-head` refusing;
  - no limit on dead letters or on a transaction's batch;
  - the client taking `QUEUE_FULL` after a lost answer as definite.

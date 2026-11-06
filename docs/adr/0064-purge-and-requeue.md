# 64. Purge and requeue

- Status: Accepted
- Date: 2026-11-06

## Context

The console's design has two actions on a queue that the broker could not do: emptying a queue, and moving the messages of a dead-letter queue back to the queues they came from once their cause is fixed. Both act on a queue's ack index, which only the queue's leader writes. Messages leave a queue for its dead-letter queue exactly once across failovers (ADR 0055); a move back should be no weaker.

## Decision

**Two requests to the queue's leader.** `PurgeQueue` (47) and `Requeue` (48) share one body: the queue, and a bound `up_to`. They share one answer, `QueueAction`: the error, a reason, how many messages this request acted on, the bound, whether more are left, and a leader hint. Each request acts on at most 10,000 messages (purge) or 1,000 (requeue) below the bound. The first request's bound is the queue's deliverable end at that moment, and the client sends the request again with that bound until nothing is left. Messages produced while a purge or requeue runs are therefore left alone, and a busy queue does not keep it going for ever. The leader acts only once it has applied an entry of its own term, as it does before delivering. While the batch is in flight its messages are held back from delivery, as claims are. Both need the tenant-admin role on the queue.

**Purge takes ready messages only** (maintainer's choice). It settles, with one ack batch, every unsettled message below the bound that no consumer holds. This is RabbitMQ's purge. A message a consumer holds stays the consumer's to ack or nack, including messages already delivered into the consumer's prefetch. Messages of open transactions are above the deliverable end and wait for their marker.

**Requeue is broker-side and exactly once** (maintainer's choice). It is the dead-letter move of ADR 0055 the other way round:
1. The leader of the dead-letter queue reads each ready message's dead-letter headers.
2. It claims the messages in RETIRE batches owed to the queue each came from, with the new reason `requeued`, one batch per source queue.
3. It forwards them with the internal `DeadLetter` request.
4. The source queue recognises a message it has already taken from the same queue and offset.

A message therefore moves once, whatever fails on the way. It arrives with dead-letter headers that describe this latest move: reason `requeued`, queue the dead-letter queue. A message whose headers name no origin, its own queue, or a queue that is gone stays where it is. If the source queue is deleted after the leader has claimed the message, the message goes with that queue, as if it had arrived just before the delete.

`lotse-client` has `Client::purge` and `Client::requeue`, which repeat the request until nothing is left and return the total. The Java SDK has the request and response types.

## Consequences

- Operators can empty a queue without deleting it, and replay dead letters into their queues without a tool of their own and without duplicates.
- A purge does not touch what consumers hold, so a queue with busy consumers is not empty right after a purge; `ready` is 0.
- A requeued message's delivery count starts over in its queue, and its earlier dead-letter headers are replaced.
- Tests: a scenario test on a three-node simulated cluster covers:
  - five messages dead-lettered, requeued exactly once each with the new headers, and nothing left;
  - a message without an origin, and one whose queue was deleted, staying;
  - a purge settling everything but what the consumer holds, with the consumer still acking what it holds and receiving nothing else;
  - a message produced after the purge being delivered.
  
  The access test checks that a consumer may not purge or requeue, and a tenant admin may.
- Mutation checks: these were broken on purpose, and a test failed for each:
  - purging held messages;
  - not holding purged messages back from delivery;
  - the wrong reason;
  - requeuing to a missing source;
  - the role check;
  - the count in the answer.
  
  Not covered by a test: the bound and the per-request limits (no test produces more than a request's limit), and the own-term check, which is timing.
- This test's first version expected a purge to leave exactly the messages handed to the application. A consumer's prefetch holds more, which ready-only purge rightly keeps, so the test was wrong and now counts what the leader reports in flight.

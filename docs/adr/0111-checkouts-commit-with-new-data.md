# 111. Checkouts commit with new data

- Status: Accepted (refines [ADR 0109](0109-deliveries-are-written-to-the-log.md))
- Date: 2026-11-14

## Context

Since ADR 0109 a queue leader writes a CHECKOUT batch before it pushes, and pushes once the batch is applied. A leader delivered only what was applied, so a new message to a consumer that waited went through two commits in a row: its produce committed and was applied, then the leader wrote the checkout, and the push waited for that commit too. In the simulation, a delivery took about one and a half times as long as the produce that wrote it.

Two things added to it. The leader looked for messages only up to what it had applied. And a node filled each append with entries when it persisted a round's writes, before its queue leaders had written that round's checkouts. Raft keeps one append in flight per follower, so a checkout written just after the data waited for the answer to the data's append.

Three ways out were weighed: let the checkout ride with the new data, push before the checkout commits, or only write fewer checkout batches.

## Decision

**A leader checks out plain data it has appended.** Past what is applied and stable, the leader looks through the tail of its log, data it has appended but not yet committed, up to 1 MiB a round. It hands out the messages of plain data batches there like any others. The checkout lands behind them in the log, so it commits only if they do. The push still waits until the checkout is applied, and by then the data is committed and applied. ADR 0109's guarantees stay as they are.

The leader passes over its own queue batches in that tail (acks, transactional acks, queue state, timers, retire claims, checkouts). They are not messages, and are left out of the round. It stops at anything that could make a message not deliverable once applied:

- transactional data, which waits for its marker;
- a control batch (a marker, Raft's own entries);
- a dead-letter batch, whose repeats are recognised only when it is applied.

Behind an open transaction the limit stays where it was: the last stable offset.

**A node fills appends last.** A node persists each group's ready writes and runs the round's client work: answers, queue deliveries and their checkouts. It then persists what that work wrote, and only then fills the appends with entries from the log. An append to a follower therefore carries the data and the checkout behind it, and both commit with one answer.

**A requeue waits for adoption.** A consumer that resumes at a new leader before the leader has adopted the earlier checkouts holds a message the leader does not yet count as in flight with it. A requeueing `Nack` that came then found nothing to take, was answered as done, and left the message with a consumer that had given it back, until the ack timeout. It now waits until the leader has adopted, or no longer leads.

Rejected options:

- **Push before the checkout commits.** It saves the commit for every delivery, also from a backlog. But a failover inside that window brings back what ADR 0109 removed: the new leader does not know the delivery, a transactional ack aborts, and a delivery count can repeat.
- **One checkout batch per queue and round.** Fewer, larger batches when many consumers share a queue. It needs a new batch format and does not shorten a delivery.
- **Several appends in flight per follower.** Raft pipelining would let a checkout go out without waiting for the data's answer. It is a change to the Raft core for something the order of a node's round achieves alone.

## Consequences

- A new message reaches a waiting consumer about when its producer hears it is written: one commit, not two. A delivery from a backlog still waits for its checkout's commit.
- A leader reads up to 1 MiB of its uncommitted tail each round in which it delivers.
- Appends carry what a node's round wrote, not only what was ready before it, so other writes of the round (acks, queue state, retire claims) also go out one round sooner.
- No format, protocol or client changes.
- Tests:
  - A unit test with two replicas checks that a leader checks out appended plain data before it commits, passes over its checkouts, checks out nothing behind transactional data or dead letters, and pushes only once applied.
  - The scenario `a_waiting_consumer_gets_a_new_message_with_its_commit` measures twenty deliveries against their produces under a 20 ms network: about equal now, about half as long again before.
- Mutation-checked:
  - stopping at nothing transactional, passing over dead letters, and treating the leader's own batches as messages each fail the unit test;
  - no tail at all fails the unit test and the scenario;
  - filling appends as each group persists (the old order) fails the scenario.
- The early requeue was a gap of ADR 0109 that its timing hid. The new timing exposed it: the failover scenario of `tests/queues.rs` failed with seed 2, its requeue at a new leader answered as done and the message never came back. That was the code, not the test; without the wait, seed 2 fails again.

# 76. Single active consumer

- Status: Accepted; in part superseded by [ADR 0097](0097-a-queue-leader-delivers-only-with-a-recent-quorum.md) (a deposed leader stops delivering once no quorum answered it within an election timeout, not at check-quorum)
- Date: 2026-11-07

## Context

Some queues must be processed in order, by one consumer at a time, with a second one ready to take over: a ledger fed from a queue, a per-customer workflow. The design listed a single active consumer that is leader-local, with handover on disconnect or credit timeout. `Subscribe` had an `exclusive` flag, but it was refused, and `CreateQueue` had a `single_active` field that was refused too. The maintainer chose a queue setting under which every subscriber is accepted and the others wait as standbys, as in RabbitMQ. The alternatives were an exclusive subscription that refuses everyone else (AMQP's exclusive consumer), or a per-subscription flag whose guarantee would depend on every client setting it.

## Decision

**The setting.** `single.active.consumer` is a queue setting, `true` or `false`, set when the queue is created (or with the request's `single_active` field) and changed with `AlterConfigs`. It is off by default. In the metadata group it is a flag byte after a queue's limits:
- in `CreateQueue` records, written only when set;
- in `AlterQueue` records, mask bit 4 followed by the value;
- in snapshots, a trailing section listing the queues that have it.

Older records and snapshots read as before.

**Who gets messages.** The leader's dispatcher delivers to one subscription, the active one. Every other subscription is a standby, in the order it subscribed. The active one keeps its place until it ends: unsubscribed and done, disconnected, or ended by the ack timeout. Standbys get nothing, and their credit does not count.

**The handover.** The oldest standby becomes active only once no message is out with anyone:
- none in flight;
- none held back for a delivery count after a nack or the ack timeout;
- none held by a transaction (ADR 0075), whether the dispatcher took it or the ack index shows it from an earlier leader.

The messages the last one had then come back first, in offset order, so the queue's order holds across the handover. Turned on while several subscriptions have messages, no one is active until all of those are back.

**What a client hears.** `SubscribeResponse` carries tag 1, `standby`, in the block that holds the leader hint, written only when set; older readers skip it. A subscription that asks with tag 1 `notify_active` on `Subscribe` gets a push when it becomes active: no error, no batch, no deliveries. Clients that do not ask never see that push, since an older client would read its empty batch as corrupt. The Rust client asks. `Consumer::is_standby` says whether the subscription still waits, and `Consumer::subscriptions` counts how often the consumer has subscribed.

**Failover.** It is all leader-local, as the design said. A new leader has no subscriptions, and whoever subscribes again first is active, once nothing the log shows as held remains. A deposed leader that has not noticed yet may still deliver to its active consumer until check-quorum steps it down. That consumer's acks then fail. So at most one consumer gets messages per leader term. One consumer across a partition is not guaranteed.

**`exclusive`.** `exclusive` on `Subscribe` stays refused: an exclusive consumer, the only one a queue may have, is not built.

## Consequences

- A queue can be processed in order by one consumer at a time, with standbys taking over without losing their place in line.
- A handover waits for every message out, including those held by an open transaction. A standby can wait as long as a transaction's timeout.
- Nacks with requeue and transactions that abort return a message to the same active consumer after newer ones. Order holds across consumers, not within one consumer's own retries.
- During a partition, a deposed leader may feed a second consumer whose acks fail. Consumers that must not overlap even then need fencing of their own.
- The Java SDK codes the new tags and reads the activation push. Its queue consumer does not ask for that push yet.
- Not yet shown in `DescribeMetrics`: which subscription is active, and how many handovers happened.

# 72. Static group membership

- Status: Accepted
- Date: 2026-11-06

## Context

A consumer group member that restarts joins as a new member (ADR 0033). Its partitions go to the others when its session times out, and come back when the assignor spreads them again: they move twice, and each move costs reading from the committed offset. The maintainer chose Kafka-style static membership: a stable instance id.

## Decision

**A member may name an instance id** (`GroupConfig::instance_id`), sent with every `GroupHeartbeat` as tag 2. The coordinator keeps it with the member, in group records and in snapshots. Both are written in the old form when no member has one, so nodes from before read them.

**Coming back.** When a member joins with the instance id of a member already in the group, it takes that member's place:
- the target, assigned and revoking partitions and the subscription carry over;
- it gets a new member id, which fences the old one, and an epoch one higher;
- nothing is rebalanced for the others, unless it subscribes to different streams;
- its assignment comes in the answer, and it reads from the committed offsets.

**Leaving.** A member with an instance id that leaves (epoch −1) keeps its place and partitions. If no instance comes back within the session timeout, the coordinator removes it, as it removes any member it has not heard from. A member without an instance id leaves at once, as before.

**Two at once.** A second live instance with the same id takes the place just like a restart. The first then gets `FENCED_INSTANCE_ID` (error code 30) on its next heartbeat. The client drops its partitions and stays out: every later `poll` returns that error, and it never joins again, which would take the place back.

## Consequences

- A rolling restart moves no partitions.
- A member that died keeps its partitions unread until its session times out. That is the price of not moving them on a restart.
- During a takeover, the earlier instance may still read its partitions until its next heartbeat, so for a moment two instances read the same partition. Its commits are refused, because its member id is gone, so offsets stay with the newer instance. Exactly-once processing, which commits offsets in the transaction, keeps its guarantee. Plain processing may handle a few records twice.
- Instance ids are the user's to keep unique within a group.

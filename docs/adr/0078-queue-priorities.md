# 78. Queue priorities

- Status: Accepted
- Date: 2026-11-07

## Context

Some queues hold work of different urgency, where an urgent message should overtake a backlog of routine ones. `CreateQueue` had `priority_levels` (1–10), but anything above 1 was refused. The design planned one sub-log per level, each its own Raft group under one queue id, with delivery taking the highest non-empty level first. That needs coordination across groups whose leaders may sit on different nodes: consumers subscribe to every level, and acks and transactions span several groups. The maintainer chose to keep one log and let the leader order the messages. The alternatives were the sub-logs, or hidden queues per level that the client reads in order.

## Decision

**The setting.** `priority.levels` is a queue setting from 1 to 10, set at creation (the request's field or the setting) and changed with `AlterConfigs`. One level means none, and is the default. The metadata group stores it after the single active consumer flag (ADR 0076):
- in `CreateQueue` records, flag bit 1 followed by the count;
- in `AlterQueue` records, mask bit 5 followed by the count;
- in snapshots, a trailing section of queue ids and counts.

Older records and snapshots read as before.

**A message's level** is the record header `x-lotse-priority`, a decimal from 0, where higher goes first. A level above the queue's highest counts as the highest, and anything that is not a number from 0 to 255 is level 0. Only data batches have a level, so dead letters and requeued messages are level 0.

**The leader** reads the levels as data becomes deliverable. Each round it reads at most 1 MiB of the log past the point it has read to. A new leader starts at the ack floor, so it reads the backlog once, over as many rounds as that takes. The dispatcher keeps the unsettled messages above level 0 in an index. It hands out the highest level first, lowest offset first within a level, then level 0 in offset order as before. A message above level 0 does not move the cursor, so the level-0 messages it overtook still come. Everything else is unchanged: acks, transactions, redelivery, delays, single active consumer and the limits all work per message, whatever its level. Candidates now come in delivery order rather than offset order, so the leader groups each push by the batch a run of offsets lies in.

**The console** creates queues with levels, and with all other options, on a real cluster too. Its per-level counts still show one level.

## Consequences

- An urgent message overtakes a backlog without new Raft groups, and acks and transactions stay in one log.
- All levels share one log's throughput, and a busy low level slows a high one's appends.
- After a failover, levels are known only for what the new leader has read so far. Until it has read the backlog, at 1 MiB a round, it may hand out messages in plain offset order.
- Changing the number of levels at runtime makes the leader read the log again from the ack floor.
- Priority orders deliveries but promises no strict order. Messages already in flight stay where they are, and a redelivered message keeps its level.
- Not yet shown in `DescribeMetrics`: ready messages per level.

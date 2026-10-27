# 12. Priorities as sub-logs; single active consumer on the leader

- Status: Accepted
- Date: 2026-10-27

## Context

Priority queues and single-active-consumer semantics must fit the one-log model.

## Decision

Each priority level of a queue is its own sub-log (its own Raft group) under one queue id; delivery takes the highest non-empty level first. Single active consumer is decided on the leader: the first subscriber is active, and the leader hands over on disconnect or credit timeout, aborting the old consumer's open ack transactions first.

## Consequences
- Transactional acks may span several sub-logs; the coordinator treats them like partitions.

See also: [design document](../design.md#weitere-entscheidungen).

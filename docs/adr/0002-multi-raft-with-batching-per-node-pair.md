# 2. Multi-Raft with batching per node pair

- Status: Accepted
- Date: 2026-10-27

## Context

Every partition, queue and priority level needs strong replication; a cluster can host thousands of them. One Raft group per unit is simple to reason about, but naive per-group heartbeats cost a network write per group and tick.

## Decision

Each partition, queue and priority level is its own Raft group. All groups on a node are driven together (`MultiRaft`); their messages are bundled per destination node into one frame per round. Writes are acknowledged once 2 of 3 replicas persisted them. Cluster metadata lives in its own metadata Raft group.

## Consequences
- Strong, per-unit guarantees that scale to many units.
- The transport must carry batched multi-group frames.

See also: [design document](../design.md#replikation--snapshots).

# 15. Membership changes through learners and joint consensus

- Status: Accepted
- Date: 2026-10-27

## Context

Replicas are added, replaced and moved between racks; Raft membership changes must stay safe under crashes and partitions.

## Decision

New replicas join as learners: they receive the log, do not vote and do not count. A caught-up learner can be promoted. Voter changes go through a joint configuration C_old,new in which every election and commit needs a majority of both sides; once it commits, the leader appends C_new itself. One change at a time; configurations take effect when appended. New members are replicated from the snapshot or index 1. A leader that removes itself leads until C_new commits, then steps down.

## Consequences
- Several replicas can be swapped in one change (rack migration).
- The Kubernetes operator drains before removing and never runs two changes per group at once.

See also: [design document](../design.md#membership-%C3%A4nderungen).

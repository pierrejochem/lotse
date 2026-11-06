# 65. Pinned leaders

- Status: Accepted
- Date: 2026-11-06

## Context

Since ADR 0050 every group's leader hands leadership to a preferred voter, which every node computes the same from the metadata state, so that leaders spread over the nodes by themselves. The console's design has an action to move a partition's leader to a chosen node: before a node is patched, or to keep a hot partition away from a busy node. A one-off Raft leader transfer would be undone by the spreading within about 1,000 ticks.

## Decision

**A pin in the metadata state** (maintainer's choice). `MoveLeader` (49, cluster admin) names a partition of a stream or queue created at runtime and a node. The node must be one of the partition's voters. A node id of 0 unpins the partition. The metadata leader records `PinLeader { group, node }`, and every node keeps the pins in its metadata state and in snapshots. The pins are written into a snapshot only when there are some, so a snapshot without pins reads as before.

**Pins come first among the preferred leaders.** When the preferred leaders are computed, every group pinned to one of its current voters prefers that voter, and its count goes to the node first. The other groups then spread around them as before. The group's leader hands over through the existing path, which waits until the target has every entry and is paced as before (one handover per node every 100 ticks, per group every 1,000). A pin therefore holds across failovers and restarts: when the pinned node comes back, leadership returns to it. A pin to a node that is no longer a voter counts for nothing, so a drain or rebalance that moves the group away is never blocked. A pin is dropped when its resource is deleted or its node leaves the cluster.

`lotse-client` has `Client::move_leader`. The Java SDK has the request.

## Consequences

- Operators can put a partition's leader where they want it and it stays there; unpinning hands it back to the automatic spreading.
- Pinned groups count towards each node's share, so many pins on one node leave the others leading the rest.
- Pins are not reported yet (neither `Metadata` nor `DescribeMetrics` shows them); the leader a client sees is the effect.
- Only runtime resources can be pinned; configured streams and the metadata and coordinator groups cannot.
- Tests:
  - unit tests of the preferred leaders with a pin, and of pins in the metadata state, its snapshot, unpinning and a node leaving;
  - a scenario test on a three-node simulated cluster: the leader moves to the pinned voter and stays there for a minute, returns there after that node crashes and restarts, and a non-voter and a missing partition are refused;
  - the access test, which checks that a tenant admin may not move leaders.
- Mutation checks: these were broken on purpose, and a test failed for each:
  - pins ignored when computing preferred leaders;
  - pins not applied to the metadata state;
  - the voter check;
  - the role;
  - pinned groups not counted first.

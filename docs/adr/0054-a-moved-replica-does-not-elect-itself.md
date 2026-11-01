# 54. A moved replica does not elect itself

- Status: Accepted
- Date: 2026-11-01
- Amends: [ADR 0050](0050-rebalancing-groups-onto-new-nodes.md) (a moved replica grants no vote until it holds a newer membership)

## Context

The metadata simulation failed at seed 1397 with "a leader asked to overwrite committed index 1". The cause: a group that started with this node as its only voter (first voters `{1}`) was moved to another node and later placed back. The returning replica opens empty, from its old term and vote, with the group's first voters as its membership: itself alone. ADR 0050 stops it from answering vote requests, but nothing stopped it from timing out and voting for itself. It won term 2 with a quorum of one, wrote a no-op and a membership entry, and committed them, while the group's real leader, in the same term, held different entries at the same indexes. When that leader reached it, the replica had committed an index the leader's log contradicts, and Raft's check fired. The check was right; the replica must not have committed those entries.

## Decision

A replica that may not vote (`may_vote` false: it was moved off this node and holds no membership newer than that move) does not tick, so it never starts an election. It stays a follower, takes appends and snapshots from the group's leader, and ticks again once it holds the membership that placed it here again. The node ticks replicas through `Replica::tick`, not through `raft()`.

The rule is the same as ADR 0050's, applied to the vote it casts for itself: a replica that may have acknowledged entries it no longer has must not take part in an election, as a voter or as a candidate.

## Consequences

- A group that returns to a node as that node's only first voter no longer forks its history.
- A returning replica without a leader to talk to stays quiet; the group's other voters elect a leader, as they would for any member that is down.
- Tests: a node test that an emptied replica with itself as the only first voter never campaigns in 200 ticks and keeps its term; broken on purpose (ticking regardless), it fails. The metadata simulation passes seed 1397.

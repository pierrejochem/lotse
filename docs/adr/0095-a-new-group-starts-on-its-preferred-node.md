# 95. A new group starts on its preferred node

- Status: Accepted; extended by [ADR 0097](0097-a-queue-leader-delivers-only-with-a-recent-quorum.md) (the quorum flag also pauses a queue leader's deliveries)
- Date: 2026-11-10

## Context
Every group has a preferred leader, computed the same on every node from the metadata state (ADR 0050, pinned by an operator with ADR 0065). A new group's first election was left to chance: whichever replica's randomized timeout ran out first won, and that node then handed leadership to the preferred one. A node hands over at most one group per 100 ticks, so the move came usually within 400 ms and, with many new groups or on a node short of CPU, seconds later.

Clients met that move as `NOT_LEADER` right after creating a queue or stream. A transactional ack goes only on the connection the message came on (ADR 0075), so after the move the transaction had to be aborted and the message came again; messages in flight, delivery counts, a single active consumer and produce counters went with the old leader. The real-node SDK tests failed on CI for this reason.

A second, related issue: `DescribeMetrics` reported a partition or queue as led by every node whose Raft role was leader. A deposed leader keeps that role until it hears of the new term, and one cut off from the others until check-quorum steps it down, up to two election timeouts. Readers had to take the leader from `Metadata` instead.

Two decisions, each the maintainer's choice of the recommended option.

## Decision
**The preferred node wins a new group's first election.** When a node opens a replica of a group that has never had a term (term 0, empty log), it looks up the group's preferred leader. If that is itself, the replica campaigns at its first tick (`Raft::campaign_soon`); if it is another voter, the replica waits one election timeout more than usual before its first campaign (`Raft::delay_election`). Starting an election early is always safe in Raft; this only decides who tries first. A replica with a term (one that lived here before, or a group moved back) waits as always. The handover of ADR 0050 stays as the fallback, for a preferred node that is down or did not have the group yet when it campaigned, and for leadership lost later.

**`DescribeMetrics` names a leader only with a quorum behind it.** A node reports a partition as led, lists a queue as its own and reports consumer groups as the coordinator's leader only while a quorum of the group's voters (itself included) answered a round it sent less than an election timeout ago (`Raft::leads_with_quorum`). A deposed leader that is still connected hears the new term within a heartbeat; one that is cut off stops claiming the group at once instead of when check-quorum steps it down. The flag is for reports only: reads and writes still go through the leader's term and `read_index`.

## Consequences
In a quiet cluster a new group keeps its first leader; a test creates six streams on three nodes and checks that no stream changes leader in 20 s, and fails without the change. If the preferred node is down when the group is created, the first election comes one election timeout later, and the group is handed over when the node returns.

For a moment after an election, before the new leader's first round is answered, no node reports the group as led in `DescribeMetrics`. The console and the planner read sizes and rates and tolerate a missing leader for one sample.

`NOT_LEADER` and a transaction aborted because the queue's leader moved still happen after a failover or an election lost to a starved node; they no longer follow from creating a queue.

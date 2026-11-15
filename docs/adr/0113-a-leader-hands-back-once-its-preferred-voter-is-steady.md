# 113. A leader hands back once its preferred voter is steady

- Status: Accepted (refines [ADR 0050](0050-rebalancing-groups-onto-new-nodes.md))
- Date: 2026-11-15

## Context

ADR 0050 spreads leaders: each group has a preferred voter, and a leader hands the group over to it as soon as that voter has every entry, at most one group per node every 100 ticks (1 s by default) and the same group again only after 1,000 (10 s). So every failover moves a group twice. The first move, away from a node that fails, cannot be avoided. The second, back to the node once it returns, came a moment after the node had caught up, while clients were still settling from the first. A node short of CPU that loses an election, or keeps losing its heartbeats, got its groups back every 10 seconds and lost them again: a group could change leaders every few seconds for as long as the node stayed slow.

Since ADR 0109 and ADR 0112 a move loses nothing; it costs clients a `NOT_LEADER`, a resubscribe and a short pause. The aim is fewer moves, not safer ones.

Four ways were weighed: hand back only once the preferred voter is steady, and wait longer after repeated elections; also tell subscribers before a hand-back so that they move first; hand back only when the leader counts are uneven; or keep the behaviour and record it as a limit.

## Decision

**A leader hands a group back only once its preferred voter has held everything committed for a while** (maintainer's choice). While it leads a settled group, a node notes since when the preferred voter's matched index has been at least the commit index. It hands over only once that has held for the wait. If the voter falls behind, or the preferred voter changes, the wait starts again; a node that does not lead forgets it.

**The wait grows with recent elections.** Every node notes, for every group it holds and in any role, each new leader of a higher term than the last it saw: an election. Terms without a leader, as split votes leave, do not count, and neither does the first leader a node sees, so a restart and a new group's first election (ADR 0095) are none. Elections older than an hour of settle times (60 times the settle time) drop out. The wait is the settle time doubled for each counted election beyond the first, at most 30 times the settle time. After one failover the wait is the settle time; a group whose preferred node keeps losing it waits 1, 4, 16 and then 30 settle times, so it stops changing hands.

**The settle time is the node's `leader_settle`**, one minute by default (`DEFAULT_LEADER_SETTLE`). It is not in the cluster file; simulations use a shorter one.

**A pin moves at once.** A group pinned with `MoveLeader` (ADR 0065) is handed to its pinned voter as before, without the wait: the operator asked for it.

The pacing of ADR 0050 stays: one hand-over per node per 100 ticks, the same group again after 1,000.

Rejected options:

- **Announcing the hand-back to subscribers.** It saves about one round trip per planned move. But it needs a new push in the protocol and in all four clients, and a failover, the move that cannot be avoided, gains nothing from it.
- **Handing back only when leader counts are uneven.** It needs every node's leader count on every node. After a whole node returns its count is zero and the others' are high, so it still hands most groups back.
- **Recording it as a limit.** The flapping under a slow node is a real cost and is cheap to stop.

## Consequences

- After a failover a group returns to its preferred node about a minute after the node has caught up, not at once. Clients meet the second move later, when the first has long settled.
- A node short of CPU no longer takes groups back every few seconds: each time it loses one again, the next hand-back waits four times as long, up to half an hour.
- Leaders stay uneven for at least the settle time after a node returns, and for as long as half an hour on a group that keeps electing. Writes lean on the nodes that took over for that time.
- New voters placed by a move (ADR 0050) also wait the settle time before they lead.
- A node forgets the elections it saw when it restarts. The node that decides is the one leading, so a restart of the preferred node does not shorten the wait.
- Tests:
  - Unit tests check that the wait starts when the preferred voter holds everything, starts again when it falls behind or another voter is preferred, and that a follower hands nothing over. They also check that terms without a leader, the first leader seen and a new group's first election do not count, that the wait doubles per election up to its cap, and that elections an hour old drop out.
  - The metadata simulation `a_group_goes_back_to_its_preferred_node_only_after_the_settle_time` crashes a new stream's leader, restarts it, and requires the interim leader to keep the group until the settle time has passed, then hand it back.
  - The membership simulation uses a settle time of 1 s and allows up to 30 settle times for leaders to spread.
- Mutation-checked:
  - a leader that hands over without the wait fails the metadata simulation above;
  - a pin that waits like any other group fails `a_pinned_leader_stays_where_it_is_put`;
  - counting terms without a leader, or not doubling the wait, fails a unit test.
- Two queue tests relied on timing that the earlier hand-back happened to give, and were fixed, not the code. One waited exactly the ack timeout for a message it had not acknowledged; it now acknowledges the message first. The other dropped a subscription and filled the queue at once; the old leader could hand the dropped subscription a message before its `Unsubscribe` arrived. That message came back only after the ack timeout, out of its priority's turn. The test now closes the subscription.

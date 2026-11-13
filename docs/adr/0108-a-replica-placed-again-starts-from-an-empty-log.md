# 108. A replica placed again starts from an empty log

- Status: Accepted
- Date: 2026-11-13

## Context

ADR 0067 lets a leader drop a tombstone once every member of the group holds it, and named one edge it left open: a replica that left the group could come back with its old log and hold an older value under a tombstone dropped meanwhile. A new scenario in `tests/compaction.rs` shows that it happens:

1. Node 3 holds `k = old` and goes down.
2. Node 4 joins, and spreading moves the stream from node 3 to node 4. The move settles without node 3.
3. `k` gets a tombstone. The leader drops it, since every member holds it.
4. Node 4 is drained, and the stream goes back to node 3, the least loaded node.
5. Node 3 starts and serves `k = old`.

A node removes a moved replica's log (ADR 0050) only while it is no voter of the group: it sees the settled move off and calls `forget_group_log`. Node 3 was down then. When it starts, the metadata group already places the group on it again, so nothing removes the log. The group's leader probes node 3 and appends from where its log ends. Compaction keeps each batch's offsets and term, so to Raft the old entries match the leader's, and node 3 keeps the value the others compacted away. The same happens to a node that stays up but is cut off from the move off until after the return: its replica is open all along, and its view of the metadata group jumps from before the move to after it.

A check by the node alone has a race. On restart a node opens its replicas from its own metadata state, which may be older than the move off. If the group's leader reaches the replica before the node's metadata view catches up, the replica takes the membership that placed it there again. Its log then looks current, though most of it is from before it left. A third variant of the scenario shows this: node 3 can reach the stream's leader but not the metadata group's leader for five seconds.

## Decision

- **The metadata state records settled moves off a group.** When a `Settled` report names the voters the metadata group currently wants for a group, every node in that group's `left` set and not among those voters is recorded in `moved_off` with the tag of that decision (`target_at`). A report of an older decision records nothing. `moved_off` is a new optional section at the end of the encoded state, so a snapshot without it reads as before.
- **A replica from before a settled move off starts over.** When a node refreshes its replicas and is a voter of a group, it looks at an open replica of it. The replica starts over if:
  - the node is recorded in `moved_off` with tag `off`;
  - the replica's membership tag is at most `off`;
  - its log was not already removed for that move (`moved_at` is absent or older than `off`).

  The node then closes it and calls `forget_group_log(off)`. As with any moved replica, only its term and vote stay, and it grants no vote until it holds a newer membership. The next round opens it empty, and it catches up as a learner. This is safe: the replica was no member when the move off settled, so no commit has rested on its log since. Once it holds the membership that placed it there again, its tag is newer than `off` and it stays.
- **A membership waits for the node's view.** A node holds back a Raft message for a runtime group whose `Append` entries or `Snapshot` carry a membership tagged newer than the decision its replicas were last refreshed with (`target_at` in that view). The leader resends once the message goes unanswered, and by then the refresh has judged the replica by the membership it held before. The metadata and coordinator groups never wait: they hold no stream or queue, and the metadata group is how a node learns of decisions at all.
- **A replica opened from an older view is checked in the next round, not at once.** Until then the hold keeps it from taking a newer membership. A check at opening was tried as well; no mutation of it made a test fail, so it is not there.

## Consequences

- A deleted key no longer comes back when a group returns to a node that missed the move off it. That holds for queues too, whose acks an old log would lack.
- A node coming back with a stale replica copies the group whole, as one that saw the move off already did.
- A node whose metadata view lags joins a group's new membership later, by as long as its view takes to catch up. Data entries are not held, only those carrying a membership.
- Tests: three variants of the scenario in `tests/compaction.rs` (down, cut off, metadata leader out of reach) and unit tests in `membership/tests.rs` and `metadata/tests.rs`. Each check was mutation-checked:
  - without the check of an open replica, all three variants bring the old value back;
  - without the hold, the third variant does;
  - the conditions of `starts_over`, `ahead_of_view` and the `Settled` rule each fail a unit test when changed.
- The scenario's own check first failed on a tombstone node 3 copied from a node that had never led. ADR 0067 allows that: only a leader drops a tombstone. The check was too strict and now asks only that `k` has no value.

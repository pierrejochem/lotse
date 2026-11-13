# 105. A forgotten node stops without seeing its drain

- Status: Accepted
- Date: 2026-11-12

## Context

A drained node stops by itself (ADR 0051): when its own metadata state shows the drain finished, or, if it was cut off then, when the metadata leader no longer lists it. The second way applied only to a node whose own state showed it **draining**, because a joining node is not listed until it has been added and must not stop for that.

The membership simulation found a node that never stopped (seed 3493, after ADR 0102 changed its timing). The drain's entry reached node 3, but not the commit that followed. Then node 3 restarted and was cut off. The cluster finished the drain without it and forgot it, and from then on nobody sent it anything. Its own state never showed it draining, so it never asked whether it was forgotten. A node switched off before `lotse node remove` and started again afterwards is in the same place.

Dropping the "draining" condition alone is not safe:
- The joining node would stop.
- `DescribeNodes` is answered by any node that believes it leads the metadata group. One deposed without knowing it may answer from a list older than the asking node's own, and leave the node out because that list predates its addition.

## Decision

**A node the cluster forgot stops, whether or not it saw its drain** (maintainer's choice). It stops when two things hold:
- Its own metadata state lists it: it is one of the first voters (the nodes the cluster started with, kept on disk in `first-voters`) or was added, and it is not forgotten.
- An answer of the metadata leader **at least as new as that state** leaves it out.

Between the two, the cluster drained and forgot it, and ids are never used twice. A joining node is never listed by its own state until its addition is applied, whatever its configuration file names, so it never counts as forgotten.

**The answer is at least as new because the node asks for that.** `DescribeNodes` gets tag 0 in its request, `applied_at_least` (one `u64`): the metadata index the asking node has applied. The metadata leader answers only if it has applied as much; otherwise it says `NOT_LEADER` without a hint, and the node asks again. The check is exact:
- Entries up to a leader's applied index are committed, so they are the same in every log.
- A leader that has applied at least the asker's index therefore knows everything the asker's own state knows.
- If such a leader leaves the asker out, the asker's id was really forgotten, even when that leader has since been deposed.

The tag is internal. Clients do not send it, and their requests and the answers are unchanged.

A node asks when its metadata group has no leader, or one it does not list, and when it is draining, every 100 ticks (ADR 0050, ADR 0051). A forgotten node hears from nobody, so its metadata group soon has no leader, and it asks.

## Consequences

- A node removed while it was down, or cut off between its drain's entry and the entry's commit, stops once it reaches a node that knows the leader. Before, it ran with stale metadata until someone switched it off.
- A node started before an operator added it still waits, as before.
- A rolling upgrade is safe: an older leader rejects the tagged request as malformed, and the node goes on asking without stopping.
- Tests:
  - Unit tests: a node that never saw its drain stops once forgotten; only a node its own state lists can be forgotten; a first voter and an added node are listed, a joining node its configuration names and a forgotten node are not; the leader refuses an asker that has applied more than it has; the request codec.
  - The membership simulation at seed 3493.
  - Mutation checks: ignoring the asker's index, ignoring the node's own listing, and taking the listing from the configuration file each make a unit test fail; never recording the answer makes the simulation fail.

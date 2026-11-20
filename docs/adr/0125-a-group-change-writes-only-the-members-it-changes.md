# 125. A group change writes only the members it changes

- Status: Accepted
- Date: 2026-11-20
- Changes [ADR 0033](0033-consumer-groups-first-cut.md) in part: what a `GroupState` record holds

## Context

Since ADR 0033, every change to a consumer group was one `GroupState` record holding the whole group: its epoch, and for every member its subscription, epoch, target, assigned and revoking partitions, instance id and pattern. A rebalance moves partitions one member at a time, each step a heartbeat that writes a record. In a group of N members a rebalance therefore wrote about N records of N members each, all through the one coordinator group that also serves every transactional id. The README listed it as a limit.

The coordinator computes each change from the group as it has applied it, and writes nothing else for that group until the record commits (the group is busy meanwhile). It serves only after applying an entry of its own term, so every earlier record is applied by then. The whole-group record already relied on this. Without it, a record computed from an older state would overwrite a newer one.

## Decision

**A `GroupState` record holds only what changed: the group's epoch, the members removed, and each member that is new or changed, in full. Members that did not change are not written** (maintainer's choice: changed members, not changed fields).

- **The record** (version 4): the group id; whether the group existed when the change was computed; the new epoch; the ids of the removed members; then each changed member with all its fields, its instance id and its pattern.
- **The coordinator** writes it against the applied group it computed from: a heartbeat, a member leaving, and members expired. A heartbeat that changes nothing still writes nothing.
- **Applying.** A replica sets the epoch, removes the named members and puts the changed ones in place. A group left without members is removed, as before.
  - A record computed while the group existed, applied after a tenant's `Forget` dropped the group (ADR 0069), changes nothing. Otherwise the group would come back holding only the members the record names.
  - A damaged record is an error, like any other coordinator record.
- **Older records and snapshots.** Records of versions 1 to 3, which hold the whole group, are still read, so a log written before this change replays as before. Snapshots hold the whole state, unchanged.
- **Older nodes** cannot read version 4. The repository defines no rolling upgrade yet; like ADR 0123's metadata change, this one needs every node on the new version.

Rejected options:

- **Changed fields.** Writing each changed member's partitions as added and removed per set saves a little more. It costs a more complex format and apply for a member that is already small next to its group.
- **Whole groups, batched.** Gathering several steps into one record per round keeps the format, but each record is still the whole group, and answers wait for the batch.
- **Splitting the coordinator into several groups.** It is the remedy for a coordinator group that is too busy, not for records that are too large, and it is a much larger change. It remains possible.

## Consequences

- A member's step in a rebalance writes one member rather than the whole group. A rebalance of N members writes about N members in total, not N². In a group of a hundred, one step is about 33 times smaller.
- A join, a leave or an expiry writes the members whose target moved: at most the whole group, as before.
- Correctness rests, as before, on a group's records being computed one at a time from the applied state. A record from any other base would now leave a group that mixes two states, where before it overwrote a newer one.
- Tests:
  - Unit tests take a group through joins, a release, leaves, a changed setting, an unchanged step and its end, and check after each step that a replica applying the records holds what the coordinator computed.
  - They check that a reconcile in a group of a hundred writes one member, that a change to a forgotten group leaves it forgotten, and that a damaged or overlong record is refused.
  - The groups simulation runs on the new records.
- Mutation-checked:
  - writing only the first changed member;
  - ignoring removed members;
  - bringing back a forgotten group;
  - a coordinator computing against no group, so removals are never written.

  The groups simulation catches the second and the last. The unit tests catch the first three. The first does not break ownership, because the coordinator converges at the next heartbeat.

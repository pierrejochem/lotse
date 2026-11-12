# 102. A leader probes after a conflict

- Status: Accepted
- Date: 2026-11-12

## Context

A Raft leader keeps one append in flight per follower. When a follower rejects an append because its log differs at `prev_index`, it names its term at that index and where that term starts. The leader then guesses where the logs agree: after its own last entry of that term, or at the term's start. It used to send the next append from that guess at once, filled with as many entries as fit. When the guess was wrong again, for instance because the follower held a long run of entries from a term the leader never had, every entry of that append was sent for nothing. The next guess went out with entries too. After a failover a lagging former leader could be sent the same terms' entries several times before the logs met.

## Decision

**After a rejection the leader sends appends without entries until one is accepted; only then does it send entries, from where the logs agree.**

- Each follower's progress on the leader has a `probing` flag. A rejected append sets it, together with the next guess as before.
- While it is set, the append carries no entries. Its `commit` is at most `prev_index`, so the follower learns no commit beyond what it is known to match.
- An accepted answer reaching `next - 1` clears the flag, and the leader then fills appends again. Such an answer may come from the probe or from a heartbeat naming an index it already knew the follower has.
- A snapshot transfer is not affected: when the guess falls below the snapshot, the leader sends the snapshot as before.
- A follower that has just been added, or that a new leader starts with, is not probed: its first append carries entries as before. Only a rejection starts probing. A new leader's first guess is its own last index, so a probe there would only cost a round trip.

## Consequences

- After a conflict, every guess costs a round trip without entries rather than a full append. Entries go out once the logs are known to meet there.
- The core stays sans-I/O; drivers do not change.
- A unit test makes a third leader meet a former leader holding forty entries of a term nobody else has. It checks that every append following a rejection carries no entries and that all nodes end with the same log. It fails without the change. The Raft simulation passes at 300 seeds locally.

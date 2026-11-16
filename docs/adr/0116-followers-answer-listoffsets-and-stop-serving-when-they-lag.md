# 116. Followers answer ListOffsets and stop serving when they lag

- Status: Accepted (refines [ADR 0104](0104-followers-serve-fetches-in-their-zone.md))
- Date: 2026-11-16

## Context

Since ADR 0104 a follower serves a `Fetch` that allows it, and a client with a zone setting reads from a voter in its zone. Two gaps were left.

- **`ListOffsets` still went to the leader.** A reader in a zone far from the leader fetched locally but crossed zones to find where to start or where a partition ends.
- **A follower served however far behind it was.** A follower cut off from its leader, or one with a slow disk, answered from a log that no longer grew. A reader in its zone saw no new records, and nothing sent it elsewhere until its connection failed or it asked for metadata.

Kafka serves fetches from followers since KIP-392, and has the leader stop choosing a replica that falls out of the in-sync set: one that has not caught up with the leader's log end within `replica.lag.time.max.ms` (30 s by default). `ListOffsets`, committed offsets and the coordinator stay with their leaders.

## Decision

**Followers answer `ListOffsets` too, and a follower that lags too far answers neither** (maintainer's choices: `ListOffsets` and the lag redirect; the lag measured in time, like Kafka).

- **`ListOffsets`** gains request tag 0 `allow_follower`, as on `Fetch`. A follower that may serve answers with its own bounds: its log start, its commit index or, for read_committed, its last stable offset. It finds a time in its own log. Its end may lag the leader's.
- **When a follower lags.** A follower serves a `Fetch` or `ListOffsets` that allows it only while both hold:
  - it hears its leader: it follows a known leader and has heard from it within an election timeout;
  - within the last `follower_max_lag`, it had applied everything the leader last said was committed.

  Otherwise it answers `NOT_LEADER` with the leader's address.
  - The Raft core now keeps the highest commit index a leader has sent (`leader_commit`), which a follower's own commit index lags while its log does. It also says whether a follower hears its leader (`hears_leader`).
  - Each round, a node notes, for every group it follows, when it last had applied up to `leader_commit`.
- **`follower_max_lag`** is a node setting, 10 s by default, set with `cluster.follower_max_lag_ms` in the cluster file.
- **Clients** send `ListOffsets` where they send `Fetch`: to the voter in their zone, with `allow_follower`. As for `Fetch`, a replica that answers with an error, a lagging follower's `NOT_LEADER` included, is left for the leader until the next metadata refresh. The Rust client and the Java (and so Kotlin), C# and Node.js SDKs all do this.
- **Committed offsets and admin reads stay with their leaders.** A consumer that reads a stale committed offset processes records again, and an admin expects to read what it just wrote.

Rejected options:

- **Admin reads from any node**, from its applied metadata. They need a "read at least index N" rule in the protocol and every client, or an admin may create something and not see it.
- **Committed offsets from followers through ReadIndex.** Safe, but each read still costs a round trip to the leader, so it saves little.
- **Lag as an entry count.** Entries vary in size, and a busy partition that is a moment behind would trip it.
- **The leader deciding, as with Kafka's ISR.** Metadata would need a new field in all four clients, and it goes stale between refreshes.

## Consequences

- A reader in a zone looks up offsets in its zone as well as fetching there.
- A reader whose follower is cut off from its leader goes to the leader within about an election timeout. One whose follower stays connected but falls behind goes within `follower_max_lag`. It sees new records again, and it returns to its zone after its next metadata refresh.
- A follower that keeps up under steady load applies up to each commit index it receives as it goes, so it keeps serving.
- The protocol gains one tag, so the golden vectors gain a `ListOffsets` sample. An older node ignores the tag and answers `NOT_LEADER` as before, and the client falls back to the leader.
- Tests:
  - A Raft unit test checks that a follower knows its leader's commit index and stops hearing its leader when cut off. A node unit test checks the lag rule at its limit.
  - The client simulation `a_reader_leaves_a_follower_that_is_cut_off` puts a reader in a follower's zone and cuts that follower off from the other nodes but not from the reader. It requires the reader to fetch, and to list the end of the partition, past what was written after the cut within 3 s.
  - Each SDK has a unit test for `ListOffsets` at the zone's replica and the fallback.
- Mutation-checked:
  - a follower that serves `ListOffsets`, or `Fetch`, despite lagging fails the simulation;
  - in the Node.js SDK, not sending `ListOffsets` to the zone, or not leaving a failing replica, fails a unit test.

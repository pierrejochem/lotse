# 141. Reads in the client's zone, with read-your-writes

- Status: Accepted (refines [ADR 0104](0104-followers-serve-fetches-in-their-zone.md) and [ADR 0116](0116-followers-answer-listoffsets-and-stop-serving-when-they-lag.md))
- Date: 2026-11-24

## Context

Since ADR 0104 and ADR 0116, a client with a `zone` setting fetches and lists offsets at a replica in its zone, and a follower that lags sends it to the leader. Two things still crossed zones, and `docs/status.md` listed both as a gap.

- **A reader sent to the leader might never come back.** It returned to its zone only at its next metadata refresh, and a reader that only fetches asks for metadata only after an error.
- **Committed offsets and admin reads went to their leaders.** That is `OffsetFetch` and the admin reads `DescribeConfigs`, `DescribeNodes`, `DescribeUsers`, `DescribeTokens`, `DescribeCertificates`, `DescribeCertificateRequests` and `DescribeUpgrade`. `Metadata`, `DescribeTenants` and `DescribeRoleBindings` were answered by any node from whatever it had applied, with no promise about how fresh it was.

ADR 0116 rejected two options for these reads. Admin reads from any node needed a "read at least index N" rule in the protocol and every client, or an admin might create something and not see it. Committed offsets through ReadIndex at a follower were safe but still cost a round trip to the leader. It also rejected the leader choosing the replica, as Kafka's ISR does, because `Metadata` would need a new field and goes stale between refreshes.

The one result that must never happen: a consumer that takes over a partition reads an older committed offset than its predecessor committed, and processes records again.

## Decision

**The leader points a reader back to its zone, and offsets and admin reads are served in the zone with read-your-writes** (maintainer's choices). Nothing is stored, so there is no cluster-version gate: these are protocol tags, not formats (ADR 0127).

### 1. The leader points back

- `Fetch` gains request tag 1 `zone` (string). A client sets it when it has a zone and sends the fetch to the leader, never to a follower.
- A leader that answers a partition of such a fetch adds partition tag 1 `preferred_replica` (node id): a voter of the partition, not itself, in that zone and current. Current is ADR 0116's rule seen from the leader:
  - the leader heard from the voter within an election timeout;
  - the voter caught up within `follower_max_lag`, measured in time. The leader keeps, per group it leads, samples of (time, commit index) over the last `follower_max_lag`, one at most every `follower_max_lag / 100` (100 ms by default). A voter counts as caught up at the latest sample whose commit index its match index has reached, or now if it has reached the current commit index. Comparing with the commit index of the moment was too strict: under steady writes the slower follower trails it at every round and was never named.
- This is not the "leader deciding" that ADR 0116 rejected: there the choice would have travelled in `Metadata`, which needs a new field and goes stale between refreshes. Here it travels in each fetch answer, as fresh as the leader's view, and `Metadata` stays as it is.
- A client honours `preferred_replica` only on a partition answered without error, and only for a node whose address it knows. It then clears its fallback for that partition and sends the next fetch there with `allow_follower`. `ListOffsets` goes wherever the partition's fetches go. If that replica fails, the client falls back as before, and the leader names it again once it is current.

### 2. Read-your-writes in the zone

- **Frame-header tags.** Response header tag 0 `applied` and request header tag 0 `read_at_least` are each a `u32` count, then per entry a group `u64` and an index `u64`.
  - Every answer given from a node's replica of the metadata group or of the coordinator group names that group and what the replica has applied, reads and refusals included, whether the node leads the group or follows it. A leader's answer to a write names at least the index at which the write applied. A coordinator answer names the coordinator group's applied index, so a heartbeat that hands a partition over names an index at or above the previous owner's commit, which came before its revoke.
  - `Metadata` carries `applied` but is never refused: its answer has no top-level error field, and all four clients would need one. A client already retries until a resource it created shows up.
- **Clients** keep one index per group, raised by every `applied` and never lowered, per client instance. They always name groups 1 and 2 in `read_at_least`, at 0 for a group they have seen nothing of: these are the fixed ids of the metadata and coordinator groups in every cluster.
- **Zone reads** are the only requests that carry `read_at_least`: `OffsetFetch`, `DescribeConfigs`, `DescribeNodes`, `DescribeUsers`, `DescribeTokens`, `DescribeCertificates`, `DescribeCertificateRequests`, `DescribeUpgrade`, `DescribeTenants` and `DescribeRoleBindings`. With a zone, a client sends an admin read to the lowest-id node in its zone among the brokers the latest `Metadata` listed, and `OffsetFetch` to the lowest-id coordinator voter in its zone among those brokers. `FindCoordinator` gains response tag 0 `voters` for that. Without a zone, or with no such node, the read goes to the leader as before.
- **A node serves a tagged read** of a group only from a replica that has applied at least the named index. A follower must also not be lagging by ADR 0116's rule (it hears its leader and caught up within `follower_max_lag`); otherwise it answers `NOT_LEADER` with the leader hint. A leader must also have applied an entry of its own term, as without the tag. A leader that has applied less than the named index refuses too, as ADR 0105's `applied_at_least` does: it answers `NOT_LEADER` with no hint, which would name itself. A leader applies what is committed before it tells anyone, and a newer leader's entries sit above everything committed before, so such a leader has been deposed without knowing it yet. Check-quorum can leave it in that state for up to about two election timeouts, and a paused process for longer. A refusal comes before the access check, so a follower whose role bindings are older than what the client saw does not answer `UNAUTHORIZED` to a read the leader allows; it reveals nothing that `Metadata` does not. `OffsetFetch` on a coordinator follower also needs the metadata index when the request names group 1, because its access check reads the metadata group. A request without the tag behaves exactly as before.
- **A refused zone read**, an error answer or a failed call sends that one read to the leader, with the same `read_at_least`. A current leader serves it as before. A deposed one refuses it, and the client's retry finds the new leader. The next read tries the zone again: reads are rare enough that a sticky fallback is not worth its state.
- Every read keeps its access check, from the serving node's applied role bindings, which the index covers.

### 3. MCP

No new tools: the tools are client calls and get zone reads through the client. `lotse mcp` takes `--zone`, or `LOTSE_ZONE`, so an agent next to a follower reads there.

### Rejected options

- **A client-side retry with backoff, or periodic metadata refreshes, to return to the zone.** The leader knows each voter's progress; a client can only guess.
- **ReadIndex from a follower.** It still pays the round trip to the leader that this saves.
- **A tag on each of about twenty messages** instead of one pair of frame-header tags. The header tags keep the change the same size however many requests take part.

## Consequences

- A reader on the leader returns to its zone's replica as soon as that replica is current, without asking for metadata.
- A client with a zone reads committed offsets and admin state in its zone and always sees its own writes, at a follower and at a leader deposed without knowing it alike. A member that takes over a partition sees its predecessor's commit.
- A client without a zone, and any read without the tag, can still get a stale answer from a deposed leader for up to about two election timeouts, as before this change. ReadIndex or leases for these reads would close it; `docs/status.md` lists it.
- No read-your-writes across unrelated clients: admin B, who never saw the index of admin A's write, may read state up to `follower_max_lag` old, as a fetch from a follower may.
- An `OffsetFetch` served by a coordinator follower after a revoke it has not yet applied is answered within `follower_max_lag`, as a `Fetch` from a follower has been since ADR 0104.
- `FindCoordinator` from a node without a coordinator replica names the planned voters during a move, which may differ from the actual ones. A read sent to such a node is refused and goes to the leader.
- Clients hard-code groups 1 and 2. The simulation found that a world with other group ids hides a missing heartbeat index (followers then refuse every offset read as untagged), so the zone runs use the production ids.
- An older node ignores the tags: on leader-only reads it still refuses from a follower, so the client goes to the leader; on `Metadata`, `DescribeTenants` and `DescribeRoleBindings` it answers as before. An older client sends no tags and sees no change. A client without a zone sends byte-identical frames.
- The golden vectors gain samples for the header tags, the `Fetch` tags and `FindCoordinator`'s `voters`.
- Tests:
  - Server unit tests: a leader names a current voter in the zone and no lagging one, itself or one outside the zone; a voter a moment behind the commit index stays current; a follower serves a read at or below its applied index and refuses one above it or while lagging; it refuses before it judges access; `OffsetFetch` waits for the metadata index; a leader deposed without knowing it refuses, with no hint, the nine metadata reads and `OffsetFetch` above what it applied, while the new leader serves them; answers carry `applied`; `FindCoordinator` names the voters.
  - The Rust client and the Java (and so Kotlin), C# and Node.js SDKs have the same scripted-node tests: indexes per group, zone routing of reads and the fallback with the same `read_at_least`, departed nodes skipped, `preferred_replica` ignored on an error or without an address, and the return through it.
  - The client simulation, in the half of its runs with two zones, adds an admin whose `DescribeConfigs` and `DescribeNodes` must show every change it was told succeeded, and three group members of whom one taking a partition over must never start below an acknowledged commit. `a_reader_returns_to_its_zone_after_the_follower_heals` expects the reader back on the follower within 3 s of the heal with no `Metadata` request. `a_member_taking_over_reads_its_predecessors_commit_past_a_follower_behind` is the deterministic case. `LOTSE_SIM_SEEDS=5000` passed (2,424 s).
- Mutation-checked:
  - dropping the applied-index check on a follower fails the unit tests and the simulation's admin oracle;
  - dropping the lag check fails the unit tests;
  - a leader that names a lagging voter fails the unit tests, and a leader that never names one fails the return scenario;
  - a heartbeat answer without `applied` fails a unit test and the deterministic takeover test; the random simulation does not catch it, since a follower almost always applies the commit before the takeover completes;
  - dropping the leader's applied-index check fails both deposed-leader unit tests; the random simulation rarely opens that window, so the tests are deterministic;
  - in each client, a fallback read sent without `read_at_least` fails its admin-read or `OffsetFetch` fallback test;
  - in each SDK, a `preferred_replica` that does not clear the fallback, dropping the error or address guard, candidates not limited to listed brokers, and an index that can fall each fail a unit test.
- The C# public records `Frames.DecodedRequest`, `Frames.DecodedResponse` and `Response.FindCoordinator` each gain a member, so their generated `Deconstruct` takes one more value and a caller's `var (h, r) = decoded` no longer compiles. This is a source-breaking change; the next release's notes in `docs/releases/` must name it.
- The Java, Kotlin and C# suites were run here on stand-ins (javac with JUnit console, the embedded Kotlin compiler, a reflection runner for the xUnit tests), since Gradle, Maven Central and NuGet were unreachable; CI's `make sdk-test` confirms them with the pinned tools.

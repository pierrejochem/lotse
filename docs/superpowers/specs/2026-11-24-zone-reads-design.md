# Reads in the client's zone: back from the leader, offsets and admin reads

Status: draft for the maintainer's approval. The ADR (0141) is written from it once it is approved.

## Intent

A client with a `zone` setting reads streams from a replica in its zone (ADR 0104, ADR 0116). Two things still cross zones, and the gap in `docs/status.md` names both.

- **It may never come back.** A reader that falls back to the leader stays there until it next asks for metadata. A reader that only fetches asks for metadata only after an error, so in practice it may never return.
- **Committed offsets and admin reads go to their leader.** That means `OffsetFetch` and the admin reads: `DescribeConfigs`, `DescribeNodes`, `DescribeUsers`, `DescribeTokens`, `DescribeCertificates`, `DescribeCertificateRequests` and `DescribeUpgrade`. `Metadata`, `DescribeTenants` and `DescribeRoleBindings` are answered by any node today, from whatever it has applied, with no promise about how fresh it is.

The maintainer's choices:

1. **The leader points the reader back to its zone.**
2. **Offsets and admin reads are served in the zone with read-your-writes.** Any replica answers from its applied state once it has applied what the client last saw.

## Goals

- A reader on the leader returns to its zone's replica as soon as that replica has caught up again, without asking for metadata.
- A client reads committed offsets and admin state from a node in its zone, and always sees its own writes. A group member that takes over a partition sees the offset its previous owner committed.
- Nothing changes for a client without a zone, for an older client, or against an older node.

Non-goals:

- **Read-your-writes across unrelated clients.** Admin A writes and admin B, who never talked to that group's leader, reads in another zone. B sees state at most `follower_max_lag` old, as for a fetch today. The one cross-client case that matters is a partition changing owner, and the coordinator covers it.
- **Writes in the zone.** Produce, commits and admin changes still go to the leader.
- **Linearisable reads in the zone.** ReadIndex from a follower is not built.

Anti-goal: **a stale committed offset reaching a consumer that should have seen a newer one.** That reprocesses records and breaks the exactly-once story for plain commits. Every rule below exists to rule it out.

## Approaches considered

**Return to the zone.**
- (a) A client-side retry with backoff.
- (b) The leader points back (chosen).
- (c) Periodic metadata refreshes.

(b) is exact: the leader knows each voter's progress. It costs tags and logic in all four clients.

**Offsets and admin reads.**
- (a) Keep them with the leaders (ADR 0116's choice).
- (b) ReadIndex from a follower.
- (c) Read-your-writes from applied state (chosen).

(c) saves the cross-zone round trip that (b) still pays, at the price of index tracking in the clients.

**Carrying the index** (my call). One pair of frame-header tags, rather than a tag on each of about twenty messages. Frames already have header tag fields, and all four codecs read them but none uses them yet. This keeps the change the same size however many requests take part.

## Design

### 1. The leader points back (`Fetch`)

- `Fetch` gains request tag 1 `zone` (string). The client sets it whenever it has a zone setting and sends the fetch to the leader. It does not set it when sending to a follower.
- When a leader answers a partition for a request that names a zone, it adds partition tag `preferred_replica` (node id). It names a voter of the partition, other than itself, that is in that zone and is current. Current means:
  - the leader heard from it within an election timeout;
  - its match index reached the leader's commit index within `follower_max_lag`.

  This is the follower's own ADR 0116 rule, seen from the leader. The leader keeps, per follower, when its match index last reached the commit index.
- A client that gets `preferred_replica` for a partition it reads from the leader:
  - clears its fallback for that partition;
  - takes the named node as that partition's zone replica;
  - sends the next fetch there with `allow_follower`.

  If that replica fails, the client falls back as today, and the leader names it again only once it is current.
- `ListOffsets` follows `Fetch`. It goes wherever the partition's fetches currently go, so it needs no tag of its own.

### 2. Read-your-writes in the zone (offsets and admin reads)

**Frame-header tags.**
- Response header tag 0 `applied`: a list of (group id, index). The leader of the metadata group or of the coordinator group adds its group and its applied index to every answer it gives as that group's leader, reads included.
  - For a write, the index is the one at which the write applied, since a write is answered after it applies.
  - For a coordinator answer, it is the coordinator group's applied index. A heartbeat that hands over a partition is answered after the previous owner's revoke, and the previous owner commits before it revokes, so that commit is at or below the index.
- Request header tag 0 `read_at_least`: the same shape. The client sends it on the reads above, carrying for each group the highest index it has seen in an `applied` tag. A client that has seen none sends 0.

**Clients keep one index per group:** the metadata group (1) and the coordinator group (2), raised by every `applied` tag and never lowered. All `LotseClient`s of one process are separate: no index is shared between them.

**Where a read goes.** With a zone setting, a read goes to a node in the client's zone that is a replica of the read's group, with `read_at_least` set. Without a zone, or when no such node exists, it goes to the leader as today.
- For the metadata group, every node is a voter or a learner.
- For the coordinator group, `FindCoordinator` gains response tag 0 `voters`.

**What a node does with a read that carries `read_at_least`** for a group it is a replica of. It answers from its applied state only if:
- it is the leader; or
- it has applied at least the named index, and the group is not lagging by ADR 0116's rule (it hears its leader and caught up within `follower_max_lag`).

Otherwise it answers `NOT_LEADER` with the leader hint. A request without the tag behaves exactly as today.

**When a zone node refuses**, the client sends that one read to the leader. The next read tries the zone again, since reads are rare enough that a sticky fallback is not worth its state.

**Every read keeps its access check.** A node answers with the same filtering as the leader, from its applied role bindings, which the index also covers.

### 3. Older nodes and clients

- An older node ignores both header tags. On the leader-only admin reads it still answers `NOT_LEADER` from a follower, so the client goes to the leader.
- On `Metadata`, `DescribeTenants` and `DescribeRoleBindings` an older node answers from its applied state, which is no worse than today.
- An older client sends no tags and sees no change.
- Nothing is stored, so the cluster version is not involved: these are protocol tags, not formats (ADR 0127).

### 4. MCP

No new tools. The tools are client calls and get zone reads through the client. `lotse mcp` gains a `--zone` flag so an agent next to a follower reads locally.

## Tests

**Server unit tests.**
- A leader names a current voter in the zone and no lagging one, itself, or one outside the zone.
- A follower serves a read at or below its applied index and refuses one above it, and refuses while lagging.
- A request without the tag behaves as before.

**Client simulation.**
- Readers and an admin in a zone. The oracles:
  - a read never returns state older than an index the client already saw;
  - a group member that takes over a partition never starts below the committed offset of the previous owner.
- A reader cut off from its follower comes back to the zone after it heals: back within 3 s without a metadata request.

**Mutation checks.** Each of these makes a test fail:
- dropping the index check on a follower;
- dropping the lag check;
- a leader that names a lagging voter;
- a coordinator answer without `applied`.

**SDKs.** Each of the four gets unit tests with scripted nodes for the tags, the index kept per group, the zone routing of reads and the fallback, and the return through `preferred_replica`.

**Golden vectors.** New samples for the header tags, the `Fetch` tags and `FindCoordinator`'s `voters`.

## Left to the builder

- Where in the node the per-follower "caught up at" time is kept: next to the core's progress, read through a getter.
- The exact tag numbers, after checking which are free per message.
- How each SDK threads the header tags through its codec.

# 104. Followers serve fetches in their zone

- Status: Accepted
- Date: 2026-11-12

## Context

Only a partition's leader answered `Fetch`; a follower said `NOT_LEADER`. In a cluster spread over zones, a consumer in one zone therefore read across zones whenever the leader was elsewhere. Cross-zone traffic costs money and adds latency, although a replica of the partition sat next to the consumer. The README listed followers serving reads as not built. Kafka solves the same problem with fetching from the closest replica: brokers name their rack, consumers name theirs, and a follower serves what it has.

## Decision

**A follower serves the committed data it has applied to a `Fetch` that allows it, and a client that names its zone fetches from a replica in that zone.**

- **What a follower promises.** It serves batches below its own commit index and, with `isolation = 1`, below its last stable offset from what it has applied. That is the leader's rule, applied to the follower's knowledge, which may lag. A record a follower serves is committed, so no term change can take it back. The term a client names (`leader_term`) is therefore not checked on a follower. An offset past the follower's log gets no records and no error: the follower may only lag, and the client waits as it does at the end of a partition. An offset below the follower's log start is `OFFSET_OUT_OF_RANGE`, as on the leader. A follower may answer with a lower high watermark than the leader; a reader moves on by what it receives.
- **Opt-in.** `Fetch` has a request tag 0, `allow_follower` (one `u8`, 1 = set). Without it a follower answers `NOT_LEADER` as before. A client that has stale metadata about the leader still learns where the leader is, and older clients see no change.
- **Zones.** A node may name its zone:
  - in the cluster file (`zone` in `[[node]]`);
  - for a node added at runtime, through `AddNode`'s tag 0 (`lotse node add --zone`), stored with the node in the metadata group. The record and the snapshot write the zone only when there is one, so a cluster without zones writes what older nodes read.
  - `Metadata` reports a node's zone as the broker's `rack`, a field that was always empty until now.
- **Clients.** The Rust client, the Java SDK (and so the Kotlin layer), the C# SDK and the Node.js SDK take a `zone` setting. With one set, a fetch goes to the first voter of the partition in that zone, with `allow_follower` set. It goes to the leader when the leader is in that zone, when no voter is, or without a zone setting. If that replica fails (an error answer, or the connection), the client reads the partition from the leader until it next asks for metadata. Such a replica may be an older node answering `NOT_LEADER`, a node that left, or one whose log starts later. Only `Fetch` changes; produce, `ListOffsets`, offsets and everything else still go to the leader.

## Consequences

- A consumer next to a follower reads without crossing zones. It may see records a little later than on the leader, and never one that is not committed. Read-your-writes across a producer and a consumer holds only through the leader.
- A follower that lags a lot makes its zone's consumers lag with it. A follower does not know how far behind it is, and nothing yet sends a consumer back to the leader for that.
- `Metadata` now carries real `rack` values. Clients that ignored the field still do.
- Tests:
  - **Server unit test:** a follower holding entries past its commit index serves none of them, ignores the client's term, answers past its log with nothing, and says `NOT_LEADER` without the tag. Four mutations each made it fail.
  - **Client simulation:** in half the runs nodes sit in two zones and consumers name a zone. Its oracles check that the stream is read without gaps and never past the high watermark. Serving past the commit index fails it, but a follower that also reports a high watermark past its commit index passes, which is why the unit test exists.
  - **Clients:** each of the four has a test with scripted nodes for choosing the replica, the flag, and going back to the leader. The Rust, Java and Node.js tests were mutation-checked; the C# one runs only in CI.

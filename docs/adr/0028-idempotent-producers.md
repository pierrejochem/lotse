# 28. Idempotent producers

- Status: Accepted
- Date: 2026-10-28

## Context

Without idempotence a client cannot retry a produce whose outcome is unknown (a lost connection, `REQUEST_TIMED_OUT`) without risking a duplicate, so ADR 0026 left that outcome to the caller. The design document fixes the producer state (epoch and the last 5 batches per producer id) and the error codes, but not where producer ids come from before the transaction coordinator exists, how the check sees batches that are not committed yet, when a partition may forget a producer, or whether the client is idempotent by default.

## Decision

**Producer ids.** A coordinator group, configured per node until the metadata group exists, hands them out: `InitProducer` proposes an allocation entry, and once it is committed under the same rule as a produce (ADR 0026) the id is that entry's Raft index. Indices are never reused, so ids are unique without further state. `FindCoordinator` (key type `PRODUCER`) names the group's leader. Idempotent producers get epoch 0; epochs come with transactional ids. New error code 18 `COORDINATOR_NOT_AVAILABLE` (retriable).

**The check sees the whole log.** A leader checks an idempotent batch against the committed producer state and the tail of its log (appended, not applied). A retry of a batch still in flight is answered once the original's commit decides, also after a leader change and after a restart (the tail is rebuilt from the log and cut when Raft truncates it). A producer the replica does not know is accepted with any sequence.

**Forgetting producers.** Each replica keeps at most `max_producers` producers (10,000 by default; the same value on every node). Applying a batch of one more evicts the producer whose last write is oldest in the log. The rule depends only on the log, so every replica evicts the same producers. The cost, accepted: a burst of new producers can evict a slow one, whose retry is then accepted as new and may duplicate.

**Snapshots, not `.pstate` files.** The committed producer state is part of the replica's Raft snapshot, next to the state machine's data, and travels with it to followers. This replaces the per-segment `.pstate` files of the design; recovery still replays only the log after the snapshot.

**Client.** `lotse-client` is idempotent by default. It numbers batches per partition and sends one at a time per partition, so sequences arrive in order. It retries an unknown outcome with the same sequence; if retrying runs out of time with the outcome still unknown, it returns `Unknown` and starts over with a new producer id, so later batches do not wait behind a gap that may never fill.

## Consequences

- A client session retries without duplicates, as long as its producer is not evicted and the retry is within the last 5 batches.
- A restarted client has a new producer id: deduplication across client restarts needs transactional ids, which come with transactions.
- Idempotent produce needs a coordinator group; without one, `InitProducer` fails with `COORDINATOR_NOT_AVAILABLE` and the client can turn idempotence off.
- Producer ids are unique only as long as the coordinator group's log is never recreated from scratch.
- The client simulation runs one idempotent and one plain producer; at 2,000 seeds it takes about 4 minutes instead of about 3 (ADR 0027).

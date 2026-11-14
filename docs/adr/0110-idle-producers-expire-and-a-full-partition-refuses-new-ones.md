# 110. Idle producers expire, and a full partition refuses new ones

- Status: Accepted (supersedes how [ADR 0028](0028-idempotent-producers.md) forgets producers)
- Date: 2026-11-14

## Context

A partition remembers what makes produce idempotent per producer id: the epoch and the last five batches (ADR 0028). It kept at most `max_producers` producers (10,000). Applying a batch of one more evicted the producer whose last write was oldest in the log, so that every replica evicted the same producers.

That rule forgets producers that are still alive. A client without a producer name gets a new producer id on every start (ADR 0071), so short-lived producers (scripts, jobs, serverless functions, restarts) fill the table. A slow producer then becomes the oldest write and is evicted between a batch and its retry. The retry is accepted as new and written twice, which is the duplicate idempotence exists to prevent, and nothing tells the producer.

Two things were open: what makes a producer go, and which clock measures idleness. The clock matters because every replica must forget the same producers, and stream batches carry the producer's own timestamps.

## Decision

**A producer is forgotten only when it has been idle.** A producer that has written nothing to a partition for `producer_expiration` (a day by default, as in Kafka) is forgotten. Nothing else forgets a producer: applying a batch never drops one to make room.

**`max_producers` limits admission.** While a partition knows `max_producers` producers, committed or in the tail of its log, the leader refuses a batch from an unknown producer with a new error code, 33 `PRODUCER_LIMIT` (retriable). Nothing was written, so the clients retry it like `NOT_LEADER`, with the same sequence, until their retry time runs out. A batch from a known producer, and a batch without a producer id, is never refused for this. A transaction marker is not refused either: its transaction began before. The limit keeps memory and snapshot size bounded. A burst of new producers waits instead of pushing out a live one.

**The leader times idleness and writes the decision.** The leader keeps an idle clock in memory: per producer, the index of its last write and the instant (from the `Clock` seam) it first saw that index. Once a second, every replica it leads looks for producers whose last write has not changed for `producer_expiration`. It skips producers with a transaction open in the partition, and proposes an `ExpireProducers` control batch (internal control type 10) listing up to 1,000 `(producer id, last write)` pairs. Every replica applies the batch and forgets each listed producer whose last write is still the one listed. A producer that wrote between the leader's decision and the batch's commit stays, because its new batch is before the expiry in the log and changed its last write. A damaged list forgets nothing on every replica. A listed producer starts its idle time again, so it is not listed twice while its batch is on its way.

**A new leader counts from when it took over.** The idle clock starts empty in every term. A failover can delay forgetting by up to the expiration, but never make it earlier.

**Configuration.** `[producers] max` and `expiration_ms` in the cluster file, the same on every node; `NodeConfig::max_producers` and `producer_expiration`. The snapshot format is unchanged: the producer state already holds each producer's last write.

Rejected options:

- **Idle time without a cap.** No producer would ever be refused, but a churn of short-lived producers would grow the table and its snapshot without bound for a whole expiration period.
- **A cap that refuses, without time.** Dead producers would fill the table for good, and new producers would stay refused.
- **Producers' timestamps as the clock.** No new batch kind would be needed, but one producer whose clock runs ahead would make a live producer with a slower clock look idle.
- **Stamping the leader's time on stream batches.** Deterministic, but streams would lose the producer's create time, which retention and `ListOffsets` by time rely on.

## Consequences

- A live producer keeps its place, so its retries are always recognised within its last five batches. A producer idle longer than the expiration loses its deduplication and its epoch fencing, as in Kafka.
- Under a burst of new producers, new producers wait (`PRODUCER_LIMIT`), and give up with "not written" once their retry time runs out. Before, an older producer silently lost its deduplication.
- Each leader looks through its partitions' producers once a second and writes a small batch when some have expired.
- `ExpireProducers` batches are in the log, so readers of a stream see one more kind of control batch. The Rust client and the SDKs skip it like every other control batch, and the SDKs name it (`EXPIRE_PRODUCERS`). A node of an older version cannot apply the batch: every node of a cluster must run this version before a leader writes one.
- The error-code vectors gain code 33, and every SDK retries it on produce.
- Mutation-checked:
  - forgetting a producer that wrote after it was listed fails a unit test;
  - ignoring open transactions, keeping the idle clock across terms, listing a producer twice while its batch is on its way, and never refusing at the limit each fail a unit test;
  - a node that never sweeps and a client that does not retry `PRODUCER_LIMIT` each fail the client simulation's scenario `a_new_producer_waits_for_an_idle_one_to_be_forgotten`;
  - a Node.js SDK that does not retry it fails its produce test. The Java and C# SDKs have the same test.
- In half its runs, the client simulation admits four producers per partition, expires them after 3 s, and runs short-lived producers next to the live ones under faults. Its checks stay the same: no duplicates, no lost acknowledged batch, and nothing written that was reported as not written.

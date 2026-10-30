# 46. Retention: trimming the log at snapshot points

- Status: Accepted
- Date: 2026-10-30

## Context

Every partition log, which is also its Raft log, grew for ever: streams kept every record, queues every settled message, and the metadata and coordinator groups every change they ever applied. A node's disk filled at the rate of its traffic, and a replica that restarted replayed everything. The design always meant segments to go whole ([ADR 0001](0001-one-log-engine-for-queues-and-streams.md)): for streams by retention, for queues once the ack floor passes them. What was missing is where a log may start without losing state, and who decides what a stream keeps.

## Decision

**A log is trimmed only at a snapshot point** (maintainer's choice). Raft index *i* is log offset *i − 1*, so a snapshot at index *S* means the log starts at offset *S*. When applying reaches the base offset of a segment, a replica records a snapshot of its state there (the boundary), in memory. Retention picks a point, and the replica cuts at the newest boundary at or below it, in this order: write the snapshot file, compact Raft to it, take it as the replica's snapshot, delete the whole segments below it. A crash between the steps finds a log that begins before the snapshot and trims it again on open. Installing a snapshot from the leader forgets the boundaries. Every node runs retention for each of its groups once a second, with its own clock.

**What each group may delete:**

- A stream: by age, the old segments whose newest record is older than `retention.ms`, by the record's timestamp; by size, the oldest segments as long as what stays still holds `retention.bytes` (the limit is a floor, as in Kafka). Never the active segment.
- A queue: below its ack floor, always; there is no setting.
- The metadata and coordinator groups: everything their state holds, once a whole segment can go; they snapshot at the point instead of using a boundary.
- In every case nothing at or above the last stable offset (an open transaction's records stay until it ends) and nothing not yet applied.

**Settings: defaults in the node configuration, per stream at runtime** (maintainer's choice). `NodeConfig.retention` holds `ms` (7 days by default) and `bytes` (no limit by default) for every stream. A stream created at runtime may set `retention.ms` and `retention.bytes` in `CreateStream`, change or reset them with `AlterConfigs`, and read them with `DescribeConfigs` (unset ones come back with the node's value and source default; −1 means no limit). An `AlterStream` metadata change names only the keys it sets or resets, so two changes of different keys both hold whichever applies last. Streams from the configuration keep the node's defaults. A node applies a runtime stream's own settings to its partitions before each retention pass.

**Readers learn where a partition starts.** `Fetch` reports the log start offset, and an offset below it is `OFFSET_OUT_OF_RANGE`. The Rust client turns that into `ClientError::OffsetOutOfRange` with the start; a consumer group member whose position was deleted goes on from the oldest record left, like Kafka's `auto.offset.reset = earliest`. The console shows a stream's retention.

## Consequences

- A disk holds what retention allows plus up to a segment per partition, and a restart replays only from the snapshot.
- A follower that falls behind the leader's start gets the snapshot and then the rest; nothing still kept is missed.
- Retention follows segments, so a partition keeps up to one segment more than its limits say, and a record lives until the newest record of its segment is old enough. Small segments delete more evenly and cost more files.
- Age uses the timestamps producers write. A producer with a clock far ahead keeps its segment; one far behind gets it deleted early.
- An open transaction holds back the whole partition's retention, up to its timeout.
- Boundaries hold one state snapshot per segment in memory until they are used; for a stream or queue that state is small (producers, transactions, the ack index), but it is per segment.
- Aborted transaction ranges and settled queue offsets below a log's start are still kept in the state; pruning them is left for later.
- Tests: unit tests for the log's trimming and for each kind of group (newest bytes kept and the log starting at a snapshot, records past their age, an open transaction, the ack floor, the state groups, a follower behind the start), a metadata test for key-by-key changes, an admin test for creating, changing and describing settings, the metadata simulation setting and checking `retention.bytes`, and a retention simulation (its own CI job) with short limits, small segments, transactions, slow readers and a consumer group under partitions and power loss. Every rule was broken on purpose and a test failed, with one exception that is not about correctness: running retention before a round's answers instead of after leaves some writes unknown for a moment, and clients retry them, so no test can tell; it runs after them anyway. A fault-free test cuts the coordinator log under six transactional producers and checks that every transaction begins, writes and commits.

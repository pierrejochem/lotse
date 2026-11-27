# 146. Segments close by age

- Status: Accepted
- Date: 2026-11-27

## Context

A replica deletes only whole segments, and only sealed ones ([ADR 0046](0046-retention.md)): retention by age or size, queue trimming below the ack floor, and compaction ([ADR 0067](0067-log-compaction.md)) all leave the active segment alone. A segment was sealed only when it reached `segment_bytes`, 128 MiB by default.

So a log that stopped growing kept its active segment for good. A stream whose producers went quiet kept records long past `retention.ms`. A queue whose consumers acked everything kept up to 128 MiB per replica. Storage quotas make this sharper: a tenant whose limit is full writes no new data, so it could never drain back under the limit, because draining frees no space.

## Decision

**A runtime stream's or queue's active segment also closes once it is old enough.**

- A replica seals its active segment once it has held a batch for the segment age, by the time its retention pass is given, and starts an empty one at the next offset. The pass runs every second on each node. Sealing is the same as the size roll: synced, then indexed, before the next segment exists (`Log::roll`).
- With everything applied, the state is then a place retention can cut, so the replica notes a boundary at the new segment's base. The old segment can go without a new batch arriving first.
- The age starts when the replica first sees the segment hold a batch. It is kept in memory, so after a restart it starts again: a restarted node closes its segments at most one age later than the others.
- **Settings:**
  - the default is an hour (`DEFAULT_SEGMENT_MS`), set for a node with `[cluster] segment_ms`, at least 60,000 ms;
  - a runtime stream or queue sets its own with `segment.ms`, at least 60,000 ms. It is changed on its own through `AlterConfigs`, like `replication.factor`, and reset there too. `DescribeConfigs` shows it, with the node's default where unset;
  - it is not taken at creation. A create and a second record could leave a created resource without its age after a leader change, so `CreateStream` and `CreateQueue` refuse it with a message.
- `segment.ms` is a record of its own in the metadata group (`SetSegmentMs`, change tag 37; snapshot section 20). It goes with its resource. It is written only while the cluster version is at least 0.2 ([ADR 0127](0127-rolling-upgrades-with-a-cluster-version-and-restart-turns.md)); before that, `AlterConfigs` refuses it with `UNSUPPORTED_VERSION`.
- The metadata and coordinator groups keep closing by size only. Their logs are cut at snapshots of their state.

## Consequences

- A drained queue and a stream past its retention free their space within about the segment age, with no new data.
- More files: up to 24 segments a day per partition at the default, each with its index file. A busy partition still rolls by size first.
- The release becomes 0.2.0, whose cluster version 0.2 is needed for `segment.ms`; the node default needs no cluster version.
- Mutation-checked:
  - a replica that never rolls by age fails `a_segment_closes_by_age`;
  - one that notes no boundary after the roll fails `an_aged_stream_is_cut_without_new_data` (replica unit tests);
  - a node that gives its groups no segment age fails the retention simulation's `drained_queues_and_aged_streams_free_their_space`;
  - `segment.ms` written before 0.2, and a deleted resource keeping its age, fail the metadata tests.
- No request or format changes outside the metadata group. `lotse mcp`'s `alter_configs` takes `segment.ms` like any other setting, with no `confirm`: closing a segment sooner deletes nothing by itself.

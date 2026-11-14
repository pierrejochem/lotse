# 67. Log compaction

- Status: Accepted; in part superseded by [ADR 0098](0098-compaction-maps-a-bounded-number-of-keys-per-sweep.md) (a bounded map of key hashes per sweep, one sweep per node)
- Date: 2026-11-06

## Context

Streams could only delete by age and size (ADR 0046). A stream that holds the current state of something, such as a table of settings or the latest position of each vehicle, wants the newest record of each key and nothing older, and a way to delete a key with a tombstone (a `null` value). The design always named compaction; nothing implemented it.

Lotse's log is the Raft log. Each Raft entry is one batch, a batch's offsets are its Raft indices, and followers copy entries from the leader's log. Whatever compaction does must leave that intact. A replica also applies its log again after a restart (producer state, transaction state), so a compacted batch must still say what its producer wrote.

## Decision

**Each replica compacts its own log, as Kafka does.** Compaction is not replicated through Raft. Replicas compact at their own pace, and their bytes may differ; their entries never do.

**Batches stay; records go.** Compaction rewrites a batch with fewer records, down to none. The header keeps its offsets, term, producer id, epoch, base sequence, timestamps and codec; attribute bit 8 marks the batch as compacted. A compacted batch:
- holds at most as many records as the offsets it spans (`span = last_offset_delta + 1`), in rising offset order, each inside the batch;
- has a record block in its codec, or none at all when it holds no record;
- is never a control batch.

Markers, Raft entries and the broker's own batches are never compacted. A produced batch with bit 8 set is refused. Code that counted offsets with `record_count` uses `span` (producer sequences, `ListOffsets`).

**What goes.**
- A record with a key goes once a newer record of the same key is in a sealed segment.
- A record of an aborted transaction goes.
- A record without a key stays.
- A tombstone, once it is the newest record of its key, goes when three things hold:
  - it is older than `delete.retention.ms` by its timestamp;
  - this replica leads, and every member of the group (learners included) is known to hold it;
  - the sweep that drops it knew it from its start.

The tombstone rules prevent an old value from coming back. If one replica dropped a tombstone while another still held an older value of the key, and the second later copied the rest of its log from the first, the old value would be the newest again. Only a leader knows what each member holds, and a member that holds the tombstone never copies that part of the log again: Raft truncates only uncommitted entries, and a snapshot replaces the whole log.

**When.**
- Only sealed segments below the last stable offset and the applied index are compacted.
- The retention loop (once a second per replica) takes one step. A sweep walks the segments oldest first; a step looks at up to eight segments and rewrites at most one.
- A sweep starts when new sealed data is there, or a minute after the last one began, so that tombstones expire.
- The map of each key's newest offset grows before a sweep and stays as it is during it.
- Nothing of this is persisted. After a restart, or after a snapshot replaces the log, the map is built again from the log.

**Rewriting a segment.** `Log::rewrite_segment` writes `<base>.log.cleaned`, syncs it and renames it over the segment, then reads it back as on open. It refuses anything but the same batches, one for one, with the same first and last offset and term. A crash leaves the old segment or the new one whole; `Log::open` removes a leftover `.cleaned` file. The active segment is never rewritten.

**Settings, per stream.**
- `cleanup.policy`: `delete` (the default), `compact`, or `compact,delete`.
  - `compact` deletes nothing by age or size.
  - `compact,delete` does both.
- `delete.retention.ms`: how long a tombstone is kept. The default is a day, as in Kafka; 0 means at once, -1 means forever.

Both are set in `CreateStream`, changed with `AlterConfigs` and listed by `DescribeConfigs`. In the metadata group they are written only when set, both in records and in snapshots, so a state without them reads on a node from before compaction.

## Consequences

- Streams can serve as tables. Readers see gaps in offsets inside a batch. A batch may hold no record, and readers step over it by its header (`next_offset`). The Rust client and the Java SDK read compacted batches; conformance vectors (`compacted.hex`) pin the format.
- A consumer that falls further behind than `delete.retention.ms` can miss a tombstone, as in Kafka.
- Compaction holds every key of a partition in memory and rebuilds that map after a restart by reading the log. It is not bounded, and it has no dirty ratio or minimum lag. Each step reads whole segments.
- The tombstone rule counts only current members. A replica that left the group returns with an empty log once its move has settled (ADR 0050 removes a moved replica's log). A replica that came back with its old log before that could hold an older value under a tombstone the leader dropped meanwhile. It happened, and [ADR 0108](0108-a-replica-placed-again-starts-from-an-empty-log.md) closes it: such a replica starts from an empty log.
- Tombstones on followers wait until those followers lead. That costs a little space, never correctness.
- Configured streams (in the node configuration) keep `delete`. Compaction is set per stream at runtime.

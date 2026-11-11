# Retention and compaction

## Retention

A log is trimmed only at a snapshot point. When applying reaches the base offset of a segment, a replica records a snapshot of its state there. Retention picks a point and the replica cuts at the newest such boundary at or below it, deleting whole segments ([ADR 0046](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0046-retention.md)). Each node runs retention for each of its groups once a second, by its own clock.

Because only whole segments go, a partition keeps up to a segment more than its limits. `cluster.segment_bytes` (default 128 MiB) sets the size of a segment. The active segment is never deleted. Nothing at or above the last stable offset is deleted, so an open transaction's records stay until it ends.

### What each kind of log keeps

| Log | Rule |
| --- | --- |
| Stream | By age: segments whose newest record is older than `retention.ms`, by the record's timestamp. By size: the oldest segments, as long as what stays still holds `retention.bytes`. The limit is a floor. |
| Queue | Everything below the queue's ack floor goes. There is no setting. |
| Metadata and coordinator groups | Everything their state holds, once a whole segment can go. |

Age goes by the timestamps producers write.

### Node defaults

In the cluster file:

```toml
[retention]
ms = 604800000   # 7 days, the default
bytes = -1       # no limit, the default
```

`-1` means no limit. See [The cluster file](cluster-file.md#retention).

### Per stream

A stream created at runtime can set its own values:

| Key | Unit | Meaning |
| --- | --- | --- |
| `retention.ms` | ms | Age limit. `-1` for none. |
| `retention.bytes` | bytes | Size floor per partition. `-1` for none. |

They are set when the stream is created (`CreateStream`), changed or reset with `AlterConfigs`, and read with `DescribeConfigs`. A client does this: `alter_configs(name, set, reset)` and `describe_configs(name)` in `lotse-client`. A key that is not set comes back with the node's value and the source `default`. A change names only the keys it sets or resets, so two changes of different keys both hold.

Streams from a node's configuration keep the node's defaults and cannot be changed at runtime. The `lotse` binary has no such streams: clients create everything.

### What readers see

`Fetch` reports the log start offset. A read below it is answered with `OFFSET_OUT_OF_RANGE`. The Rust client reports that as `ClientError::OffsetOutOfRange` with the start. A consumer group member whose position was deleted goes on from the oldest record left.

## Compaction

A stream with `cleanup.policy` set to `compact` or `compact,delete` is compacted ([ADR 0067](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0067-log-compaction.md)). Only streams created at runtime can be compacted.

| Key | Values | Default |
| --- | --- | --- |
| `cleanup.policy` | `delete`, `compact`, `compact,delete` | `delete` |
| `delete.retention.ms` | ms; `0` removes tombstones at once, `-1` keeps them for ever | 86400000 (1 day) |

`compact` deletes nothing by age or size. `compact,delete` does both. Set both keys in `CreateStream` or change them with `AlterConfigs`.

### How it works

- Each replica compacts its own log, at its own pace. Compaction is not replicated, so replicas' bytes may differ. Their entries never do.
- Sealed segments below the last stable offset and the applied index are rewritten whole. Batches keep their offsets, term and producer fields; records go.
- A record with a key goes once a newer record of the same key is in a sealed segment. A record of an aborted transaction goes. A record without a key stays.
- A tombstone, once it is the newest record of its key, goes when all three hold: it is older than `delete.retention.ms` by its timestamp; the replica leads and every member of the group, learners included, is known to hold it; and this replica's older values of the key are gone.
- A sweep first maps keys to their newest offset, from where the last sweep's map ended, until the map is full or the sealed data ends; then it rewrites the segments below that point. A node runs one sweep at a time and its map may take `[compaction] map_bytes` (default 64 MiB, about 1.4 million keys); partitions take turns. A partition with more keys than that takes several sweeps.
- A sweep starts when new sealed data is there or the last one did not reach the end, otherwise a minute after the last one began. A step looks at up to eight segments and rewrites at most one.
- A segment is written as `<base>.log.cleaned`, synced and renamed over the old one. A crash leaves the old segment or the new one whole, and opening the log removes a leftover `.cleaned` file.

### Limits

- Keys are told apart by 16 bytes of their SHA-256: two keys with one hash (odds of 2^-128 per pair) would count as one. There is no dirty ratio or minimum lag. After a restart the sweeps start from the log start again.
- Markers, Raft entries and the broker's own batches are never compacted.
- A tombstone dropped while a replica that left the group still holds an older value is not covered by a test.

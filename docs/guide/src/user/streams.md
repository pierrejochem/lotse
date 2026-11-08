# Streams

A stream is a log you append to and read by offset. This chapter creates one, writes to it, reads it back, reads it as a consumer group and describes what keeps it from growing for ever.

## Create, write and read

The example creates a stream with two partitions on three nodes, writes ten messages keyed by customer, and reads each partition from offset 0.

```java
{{#include ../../../../sdk/java/src/examples/java/dev/lotse/examples/StreamExample.java}}
```

Points to note:

- `createStream(name, partitions, replication)` throws `ClientException.ResourceExists` when the name is taken in your tenant. Catching it makes a start-up step safe to run twice.
- **You choose the partition.** The client does not hash keys for you. Pick it from the key, as the example does, so that messages with one key stay in order.
- `produce` returns once the write is committed. With an idempotent producer (the default), a retry after a lost answer does not write twice.
- `fetch(stream, partition, offset, maxWait)` waits up to `maxWait` for data. `committedMessages()` returns only what a reader may see: messages of open or aborted transactions are left out (read_committed). `nextOffset(offset)` gives the offset to ask for next.
- Offsets are unsigned 64-bit numbers; print them with `Long.toUnsignedString`.

An offset below the start of the log, after retention removed it, throws `ClientException.OffsetOutOfRange`, which carries `logStart()`. To find a position without reading, `listOffset(stream, partition, at)` takes `OffsetAt.EARLIEST`, `OffsetAt.LATEST` or `OffsetAt.time(epochMillis)` ([ADR 0059](/0059-list-offsets.md)).

## Kotlin

`records(stream, partition, from)` in the Kotlin layer is a `Flow` of records, so a stream reads like a collection:

```kotlin
lotse.records("example-invoices-kt", 0, from = 0).take(10).collect { println(it.value().toUtf8()) }
```

This line is taken from `KotlinExample.kt`, shown in full in [Transactions and exactly-once](transactions.md).

## Settings per stream

`createStream` has a fourth argument, a map of settings; `alterConfigs(name, set, reset)` changes them later and `describeConfigs(name)` reads them. Streams made at runtime accept these keys:

| Key | Meaning | Values |
| --- | --- | --- |
| `retention.ms` | Delete segments whose newest record is older than this | A positive number of milliseconds, or `-1` for no limit. Default: the node's, 7 days |
| `retention.bytes` | Delete the oldest segments while the rest still holds at least this many bytes | A positive number of bytes, or `-1` for no limit. Default: no limit |
| `cleanup.policy` | What the log does with old data | `delete` (default), `compact`, or `compact,delete` |
| `delete.retention.ms` | How long a tombstone stays in a compacted stream | Milliseconds from 0, or `-1` to keep them. Default: 1 day |

A key is reset by naming it in the `reset` list. Changing settings needs an admin role on the stream.

```java
// Not from the compiled examples: keep a week, at most 10 GiB per partition.
client.createStream("clicks", 4, 3, Map.of(
        "retention.ms", Long.toString(Duration.ofDays(7).toMillis()),
        "retention.bytes", Long.toString(10L * 1024 * 1024 * 1024)));
```

## Retention

A log is trimmed in whole segments, and never past the last stable offset, so an open transaction's records stay. The active segment is never deleted. The size limit is a floor: the stream keeps at least `retention.bytes`. Details: [ADR 0046](/0046-retention.md).

## Compaction

With `cleanup.policy = compact`, Lotse keeps at least the newest record of each key. Older records of a key are removed from sealed segments. Records without a key stay. A record with a key and a null value is a **tombstone**: it deletes the key, and is itself removed once it is older than `delete.retention.ms` and every replica holds it. Offsets never change; a compacted batch may have gaps. `compact` alone deletes nothing by age or size; `compact,delete` does both. The Rust and Java clients read compacted batches. Details: [ADR 0067](/0067-log-compaction.md).

## Consumer groups

A group reads a stream, or several, as a team and remembers where it is. `ExactlyOnceExample` in [Transactions and exactly-once](transactions.md) joins one:

```java
// Not from the compiled examples.
GroupConfig config = GroupConfig.defaults(); // auto-commit on: a revoked partition's position is committed first
try (GroupConsumer group = client.joinGroup("billing", List.of("orders"), config)) {
    while (running) {
        for (GroupRecord r : group.poll(Duration.ofMillis(500))) {
            handle(r.record());
        }
    }
}
```

- Call `poll` often. It also sends the heartbeat; a member that stops polling loses its partitions when its session times out.
- `GroupConfig` sets the session and rebalance timeouts, auto-commit (on by default: the position of a partition taken away is committed first), `startFrom(OffsetAt)` for partitions without a committed offset, and an `instanceId` for a member that keeps its place across restarts. `joinGroupMatching(group, pattern, config)` subscribes by regular expression.
- If a member's position was deleted by retention, it goes on from the start of the log.
- The group id lives in your tenant and needs the consumer role.

## When a write fails

| Exception | Meaning |
| --- | --- |
| `ClientException.Unknown` | The answer was lost. The write may or may not be in the log. The client does not guess; an idempotent producer lets you try again without a duplicate |
| `ClientException.GaveUp` | The client retried for `retryFor` and stopped |
| `ClientException.UnknownStream` | No such stream or partition in your tenant, or you may not see it |
| `ClientException.Broker` | The node refused; `code()` names the reason |

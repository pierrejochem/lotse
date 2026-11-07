# 74. Adding partitions to a stream

- Status: Accepted
- Date: 2026-11-07

## Context

A stream's partition count was fixed when it was created (ADR 0034). A stream that outgrows it, with more producers or more consumers in a group than partitions, had to be created again under another name. Of the settings still missing at runtime, the maintainer chose this one first, over changing replication or a queue length limit.

## Decision

**`CreatePartitions` (key 51)** raises the partition count of a stream created at runtime to a given total.
- It needs the tenant-admin role on the stream, as `AlterConfigs` does.
- The count only goes up, to at most 1,024; asking for the same count or fewer is `INVALID_REQUEST`.
- Configured streams cannot be changed.
- `Client::create_partitions` sends it.

**Placement.** The metadata leader places the new partitions like those of a new stream (ADR 0034). Each gets as many replicas as the stream's first partition has now. It proposes `AddPartitions` (metadata tag 19), which appends them in order. Applying it does nothing if the stream was deleted meanwhile, or if the total would pass the limit.

**Group ids.** A new partition's Raft group id comes from the stream's own creating index and its partition number, the same rule as for the first partitions. Partitions are never removed, so no id is used twice. Deleting the stream finds every partition's group by that rule, new ones included.

**Consumer groups.** On every heartbeat the coordinator checks whether the group's subscribed streams have partitions that no member targets. If so, it raises the group's epoch and spreads them as it would for a new member, and the new partitions are read from where the group starts a partition without a committed offset (ADR 0059).

## Consequences

- Streams grow without being created again.
- Existing partitions are not touched, so a producer that picks a partition from a key's hash may send a key elsewhere afterwards, as in Kafka. Order per key holds only within each period.
- Lowering the count is not possible.
- The Java SDK carries the request in its codec; its admin client does not send it yet.

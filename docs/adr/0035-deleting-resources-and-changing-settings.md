# 35. Deleting resources and changing settings

- Status: Accepted; in part superseded by [ADR 0100](0100-tombstones-go-once-every-node-removed-the-files.md) (a tombstone goes once every node removed the files)
- Date: 2026-10-28

## Context

ADR 0034 lets admins create streams, queues and exchanges at runtime, but not delete them or change their settings; `DeleteResource`, `DescribeConfigs` and `AlterConfigs` were left for later. Deleting is the hard part: every replica of a group has to stop it and remove its data, a node that is down or lagging must not bring it back, clients and transactions that still use it must end, and a name that is free again may be created once more. Retention, the main stream setting, is not built yet.

## Decision

**A delete is a metadata record that leaves a tombstone.** `DeleteResource` (25) proposes a `Delete` record. Applying it removes the resource and keeps its id and number of groups in the metadata state (snapshot format 7). Ids are never reused, so the tombstone says, for ever, that the id and its groups are gone. Bindings go with a deleted queue (on every exchange, configured ones too) or exchange, and a bind applied after a delete of either side changes nothing.

**Every node closes and removes deleted groups.** When a node's metadata replica has applied a tombstone, the node drops the group's replica and removes its files and directories (`Disk::remove_dir` is new in the runtime seam). The tombstone is durable before anything is removed, and runtime groups are opened only from the metadata state, so a node that stops halfway never opens the group again and finishes the removal after a restart. Requests waiting on a deleted group are answered `UNKNOWN_RESOURCE`, queue subscriptions end with it, and a node answers `UNKNOWN_RESOURCE`, no longer `NOT_LEADER`, for a deleted id. A transaction coordinator stops writing markers to a deleted partition once its node has applied the tombstone: the data the marker would settle is gone, so the transaction can end.

**Only runtime resources can be deleted or changed** (maintainer's choice). Configured ones come back with the next restart, so they are refused with `INVALID_REQUEST`. A deleted name is free again and is created under a new id.

**`if_empty` is refused for now** (maintainer's choice). Checking it needs the metadata leader to ask the leader of the resource's own group, which may run elsewhere. Deletes are unconditional until then.

**Settings: only a queue's ack timeout can change** (maintainer's choice). `AlterConfigs` (27) sets `ack.timeout.ms` or resets it to the default; anything else is refused. `CreateQueue` stores 0 for an unset timeout, so the default stays the default. `DescribeConfigs` (26) reports each resource's settings with their source (default, set at runtime, or from the configuration). It is served by the metadata leader from its applied state, like the changes, so a client reads its own acknowledged changes. Delete and alter are answered with a new `AdminResponse`; retention and other stream settings come with retention itself.

**Answers rest on the applied state**, as in ADR 0034: a delete is done once the resource is gone, whoever deleted it; a bind holds only if neither side was deleted before it; an alter only if the queue still exists. The client treats a delete that finds the resource gone as done, so retries are safe.

**The client forgets what a delete invalidates.** A produce, fetch or subscription answered `UNKNOWN_RESOURCE` drops its cached route, so a name created again is looked up afresh. An idempotent producer keeps its sequence: a partition accepts any sequence from a producer it does not know. When brokers disagree about a name, the client takes the highest id: ids grow with the metadata log, so that is the newest resource.

**The metadata simulation covers it** (maintainer's choice): admins also delete and recreate `s3`, `q2` and `x1` and change queues' ack timeouts, and a transactional producer writes to a stream that is deleted under it, in the same CI job (2,000 seeds per push, 5,000 nightly).

## Consequences

- Resources can be removed without restarting nodes, and their disk space is freed on every replica, including those down at the time.
- Tombstones stay in the metadata state for ever: a few bytes per deleted resource. Compacting them would need every node to have removed the files first.
- A delete does not wait for replicas to remove their data; a replica that is down removes it when it comes back.
- Committed offsets of consumer groups for deleted streams stay in the coordinator until the group is removed; they name ids that are never reused, so they do no harm.
- No conditional delete, no stream or exchange settings, and no authorisation yet.
- All 13 seeded deletion and settings bugs are caught: groups that keep running or keep their files, a deleted id answered `NOT_LEADER`, markers that wait for a deleted partition, a configured resource deleted, a binding kept for a deleted queue or added after a delete, tombstones lost from the snapshot, an alter ignored, a client that keeps a deleted route or takes the older of two ids. Two of them, a bind or an alter acknowledged although a delete came first, need a history the simulation meets too rarely; a deterministic test admits both before a delete in one round. A fourteenth, resetting an idempotent producer's sequence after a delete, turned out equivalent (partitions accept any sequence from a producer they do not know), so that code was removed. Writing the codec, two array minimums were set too high; a codec test of minimal elements covers them.

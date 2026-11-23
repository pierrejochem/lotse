# 135. `alter_configs` asks for `confirm` when a setting can delete data

- Status: Accepted
- Date: 2026-11-23

## Context

ADR 0134 gave `lotse mcp` a `confirm` argument for the tools that destroy data (`delete_resource`, `purge_queue`, `requeue_dead_letters`): each runs only when `confirm` is the resource's name again. `alter_configs` was only marked `destructiveHint`, because most of its settings delete nothing. But some of them do: a shorter `retention.ms` or `retention.bytes` trims a stream at the next snapshot point, `cleanup.policy = compact` drops the records a newer one of their key replaces, a shorter `delete.retention.ms` drops tombstones sooner, and a queue's `max.length`, `max.length.bytes` or `overflow = drop-head` retires waiting messages (ADR 0046, 0067, 0118). Whether an agent asks before such a change was left to the MCP client, and many clients ask for nothing.

## Decision

- `alter_configs` runs a change that sets or resets `retention.ms`, `retention.bytes`, `delete.retention.ms`, `cleanup.policy`, `max.length`, `max.length.bytes` or `overflow` only when its `confirm` argument is the resource's name, exactly, as the destroying tools do. Otherwise it answers `CONFIRMATION_MISMATCH`, names the setting and changes nothing, not even the other settings of the same call.
- `confirm` is an optional property of `alter_configs`: a change of only other settings (`ack.timeout.ms`, `max.deliveries`, `replication.factor` and the rest) runs without it.
- The tool keeps its `destructiveHint`. The check is in `lotse mcp` only; `AlterConfigs` over the native protocol and in the console is unchanged.

## Consequences

- An agent cannot cut a stream's retention or limit a queue in one step. It has to name the resource twice, which shows the user what is about to change.
- `message.ttl.ms` and `max.deliveries` can also retire messages, but without a dead-letter queue only; they stay without `confirm`, as decided here. Adding a key to the list is a change of `DATA_LOSING` in `crates/lotse-mcp/src/change.rs` and of this record.

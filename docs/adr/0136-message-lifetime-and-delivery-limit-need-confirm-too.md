# 136. Message lifetime and delivery limit need `confirm` too

- Status: Accepted
- Date: 2026-11-23

## Context

ADR 0135 made `alter_configs` in `lotse mcp` ask for `confirm` when a change can delete data: retention, the cleanup policy and a queue's length limits. It left out `message.ttl.ms` and `max.deliveries`, because they retire messages into a dead-letter queue when the queue has one. Without one, they drop the messages (ADR 0055): a short lifetime or a low delivery limit set by an agent can lose a queue's backlog as surely as a length limit can.

## Decision

- `message.ttl.ms` and `max.deliveries` join the settings of ADR 0135: `alter_configs` sets or resets them only when `confirm` is the resource's name, exactly.
- They need `confirm` whether or not the queue has a dead-letter queue. The tool does not look the queue up first: a dead-letter queue can be removed in the same call or a moment later, and one rule is easier for an agent and its user to know.
- Everything else of ADR 0135 stands.

## Consequences

- Every setting that can make a stream or queue lose data now needs the resource's name twice through `lotse mcp`; `ack.timeout.ms`, `replication.factor`, `single.active.consumer` and the other settings do not.
- An agent that only raises a delivery limit on a queue with a dead-letter queue has to confirm too, though nothing would be lost.

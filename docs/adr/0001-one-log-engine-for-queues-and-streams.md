# 1. One log engine for queues and streams

- Status: Accepted
- Date: 2026-10-27

## Context

Lotse offers RabbitMQ-style reliable queues (ack, nack, DLQ, retry, TTL, priorities) and Kafka-style persistent streams (partitions, offsets, replay, retention, compaction). Two storage and replication engines would double the code that has to be correct under failure.

## Decision

Queues and streams share one segmented log engine. A queue is a log plus its acknowledgement state; acks, delivery counts and timers are records in the same log (`ACK`, `QUEUE_STATE`, `TIMER` batch kinds). Segments are deleted only whole: for streams by retention, for queues once the ack floor passes them; a segment rewrite frees segments held by single unacked messages.

## Consequences
- One replication and one exactly-once implementation serve both models.
- Queue state is replicated by the same Raft log as the data, so failover never loses an ack.
- Queues need the segment rewrite to keep disk usage bounded.

See also: [design document](../design.md#storage-layout).

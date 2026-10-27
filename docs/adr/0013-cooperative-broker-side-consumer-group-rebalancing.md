# 13. Cooperative, broker-side consumer group rebalancing

- Status: Accepted
- Date: 2026-10-27

## Context

Eager rebalancing stops whole groups; client-side assignment needs several round trips and a leader client.

## Decision

Consumer groups rebalance cooperatively with broker-side assignment (KIP-848 style). `GroupHeartbeat` replaces join, sync and heartbeat. Group coordinators are the leaders of the internal `__groups` partitions, groups spread by hash; offset commits are records and join transactions via `AddOffsetsToTxn`.

## Consequences
- No stop-the-world rebalances.

See also: [design document](../design.md#consumer-groups).

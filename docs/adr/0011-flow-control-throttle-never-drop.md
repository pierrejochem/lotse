# 11. Flow control: throttle, never drop

- Status: Accepted
- Date: 2026-10-27

## Context

Overload must slow producers down without losing data or starving consumers that would drain the backlog.

## Decision

Responses carry `throttle_ms`; a client that ignores it is paused by the broker. Queue consumers get credit windows in messages and bytes (default 100 messages / 1 MiB). Disk: a soft limit at 20 % free raises throttling, a hard limit at 10 % free stops writes with `STORAGE_FULL`; reads and acks always continue. Storage quotas are set per tenant, optionally narrower per principal or client id; exceeding them stops writes with `QUOTA_EXCEEDED`.

## Consequences
- Backlogs can always drain.
- Clients must honour `throttle_ms`.

See also: [design document](../design.md#flow-control--quotas).

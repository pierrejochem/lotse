# 14. Permissions per queue and stream, never per record

- Status: Accepted
- Date: 2026-10-27

## Context

Per-record access checks would force the broker to decode every batch it delivers.

## Decision

Read and write permissions apply to whole queues and streams. Schemas are referenced by a reserved header; a registry comes after the MVP and the broker does not validate payloads.

## Consequences
- `Deliver` and `Fetch` stay zero-copy.

See also: [design document](../design.md#weitere-entscheidungen).

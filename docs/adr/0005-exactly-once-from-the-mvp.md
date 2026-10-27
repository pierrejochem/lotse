# 5. Exactly-once from the MVP

- Status: Accepted
- Date: 2026-10-27

## Context

Consume-ack-produce pipelines across queues and streams are a core use case; retrofitting transactions into a storage format is expensive.

## Decision

Exactly-once is in the MVP: idempotent producers (producer id, epoch, sequence numbers) plus transactions coordinated on the internal `__txn_state` log. COMMIT and ABORT markers are control batches written into every partition a transaction touched; `read_committed` reads stop at the last stable offset. The storage format knows transaction markers from day one.

## Consequences
- Atomic consume-ack-produce across queues and streams.
- Every read path deals with the LSO and aborted-transaction filtering.

See also: [design document](../design.md#transaktionen--exactly-once).

# 9. Own sans-I/O Raft core where the partition log is the Raft log

- Status: Accepted
- Date: 2026-10-27

## Context

Existing Raft libraries keep their own log, which would duplicate every record into a WAL next to the partition log.

## Decision

Lotse has its own Raft core (`lotse-raft`) without I/O: the driver feeds it messages and ticks and persists what a `Ready` asks for. The partition log is the Raft log: a Raft entry is one record batch covering the indices `first..=last`, one per record, with index = offset + 1. Terms are kept as runs. Outgoing appends name the index to send from and the driver fills them from its log, so records never pass through Raft memory. Time is logical (`tick()`).

## Consequences
- No second log, batching built in.
- The correctness burden is ours, hence ADR 0010.

See also: [design document](../design.md#replikation--snapshots).

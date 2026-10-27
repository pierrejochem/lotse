# 7. Record format: fixed batch header and varint-delta records

- Status: Accepted
- Date: 2026-10-27

## Context

The batch format is used on disk, on the wire and in Raft; it can never change cheaply.

## Decision

A batch has a 65-byte little-endian header (base offset, length, term, magic, CRC32C over the rest, attributes, last offset delta, timestamps, producer id/epoch, base sequence, record count) followed by records encoded with ZigZag varint deltas. Attributes carry compression, the transactional flag, the control flag and the batch kind. Raft's own entries (no-ops, membership changes) are control batches readers skip.

## Consequences
- Compact records that compress well as a block.
- Offset and term can be stamped in place without recomputing the checksum.

See also: [design document](../design.md#batch-header-fest-little-endian).

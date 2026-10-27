# 8. Snapshots as state plus segment references

- Status: Accepted
- Date: 2026-10-27

## Context

Copying immutable segments into every Raft snapshot would multiply disk and network use.

## Decision

A snapshot is the state (producer state, queue state, metadata) plus a manifest that references segments by name, base offset, size and CRC. Followers catching up fetch the manifest, then only missing or differing segments, then the state. Retention alone deletes segments; a snapshot pins its segments until a newer one exists. In the Raft core a snapshot is only `(index, term, membership)`; content and transfer belong to the server.

## Consequences
- No copies of immutable data.
- Segment lifetime depends on both retention and snapshots.

See also: [design document](../design.md#snapshot-inhalt).

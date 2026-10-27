# 17. Linearizable reads: ReadIndex by default, leases opt-in

- Status: Accepted
- Date: 2026-10-27

## Context

Reads such as queue state and metadata must be linearizable. A leader may have been deposed without knowing it.

## Decision

ReadIndex is the default: every append and heartbeat carries a round number that followers echo, and a read is confirmed once a quorum answered a round sent after the read arrived. Reads between two `Ready`s share a round; a new leader first commits an entry of its own term; reads are aborted when the leader steps down; followers forward to the leader. Leader leases are opt-in per cluster (`lease_ticks`, shorter than the election timeout by more than the clock drift): with leases, a restarted node ignores votes for an election timeout, and a leader that tried a transfer gives up its lease for the rest of its term.

## Consequences
- Correct reads without clock assumptions by default; one round trip per batch of reads.
- Leases trade that round trip for a bounded-drift assumption the operator must guarantee.

See also: [design document](../design.md#linearisierbare-lesezugriffe).

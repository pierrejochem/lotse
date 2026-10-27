# 18. Leader transfer with TimeoutNow

- Status: Accepted
- Date: 2026-10-27

## Context

Rolling upgrades, draining and rebalancing move leadership on purpose.

## Decision

`transfer_leadership(to)` stops taking proposals (clients are pointed at the target), brings the target up to date and sends it `TimeoutNow`. The target campaigns at once without pre-vote; its vote requests carry a transfer flag that passes leader stickiness. If nothing happens within an election timeout, the leader carries on.

## Consequences
- Planned moves cause no write unavailability beyond one election.
- A delayed `TimeoutNow` is why a leader gives up its lease after trying a transfer (ADR 0017).

See also: [design document](../design.md#leader-transfer).

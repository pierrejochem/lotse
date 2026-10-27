# 16. Elections: check-quorum always on, pre-vote optional

- Status: Accepted
- Date: 2026-10-27

## Context

The membership simulation found that a node dropped from the voters before it learned so keeps raising the term and unseats every leader, and that filtering votes by membership deadlocks elections when a lagging voter does not know a newly promoted one.

## Decision

Check-quorum (a leader steps down without a quorum; nodes ignore vote requests while they hear from a leader) is always on and not configurable. Votes are not filtered by membership. Pre-vote stays configurable. A node that the newest, uncommitted configuration drops but the previous one counted may still campaign, counting votes by the newest configuration: it often holds the longest log, and without it nobody might win.

## Consequences
- Removed nodes cannot disrupt a group.
- These rules came from simulation failures; their mutants are part of the test suite.

See also: [design document](../design.md#membership-%C3%A4nderungen).

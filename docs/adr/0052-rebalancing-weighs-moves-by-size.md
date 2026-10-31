# 52. Rebalancing weighs moves by size

- Status: Accepted
- Date: 2026-10-31

## Context

Rebalancing (ADR 0050) counts replicas. When a move was due, it took the first group that could go from the fullest node to the emptiest, whatever its size, so a node could hold as many replicas as the others and still carry far more bytes, and nothing evened that out.

## Decision

**Size breaks the tie; counts still decide** (maintainer's choice). When and between which nodes a move happens stays as in ADR 0050: by replica count, with the same threshold. Of the groups that may make that move, the planner now takes the one that brings the two nodes' bytes closest together: the group whose size is nearest half the difference between them. Without sizes, it takes the first, as before.

**Sizes come from `DescribeMetrics`.** Every 1,000 ticks the metadata group's leader asks every node for its metrics (nodes may now send `DescribeMetrics`) and keeps each group's size, the largest any replica reported. The planner runs only on that leader, so the sizes need not be the same anywhere else, and they stay out of the metadata group.

## Consequences

- Moves that the counts call for anyway even out bytes as far as they can; replica counts stay within one (or a tenth) as before.
- No move happens for bytes alone: two nodes with equal counts and very different sizes stay as they are. Balancing by bytes would need sizes in the metadata group and moves that follow growth.
- Sizes may be several seconds old, and missing for a node that does not answer; they only choose between moves, so a stale size makes a less good choice, never a wrong one.
- A replica that is still catching up reports a smaller log; taking the largest report per group avoids most of that.
- Tests: a planner unit test that, of the groups a move may take, the one that evens out bytes most goes, and that without sizes the moves are those by count alone. The choice was broken on purpose and that test failed.

# 87. Group subscriptions are kept as named

- Status: Accepted
- Date: 2026-11-08

## Context
A consumer group member names the streams it reads only when it joins (ADR 0033); later heartbeats carry no subscription. The node that serves the join filtered the named ids to the streams it had already applied. A client that creates a stream and joins at once can reach a node whose metadata replica has not applied the creation yet. The filtered subscription was empty, the member stored it, and no later heartbeat could correct it: the member stayed without partitions until it left or lost its session. The C# and Node.js SDKs hit this in tests against three real nodes (a static member failed about half the runs until all nodes knew the stream).

## Decision
The coordinator stores the stream ids a join names as they are, also ids it does not know yet. Retargeting gives an unknown stream no partitions, and every heartbeat already spreads the partitions of a subscribed stream that nobody covers, so the member gets them with the first heartbeat after the node has applied the stream. Patterns (ADR 0073) are unchanged: they are matched anew with every heartbeat.

## Consequences
A stream that appears a moment after the join reaches the member without the client doing anything, and no client has to wait until all nodes know a stream. An id that never becomes a stream stays in the member's subscription and does nothing. Access is unchanged: every named id is checked against the principal's read role before the request reaches the coordinator (`access.rs`), so with roles in force a node that does not know the stream yet refuses the join as it refuses any unknown resource.

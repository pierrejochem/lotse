# 101. A publish refused by a node behind is sent again

- Status: Accepted
- Date: 2026-11-12
- Changes: [ADR 0079](0079-publish-routed-by-the-node.md)

## Context

Any node takes a `Publish` and routes it by the bindings it has applied (ADR 0079). A node that had not yet applied an exchange declared a moment ago answered the entry with `UNKNOWN_RESOURCE`. The client then looked the exchange up again, once, in case the name had been made again with a new id. If that lookup found the same id and the node was still behind, the client reported the exchange as unknown. A publish just after `DeclareExchange` could fail like this although nothing was wrong. `AddPartitionsToTxn` had the same race and now answers `NOT_LEADER`, as produce and fetch do.

## Decision

**A node that does not know a runtime exchange that is not deleted refuses the entry with `NOT_LEADER`, and a client sends the publish again to the next node, or later, until `retry_for` runs out.**

- The node decides as for produce and fetch (`ClusterView::absent`, ADR 0100). A runtime id that is not deleted may be new, so the node answers `NOT_LEADER`. A deleted id, or an exchange id that is not a runtime one, gets `UNKNOWN_RESOURCE`, as before.
- An entry-level error comes only from refusals made before anything is written. That is unlike a queue's error in `routed`, which can come after a write. So a client may send a publish again whenever the entry's error is retriable (`NOT_LEADER` and the rest of `ErrorCode::retriable`). This does not change ADR 0079's rule that a publish whose answer is lost is not sent again.
- The Rust client and the Java, C# and Node.js SDKs do this inside the loop that already tries each node in turn and then backs off. `UNKNOWN_RESOURCE` is handled as before: the client looks the name up once more.

## Consequences

- A publish right after `DeclareExchange` waits until some node knows the exchange, at most `retry_for`, instead of failing.
- A client of an older version still treats `NOT_LEADER` on an entry as final, so against a newer node it fails where it would have retried the lookup once. The answer is the one produce and fetch give for the same case.
- Tests: a server unit test checks the answer for an exchange the node has not heard of yet. In each of the four clients a test makes a scripted broker refuse a publish twice before taking it. Each test fails without its change (the C# one is checked by CI only).

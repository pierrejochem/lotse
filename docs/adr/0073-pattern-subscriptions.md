# 73. Pattern subscriptions

- Status: Accepted
- Date: 2026-11-06

## Context

A consumer group member subscribed to a list of stream ids (ADR 0033). A member that wants every stream of a kind, `orders-eu`, `orders-us` and those not created yet, had to know them all and join again for each new one. The maintainer chose to resolve patterns on the coordinator, as KIP-848 does, over matching in the client or allowing only a prefix.

## Decision

**A member may subscribe by a regular expression** (`GroupHeartbeat` tag 3, sent when joining; `Client::join_group_matching`) instead of by stream ids.

**Matching.** The coordinator matches the pattern against the whole name of each stream:
- it is anchored, as `^(?:pattern)$`;
- only streams of the group's tenant count;
- only streams the member's principal may read count (the consumer role and its prefix);
- the `regex` crate compiles patterns, at most 1 KiB long and with a 1 MiB size limit; compiled patterns are cached on the coordinator leader;
- a pattern that does not compile is refused with `INVALID_REQUEST`.

**Keeping up.** The member keeps its pattern in the group's state, in records and snapshots (written only when a member has one). On every heartbeat from the member, the coordinator matches the pattern again. When the set of streams changes, the group's assignment changes with it, just as for a changed list of ids. A stream created later joins within a heartbeat interval; a deleted one leaves. A member that takes over its instance's place (ADR 0072) keeps its predecessor's pattern unless it sends a new one.

**Client.** The client asks for all the metadata it may see when an assignment names a stream it has not looked up yet.

## Consequences

- One member can read a family of streams that grows, without knowing it in advance.
- A role change takes effect on the next heartbeat: a stream the member may no longer read leaves its subscription.
- Members of one group may have different patterns. Each sees new streams on its own next heartbeat, so for up to a heartbeat interval they may disagree.
- Matching costs a scan of the streams per heartbeat of a member with a pattern.
- The Java SDK carries the tag in its codec; its group consumer does not exist yet.

# 69. Tenant incarnations

- Status: Accepted
- Date: 2026-11-06

## Context

Since ADR 0062, transactional and consumer group ids live in a tenant: the coordinator keys them by tenant and id. Deleting a tenant left that state in the coordinator group: transactional ids with their producer ids and epochs, groups, and committed offsets. A tenant created again under the same name used the same keys. Its transactional ids would have taken up the old producers' ids and epochs, and its groups the old groups' state. The maintainer chose to give each tenant an incarnation rather than clean up before a name may be used again, or refuse the delete while ids exist.

## Decision

**Each tenant created at runtime has an incarnation.**
- The incarnation is the index of the metadata record that created the tenant. `CreateTenant` records carry a flag for this; records from before carry none, and their tenants keep incarnation 0.
- The metadata state keeps the incarnation of every tenant that has one, live or deleted, and so do snapshots. The section is written only when there is one, so older states read as before.
- Configured tenants, `default` among them, are always incarnation 0.

**Keys carry it.**
- Incarnation 0 keeps the key of ADR 0062: the id for `default`, otherwise tenant, NUL, id.
- Any other incarnation's key is NUL, the incarnation in decimal, NUL, tenant, NUL, id.
- A node scopes each request with the incarnation its applied metadata state knows. A tenant created again therefore starts with new keys at once, without the old ids.
- Tenant names may no longer contain NUL (`CreateTenant` refuses them), so the new keys cannot be confused with older ones.

**The coordinator leader forgets what is gone.**
- About once a second, the coordinator leader looks for keys of an incarnation that is gone: older than the tenant's current one, or at most the incarnation of a tenant that was deleted.
- It proposes a `Forget` record (internal control type 9) naming up to 256 KiB of such keys. Applying the record drops each key's transactional id, group and committed offsets.
- It leaves two kinds of key for later:
  - a transaction that is still running, until its client or its timeout ends it, because its partitions wait for the markers;
  - a key with a record in flight.
- A tenant the node does not know at all is not taken for gone: its creation may simply not have reached this node yet.

## Consequences

- A tenant created again shares nothing with the one deleted before it.
- The old incarnation's state disappears shortly after the delete, so coordinator state no longer grows with deleted tenants.
- Keys of tenants created before this change stay as they were; they change only once the tenant is deleted and created again.
- The coordinator does a little more work: a scan over its keys once a second on the leader.
- Not covered: a client that cached the old tenant's stream ids still sends them until its metadata refreshes. That is the same window as for any deleted and re-created resource.

# 62. Transactional and group ids live in a tenant

- Status: Accepted
- Date: 2026-11-06

## Context

ADR 0039 scoped every resource to a tenant but left transactional ids and consumer group ids cluster-wide. Nothing checked `InitProducer` or `EndTxn`. A principal with write access anywhere could take any transactional id, which fenced another tenant's producer and aborted its open transaction. A principal with read access anywhere could join or read the offsets of any group id, as long as the streams it named were its own. The same name chosen by two tenants was one id.

## Decision

**Ids live in a tenant** (maintainer's choice). Every request that names a transactional or group id names its tenant too: `Produce` (with a transactional id), `InitProducer`, `AddPartitionsToTxn`, `EndTxn`, `GroupHeartbeat`, `CommitOffsets`, `OffsetFetch`, `AddOffsetsToTxn`, and the internal `VerifyTxn`. The tenant goes in the request's trailing block of tagged fields, as tag 1 holding a string. It is left out for `default`, so requests in the default tenant are byte for byte what they were, and a reader that finds no tag takes `default`. `lotse-client` and the Java SDK send the tenant their configuration names, the one they already create resources in. `DescribeMetrics` reports each group with its tenant.

**They are checked like resources.** A transactional id needs the producer role (write), and a group id the consumer role (read), in the request's tenant. The binding's name prefix is matched against the id, as it is against a resource's name. The tenant must exist, even for a cluster admin. These checks come on top of the existing ones on the resources a request names: a transaction still needs write access to each partition it adds, and a group needs read access to each stream. A plain idempotent `InitProducer`, without a transactional id, still needs nothing by itself. `DescribeMetrics` shows a group only to whoever may see its id, and every resource it touches.

**The coordinator keys ids by tenant.** Once a request has passed the check, the node replaces each id with the coordinator's key for it:
- In `default`, the key is the id itself, so state written before this change keeps its meaning.
- In any other tenant, the key is the tenant, a NUL byte and the id.

Ids containing NUL are refused (`INVALID_REQUEST`), so the last NUL of a key is where the id starts and no two (tenant, id) pairs share a key. The coordinator's records, snapshots and timers are unchanged and never see a bare id; no format version changes.

## Consequences

- The same transactional id in two tenants is two producers: neither fences the other. The same group id in two tenants is two groups. A tenant's producers and consumers can use only the ids their bindings' prefixes allow.
- Clients outside `default` must name their tenant. Before this change they sent none, so their ids lived in `default`; after it, they find their transactions and groups in their own tenant and start fresh there. Their old state stays in `default`.
- Deleting a tenant does not remove its ids' coordinator state. A tenant created again under the same name finds it. Ids are not resources, so they never stop a tenant from being deleted.
- `FindCoordinator` takes no tenant, because one coordinator group serves every id. Splitting the coordinator by key would need one.
- Tests:
  - A deterministic access test of every case:
    - a transactional id outside the producer's prefix, in another tenant, in the default tenant and in a tenant that does not exist;
    - the same transactional id in two tenants, where the second instance does not fence the first, while a second instance in the same tenant still does;
    - a group id outside the consumer's prefix;
    - the same group id in two tenants as two groups of one member each;
    - a consumer seeing only its tenant's groups, and not a group whose id is outside its prefix, even on a stream it reads;
    - an id with NUL refused.
  - The key's unit tests, and the round trips of the new vectors in Rust and Java.
  - The client, group, access, queue and retention simulations, run at 300 seeds.
- Mutation checks: these were broken on purpose, and the test failed for each:
  - ids left unscoped;
  - a key without the tenant;
  - the NUL check;
  - the check on `InitProducer`;
  - the check on `GroupHeartbeat`;
  - the check that the tenant exists;
  - the group's visibility in metrics;
  - `VerifyTxn` without the tenant;
  - the client sending no tenant.

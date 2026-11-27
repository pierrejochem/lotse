# 147. Storage quotas per tenant and name prefix

- Status: Accepted
- Date: 2026-11-27

## Context

Many tenants share one cluster ([ADR 0039](0039-tenants-and-roles.md)), but nothing limited what a tenant stored. A tenant that filled a node's volume dropped it below its free space limit, and then every group with a voter on that node refused new data ([ADR 0122](0122-a-node-low-on-disk-stops-new-data-in-its-groups.md)), whichever tenant it belonged to. [ADR 0011](0011-flow-control-throttle-never-drop.md) planned storage quotas per tenant, optionally narrower per principal or client id, and reserved `QUOTA_EXCEEDED`; none of it was built.

A group's size does not say who wrote it, and retention deletes bytes without knowing the writer, so storage per principal would need accounting per segment. Applications usually own a name prefix, as role bindings already assume.

## Decision

**Each tenant has an optional storage limit, and its tenant admins may divide it with limits on name prefixes. A full limit refuses new data under it with `QUOTA_EXCEEDED`.**

### Limits

- **Who sets them:**
  - a tenant's own limit (the empty prefix): a cluster admin only;
  - a prefix limit: a cluster admin, or a tenant admin of that tenant whose binding prefix covers the limit's prefix;
  - a prefix limit a cluster admin set: only a cluster admin changes or removes it. Each limit records whether a cluster admin set it.
- **What they hold:** bytes, at least 1; absent removes the limit.
- **Limits per tenant:** at most 100 prefix limits per tenant.
- **Overlap:** a resource counts against its tenant and every prefix limit its name starts with.
- **Below usage:** a limit may be set below current usage; it is full at the next count.
- **Lifetime:** limits live in the metadata state and go with their tenant, so a tenant created again starts without any.

### Counting

- **Who counts:** the metadata group's leader, after each size poll. The poll runs every `[quotas] check_interval_ms`, 10,000 ms by default, 1,000 to 600,000. It is the poll that weighs rebalancing ([ADR 0052](0052-rebalancing-weighs-moves-by-size.md)).
- **What a limit uses:** the sum, over the groups of the resources it covers, of the group's size times the number of voters the metadata state lists. A group's size is the largest log size any replica reported.
  - Learners, and replicas a node runs until a move off it settles, do not count, so the cluster's own moves never push a tenant over.
  - Snapshot and index files are not counted, nor are the metadata and coordinator groups.
- **Missing sizes:** a group no node reported this time keeps its last size on this leader. A node that stops leading forgets the sizes. The leader counts only once every runtime group has a size, so missing data never shrinks a count or clears a limit.
- **Full and clear:** a limit is full once usage reaches it, and clear once usage is below 95 % of it. The leader proposes only changes:
  - `SetQuota`, metadata change tag 38;
  - `QuotaFull`, tag 39;
  - the limits are in snapshot section 21.

  Usage is not written to the log; the leader keeps its last count for `DescribeQuotas` and `/metrics`.

### Enforcement

- **Who refuses:** the leader of a group refuses a batch new to it with `QUOTA_EXCEEDED` while its resource's tenant limit, or a prefix limit its name starts with, is recorded full. The message names the limit. The check sits in the gate a low disk uses ([ADR 0122](0122-a-node-low-on-disk-stops-new-data-in-its-groups.md)), after the duplicate check: a retry of a taken batch is answered as written, and a refused batch's sequence stays free.
- **What is refused:** produces, transactional ones too, and the writes a node makes for a `Publish`. Forwarded dead letters stay owed in their source.
- **What passes:** reads, acks, markers, checkouts, timers, retirements, metadata changes, and creating, changing and deleting resources. A tenant can always drain and delete its way back.
- **Clients:** `QUOTA_EXCEEDED` is not retriable. The Rust client and the SDKs end the produce or publish at once with the broker's message.

### Requests

- **`AlterQuota`** (key 69), answered with an `AdminResponse`: tenant, prefix, bytes.
  - It needs cluster version 0.2, or it is refused with `UNSUPPORTED_VERSION`.
  - Any node judges the role ([ADR 0039](0039-tenants-and-roles.md)); the metadata leader judges the record of who set the limit, and the count of prefix limits.
- **`DescribeQuotas`** (key 70): every limit of the tenants the asker may see, each such tenant without a limit of its own, whether each is full, and the usage and time of the last count.
  - Only the metadata leader answers, since it holds the count; any other node answers `NOT_LEADER`.
  - It is not a zone read ([ADR 0141](0141-reads-in-the-client-s-zone-with-read-your-writes.md)).

### Around it

- **Observability:** every node reports `lotse_quota_bytes` and `lotse_quota_full` per limit; the metadata leader also reports `lotse_quota_used_bytes` per limit and per tenant.
- **`lotse mcp`:** `describe_quotas` and `set_quota`, which needs no `confirm`, since a limit deletes nothing.
- **Clients:** the Rust client has `alter_quota` and `describe_quotas`; the SDKs follow.
- **Upgrades:** the formats are written only from cluster version 0.2 ([ADR 0127](0127-rolling-upgrades-with-a-cluster-version-and-restart-turns.md)), the first in release 0.2.0.
- **Freeing space:** draining frees space because segments also close by age ([ADR 0146](0146-segments-close-by-age.md)).

## Consequences

- **One tenant no longer fills the disks** that hold the other tenants' data.
- **Overshoot:** a tenant may go over its limit by what it writes in about two check intervals, and by more while no metadata leader counts. Space frees only as whole segments go, within about their age after draining.
- **Throughput is not limited yet.** Byte-rate, connection and request-rate quotas through `throttle_ms` are the next step.
- **Mutation-checked:**
  - **Counting:**
    - a group counted once instead of per voter;
    - only the longest prefix counted;
    - a limit clearing at 100 %;
    - a missing size counted as 0 (the `quota` unit tests).
  - **The admission gate:**
    - the gate ignoring the record;
    - prefix limits ignored;
    - dead letters ignoring limits;
    - the check before the duplicate test (`a_full_storage_limit_refuses_only_new_data_under_it`).
  - **The metadata state:**
    - a deleted tenant keeping its limits;
    - no cap on prefix limits;
    - the snapshot section written before 0.2.
  - **Access:**
    - a tenant admin changing a cluster admin's limit;
    - the binding prefix ignored;
    - a tenant admin of the whole tenant setting its own limit.
  - **The node and the simulation:**
    - the node showing no full limits;
    - the leader recording none;
    - a tenant's limit refusing every tenant (`tests/quotas.rs`, and the quota simulation, which the release workflow runs at 5,000 seeds);
    - the client retrying `QUOTA_EXCEEDED`.
- **Not mutation-checked:** the planner's two guards against counting from missing sizes. Each alone keeps a new leader from clearing a limit, so neither fails a test alone; the unit test pins one.
- **A principal without a role in a tenant** gets `UNAUTHORIZED` for an `AlterQuota` there, as for a create, not `UNKNOWN_RESOURCE`.

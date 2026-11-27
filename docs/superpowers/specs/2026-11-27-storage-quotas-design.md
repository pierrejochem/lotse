# Storage quotas: design

Status: draft for the maintainer's approval (builder check done)
Date: 2026-11-27

## Intent

Lotse is meant to run many tenants on one cluster, and making it enterprise ready starts with keeping one tenant from hurting the others. Today nothing limits how much a tenant stores. A tenant that fills a node's volume drops it below `[disk] free_limit`. Then every group with a voter on that node refuses new data with `STORAGE_FULL` (ADR 0122), whichever tenant it belongs to. One tenant's backlog can stop everyone's writes.

Storage quotas give every tenant a ceiling, so it fills its own allowance and not the disks. Limits on name prefixes inside a tenant let a tenant admin divide that allowance among applications. That is the closest thing to "per user" the storage model allows: a group's size does not say who wrote it.

The design document (`Flow Control & Quotas`) and ADR 0011 already plan storage quotas per tenant. The protocol already reserves `QUOTA_EXCEEDED` (error 10), and the console already has `quota_bytes` and `used_bytes` per tenant, which only the demo cluster fills in. This change builds them and settles what was open.

Throttling (`throttle_ms`, byte-rate, connection and request-rate quotas) is the next step, not this one.

## Goals

- A cluster admin sets a storage limit per tenant, in bytes, or removes it.
- A cluster admin, or a tenant admin of that tenant, sets limits on name prefixes inside the tenant.
- Usage counts every replica's bytes on disk, streams and queues alike.
- When a limit is reached, new data into the resources it covers is refused with `QUOTA_EXCEEDED`, and clients do not retry it. Everything else keeps working.
- A limit clears below 95 % of itself.
- Usage, limits and which limits are full are visible in an admin read, on `/metrics`, in the console, in `lotse mcp`, and in the Rust client and all three SDKs.

## Non-goals

- Throughput, connection and request-rate quotas, and `throttle_ms`: the next change.
- Storage counted per writing principal. The maintainer chose per name prefix instead; per-writer accounting would need per-segment records.
- Quotas on the number of resources. The design document already rules these out.
- Exact enforcement. Usage is counted periodically, so a tenant may overshoot by what it writes between two counts. Space also frees only as whole segments go, so after draining it frees within about the segment age.

## Anti-goals (a failure even if it works)

- A full tenant stops another tenant's writes, or stops its own reads, acks or deletes. A tenant must always be able to drain and delete its way back under its limit. Draining works within about the segment age (decision 11).
- A metadata leader change, a lost metrics answer or a node that is down clears a full limit, or marks one full, from missing data.
- Any message is lost or duplicated because of a refusal. A refused batch is not written, and a retry of a batch already taken still counts as written, as for `STORAGE_FULL` and `QUEUE_FULL`.
- A tenant admin raises their tenant past what the cluster admin set.

## Decisions (each taken by the maintainer, one at a time)

1. **Who sets a tenant's limit:** a cluster admin only. Tenant admins see it but cannot change it.
2. **What counts:** every replica's bytes on disk. A resource with replication 3 uses three times its data.
3. **A full tenant:** new data is refused with `QUOTA_EXCEEDED` and fails at once. It is not retriable, and the client gives up with the refusal instead of waiting out its retry time.
4. **What still works:** everything but new data. That means reads, acks, nacks, transaction markers, checkouts, timers, retirements, metadata changes, and creating, changing and deleting resources. The console demo's refusal of new queues in a full tenant goes.
5. **When it clears:** once usage is below 95 % of the limit.
6. **Counting interval:** a setting, `[quotas] check_interval_ms`, default 10,000 ms. That is the existing size poll of ADR 0052, which runs every 1,000 ticks; its interval becomes this setting.
7. **Narrower limits:** built now, not later.
8. **What "narrower" means:** a limit on the resources whose names start with a prefix, within one tenant, like a role binding's prefix.
9. **Who sets a prefix limit:** a cluster admin, or a tenant admin of that tenant. A prefix limit only narrows: the tenant's limit always applies as well.
10. **Overlapping prefixes:** a resource counts against every prefix limit its name matches. A write needs room in the tenant and in each of them.
11. **The way back from full is draining too, so segments also close by age.** Today a segment closes only at `segment_bytes` (128 MiB), and retention and queue trimming cut only at closed segments. A full tenant writes no new data, so nothing would ever be freed. A segment now also closes once its first batch is older than the segment age:
    - the default is 1 hour;
    - the cluster file sets it for a node (`[cluster] segment_ms`);
    - a runtime stream or queue sets it with `segment.ms`, like `retention.ms`.

    This is a log change with its own ADR.
12. **The release becomes 0.2.0 in this change.** That covers `Cargo.toml`, the Helm chart, every SDK (`packaging/sdk-versions.sh`) and `docs/releases/0.2.0.md`. So `QUOTAS_SINCE = 0.2` is reachable, quotas work end to end, and the 0.1 window is tested. A 0.2 node runs beside 0.1 nodes (ADR 0127).
13. **A prefix limit set by a cluster admin** can be changed or removed only by a cluster admin. Each limit records whether a cluster admin set it. A tenant admin changes only the prefix limits tenant admins set.
14. **At most 100 prefix limits per tenant.**
15. **A limit may be set below current usage;** it becomes full at the next count and stops growth at once.
16. **The console warns from 80 % of a limit.**

## Approaches considered

- **Chosen: the metadata leader counts and records; group leaders enforce.** This mirrors the low disk of ADR 0122 and ADR 0124.
  1. The metadata leader already collects every group's size from every node.
  2. It adds those sizes up per tenant and per prefix limit.
  3. It records which limits are full in the metadata state, through Raft.
  4. Every group leader checks that record at the point where it already checks for a low disk.

  The state is replicated, survives leader changes and is the same on every node.
- **Rejected: each leader judges on its own.** A node knows only the groups it hosts, never a tenant's total across the cluster.
- **Rejected: each partition gets a share of the limit up front.** That needs constant rebalancing of every partition's share, and an idle partition wastes its share.

## How it works

### Limits

- **What a limit is:** a tenant, a prefix (empty for the tenant itself) and a number of bytes, at least 1. Setting no number removes the limit.
- **Where limits live:** in the metadata state, so they are in its snapshot.
- **Deleting a tenant** removes its limits. A tenant created again starts with none (incarnations, ADR 0069).
- **At most 100 prefix limits per tenant** (decision 14). One more is refused with `INVALID_REQUEST` and a message.
- **A limit below current usage** is allowed, and the limit becomes full at the next count (decision 15).
- **A prefix limit above the tenant's limit** is allowed; the tenant's limit still applies first.
- **A prefix limit on a tenant without a limit** is allowed.

### Counting

- **When:** after every round of the size poll, which runs every `check_interval_ms`.
- **What:** for each limit, the sum over the groups of the resources it covers of the group's size times the number of voters the metadata state lists for that group. Replication counts.
  - A group's size is the largest log size any replica reported, the same reading the rebalancer takes today (ADR 0052). It is log segment bytes (`Log::size_bytes`); snapshot and index files are not counted.
  - Learners catching up during a move, and replicas a node runs until a move off it settles, do not count, so the cluster's own rebalancing never pushes a tenant over its limit.
  - A tenant limit covers all the tenant's runtime resources.
  - A prefix limit covers those whose name starts with its prefix.
  - The cluster's own groups (metadata, coordinator) are not counted.
- **A node that did not answer:** another replica of the same group gives the size, so a node that is down does not stop counting. A group no replica reported keeps its last known size on this leader. A leader new to the role does not count it until a replica reports, and keeps its limits' recorded state meanwhile. Missing data never shrinks usage to zero, and it never clears a full limit.
- **A metadata leader new to the role:** it proposes nothing until its first complete poll round. A full limit stays full until a count shows it below 95 %.
- **The record:** the metadata leader proposes a change marking a limit full when usage reaches the limit, and clear when usage falls below 95 % of it. Only changes are proposed, never a record per count.
  - Usage itself is not written to the log; it lives on the metadata leader.
  - For reads and `/metrics`, the metadata leader keeps the usage of its last count, with the time of that count.

### Enforcement

- **Who refuses:** the leader of a group refuses a new data batch with `QUOTA_EXCEEDED` when the resource's tenant limit, or any prefix limit its name matches, is recorded full in that leader's applied metadata state.
- **Where it is checked:** through the same gate as the low disk, `Replica::propose_admitted`, and only for a batch that is new after the duplicate check. A retry of a taken batch is answered as written.
- **What is refused:**
  - produces, transactional ones included;
  - the writes a node makes for a `Publish`;
  - dead letters forwarded into the resource, which stay owed in their source queue as with `STORAGE_FULL`.
- **What passes:** everything else in decision 4.
- **The answer's message** names which limit is full: the tenant's, or the prefix.
- **Clients:** `QUOTA_EXCEEDED` is not retriable. The Rust client and the SDKs end the produce or publish with an error naming the quota. A transactional producer that meets it ends the way it does for any other non-retriable produce error.
  - A publish reports the queues refused, as for other per-queue errors (ADR 0079).
  - The sequence of a refused idempotent batch stays free, as for `STORAGE_FULL`.

### Admin requests

Two new requests, keys 69 and 70 (names proposed):

- **`AlterQuota`:** tenant, prefix (empty for the tenant) and bytes (absent to remove). It goes to the metadata leader.
  - Access: a tenant limit needs cluster admin. A prefix limit needs cluster admin, or tenant admin in that tenant with a binding prefix that covers the limit's prefix. A prefix limit a cluster admin set needs cluster admin to change or remove (decision 13); `DescribeQuotas` says which kind each limit is.
  - A tenant that is unknown or invisible to the principal is answered `UNKNOWN_RESOURCE`, as elsewhere (ADR 0039).
- **`DescribeQuotas`:** optional tenant. For each limit: tenant, prefix, bytes, whether it is full, and the usage of the last count with its time. A tenant without limits lists its usage too, with no limit, so an admin can choose one.
  - Access: cluster admin and cluster viewer see everything; anyone with a role in a tenant sees that tenant's.
  - It is answered by the metadata leader, which holds the usage. A node that is not the leader answers `NOT_LEADER`, so the request does not take part in the zone reads of ADR 0141.

### Segments close by age

- **The roll:** a replica closes its active segment once the segment's first batch was appended longer ago than the segment age, by the replica's `Clock`, checked on its tick. The segment is closed even when no new batch arrives. The next append starts a new segment.
- **What then frees space:**
  - stream retention by age, which keeps whole segments (ADR 0046);
  - queue trimming below the ack floor;
  - compaction, which rewrites sealed segments.
- **Retention cuts at snapshot points** where applying reaches a segment base. The builder checks that a segment closed by age, followed by no new data, can still be cut. If it cannot, that is part of this work, with a test.
- **Settings:**
  - `[cluster] segment_ms` (node default, 1 hour);
  - `segment.ms` per runtime stream or queue through `CreateStream`, `CreateQueue`, `AlterConfigs` and `DescribeConfigs`, at least 60,000 ms.
  - Whether a per-resource value needs the cluster version: it is a config entry, read either way. The builder checks how runtime configs are stored and gates them if their format changes.
- **Tests:**
  - a log unit test: a segment closes by age with no new appends;
  - a retention simulation case: a drained queue and an aged stream free their space within the segment age plus a counting interval.
- **Mutation-checked:** a log that never rolls by age must fail a test.
- **Its own ADR**, the log section of `docs/design.md`, and `lotse mcp` `alter_configs` (no `confirm` needed: closing segments sooner deletes nothing by itself).

### Configuration

- **`[quotas] check_interval_ms`:** 1,000 to 600,000, default 10,000. It is the interval of the metadata leader's size poll, which ADR 0052 and ADR 0100 also use. Lower means less overshoot and more polling.
- **Simulations** keep the size poll's current 1,000 ticks unless a test sets it.

### Observability

- **`/metrics`:** every node exports the limits and which are full, from its applied state: `lotse_quota_bytes{tenant,prefix}` and `lotse_quota_full{tenant,prefix}`. The metadata leader also exports `lotse_quota_used_bytes{tenant,prefix}` for every tenant, from its last count.
- **`DescribeMetrics`:** nothing new beyond what the builder needs.

### Release 0.2.0

This change raises the workspace and every SDK to 0.2.0 and adds `docs/releases/0.2.0.md`, with storage quotas and segment age as its first entries. `CLUSTER_VERSION` becomes 0.2 and `OLDEST_CLUSTER_VERSION` 0.1, so a 0.2 node runs beside 0.1 nodes until `lotse upgrade finalize`.

### Upgrades

- **When the new formats are written:** the limit and full records, and the snapshot section that holds them, only while the cluster version in force is at least `QUOTAS_SINCE`. That is the next minor release's cluster version, 0.2. They are read either way.
- **Before that:** `AlterQuota` is refused with a message naming the cluster version it needs.
- **Test:** one runs with 0.1 in force (Conventions in AGENTS.md: formats follow the cluster version).

### Console

- **Tenant page (`Tenants.tsx`):**
  - each tenant's limit, usage, and prefix limits;
  - a warning from 80 % of a limit (decision 16), and a full state at 100 %.
- **Who can change what:** cluster admins set and remove tenant limits, and tenant admins set and remove prefix limits, all as themselves (rule 8). Follow ADR 0140 and the v2 mockups.
- **The create-queue dialog** no longer refuses in a full tenant. It may still say the tenant is full, since writes will be refused.

### `lotse mcp`

- **`describe_quotas`** (look) and **`set_quota`** (change: tenant, prefix, bytes or none).
- No `confirm`: changing a limit deletes no data.
- Tested in `crates/lotse-mcp/tests/tools.rs`.

### Clients and SDKs

- **New calls:** `alter_quota` and `describe_quotas` in `lotse-client`; the same in the Java SDK, the Kotlin layer (as `suspend`), the C# SDK and the Node.js SDK.
- **Errors:** each SDK names `QUOTA_EXCEEDED` as not retriable.
- **Shared test data:** golden vectors regenerated with `make sdk-vectors`; `sdk/PROTOCOL.md` describes the requests and the error.

### Documentation

- An ADR (next number) for the decisions above, superseding the open parts of ADR 0011.
- `docs/design.md` (German): the Flow Control & Quotas section and the field layouts.
- `docs/status.md`: the gap closed, and the overshoot limit under "Limits by design".
- `AGENTS.md`: the current-state sentence.
- The admin guide: setting quotas.

## Testing

- **Unit tests:**
  - counting: replicas counted, overlapping prefixes, last known sizes kept, the 95 % clearing;
  - the admission gate;
  - access for `AlterQuota` and `DescribeQuotas`;
  - the cluster version gate.
- **A simulation in `crates/lotse-server/tests/`** with a low limit and a small counting interval, under faults (crashes, partitions, metadata leader changes). Producers, a publisher and a queue consumer run in two tenants, one with a limit. The oracle, written in the test, checks:
  - writes in the limited tenant are refused once its usage has passed its limit plus at most what it could write in two counting intervals;
  - the other tenant's writes are never refused with `QUOTA_EXCEEDED`;
  - reads and acks in the full tenant keep succeeding;
  - after the limit is raised, or data is deleted, writes are taken again;
  - no refused batch is in the log and no taken batch is lost, using the existing oracles;
  - a metadata leader change never clears a full limit without a count below 95 %.
- **Mutation checks** (rule 5). Each of these must make a test fail:
  - a tenant admin changes a prefix limit a cluster admin set;
  - the gate ignores the record;
  - a prefix limit is ignored;
  - replicas are counted once;
  - learners or settling replicas are counted;
  - a missing node's sizes count as zero;
  - a new metadata leader clears limits before counting;
  - the clearing threshold is 100 %;
  - a duplicate of a taken batch is refused.
- **Client tests:** in every SDK, `QUOTA_EXCEEDED` ends a produce without retrying.

## Left to the builder

- The exact wire layout of the two requests, the metadata change tags (the next free are 37 and up) and the snapshot section (20).
- The names of the requests, calls, tools and metrics, within the conventions.
- Whether the full limits are recorded as one change per limit or one change listing the full set, provided only changes are proposed.
- How the console lays out prefix limits, within ADR 0140 and the mockups.
- Splitting the work into commits and pull requests.

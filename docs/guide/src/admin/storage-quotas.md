# Storage quotas

A storage quota keeps one tenant from filling the disks that hold every tenant's data ([ADR 0147](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0147-storage-quotas-per-tenant-and-prefix.md)). It needs cluster version 0.2 (see [Upgrades](upgrades.md)).

## Limits

| Limit | Who sets it | Covers |
| --- | --- | --- |
| A tenant's own | a cluster admin | every stream and queue of the tenant |
| A name prefix | a cluster admin, or a tenant admin under the prefix of their binding | the tenant's streams and queues whose names start with the prefix |

- **Units:** a limit is in bytes, and every replica counts. A queue with three replicas uses three times its data.
- **Nesting:** a resource counts against its tenant's limit and against every prefix limit its name starts with, so `orders.` caps everything under `orders.eu.` too.
- **How many:** a tenant holds at most 100 prefix limits.
- **Ownership:** a prefix limit a cluster admin set can be changed only by a cluster admin. A tenant admin can only narrow what the cluster admin allows: the tenant's own limit always applies.

## When a limit is full

- **When it is full:** a limit is full once its usage reaches it.
- **What is refused:** new data under it, with `QUOTA_EXCEEDED`. That covers produces, publishes and dead letters forwarded into it, which wait in their source queue. Clients do not retry the error: the application learns at once, with a message naming the limit.
- **What keeps working:** reading, acknowledging, transactions ending, and creating, changing and deleting resources.
- **When it clears:** once usage falls below 95 % of the limit, at the next count. That happens when you raise or remove the limit, delete resources, or consumers drain queues. Space is freed one whole segment at a time; a segment closes by size or at its age, an hour by default, so a drained queue frees its space within about that ([Retention and compaction](retention-and-compaction.md)).
- **Setting a limit below current usage** is allowed. The limit is full at the next count, which stops a tenant from growing without deleting anything.

## Counting

The metadata group's leader counts usage every `quotas.check_interval_ms`, 10,000 ms by default:

```toml
[quotas]
check_interval_ms = 10000   # 1000 to 600000
```

- **Overshoot:** a tenant can go over its limit by what it writes in about two of these intervals. Lower values mean less overshoot and more polling of every node.
- **What counts:** log segments only; snapshot and index files do not count.

## Setting and reading limits

| With | Set | Read |
| --- | --- | --- |
| Rust client | `alter_quota(tenant, prefix, bytes)` | `describe_quotas(tenant)` |
| `lotse mcp` | `set_quota` | `describe_quotas` |
| The console | the *Tenants* page | the *Tenants* page |

- **An empty prefix** is the tenant's own limit. No `bytes` removes the limit.
- **What `DescribeQuotas` lists:** every limit of the tenants you may see, and each such tenant without a limit of its own, with the usage at the last count.

## Monitoring

| Metric | From | Meaning |
| --- | --- | --- |
| `lotse_quota_bytes{tenant,prefix}` | every node | the limit |
| `lotse_quota_full{tenant,prefix}` | every node | 1 while new data under the limit is refused |
| `lotse_quota_used_bytes{tenant,prefix}` | the metadata leader | usage at the last count, for every limit and every tenant (`prefix=""`) |

Alert on `lotse_quota_used_bytes / lotse_quota_bytes` well before 1; the console warns from 80 %.

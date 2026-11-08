# Monitoring

Every node answers `DescribeMetrics` for itself. Nothing is pushed and nothing needs a separate exporter ([ADR 0036](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0036-describemetrics-and-the-console-against-a-real-cluster.md)). To see the cluster, ask every node and combine the answers. The console does this; `lotse-client` has `node_metrics()`, which returns one answer or error per node. The answer shows what the principal may see; the cluster's own groups are shown to cluster admins and cluster viewers. A monitoring principal is meant to hold `cluster-viewer`.

## What a node reports

Counters run since the node started and only grow. A rate is the difference of two answers.

| Field | Unit | Meaning |
| --- | --- | --- |
| `node_id`, `version`, `uptime_ms` | none, none, ms | Which node, its version, time since start. |
| `connections` | count | Open client connections. |
| `client_in_bytes`, `client_out_bytes` | bytes | Traffic with clients. |
| `peer_in_bytes`, `peer_out_bytes` | bytes | Traffic with other nodes. |
| `commit_latency` | 17 buckets | Acknowledged produces by latency. Bucket `i` counts those under `2^i` ms; the last bucket counts all slower ones. |
| `host.cpu_permille` | per mille | CPU use over the last 5 s. |
| `host.memory_bytes`, `host.memory_limit_bytes` | bytes | Memory the node uses, and what is available to it. |
| `host.disk_free_bytes`, `host.disk_total_bytes` | bytes | Free and total space of the data volume. |

Host numbers are read every 5 s, not per request. A value is absent where the node cannot tell.

For every Raft group the node hosts (`partitions`):

| Field | Meaning |
| --- | --- |
| `group`, `resource_id`, `partition` | The group, the stream or queue it belongs to (0 for the metadata and coordinator groups), the partition. |
| `leader`, `term` | Whether this node leads, and the Raft term. |
| `high_watermark`, `last_stable`, `log_start`, `log_end` | Offsets: committed end, below the first open transaction, first stored, after the last in the log. |
| `size_bytes` | Bytes of the log on this node. |
| `produce_bytes`, `produce_records`, `fetch_bytes` | Counters, growing only while this node leads the group. Summing over all nodes counts each produce once. |
| `replicas` | On the leader: the last offset each replica holds, plus one. |

For each queue the node leads (`queues`): `ready`, `in_flight`, `in_open_txns`, the counters `delivered` and `acked`, and per subscription its `in_flight` and `credit`. On the coordinator's leader, `groups` lists consumer groups with their members, the partitions they own and the committed offsets.

## The console

The console (`kind = "native"`) asks every node and turns counters into rates. It keeps the samples of the last few minutes and takes each rate over the newest sample at least 5 s old. Until there is one, and for a node that restarted in between, there is no rate. It takes p99 commit latency from the difference of two histogram samples, to the bucket. Chart history gets coarser with age ([ADR 0045](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0045-chart-history-coarser-with-age.md)).

The console shows each user what their roles allow, and its metrics sampler signs in with client credentials. Charts sum only what the sampler may see; give it the `cluster-viewer` role so that one binding covers all tenants, also those created later ([ADR 0043](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0043-cluster-viewer-role.md), [ADR 0041](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0041-console-sessions-sampler-sign-in-and-charts-per-user.md)).

What a real cluster does not report shows as a dash: zones, disk limits, quotas. Not reported at all: the age of the oldest record, open transactions per stream, bytes in flight per consumer, which subscription of a single-active-consumer queue is active, and per-level counts of a priority queue.

## What to alert on

The repository ships no alert rules. These follow from the fields above; the thresholds are yours to choose.

- A node that does not answer `DescribeMetrics`.
- `host.disk_free_bytes` falling toward 0 on a data volume.
- A group with no leader on any node, or with `leader` flapping and `term` rising.
- A replica whose `log_end` in `replicas` lags the leader's for long.
- `commit_latency` moving to higher buckets.
- A queue whose `ready` grows while `consumers` is empty, or whose `in_flight` does not fall.
- A consumer group whose committed offsets fall behind `log_end`.
- Certificates within 30 days of expiry: run `lotse check` on each node.

Logs: set `LOTSE_LOG` for the filter (default `info`). A node logs "starting", "stopping", "stopped", "drained", and "TLS files changed: reloaded" with the number of revoked connections closed. It logs "TLS files do not load; keeping the material in use" once when new certificate files are broken.

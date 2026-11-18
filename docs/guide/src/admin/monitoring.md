# Monitoring

A node tells what it sees in two ways, and both need no separate exporter:

- Its operator's endpoint, `/metrics`, `/healthz`, `/readyz` and `/safe-to-stop` over HTTP, for Prometheus, Kubernetes and restart scripts ([ADR 0117](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0117-prometheus-metrics-and-health-endpoints-on-every-node.md)).
- `DescribeMetrics` over the native protocol, for the console and clients ([ADR 0036](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0036-describemetrics-and-the-console-against-a-real-cluster.md)).

## The operator's endpoint

Give a node `metrics = "host:port"` in its `[[node]]` entry. Without it, nothing listens.

```toml
[[node]]
id = 1
peer = "10.0.0.1:7000"
client = "10.0.0.1:9000"
metrics = "10.0.0.1:9464"
```

On a loopback address (`127.0.0.1:9464`) the endpoint speaks plain HTTP, and anyone on the machine may read all of it, for a Prometheus agent on each machine. Anywhere else it speaks only TLS 1.3, with the node's certificate; `lotse certs` puts the metrics host into new node certificates (not a wildcard such as `0.0.0.0`).

| Path | Answers | Who |
| --- | --- | --- |
| `/metrics` | OpenMetrics text: everything the node reports, with names. | Over TLS, a client certificate from the cluster CA whose principal holds `cluster-viewer` or `cluster-admin`, and which no cluster admin revoked. Anyone else gets 403. |
| `/healthz` | 200 `ok` while the node's main loop answers within 5 s, else 503. | Anyone. |
| `/readyz` | 200 `ready` once every group on the node is ready, else 503 and each group with its reason. | Anyone. |
| `/safe-to-stop` | 200 unless stopping the node now would leave a group without a quorum, else 503 and the groups. | Anyone. |

A group is ready on a node that leads it with a quorum that answered within an election timeout, or that follows a leader it hears and keeps up with, the rule followers serve reads by (ADR 0116). `/safe-to-stop` asks every node: for each group the node votes in, the newest leader's list of members that answered lately, without this node, must still be a quorum. A group no node reports leading counts as not safe; one whose only voter is this node does not count, since waiting would not help. The health paths name group and node ids only, so a Kubernetes HTTPS probe needs no certificate.

Prometheus scrapes each node with a client certificate. Make one and bind it:

```sh
lotse certs --config cluster.toml --client prometheus --out prometheus/
```

Then bind the user `prometheus` to `cluster-viewer`, in the console or with `AlterRoleBindings`.

```yaml
scrape_configs:
  - job_name: lotse
    scheme: https
    tls_config:
      ca_file: /etc/prometheus/lotse/ca.pem
      cert_file: /etc/prometheus/lotse/prometheus.pem
      key_file: /etc/prometheus/lotse/prometheus.key
    static_configs:
      - targets: ["10.0.0.1:9464", "10.0.0.2:9464", "10.0.0.3:9464"]
```

In Kubernetes:

```yaml
readinessProbe:
  httpGet: { path: /readyz, port: 9464, scheme: HTTPS }
livenessProbe:
  httpGet: { path: /healthz, port: 9464, scheme: HTTPS }
  failureThreshold: 6
```

A rolling restart waits for every node's `/readyz` and for the next node's `/safe-to-stop` before it stops that node. `/readyz` is 503 on every node while a group has no leader anywhere.

### Metrics

Every name starts with `lotse_`; every sample has the label `node`. Partition metrics carry `group`, `kind` (`stream`, `queue`, `metadata` or `coordinator`), `tenant`, `resource` and `partition`. Counters run since the node started, and a group's counters grow only while this node leads it, so summing over nodes counts each produce once.

| Metric | Type | Meaning |
| --- | --- | --- |
| `lotse_node_info{version}` | info | The node and its version. |
| `lotse_node_uptime_seconds` | gauge | Time since start. |
| `lotse_node_ready` | gauge | 1 while `/readyz` would answer 200. |
| `lotse_node_connections` | gauge | Open client connections. |
| `lotse_certificate_requests_pending` | gauge | Client certificate requests waiting for the CA holder, as this node has applied them ([ADR 0119](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0119-users-request-certificates-in-their-own-name.md)). |
| `lotse_client_received_bytes_total`, `lotse_client_sent_bytes_total` | counter | Traffic with clients. |
| `lotse_peer_received_bytes_total`, `lotse_peer_sent_bytes_total` | counter | Traffic with other nodes. |
| `lotse_commit_latency_seconds` | histogram | Produces by time to acknowledgement, at the leader; buckets at powers of two in ms. |
| `lotse_host_cpu_ratio`, `lotse_host_memory_bytes`, `lotse_host_memory_limit_bytes`, `lotse_host_disk_free_bytes`, `lotse_host_disk_total_bytes` | gauge | The host, read every 5 s; absent where the node cannot tell. |
| `lotse_partition_leader` | gauge | 1 while this node leads the group with a quorum. |
| `lotse_partition_term` | gauge | The Raft term here. |
| `lotse_partition_high_watermark`, `lotse_partition_last_stable_offset`, `lotse_partition_log_start_offset`, `lotse_partition_log_end_offset` | gauge | Offsets, as in `DescribeMetrics`. |
| `lotse_partition_size_bytes` | gauge | Bytes of the log on this node. |
| `lotse_partition_produced_bytes_total`, `lotse_partition_produced_records_total`, `lotse_partition_fetched_bytes_total` | counter | While this node led. |
| `lotse_partition_voters` | gauge | Voters of the group. |
| `lotse_partition_responsive_voters` | gauge | On the leader: voters that answered within an election timeout. |
| `lotse_partition_under_replicated` | gauge | On the leader: 1 while a voter has not. |
| `lotse_replica_log_end_offset{replica}` | gauge | On the leader: what each replica holds. |
| `lotse_queue_ready_messages`, `lotse_queue_in_flight_messages`, `lotse_queue_in_open_transactions_messages`, `lotse_queue_consumers` | gauge | On the queue's leader; labels `tenant`, `queue`. |
| `lotse_queue_delivered_messages_total`, `lotse_queue_acked_messages_total` | counter | While this node led the queue. |
| `lotse_consumer_group_members`, `lotse_consumer_group_epoch`, `lotse_consumer_group_committed_offset{resource,partition}` | gauge | On the coordinator's leader; labels `tenant`, `consumer_group`. |

## DescribeMetrics

To see the cluster through the native protocol, ask every node and combine the answers. The console does this; `lotse-client` has `node_metrics()`, which returns one answer or error per node. The answer shows what the principal may see; the cluster's own groups are shown to cluster admins and cluster viewers. A monitoring principal is meant to hold `cluster-viewer`.

### What a node reports

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

### The console

The console (`kind = "native"`) asks every node and turns counters into rates. It keeps the samples of the last few minutes and takes each rate over the newest sample at least 5 s old. Until there is one, and for a node that restarted in between, there is no rate. It takes p99 commit latency from the difference of two histogram samples, to the bucket. Chart history gets coarser with age ([ADR 0045](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0045-chart-history-coarser-with-age.md)).

The console shows each user what their roles allow, and its metrics sampler signs in with client credentials. Charts sum only what the sampler may see; give it the `cluster-viewer` role so that one binding covers all tenants, also those created later ([ADR 0043](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0043-cluster-viewer-role.md), [ADR 0041](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0041-console-sessions-sampler-sign-in-and-charts-per-user.md)).

What a real cluster does not report shows as a dash: zones, disk limits, quotas. Not reported at all: the age of the oldest record, open transactions per stream, bytes in flight per consumer, which subscription of a single-active-consumer queue is active, and per-level counts of a priority queue.

## What to alert on

The repository ships no alert rules. These follow from the metrics above; the thresholds are yours to choose.

- A node whose scrape fails (`up == 0`), or `lotse_node_ready == 0` for longer than a restart takes.
- `lotse_host_disk_free_bytes` falling toward 0 on a data volume.
- `lotse_partition_under_replicated == 1` for longer than a minute.
- A group with no `lotse_partition_leader == 1` on any node, or with `lotse_partition_term` rising fast.
- A replica whose `lotse_replica_log_end_offset` lags the leader's `lotse_partition_log_end_offset` for long.
- `lotse_commit_latency_seconds` moving to higher buckets: `histogram_quantile(0.99, sum by (le) (rate(lotse_commit_latency_seconds_bucket[5m])))`.
- A queue whose `lotse_queue_ready_messages` grows while `lotse_queue_consumers` is 0, or whose in-flight count does not fall.
- A consumer group whose committed offsets fall behind the partition's high watermark.
- Certificates within 30 days of expiry: run `lotse check` on each node.
- Certificate requests waiting for the CA holder: `max(lotse_certificate_requests_pending) > 0` for an hour or so, routed to whoever holds the CA key. A request lapses after 7 days.

Logs: set `LOTSE_LOG` for the filter (default `info`). A node logs "starting", "stopping: handing the groups this node leads to other voters", "stopped", "drained", and "TLS files changed: reloaded" with the number of revoked connections closed. It logs "TLS files do not load; keeping the material in use" once when new certificate files are broken.

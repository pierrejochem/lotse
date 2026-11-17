# 117. Prometheus metrics and health endpoints on every node

- Status: Accepted (carries out [ADR 0021](0021-metrics-admin-api-for-the-console-prometheus-for-operations.md); refines [ADR 0036](0036-describemetrics-and-the-console-against-a-real-cluster.md))
- Date: 2026-11-17

## Context

ADR 0021 decided that every node serves a Prometheus endpoint for Grafana and alerting, not filtered by tenant. It was never built. ADR 0036 then said nothing needs a separate exporter: an operator could ask every node with `DescribeMetrics`. That needs a native-protocol client and a role. The console keeps 24 hours and does not alert.

Operators also had nothing to point a supervisor at:

- Kubernetes takes a pod out of its Service until it is ready, and waits for readiness between pods in a rolling restart.
- A rolling restart should not stop a node while another is down, if the two hold a quorum of some group between them. RabbitMQ answers this with its `node-is-quorum-critical` health check. Kafka has the `UnderReplicatedPartitions` metric and controlled shutdown.

## Decision

**Every node can serve `/metrics`, `/healthz`, `/readyz` and `/safe-to-stop` over HTTP** (maintainer's choices: Prometheus with health checks; TLS off loopback; scrapers authorised by role; readiness plus a safe-to-stop check).

- **Where.** `metrics = "host:port"` in a node's `[[node]]` entry. Without it, nothing listens.
  - On a loopback address the endpoint speaks plain HTTP, and whoever can reach it may read everything: they are on the node's machine. This suits a Prometheus agent on each machine.
  - Anywhere else it speaks TLS 1.3 with the node's certificate. The node never serves plain HTTP off loopback. `lotse certs` adds the metrics host to the node certificate unless it is a wildcard address.
  - Certificates the node reloads (ADR 0060) reach the next handshake.
- **`/metrics`: OpenMetrics text.**
  - The endpoint renders the node's full `DescribeMetrics` answer, unfiltered, with names. Every metric starts with `lotse_`, and partitions are labelled by group, kind, tenant, resource and partition.
  - Counters behave as in `DescribeMetrics`: they run since the node started, and a group's grow only while the node leads it. Commit latency is a histogram in seconds.
  - New are `lotse_node_ready`, `lotse_partition_voters`, `lotse_partition_responsive_voters` and `lotse_partition_under_replicated`, which is 1 on the leader while a voter has not answered within an election timeout.
- **Who may read `/metrics`.** It names every tenant's resources, as ADR 0021 says, so over TLS the scraper shows a client certificate from the cluster CA.
  - The principal it names must see the whole cluster: bound to `cluster-viewer` or `cluster-admin`, as for every other request (ADR 0039, ADR 0043). It is checked against the node's applied metadata.
  - A certificate a cluster admin revoked is refused (ADR 0090). So is a node certificate, and a principal without such a role (403).
  - A cluster with `auth.required = false` lets anyone read, as it lets anyone do anything else.
- **`/healthz`**: 200 while the node's main loop answers within 5 s.
- **`/readyz`**: 200 once every group the node hosts is ready, else 503 with each group's reason. A group is ready when the node:
  - leads it, and a quorum answered within an election timeout (ADR 0095); or
  - follows a leader it hears and keeps up with, by the rule followers serve reads by (ADR 0116).
- **`/safe-to-stop`**: 200 unless stopping this node now would leave a group without a quorum, else 503 with the groups.
  - The node asks every node for `DescribeMetrics`.
  - From each group it votes in, it takes the newest leader's list of members that answered within an election timeout. Without this node, those must still be a quorum of every configuration in force.
  - A group none of the answers leads counts as critical: a rolling restart waits rather than guesses.
  - A group whose only voter is this node is left out. It stops with the node whenever that happens, and waiting would not help.
- **Probes need no certificate.** `/healthz`, `/readyz` and `/safe-to-stop` carry group and node ids, no names. The TLS listener asks for a client certificate but does not require one, so a Kubernetes HTTPS probe gets through.
- **What carries it.**
  - The Raft core keeps when each member last answered the leader (`responsive`).
  - `DescribeMetrics` gains tag 2, `responsive`: per group a node leads with a quorum, its term and the members heard lately. Nodes need it to judge `/safe-to-stop`. A principal that sees the whole cluster gets it too; others do not.
  - `lotse-rt-tokio` has a small HTTP/1.1 listener for GET and HEAD, one request per connection, at most 32 at once.

Rejected options:

- **Plain HTTP anywhere**, as RabbitMQ's port 15692 and the JMX exporter do: it relies on the network to keep tenant names private, against rule 7.
- **Any certificate from the cluster CA.** Tenant users hold such certificates and would read every tenant's names.
- **A list of scraper principals in the cluster file.** It sits outside the role model and is not revoked with it.
- **A bearer token in the cluster file.** One more secret to rotate.
- **Readiness that only means "listening".** A node still catching up after a restart would count as ready, and a rolling restart would move on too early.
- **A separate exporter process speaking the native protocol.** One more thing to deploy, and ADR 0021 put the endpoint on the node.

## Consequences

- Prometheus scrapes each node directly, and Grafana and Alertmanager work from the scraped data. Under-replication, a node that is not ready, disk space and commit latency alert without the console.
- A Kubernetes StatefulSet can use `/readyz` as its readiness probe and `/healthz` as its liveness probe. A rolling restart script waits for `/safe-to-stop` before it stops the next node.
- Metric names and labels are now a contract, documented in the monitoring guide.
- `/readyz` is 503 on every node while a group has no leader anywhere. A Service then has no endpoints for that time; clients learn the leader through metadata anyway.
- The endpoint is optional and off by default. The packages and the image ship it off.
- Tests:
  - Raft and proto unit tests cover `responsive` and the new tag.
  - Unit tests of `ops` cover the quorum judgement (joint configurations, a deposed leader, learners, a group of one), the readiness rule and the OpenMetrics output. The output was also checked against the OpenMetrics parser of `prometheus_client`.
  - The scenario `nodes_say_whether_they_are_ready_and_may_stop` runs three nodes. All are ready and may stop. With one down, the others are ready and none may stop. A node cut off from the others has no group ready, and after healing all may stop again.
  - `lotse-node`'s `tests/metrics.rs` serves the endpoint over TLS from a cluster file. It checks the health answers without a certificate, and that `/metrics` serves a bootstrap admin and a principal once it is bound to `cluster-viewer`. It is refused without a certificate, for a principal without a role, for a node certificate and for a revoked certificate.
  - `lotse-rt-tokio`'s `tests/serve.rs` checks that plain HTTP is refused off loopback.
  - The access test checks that a tenant's principals are not given `responsive`.
- Mutation-checked:
  - members counted as answering past the election timeout;
  - this node counted in its own quorum;
  - each arm of the readiness rule;
  - each of the scraper rules (role, revocation, node certificate, anonymous only without authentication);
  - plain HTTP off loopback;
  - `responsive` given to a tenant's principals.

  Not caught: passing "not lagging" for every follower into the readiness rule. The lag set itself is checked by ADR 0116's tests.

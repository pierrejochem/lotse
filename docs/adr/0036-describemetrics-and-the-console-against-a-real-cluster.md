# 36. DescribeMetrics and the console against a real cluster

- Status: Accepted
- Date: 2026-10-28

## Context

The console ran only against a built-in demo cluster. Everything it needs to list and change now exists in the native protocol (ADRs 0034, 0035), but its screens are built around numbers no node reported: throughput, commit latency, node CPU, memory and disk, queue depth and consumers, consumer group lag. ADR 0021 put those behind an admin key, `DescribeMetrics`, without saying who answers, how rates come about or what a node measures. Some of what the demo shows has no counterpart in Lotse at all yet: tenants, zones, disk limits, an operator running upgrades.

## Decision

**`DescribeMetrics` (29) is answered by every node for itself** (maintainer's choice). A node reports its traffic and open connections, every Raft group it hosts (offsets, log size, term, and on the leader every replica's progress), the queues it leads (ready, in flight, in open transactions, consumers), and, on the coordinator's leader, the consumer groups with their members and committed offsets. The console asks every node the metadata names and combines the answers; a node that does not answer shows as such. No node-to-node request is involved.

**Nodes report running totals** (maintainer's choice). Bytes and records produced, bytes fetched, messages delivered and acknowledged, client and peer traffic: counters since the node started, which only grow. A group's counters grow only while the node leads it, so the sum over all nodes counts each produce once. The console keeps the samples of the last few minutes and takes each rate over the newest sample at least 5 s old; until there is one, and for a node that restarted in between, there is no rate.

**Commit latency is a histogram** (maintainer's choice): per node, acknowledged produces in 17 buckets, bucket `i` for under `2^i` ms. The console takes p99 from the difference of two samples, to the bucket.

**Host numbers come through the runtime seam** (maintainer's choice). A new `Host` trait in `lotse-rt` reads CPU use, the process's memory and its limit, and the free and total space of the data volume; `lotse-rt-tokio` reads them with `sysinfo` (0.36, the newest that builds on `rust-version` 1.85), the simulation returns fixed values. A node reads them every 5 s, not per request, so CPU use covers the last 5 s. Nothing may decide anything from them.

**The console has a native cluster.** `kind = "native"` with `bootstrap` addresses connects through `lotse-client` on Tokio. Values a real cluster does not report are `null` in the API and `—` in the UI with a note saying why; a cluster without tenants hides the tenants screen, and one without queue options gets a create-queue dialog for plain queues. Records are read with `read_committed` or, new in the client, past the last stable offset.

**No credentials yet.** Lotse has no authentication, so the console connects without credentials and the cluster cannot tell users apart. The console gets no account of its own: once brokers authenticate, the user's identity goes with each request (ADR 0019).

## Consequences

- The console works against a real cluster, and `cargo run -p lotse-console --example local_cluster` starts three nodes with it.
- Every node answers every console poll; with one poll per topic and user (ADR 0019), that is cheap at the cluster sizes in sight. Many consoles or users multiply it.
- Rates need a few seconds after the console starts; history fills from then on, as before.
- Metrics are not filtered by tenant, since there are none; ADR 0021's tenant scoping comes with tenants and authentication.
- Not reported yet: zones, disk limits, the age of the oldest record, open transactions per stream, bytes in flight per consumer, dead letters (Lotse has none yet).
- A test runs three nodes in the simulation and checks that every produce, fetch, delivery and ack is counted once and where, and that the queue's and group's numbers add up; another starts three real nodes on localhost behind the console's HTTP API. Finding that a queue without subscribers was not reported at all, the second test caught a bug in the first version.

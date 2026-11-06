# Architecture decision records

Each record states one decision: why it was needed, what was decided and what follows. Records are never rewritten to mean something else; a changed decision gets a new record that supersedes the old one ([ADR 0000](0000-record-architecture-decisions.md)). The [design document](../design.md) describes the current system; these records keep the reasons.

| # | Decision | Status |
| --- | --- | --- |
| 0000 | [Record architecture decisions](0000-record-architecture-decisions.md) | Accepted |
| 0001 | [One log engine for queues and streams](0001-one-log-engine-for-queues-and-streams.md) | Accepted |
| 0002 | [Multi-Raft with batching per node pair](0002-multi-raft-with-batching-per-node-pair.md) | Accepted |
| 0003 | [One broker process per node](0003-one-broker-process-per-node.md) | Accepted |
| 0004 | [Native protocol first, adapters later](0004-native-protocol-first-adapters-later.md) | Accepted |
| 0005 | [Exactly-once from the MVP](0005-exactly-once-from-the-mvp.md) | Accepted |
| 0006 | [Tokio behind a runtime seam](0006-tokio-behind-a-runtime-seam.md) | Accepted |
| 0007 | [Record format: fixed batch header and varint-delta records](0007-record-format-fixed-batch-header-and-varint-delta-records.md) | Accepted |
| 0008 | [Snapshots as state plus segment references](0008-snapshots-as-state-plus-segment-references.md) | Accepted |
| 0009 | [Own sans-I/O Raft core where the partition log is the Raft log](0009-own-sans-i-o-raft-core-where-the-partition-log-is-the-raft-log.md) | Accepted |
| 0010 | [Deterministic simulation and mutation checking](0010-deterministic-simulation-and-mutation-checking.md) | Accepted; seed count amended by 0027 |
| 0011 | [Flow control: throttle, never drop](0011-flow-control-throttle-never-drop.md) | Accepted |
| 0012 | [Priorities as sub-logs; single active consumer on the leader](0012-priorities-as-sub-logs-single-active-consumer-on-the-leader.md) | Accepted |
| 0013 | [Cooperative, broker-side consumer group rebalancing](0013-cooperative-broker-side-consumer-group-rebalancing.md) | Accepted; coordinator placement amended by 0033 |
| 0014 | [Permissions per queue and stream, never per record](0014-permissions-per-queue-and-stream-never-per-record.md) | Accepted |
| 0015 | [Membership changes through learners and joint consensus](0015-membership-changes-through-learners-and-joint-consensus.md) | Accepted |
| 0016 | [Elections: check-quorum always on, pre-vote optional](0016-elections-check-quorum-always-on-pre-vote-optional.md) | Accepted |
| 0017 | [Linearizable reads: ReadIndex by default, leases opt-in](0017-linearizable-reads-readindex-by-default-leases-opt-in.md) | Accepted |
| 0018 | [Leader transfer with TimeoutNow](0018-leader-transfer-with-timeoutnow.md) | Accepted |
| 0019 | [Management console on a dedicated node with the user's identity](0019-management-console-on-a-dedicated-node-with-the-user-s-identity.md) | Accepted |
| 0020 | [Console user groups, role mapping and audit](0020-console-user-groups-role-mapping-and-audit.md) | Accepted |
| 0021 | [Metrics: admin API for the console, Prometheus for operations](0021-metrics-admin-api-for-the-console-prometheus-for-operations.md) | Accepted |
| 0022 | [Brand: Lotse and the Fairway mark](0022-brand-lotse-and-the-fairway-mark.md) | Accepted |
| 0023 | [Persist the hard state after the log](0023-persist-the-hard-state-after-the-log.md) | Accepted |
| 0024 | [Tool-independent project memory and agent](0024-tool-independent-project-memory-and-agent.md) | Accepted |
| 0025 | [Frame-oriented network seam and Raft wire format](0025-frame-oriented-network-seam-and-raft-wire-format.md) | Accepted |
| 0026 | [Native protocol: the first cut](0026-native-protocol-first-cut.md) | Accepted |
| 0027 | [Client simulation depth in CI](0027-client-simulation-depth-in-ci.md) | Accepted; job layout amended by 0030 |
| 0028 | [Idempotent producers](0028-idempotent-producers.md) | Accepted |
| 0029 | [Transactions](0029-transactions.md) | Accepted |
| 0030 | [Client simulation in its own CI job](0030-client-simulation-in-its-own-ci-job.md) | Accepted |
| 0031 | [Queues: the first cut](0031-queues-first-cut.md) | Accepted |
| 0032 | [Publish: exchanges routed by the client](0032-publish-routed-by-the-client.md) | Accepted |
| 0033 | [Consumer groups: the first cut](0033-consumer-groups-first-cut.md) | Accepted |
| 0034 | [The metadata group: resources created at runtime](0034-metadata-group.md) | Accepted |
| 0035 | [Deleting resources and changing settings](0035-deleting-resources-and-changing-settings.md) | Accepted |
| 0036 | [DescribeMetrics and the console against a real cluster](0036-describemetrics-and-the-console-against-a-real-cluster.md) | Accepted |
| 0037 | [TLS and node identity](0037-tls-and-node-identity.md) | Accepted |
| 0038 | [Authentication](0038-authentication.md) | Accepted |
| 0039 | [Tenants and roles](0039-tenants-and-roles.md) | Accepted |
| 0040 | [Console sign-in](0040-console-sign-in.md) | Accepted |
| 0041 | [Console sessions in a sealed cookie, the sampler's client credentials, charts per user](0041-console-sessions-sampler-sign-in-and-charts-per-user.md) | Accepted |
| 0042 | [The console keeps its session key](0042-the-console-keeps-its-session-key.md) | Accepted |
| 0043 | [A cluster-wide viewer role](0043-cluster-viewer-role.md) | Accepted |
| 0044 | [Sign-ins in progress in a sealed cookie](0044-sign-ins-in-progress-in-a-sealed-cookie.md) | Accepted |
| 0045 | [Chart history coarser with age](0045-chart-history-coarser-with-age.md) | Accepted |
| 0046 | [Retention: trimming the log at snapshot points](0046-retention.md) | Accepted |
| 0047 | [The `lotse` binary: one cluster file, resources at runtime](0047-the-lotse-binary.md) | Accepted |
| 0048 | [Adding and removing nodes](0048-adding-and-removing-nodes.md) | Accepted, in part superseded by 0050 |
| 0049 | [Packaging: an image, a systemd unit, a Debian package and releases](0049-packaging.md) | Accepted |
| 0050 | [Rebalancing groups onto new nodes](0050-rebalancing-groups-onto-new-nodes.md) | Accepted |
| 0051 | [A drained node stops](0051-a-drained-node-stops.md) | Accepted |
| 0052 | [Rebalancing weighs moves by size](0052-rebalancing-weighs-moves-by-size.md) | Accepted |
| 0053 | [Snapshots travel in chunks](0053-snapshots-travel-in-chunks.md) | Accepted |
| 0054 | [A moved replica does not elect itself](0054-a-moved-replica-does-not-elect-itself.md) | Accepted |
| 0055 | [Dead-letter queues and message TTL](0055-dead-letter-queues-and-message-ttl.md) | Accepted |
| 0056 | [Client SDKs for the JVM](0056-client-sdks-for-the-jvm.md) | Accepted |
| 0057 | [Partitions verify transactions with the coordinator](0057-partitions-verify-transactions-with-the-coordinator.md) | Accepted |
| 0058 | [zstd and lz4 compression](0058-zstd-and-lz4-compression.md) | Accepted |
| 0059 | [ListOffsets](0059-list-offsets.md) | Accepted |
| 0060 | [Certificate renewal and revocation](0060-certificate-renewal-and-revocation.md) | Accepted |
| 0061 | [Optional codecs in the Java SDK](0061-optional-codecs-in-the-java-sdk.md) | Accepted |
| 0062 | [Transactional and group ids live in a tenant](0062-transactional-and-group-ids-live-in-a-tenant.md) | Accepted |
| 0063 | [Conditional delete](0063-conditional-delete.md) | Accepted |
| 0064 | [Purge and requeue](0064-purge-and-requeue.md) | Accepted |

To add one: copy [template.md](template.md), take the next number, and link it here in the same change.

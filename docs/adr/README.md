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
| 0013 | [Cooperative, broker-side consumer group rebalancing](0013-cooperative-broker-side-consumer-group-rebalancing.md) | Accepted |
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
| 0027 | [Client simulation depth in CI](0027-client-simulation-depth-in-ci.md) | Accepted |
| 0028 | [Idempotent producers](0028-idempotent-producers.md) | Accepted |
| 0029 | [Transactions](0029-transactions.md) | Accepted |

To add one: copy [template.md](template.md), take the next number, and link it here in the same change.

# 115. Replication changes at runtime

- Status: Accepted (refines [ADR 0050](0050-rebalancing-groups-onto-new-nodes.md), [ADR 0074](0074-adding-partitions-to-a-stream.md))
- Date: 2026-11-16

## Context

A stream's or queue's replica count was fixed when it was created. The planner of ADR 0048 moved replicas only off draining nodes and, by ADR 0050, from the fullest node to the emptiest. Each kept the replica count. Nothing let an operator raise a stream to five replicas after a cluster grew, lower a queue that no longer needs three, or put a partition on particular machines (a zone, faster disks).

Kafka has both: a topic's replication changes with a partition reassignment, which names each partition's replicas. RabbitMQ grows and shrinks a quorum queue's members with `rabbitmq-queues add_member` and `delete_member`.

Lotse's planner already changes a group's voters through learners and joint consensus, and every group's leader carries out whatever voters the metadata group records (`SetVoters`).

## Decision

**Both** (maintainer's choice): a setting for everyday use and an explicit reassignment for placement.

- **`replication.factor`**, a setting of runtime streams and queues, changes through `AlterConfigs` and is reported by `DescribeConfigs`. It is changed on its own, from 1 to the number of active nodes, by anyone who may alter the resource's settings: a tenant admin already chooses the replica count at creation. The metadata group records it (`SetReplication`). Resetting it removes the setting, and partitions keep the voters they have. `DescribeConfigs` reports the setting, or else the first partition's voter count as the default.
- **The planner** moves each partition of a resource with a factor to that many voters, at most as many as there are active nodes. It takes up to four moving groups at a time, as for drains (ADR 0048). New voters are the least loaded nodes. The busiest go, the highest id among equals. New partitions (ADR 0074) get the factor too.
- **`ReassignPartition`** (key 58, cluster admin) names a partition's voters: active nodes, each once. The metadata group records them (`Reassign`), and the group's leader moves its replicas like any other move. The partition is then *placed*: rebalancing (ADR 0050) and the factor leave it alone, and only a drain moves a replica off it. An empty list releases it to the planner. A placement is refused while the partition is moving. A leader pin (ADR 0065) on a node the placement leaves goes.
- **Setting `replication.factor`** releases the resource's placed partitions: the factor is about every partition.

The metadata state keeps the factors and the placed groups in a new optional snapshot section, so a snapshot without them reads as before.

Rejected options:

- **Only the setting.** It does not let an operator choose nodes.
- **Only reassignment.** Every replica count change would mean naming nodes for every partition.

## Consequences

- An operator raises or lowers replication at runtime. Each move copies the partition to its new voters through Raft, four groups at a time.
- A placed partition stays where it was put, so rebalancing has less to work with. Nodes may stay further apart than one replica when only placed or full-width groups could even them out.
- Queue length limits still cannot change after creation.
- Clients: `Client::reassign_partition` in Rust, `reassignPartition` in Java, Kotlin and Node.js, `ReassignPartitionAsync` in C#. `replication.factor` goes through each client's `alterConfigs`. The console shows neither yet; ADR 0121 adds both.
- Tests:
  - Planner unit tests check that partitions move to their factor a few at a time, up and down and capped at the active nodes. They check that a placed partition stays put through the factor and rebalancing but not through a drain, and is managed again once released. They also check that a placement drops a pin on a node it leaves, and that factors and placements survive a snapshot and go with a delete.
  - The membership simulation, under crashes and partitions, sets s1's factor to 2 or 4 and places s2/0 on three named nodes after a drain. It then requires those voters, the other partitions at three, and every acknowledged write kept.
- Mutation-checked: a planner that ignores the factor, and rebalancing that moves a placed partition, each fail the membership simulation.

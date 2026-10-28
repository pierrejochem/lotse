# 34. The metadata group: resources created at runtime

- Status: Accepted
- Date: 2026-10-28

## Context

Streams, queues, exchanges and their bindings are fixed in each node's configuration (ADRs 0026, 0031, 0032), and every node replicates every group. The design document puts cluster metadata in a metadata Raft group and lists the admin keys `CreateStream`, `CreateQueue`, `DeclareExchange`, `Bind`, `DeleteResource`, `DescribeConfigs` and `AlterConfigs`. Building it leaves open which nodes replicate a new group, what becomes of the configured resources, how concurrent creates of one name end, and how clients find the leader of a group their broker does not host.

## Decision

**A metadata group holds runtime resources.** `NodeConfig::metadata` names it, and every node replicates it. Every admin change is one `Metadata` record (an internal control batch) that each replica applies into its metadata state, saved in the snapshot (format 6). Ids come from the record's Raft index: the resource gets `DYNAMIC_BASE + index` (`DYNAMIC_BASE` = 2^40), partition `p` the group `DYNAMIC_BASE + index * 4096 + p`. Configured ids are small, so they never collide.

**Configured resources stay** (maintainer's choice). Streams, queues and exchanges in the configuration work as before. A node's view of the cluster is the configured resources plus the runtime ones its metadata replica has applied, with runtime bindings added to configured exchanges too.

**Placement by replication factor** (maintainer's choice). The metadata leader picks `replication` voters for each new group: the nodes with the fewest groups, ties by node id. It stores them in the record, so applying never decides anything that could differ between nodes. A node opens a runtime group only if it is a voter. A partition whose group a node does not host is described in `Metadata` with leader 0 and its voters, and the client asks one of the voters for the leader. A request for such a partition, or for a runtime id the node has not learnt yet, is answered `NOT_LEADER`, so the client looks again.

**First-cut keys** (maintainer's choice): `CreateStream` (21), `CreateQueue` (22), `DeclareExchange` (23), `Bind` (24) and `Unbind` (28, not in the design's list). `DeleteResource`, `DescribeConfigs` and `AlterConfigs` come later; deleting needs groups stopped and data removed on every replica. Queues may set `ack.timeout.ms`; priorities, single active consumer, dead-letter queues and stream settings are refused (`INVALID_REQUEST`).

**Admin requests go to the metadata leader**, which acts only after applying an entry of its own term. Others answer `NOT_LEADER` with a hint. The leader refuses names that exist, configured or created (new error code 26 `RESOURCE_EXISTS`, with the existing id). Applying a create for a name that exists changes nothing. So of two concurrent creates of one name the first in the log wins. The leader answers once the record is applied: `NONE` only if the name has exactly the id this record gives it, otherwise `RESOURCE_EXISTS` with the winner's id. The client retries unknown outcomes; a retry of a create that went through gets `ResourceExists` with the id. This answer is true even if the record was replaced in the log, since the answer looks at what exists, not at the record.

**Bindings change at runtime**, so the client's exchange cache expires after `ClientConfig::metadata_max_age` (30 s by default) and is dropped on `bind`/`unbind` by the same client. Others may route by bindings up to that old.

## Consequences

- Streams, queues and exchanges can be created and bound without restarting nodes. Each group runs on as many nodes as asked.
- Metadata is eventually consistent per node: right after a create, some brokers do not know the resource yet. Clients ask every broker before calling a name unknown, and a broker that does not know a runtime id answers `NOT_LEADER`, not `UNKNOWN_RESOURCE`.
- Placement does not move groups when nodes are added, and a node that is down still counts as a candidate. Rebalancing and node removal come with membership changes per group.
- No deletion, no settings beyond the queue's ack timeout, and no authorisation yet.
- The metadata simulation runs four or five nodes in which two admin clients race to create the same resources while a producer and a publisher use them. It checks that all nodes agree on one id per name, that acknowledged creates exist with their partitions and replication, that each node runs exactly the groups it is a voter of, that configured names cannot be created again, and that acknowledged produces to runtime streams survive. It found a real bug: `Metadata` decoding refused bindings with an empty key (an array's minimum element size was too large), which a codec test now covers. It runs in its own CI job: 2,000 seeds per push and 5,000 nightly (maintainer's choice).

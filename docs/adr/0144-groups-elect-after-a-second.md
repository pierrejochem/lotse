# 144. Groups elect after a second

- Status: Accepted
- Date: 2026-11-26

## Context

A group's election timeout was 10 ticks of 10 ms, a random time from 100 to 200 ms, with a heartbeat every 2 ticks. Neither could be set: every node ran its groups with the Raft core's defaults.

On loaded CI runners, groups still went without a leader for seconds in tests against real nodes, after a round started persisting its groups together ([ADR 0143](0143-a-round-persists-its-groups-together.md)). The logs of [ADR 0139](0139-nodes-log-their-groups-elections-and-stalls.md) show why. A node persisted rounds that took up to 257 ms, and its loop ran up to 250 ms late. That one log sync and one hard-state write per group take longer than an election timeout is ordinary for a shared runner, and for a busy disk or a starved CPU in production. The fast followers of every group that node led then elected another leader, and clients met `NOT_LEADER` until the groups settled again.

Other Raft systems set a longer election timeout by default for this reason. etcd elects after 1,000 ms with a heartbeat every 100 ms. Kafka's KRaft elects after 1,000 ms. Both let an operator change it.

## Decision

**A group elects after a second by default, and the cluster file can change it.**

- `NodeConfig` takes `election_timeout` and `heartbeat_interval`. A node gives every group they converted to ticks, rounded up: `election_ticks` and `heartbeat_ticks` of the Raft core.
  - A follower that hears no leader for a random time between one and two election timeouts starts an election.
  - A leader that hears no quorum for an election timeout steps down (check-quorum).
- The defaults are `DEFAULT_ELECTION_TIMEOUT` (1 s) and `DEFAULT_HEARTBEAT_INTERVAL` (100 ms).
- The cluster file sets them with `[cluster] election_timeout_ms` and `heartbeat_ms`. A heartbeat shorter than the tick is refused. So is an election timeout shorter than two heartbeats, since followers would elect between two heartbeats.
- Whatever counts an election timeout counts this one. That covers:
  - pre-votes;
  - the wait of a new group's other replicas ([ADR 0095](0095-a-new-group-starts-on-its-preferred-node.md));
  - a queue leader's recent quorum ([ADR 0097](0097-a-queue-leader-delivers-only-with-a-recent-quorum.md));
  - the voters a leader counts as answering;
  - the stall warnings of ADR 0139.
- A node's own clients (transaction markers, publishes) retry for five election timeouts when that is longer than a second, so they outlast a failover.
- The simulations keep the timing they were tuned for, 10 and 2 ticks, which they set explicitly. Two simulations show the difference:
  - one slow disk making rounds of about 300 ms keeps every group's leader with the default;
  - with 10 ticks, it elects (`with_the_default_election_timeout_rounds_of_several_hundred_ms_keep_their_leaders` in `crates/lotse-server/tests/nodes.rs`).

## Consequences

- A node whose disk or CPU stalls for up to about a second keeps the leadership of its groups.
- After a real crash, a group takes one to two seconds to elect a new leader, not 0.1 to 0.2 s. A stopped node hands its groups over first ([ADR 0114](0114-a-stopping-node-hands-its-groups-over-first.md)), and a handover does not wait for the timeout, so rolling restarts are not slower. An operator who wants faster failover and trusts the machines sets a shorter timeout in the cluster file.
- Mutation-checked: when a node does not pass its election timeout to the Raft core, the simulation with the default timing fails (group 1 elects).
- During a rolling upgrade, nodes of the previous release still elect after 100 to 200 ms. Raft does not need every node to use the same timeout, so the cluster works throughout; until the upgrade ends, the old nodes may elect sooner than the new ones would.
- Clients and SDKs are unchanged. They retry `NOT_LEADER` and broken connections for longer than a failover takes.
- No request, format or stored state changes, so there are no `lotse mcp` tools and no cluster-version gate.
- A disk or CPU that stalls for longer than a second still costs leadership. One log shared by all groups on a node would remove the remaining per-group syncs (ADR 0143); heartbeats sent while a round persists would keep a slow disk from costing leadership at all. Both are left for later.

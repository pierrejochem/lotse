# 48. Adding and removing nodes

- Status: Accepted
- Date: 2026-10-30

## Context

A cluster's nodes were fixed by its configuration: every node linked to the nodes it was started with, accepted links only from them, and every group's voters were chosen when the group was created and never changed (ADR 0047 listed this as a gap). A node could not be added to a running cluster, a failing machine could not be replaced, and every node was a voter of the metadata and coordinator groups, so their writes waited for a majority of the whole cluster. The Raft core already had learners and joint consensus; nothing used them.

## Decision

**The node list lives in the metadata group** (maintainer's choice). The nodes of the configuration are the first ones; `AddNode` adds a node with its addresses, `RemoveNode` marks one draining, and the metadata group's leader forgets it once nothing holds it. Ids are never used twice. Every node follows the list: it opens links to added nodes, closes those to forgotten ones, and accepts a node link (and its frames) only from a listed node. `Metadata` reports the current nodes. A new node starts with `join`: it starts outside the configured groups and is made a member by their leaders. The voters the configured groups started with are kept on each node's disk (`first-voters`) from its first start, so a configuration that lists more nodes later never changes them.

**Replicas move off a draining node by themselves** (maintainer's choice). The work is split in two:

- The metadata group's leader plans. It records the voters each group is to have (`SetVoters`): for a draining node's groups, the node is replaced by the least loaded active node that was never a voter of that group, at most four groups changing at a time across the cluster; once no group counts the node and no change that moved it off is still under way, it is forgotten.
- Each group's own leader carries it out. New voters join as learners first; once a learner has caught up, one joint change gives the group its new voters; then the leader reports the membership in place (`SettleVoters`, a request only nodes may send). A replica that is no longer a voter is closed and its files removed once that report is in the metadata group. A group's Raft membership carries the metadata index of the decision it carries out (a tag Raft keeps but does not read), and a leader whose view of the metadata group is older than that tag waits instead of undoing the newer change. A request still waiting on a group that moved away is answered as outcome unknown.

**Up to five voters for the core groups, kept by the leader** (maintainer's choice). The metadata and coordinator groups have as many voters as there are active nodes, at most five, and an odd number where possible; the planner grows and shrinks them as nodes come and go, the busiest voter leaving first. Every other node follows the metadata group as a learner, so every node has the metadata state.

`lotse node add`, `lotse node remove` and `lotse node list` do this from the command line as an admin (a client certificate or a SCRAM user); `lotse run --join` starts a new node. Adding and removing nodes needs a cluster admin; listing them a cluster viewer.

## Consequences

- A machine can be added or replaced without stopping the cluster; a drained node can be switched off once it is no longer listed.
- Moving a group copies it in full through Raft (a snapshot and the log after it); a large partition takes as long as that copy, and four groups move at a time.
- A removed node keeps its replicas of the metadata and coordinator groups (as a non-member) until it is switched off; it never returns, as its id is never used again.
- Removing a node is refused while too few nodes would be left for the widest group.
- Nothing yet rebalances groups onto a new node: it takes new groups and the replicas of drained nodes, not existing ones.
- Tests: unit tests of the metadata changes, of a leader's steps (learners first, a joint change, waiting while joint) and of the planner (core group sizes, a drain a few groups at a time and then the node forgotten, never back to a group a node left); a simulation in its own CI job that adds two nodes and drains one of the first three under load, partitions and power loss, and checks that the drain finishes, every group has three voters, none of them the removed node, as its Raft membership says, the removed node keeps no runtime group's files, nothing acknowledged is lost, and ids are never used twice; and the binary's test adds a fourth node and removes the first over real sockets. The simulation found two bugs before this was committed: a leader that acted on an older view of the metadata group undid a newer change (hence the tag), and the client never reached a metadata leader whose address it had not seen yet.

# 145. A node behind does not call a name unknown

- Status: Accepted
- Date: 2026-11-27

## Context

A node answers `Metadata` from the metadata state it has applied. For a name that state lacks, it answers `UNKNOWN_RESOURCE`. A client that hears this from one broker asks every broker. It calls the stream unknown only when every broker answered and none knew the name, and that ends the call: `UnknownStream` is not retried (PROTOCOL.md).

After a node restarts, its state holds what it applied before it stopped, up to the commit index on disk. It learns the rest only once it hears its leader. After the whole cluster restarts, every node may be in that position for a moment. Every broker then answers, and none knows a stream created just before the restart. The client calls the stream unknown, though it exists.

This surfaced while ADR 0143 was being written. A replica that stopped writing its commit index for every commit made the window wide enough that `a_cluster_from_one_file` failed in 5 of 15 runs. With the commit index written every time, as now, the window is narrow but still there.

A node can tell when its state may be behind:

- a follower or learner of the metadata group that does not hear its leader, or has not caught up with what the leader said was committed (ADR 0116);
- a leader that has not yet applied an entry of its own term, so it may hold committed entries it has not applied.

## Decision

**A node whose metadata state may be behind answers a name it does not know with `NOT_LEADER`, not `UNKNOWN_RESOURCE`.**

- "Behind" means: the node follows the metadata group and lags, as ADR 0116 defines lagging (it does not hear its leader, or has not caught up with what the leader said was committed within `follower_max_lag`); or it leads the metadata group and has not yet applied an entry of its own term.
- This covers the per-resource error in `Metadata`. A name hidden from the principal is answered the same way, so the answer gives nothing away.
- **Clients:** a broker that answers a name with a retriable error has not answered for that name. Clients keep asking, as they do while a broker does not answer at all, until every broker answered and none knew the name, or until their retry time runs out. That applies to the Rust client and to the Java, C# and Node.js SDKs.
- No new code: `NOT_LEADER` is already retriable, and it already means "ask another node or wait" for zone reads (ADR 0141) and publishes (ADR 0101). A client of the previous release takes the answer as "not known here" and behaves as before.

## Consequences

- After a whole cluster restarts, a client asking for a stream waits until a node that has caught up answers. It does not call the stream unknown.
- A name that truly does not exist is called unknown a moment later: once every node has caught up, about an election timeout after a restart.
- Mutation-checked:
  - when the node answers as current while behind, `a_restarted_node_does_not_call_a_name_unknown_before_it_catches_up` (`crates/lotse-server/tests/clients.rs`) fails;
  - when a lagging follower counts as current, the same test fails;
  - when the Rust client takes a retriable answer as an answer, `a_broker_behind_does_not_make_a_stream_unknown` fails. Each SDK has the same test.
- `Metadata` without names lists what the node knows, which a node behind may give incompletely; only a named lookup is answered this way.
- The other requests that look up a name are left as they were:
  - `Produce`, `Fetch` and `Subscribe` by id already answer `NOT_LEADER` for a runtime id a node does not know (ADR 0034);
  - admin reads go through `read_at_least` (ADR 0141);
  - admin changes go to the metadata leader.
- The client's lookup of a resource id by name for admin calls (`resource_id`) still takes the merged answer without waiting for every broker. That gap was already there.
- No format or stored state changes, so there is no cluster-version gate. No request changes, so there are no `lotse mcp` tools: the tools use the Rust client and get its rule.
- With this in place, the hard state could again skip a commit-only write (ADR 0143). That is left to a measurement of its own.

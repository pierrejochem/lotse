# 121. The console places partitions and lists open sessions

- Status: Accepted
- Date: 2026-11-18

## Context

Since ADR 0115 a cluster admin can change a resource's `replication.factor` and place a partition on named nodes, but only through the clients; the console offered neither. Since ADR 0065 a partition's leader can be pinned, but no client could read a pin back, nor whether a partition was placed: both live only in the metadata state. And while a cluster admin can revoke a user's or IdP group's sessions (ADR 0070), nothing showed which sessions are open, where, or since when: each node knows only the connections it holds, and only inside each connection's task.

The README listed these as console gaps: "open sessions cannot be listed, a pinned partition is not shown", and ADR 0115 said "the console shows neither yet".

## Decision

**`Metadata` says which partitions are pinned and placed, a new request lists a node's open sessions, and the console uses both: it shows pins and placements, changes the replication factor, places partitions, and lists open sessions** (maintainer's choices: placement and sessions together; a tag on `Metadata`; admins see all sessions and users their own; a new request rather than a tag on `DescribeMetrics`).

- **Pins and placements in `Metadata`.** A partition's tag block gains two tags:
  - tag 0, `pinned_leader`, a `u32`, the node a cluster admin pinned its leader to;
  - tag 1, `placed`, empty, present when a cluster admin placed the partition on named nodes.

  Each appears only when set, so a partition without either is written as before and earlier readers skip both. They come from the answering node's applied metadata state, like the voters of runtime groups, and go to whoever may see the resource.
- **`DescribeSessions` (key 59).** The node asked lists its open client sessions, oldest first. Each entry has:
  - the principal (kind and name: a user, a node by id, or anonymous) and its IdP groups;
  - how it signed in (none, certificate, SCRAM-SHA-512, OAUTHBEARER);
  - the peer address and when the connection was made;
  - when its credential dates from (what a session revocation compares against, ADR 0070);
  - when it ends unless renewed (0 for one that lasts the connection).

  A connection that has not signed in yet, or whose session ended, is not listed. Each connection task keeps its entry current in a registry the node shares with its main loop, and removes it when the connection closes.
- **Who sees what.** Cluster admins and cluster viewers, and everyone on a node without authentication, get every session. Anyone else gets the sessions made with their own name. Nodes are refused. The request is let through and filtered when answered, like `DescribeMetrics`.
- **Clients.** `Client::node_sessions` in Rust, `nodeSessions` in Java, Kotlin and Node.js, and `NodeSessionsAsync` in C# ask every node the cluster's metadata names, like their metrics calls. The SDKs decode the two partition tags.
- **Console.**
  - A stream's partitions and a queue show a pinned leader and a placement.
  - *Change replication* sets `replication.factor` on a stream or queue. *Place* names a partition's nodes, or releases a placed one.
  - The *Sessions* tab lists the open sessions on every node, with a revoke per user for cluster admins and viewers, above the existing revoke by name.

Rejected options:

- **A `DescribePlacements` request for cluster admins only.** It would keep `Metadata` unchanged, but adds a request key in every SDK. A pin or placement is no more sensitive than the voters `Metadata` already shows.
- **Pins and placements as `DescribeConfigs` entries.** Config entries are settings, not state, and would need one entry per partition.
- **Sessions in `DescribeMetrics`.** No new key, but per-user data and its access rule would join an answer that cluster viewers and the console's sampler read for counters.
- **Cluster admins only.** That rule is simpler, but users could not see where they are signed in.

## Consequences

- An operator sees in the console which partitions are pinned or placed, can place them and change replication without a client, and sees who is connected where before revoking.
- The protocol gains key 59; the golden vectors gain the request, its answer and the two partition tags.
- A node holds one small entry per client connection. The list is each node's own: a client connected to several nodes appears once per node.
- The console names nodes as `node-<id>`; a placement names them the same way.
- The console's table wrapper is now the containing block for what it scrolls. A visually hidden column label had widened the page on phones; it did so on the Users tab before this change too.
- Tests:
  - A protocol test shows a partition without a pin or placement is written as before, and the tags round-trip.
  - The pin test (`a_pinned_leader_stays_where_it_is_put`) checks that `Metadata` names the pin, drops it when unpinned, shows a placement, and stops showing it once `replication.factor` releases it.
  - A unit test covers who sees which sessions.
  - A simulation test logs in with SCRAM, tokens and a raw connection. It checks that a cluster admin sees everyone with mechanism, groups and expiry; that users see only their own sessions on every node; and that a closed connection leaves the list.
  - Each SDK's conformance tests read the partition tags. The Java client tests `nodeSessions` against fake nodes, and the C# and Node.js SDKs list sessions on real nodes.
  - The console's test against real nodes pins, places, sets the factor and lists sessions.
- Mutation-checked:
  - `Metadata` without the pin or the placement;
  - a user seeing every session, or every user's;
  - an ended session still listed;
  - a closed connection still listed;
  - a login not recorded.

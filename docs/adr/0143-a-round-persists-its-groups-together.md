# 143. A round persists its groups together

- Status: Accepted (refines [ADR 0023](0023-persist-the-hard-state-after-the-log.md))
- Date: 2026-11-26

## Context

On loaded CI runners, groups went seconds without a leader in tests against real nodes ([docs/status.md](../status.md); [ADR 0139](0139-nodes-log-their-groups-elections-and-stalls.md) added the logs to find out why). The election timeout is 10 to 20 ticks of 10 ms, 100 to 200 ms.

A node's loop persists a round's groups before it sends anything. Each group with something to write ran one after the other:

1. the log is synced;
2. the hard state is written atomically: a temporary file, its sync, a rename and the directory's sync;
3. often a second pass rewrites the hard state, because the commit index moved after the first.

The hard state is dirty whenever the commit index moves, so after nearly every committed write. A round therefore cost about the number of groups times two or three syncs.

Measured on the console's real-cluster test, with the CPU and the disk under load:

- A single `fdatasync` took at most 11 ms.
- Persisting a round took 30 to 85 ms.
- Ticks came up to 100 ms late, and followers started elections in the middle of the run, not only at the start.
- Hard-state writes were about 45 % of all syncs (`strace`).

In the simulation, every sync of the node leading the most groups was made to take a fixed time. Its rounds took the sum of its groups' syncs, and the fast followers of a group it led elected another leader. When every node is equally slow, nobody elects, because a slow node also counts its own ticks slowly. The harm comes from one slow node among fast ones, which is what a shared CI runner, or a node with a busy disk, produces.

## Decision

**A round persists its groups at the same time.** The node loop runs every group's `process_unfilled` together (`join::all`, which polls the futures in order, so simulations stay repeatable). It does so again for the groups that have more to write after `after_round`. Only then does it fill and send messages.

Each group keeps its own order: snapshot, truncate, entries synced, hard state, apply (ADR 0023). No group's message leaves before the whole round is on disk. A round now costs about the slowest group's syncs, not their sum.

The hard state is still written, synced, whenever it changes, the commit index included.

## Alternatives

**Not writing a hard state that changes only the commit index.** Raft needs the term and the vote to survive a crash, not the commit index, which a restarted replica learns again from its leader. Skipping those writes cut the stalls under load by about 2.5×, on top of persisting together. It was built and then dropped: a restarted replica applies only up to the commit index on disk until its group elects a leader and commits, so after a whole cluster restarted, a node answered `UnknownStream` for a stream its log held but its metadata state had not applied yet. The real-node test `a_cluster_from_one_file` failed this way in 5 of 15 runs, against none on the previous release and none with the groups persisted together alone. The skip can come back once a node no longer answers "unknown" from a state it knows is behind its leader; that gap exists in a narrower window already (status.md).

**One log shared by all groups on a node,** with one sync per round, as in TiKV's raft-engine. It would remove the per-group syncs that remain. A larger change, left for later.

**A longer tick.** Clusters would be less sensitive to slow disks, but failovers would take longer. The 10 ms tick stays.

## Consequences

- With the slow disk in the simulation (20 ms a sync), a round costs about one group's syncs, and no group holds an election while all of them take writes (`groups_keep_their_leaders_while_a_slow_disk_takes_writes_to_all_of_them` in `crates/lotse-server/tests/nodes.rs`). The simulated disk can now delay its syncs (`SimRuntime::set_sync_delay`) and counts the most that were under way at once (`SimDisk::most_syncs_at_once`).
- Mutation-checked: persisting the groups one after the other fails the slow-disk test. A deterministic test checks that a vote cast in a term the replica already knew survives a power loss (`a_vote_in_a_term_it_already_knew_survives_a_power_loss`).
- A node with very many busy groups still pays a log sync and a hard-state write per group per round, at once rather than in turn.
- No request, format or setting changes, so there are no `lotse mcp` tools and no cluster-version gate.

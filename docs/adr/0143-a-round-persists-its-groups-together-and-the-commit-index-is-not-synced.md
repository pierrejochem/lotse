# 143. A round persists its groups together, and the commit index is not synced

- Status: Accepted (refines [ADR 0023](0023-persist-the-hard-state-after-the-log.md))
- Date: 2026-11-26

## Context

On loaded CI runners, groups went seconds without a leader in tests against real nodes ([docs/status.md](../status.md); [ADR 0139](0139-nodes-log-their-groups-elections-and-stalls.md) added the logs to find out why). The election timeout is 10 to 20 ticks of 10 ms, 100 to 200 ms.

A node's loop persists a round's groups before it sends anything. Each group with something to write runs one after the other:

1. the log is synced;
2. the hard state is written atomically: a temporary file, its sync, a rename and the directory's sync;
3. often a second pass rewrites the hard state, because the commit index moved after the first.

The hard state is dirty whenever the commit index moves, so after nearly every committed write. A round therefore costs about the number of groups times two or three syncs.

Measured on the console's real-cluster test, with the CPU and the disk under load:

- A single `fdatasync` took at most 11 ms.
- Persisting a round took 30 to 85 ms.
- Ticks came up to 100 ms late, and followers started elections in the middle of the run, not only at the start.
- Hard-state writes were about 45 % of all syncs (`strace`).

Skipping the commit-only hard-state writes cut the stalls by about 2.5×.

In the simulation, every sync of the node leading the most groups was made to take 40 ms. Its rounds took 125 ms, five syncs in a row, and the fast followers of a group it led elected another leader. When every node is equally slow, nobody elects, because a slow node also counts its own ticks slowly. The harm comes from one slow node among fast ones, which is what a shared CI runner, or a node with a busy disk, produces.

## Decision

**A round persists its groups at the same time.** The node loop runs every group's `process_unfilled` together (`join::all`, which polls the futures in order, so simulations stay repeatable). It does so again for the groups that have more to write after `after_round`. Only then does it fill and send messages.

Each group keeps its own order: snapshot, truncate, entries synced, hard state, apply (ADR 0023). No group's message leaves before the whole round is on disk. A round now costs about the slowest group's syncs, not their sum.

**A hard state that changes only the commit index is not written.** Each replica remembers the term and vote on disk. It writes `raft.state`, synced as before and after the log, only when one of them changes. Raft needs the term and the vote to survive a crash, or a node could vote twice in one term. It does not need the commit index:

- A restarted replica starts applying at the larger of the commit index on disk and its snapshot.
- It learns the rest from its leader.
- A new leader commits its no-op, and with it everything before.

The commit index in `raft.state` is therefore a lower bound that may be old. It still only refers to a log already on disk, since it is written after the log, as ADR 0023 requires.

## Consequences

- With the slow disk in the simulation, a round costs about one log sync, and no group holds an election while all of them take writes (`groups_keep_their_leaders_while_a_slow_disk_takes_writes_to_all_of_them` in `crates/lotse-server/tests/nodes.rs`). The simulated disk can now delay its syncs (`SimRuntime::set_sync_delay`) and counts the most that were under way at once (`SimDisk::most_syncs_at_once`).
- Mutation-checked:
  - persisting the groups one after the other fails the slow-disk test;
  - writing the hard state for every commit fails it too;
  - never writing the hard state gives two leaders in a term in the power-loss simulation (`cluster.rs`);
  - writing it only when the term changes, losing a vote cast in a term already known, survives 1,000 seeds of both simulations. A deterministic test catches it instead (`a_vote_in_a_term_it_already_knew_survives_a_power_loss`).
- After a crash, a replica may apply less at first than before it, until it hears its leader. In a single-voter group, entries after the commit index on disk become applied again once the restarted node is elected and commits its no-op.
- A round still waits for each group's own log sync, and every group syncs its own segment file. A node with very many busy groups pays one sync per group per round, at once rather than in turn. One log shared by all groups on a node, with one sync per round as in TiKV's raft-engine, would remove that too. It is a larger change, left for later.
- No request, format or setting changes, so there are no `lotse mcp` tools and no cluster-version gate: `raft.state` keeps its format, and a node of the previous release reads it either way.
- The 10 ms tick stays. A longer one would make clusters less sensitive to slow disks, but failovers would take longer.

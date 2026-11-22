# 131. Long release simulations run in parallel shards

- Status: Accepted
- Date: 2026-11-22
- Changes [ADR 0088](0088-simulations-gate-the-release.md) in part: a simulation's 5,000 seeds may run over several jobs

## Context

The release workflow runs every simulation at 5,000 seeds before it builds anything (ADR 0088), each simulation in one job. A run took about four hours, set by the longest job: the compaction simulation took four hours, the queue, metadata and general simulations about two each, the rest between ten minutes and an hour and a half. Every change to code is run through this workflow on its branch before it is merged, so four hours was also the wait for every merge.

The seeds are independent: each is a whole run of the simulation from its number. The runners are separate machines.

## Decision

**A simulation's seeds may be split into equal ranges over parallel jobs. The release workflow runs the compaction simulation in 4 jobs, and the queue, metadata and general simulations in 2 each; the others stay whole, since each finishes within about an hour and a half.**

- **`LOTSE_SIM_SHARD=<i>/<n>`** (from 1) makes `lotse_sim::seeds` keep the i-th of n equal ranges of the seeds `LOTSE_SIM_SEEDS` asks for. Together the ranges hold every seed once, so the depth stays 5,000 per simulation. A seed is the same number with or without shards, and `LOTSE_SIM_SEED` reproduces it as before.
- **Scenario tests** (`check_few_seeds`) do not shard: they run their few seeds in every shard of a job, which costs seconds.
- **Checks over all seeds of a test**, such as that some message was dead-lettered at all, now hold per shard, over 1,250 to 2,500 seeds each.
- **The workflow** makes each sharded simulation a job matrix, without `fail-fast`, so one failing shard does not hide what the others find. `build` waits for every job of every matrix, as before. Only the first shard of the general simulation saves the shared build cache.

Rejected options:

- **Every simulation in 4 shards.** Under 45 minutes a run, but about a third more runner minutes, most of them spent building the same test binaries again.
- **Only compaction in 4.** The run would still take two hours, set by the general simulation.
- **Fewer seeds for the slow simulations.** That would buy time with depth, which ADR 0088 decided against.

## Consequences

- A release run takes about an hour and a half, set by the longest unsharded job, instead of four hours.
- Each extra job builds the test binaries again: about one more runner hour per release run.
- A slower simulation is split by adding a shard to its matrix, with nothing else changed.
- Tested by `shards_cover_every_seed_once` in `lotse-sim`.

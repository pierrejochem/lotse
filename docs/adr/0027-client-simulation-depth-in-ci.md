# 27. Client simulation depth in CI

- Status: Accepted (amends the seed count in [ADR 0010](0010-deterministic-simulation-and-mutation-checking.md))
- Date: 2026-10-28

## Context

ADR 0010 has CI run 5,000 seeds per simulation. The client simulation (`crates/lotse-server/tests/clients.rs`: three or five whole nodes, producers and consumers over the simulated network) takes about 7.5 minutes at that depth, which raised the simulation job from about 5 to about 13 minutes per push.

## Decision

The client simulation runs 2,000 seeds on pushes and pull requests (about 3 minutes) and 5,000 seeds in a nightly scheduled run and on manual runs. Every other simulation stays at 5,000 seeds on every push.

## Consequences

- The simulation job takes about 8 minutes per push.
- A bug in the client path that only a rare seed reaches may surface in the nightly run, up to a day after the push that caused it. The failing run prints the seed, so it is reproduced exactly with `LOTSE_SIM_SEED`.
- Protocol rules that depend on rare histories keep deterministic unit tests (as the five-voter test of ADR 0026), so they do not rely on seed count.

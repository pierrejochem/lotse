# 88. Simulations gate the release, not the pull request

- Status: Accepted
- Date: 2026-11-09
- Changes: [ADR 0082](0082-simulations-run-on-pull-requests-and-merging-needs-green.md), which ran the simulations on every pull request and nightly; amends ADRs 0027, 0030, 0031, 0033, 0034, 0039, 0046, 0048 and 0067, which set their depths

## Context

Under ADR 0082 every pull request ran nine simulation jobs, 1,000 to 2,000 seeds each, and a nightly run on `main` ran them at 5,000. They are the slowest part of CI by far. A pull request waited for all of them, also for a one-line change to the docs or a dependency.

The maintainer chose to move them entirely into the release workflow, as a prerequisite of every release, so that the pipeline for a pull request runs much faster.

## Decision

**The `ci` workflow** runs `check`, `console-web`, `packaging` and `sdk` on every pull request and every push to `main`. It has no simulation jobs and no nightly schedule. `main` still takes a change only through a pull request with every job of `ci` green (ADR 0082). The required checks are those four.

**The `release` workflow** runs on a version tag:
- first, the nine simulation jobs, each at 5,000 seeds: `simulation` and the eight `*-simulation` jobs;
- then the build, which needs every one of them. Nothing is built, packaged, released or pushed to the registry unless all nine passed.

Run by hand (`workflow_dispatch`) on any branch, the release workflow runs the simulations only, and builds and publishes nothing.

Only the jobs that publish get write permissions. The `packaging` job on `main` now fills the shared `release` build cache, which the nightly run filled before.

## Consequences

- A pull request waits for four jobs instead of thirteen.
- A failing seed is found at the latest when a release is tagged, not before the merge. It then blocks that release until it is fixed. Between releases, `main` may hold a bug that only a simulation finds.
- Whoever changes code a simulation covers runs that simulation locally, at CI depth where it matters (`LOTSE_SIM_SEEDS=5000 cargo test -p lotse-server --release --test <file>`), or runs the release workflow by hand on the branch.
- A release takes as long as the slowest simulation job at 5,000 seeds, plus the build.
- The required checks of `main`'s branch protection must name only `check`, `console-web`, `packaging` and `sdk`. A required job that no longer runs would block every pull request.

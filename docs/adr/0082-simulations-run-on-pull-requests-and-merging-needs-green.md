# 82. Simulations run on pull requests; merging needs a green CI

- Status: Accepted
- Date: 2026-11-08

## Context
The simulation jobs ran on every push to `main` and on every pull request, and on `main` after the merge, so a failing seed was found after the code was already there. Seven jobs of 3 to 8 minutes each also ran twice for the same change.

## Decision
The simulation jobs (`simulation` and the eight `*-simulation` jobs) run on pull requests, on the nightly schedule and on manual runs, and not on pushes to `main`. Seeds per job stay as in ADRs 0027, 0030, 0031, 0033, 0034, 0039, 0046, 0048 and 0067: the push depth now applies to pull requests; the nightly depth is unchanged.

`main` is protected: a change reaches it through a pull request in which every job of the `ci` workflow (`check`, `console-web`, the simulation jobs, `packaging`, `sdk`) is green. The `docs` workflow is not required, because it only runs when the guides change.

The nightly run on `main` also fills the shared `release` build cache, since no push to `main` builds the release profile any more.

## Consequences
A failing seed blocks the merge instead of surfacing on `main`. Every change takes a pull request, also a one-line fix. Pushes to `main` run `check`, `console-web`, `packaging` and `sdk` only. The release cache on `main` is up to a day old; pull requests rebuild what changed since.

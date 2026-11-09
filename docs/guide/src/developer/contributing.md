# Contributing

The rules for everyone who changes the repository, people and AI agents alike, are in [AGENTS.md](https://github.com/pierrejochem/lotse/blob/main/AGENTS.md). They are binding. This page is a map for newcomers and does not replace it: when the two differ, AGENTS.md wins.

For the design, read [docs/design.md](https://github.com/pierrejochem/lotse/blob/main/docs/design.md) (German) and the decision records in [docs/adr](https://github.com/pierrejochem/lotse/tree/main/docs/adr). The design document describes the current system; the ADRs keep the reasons.

## Crate map

| Crate | What |
| --- | --- |
| `lotse-rt` | The only seam to the outside world: `Clock`, `Disk`, `SegmentFile`, `Spawn`, `Rng`, `Net`, `Host`, `Http` traits |
| `lotse-rt-tokio` | Production runtime: Tokio, file system, TLS 1.3 or plain TCP on loopback |
| `lotse-record` | Batch format: header, ZigZag-varint records, CRC32C, zstd and lz4 blocks, control batches |
| `lotse-log` | Segmented partition log, which is also the Raft log |
| `lotse-raft` | Sans-I/O Raft core and Multi-Raft |
| `lotse-queue` | Queue semantics free of I/O: ack index and dispatcher |
| `lotse-auth` | SCRAM, OAUTHBEARER, roles; free of I/O |
| `lotse-route` | Exchanges and bindings: direct, topic, fanout, headers |
| `lotse-proto` | Native protocol: frames, error codes, requests and responses |
| `lotse-server` | Broker node: `Replica`, `Node`, client serving, metadata, membership |
| `lotse-node` | The `lotse` binary |
| `lotse-client` | Rust client |
| `lotse-sim` | Deterministic simulation |
| `lotse-console` | Management console backend; the frontend is in `console/web` |

The full map, with the files inside `lotse-server`, is in AGENTS.md.

## The runtime seam

Nothing above `lotse-rt` touches the real world directly. Code takes a `Clock`, `Disk`, `Spawn`, `Rng` or `Net` instead of calling `Instant::now`, `SystemTime::now`, `std::thread`, OS randomness or ambient Tokio APIs. Use `BTreeMap` and `BTreeSet`, never `HashMap` or `HashSet`, because iteration order would make simulations unrepeatable. `clippy.toml` enforces most of this. `unsafe` is forbidden. The reasons are in [ADR 0006](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0006-tokio-behind-a-runtime-seam.md).

The Raft core is sans-I/O: it never does I/O. A driver persists a `Ready` in a fixed order: snapshot, truncate, entries (synced), hard state, apply, send ([ADR 0023](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0023-persist-the-hard-state-after-the-log.md)).

## Deterministic simulation

`lotse-sim` runs the real code on a seeded executor with virtual time, disks that crash and lose or tear unsynced writes, and a network that drops, delays and partitions. A run is a function of its seed, so a failure is reproduced exactly.

```sh
cargo test --workspace                                                   # unit tests and short simulations
LOTSE_SIM_SEEDS=5000 cargo test -p lotse-sim -p lotse-server --release   # CI depth
LOTSE_SIM_SEED=1234 cargo test -p lotse-sim                              # exactly one seed
```

- Simulations take seeds from `lotse_sim::check_seeds`, which panics with the seed and the command that reproduces it. Scenario tests whose value does not grow with seeds use `check_few_seeds`.
- A simulation's oracle is written in the test, never borrowed from the code it checks.
- Simulations live in `crates/lotse-sim/tests/` and in a crate's `tests/`; for example `crates/lotse-server/tests/clients.rs` runs the Rust client against a faulty cluster.
- A failing seed is a bug until proven otherwise. Find the cause, then decide whether the code or the check is wrong, and say which in the commit. Never weaken a check to make a test pass.
- After you add faults or operations, check with a quick statistic that they happen (crashes per run, snapshots installed, reads served) before you trust a green run.

Pull requests do not run the simulations at depth. The release workflow runs all of them at 5,000 seeds before anything is built for a release, and a failure there blocks the release ([ADR 0088](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0088-simulations-gate-the-release.md)). So run the simulations your change touches yourself, at CI depth where it matters, or run the release workflow by hand on your branch: run by hand, it runs the simulations only ([ADR 0010](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0010-deterministic-simulation-and-mutation-checking.md)).

## Mutation checking

When you add or change a rule that safety depends on, break it on purpose and confirm that a test fails. If none does, add one: a simulation, or a deterministic unit test for rules that depend on timing, because random simulation rarely reaches those. Restore the rule afterwards. State which mutants you tried in your report.

## Before you push

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Also run the affected simulation at CI depth. After you change a message or the batch format, run `make sdk-vectors` and `make sdk-test`; a change to a message needs the codecs of the Java, C# and Node.js SDKs updated. After you change an API type of the console, run `npm run api` in `console/web`.

## Decisions: the ADR process

A changed decision gets an ADR in the same change.

1. Copy `docs/adr/template.md` to the next number, for example `0082-short-title.md`.
2. Write context, decision and consequences. State what you rejected and why.
3. Add it to the index in `docs/adr/README.md`.
4. Update the design document to match. It stays German.

A superseded decision keeps its ADR, marked as superseded, so the reasons stay readable.

## Conventions

- Code, comments, docs and commit messages are in English; the design document is German.
- Comments explain why, not what. Public items have doc comments.
- Errors use `thiserror` and say what happened and, where useful, what to do.
- Dependencies go through `[workspace.dependencies]`.
- Docs and UI copy follow the [brand voice](https://github.com/pierrejochem/lotse/blob/main/docs/brand/README.md): calm, exact, short, units named, no hype, no exclamation marks, no emoji.
- Commits have an imperative subject line and a body that explains why.

## Working on this guide

The guides live in `docs/guide/` and are built with mdBook ([ADR 0081](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0081-user-guides-with-mdbook-on-github-pages.md)). Examples that could drift are included with `\{{#include}}`, not pasted. Where the repository documents no procedure, the guide says so.

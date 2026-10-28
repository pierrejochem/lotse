# AGENTS.md

Project memory for everyone who changes this repository: people and AI coding agents alike, whatever the tool. Tool-specific files (`CLAUDE.md`, `.claude/`) only point here. When something here is wrong or missing, fix it here, in the same change.

## What Lotse is

A message broker in Rust: RabbitMQ-style reliable queues and Kafka-style persistent streams on one replicated log, with exactly-once delivery. Read [docs/design.md](docs/design.md) (German) for the design and [docs/adr/](docs/adr/) for why each decision was made. The design document describes the current system; ADRs keep the reasons.

## Repository map

| Path | What |
| --- | --- |
| `crates/lotse-rt` | The only seam to the outside world: `Clock`, `Disk`, `SegmentFile`, `Spawn`, `Rng`, `Net` traits |
| `crates/lotse-rt-tokio` | Production runtime: Tokio, file system, TCP frames; the only crate that reads the real clock |
| `crates/lotse-record` | Batch format: 65-byte header, ZigZag-varint records, CRC32C, transaction and Raft control batches, queue acks and delivery counts |
| `crates/lotse-log` | Segmented partition log, which is also the Raft log |
| `crates/lotse-raft` | Sans-I/O Raft core and Multi-Raft; `testing::MemNode` is the in-memory driver |
| `crates/lotse-queue` | Queue semantics free of I/O: `QueueLog` (ack index every replica applies) and `Dispatcher` (the leader's subscriptions, credit, in-flight messages, redelivery, ack timeout) |
| `crates/lotse-proto` | Native protocol: frames, codec conventions, error codes, request and response types |
| `crates/lotse-server` | Broker node: `Replica` (a Raft group on `lotse-log`, with producer, transaction and coordinator state), `Node` (many groups, one transport), `wire` (node-to-node format), `client` (serving the native protocol, `client/coord.rs` the transaction coordinator, `client/queues.rs` queue serving); simulations in `tests/cluster.rs`, `tests/nodes.rs`, `tests/clients.rs` and `tests/queues.rs`, real TCP in `tests/tokio_cluster.rs` |
| `crates/lotse-client` | Rust client over the native protocol, on `lotse-rt` (runs in the simulation too) |
| `crates/lotse-sim` | Deterministic simulation: seeded executor, virtual time, crashing disks, a faulty network; Raft fault simulation in `tests/raft.rs` |
| `crates/lotse-console` | Management console backend (axum), serves `console/web` |
| `console/web` | Console frontend: React 18, TypeScript, Radix primitives, TanStack Query, generated OpenAPI client |
| `docs/` | Design document, ADRs, architecture, brand, console screenshots and mockups |
| `agents/` | Tool-neutral descriptions of the project's coding agents |

## Commands

```sh
cargo fmt --all                                            # format (max_width 120)
cargo clippy --workspace --all-targets -- -D warnings      # lint; must be clean
cargo test --workspace                                     # all unit tests and short simulations
LOTSE_SIM_SEEDS=5000 cargo test -p lotse-sim -p lotse-server --release   # simulations at CI depth
LOTSE_SIM_SEED=1234 cargo test -p lotse-sim                # reproduce one failing seed exactly

cd console/web && npm ci && npm run build && cd ../..       # build the web app (needed before the console binary)
cargo run -p lotse-console -- --config console/lotse-console.example.toml
cd console/web && npm run api                              # after changing an API type in Rust: regenerate openapi.json + TS client
cd console/web && npm run typecheck && npm test
```

CI (`.github/workflows/ci.yml`) runs format, clippy, tests, the web app build with an OpenAPI drift check, and the simulations at 5,000 seeds; the client and queue simulations run in jobs of their own, 2,000 seeds per push and 5,000 nightly ([ADR 0027](docs/adr/0027-client-simulation-depth-in-ci.md), [ADR 0030](docs/adr/0030-client-simulation-in-its-own-ci-job.md), [ADR 0031](docs/adr/0031-queues-first-cut.md)). Keep it green, and check the nightly run.

## Rules that are not negotiable

1. **Nothing above `lotse-rt` touches the real world directly.** No `Instant::now`, `SystemTime::now`, `std::thread`, OS randomness or ambient Tokio APIs; take a `Clock`, `Disk`, `Spawn` or `Rng`. No `HashMap`/`HashSet`: iteration order would make simulations unrepeatable; use `BTreeMap`/`BTreeSet`. `clippy.toml` enforces most of this. ([ADR 0006](docs/adr/0006-tokio-behind-a-runtime-seam.md))
2. **`unsafe` is forbidden** (workspace lint).
3. **Raft core stays sans-I/O.** `lotse-raft` never does I/O; drivers persist a `Ready` in its documented order: snapshot, truncate, entries (synced), hard state, apply, send ([ADR 0023](docs/adr/0023-persist-the-hard-state-after-the-log.md)).
4. **Never weaken a check to make a test pass.** A failing simulation seed is a bug until proven otherwise; find the cause, then decide whether the code or the check is wrong, and say which in the commit.
5. **Safety rules are mutation-checked.** When you add or change a rule that safety depends on, break it on purpose and confirm a test fails; if none does, add one (simulation, or a deterministic unit test for timing-dependent rules). ([ADR 0010](docs/adr/0010-deterministic-simulation-and-mutation-checking.md))
6. **A changed decision gets an ADR.** New or superseded decisions go to `docs/adr/` (copy `template.md`, next number, add it to the index) in the same change, and the design document is updated to match.
7. **The user acts as themselves.** The console never gets its own admin rights; every action uses the signed-in user's identity ([ADR 0019](docs/adr/0019-management-console-on-a-dedicated-node-with-the-user-s-identity.md)).

## Conventions

- **Language:** code, comments, docs and commit messages in English. The design document is German; keep it German when editing it.
- **Comments** explain why, not what. Public items have doc comments (`missing_docs` is on in library crates).
- **Errors** use `thiserror`; messages say what happened and, where useful, what to do.
- **Tests:** unit tests next to the code (`mod tests;` in `tests.rs`), simulations in `crates/lotse-sim/tests/` or a crate's `tests/`. Simulations take seeds from `lotse_sim::check_seeds` and print the seed on failure.
- **UI copy and docs** follow the brand voice in [docs/brand](docs/brand/README.md): calm, exact, short; units always named; no hype, no exclamation marks, no emoji. UI colours, type and spacing come from [docs/brand/tokens.json](docs/brand/tokens.json).
- **Commits:** imperative subject line, a body that explains why. The author is the human maintainer; do not add AI co-author or session trailers.
- **Dependencies** go through `[workspace.dependencies]`. Prefer few, well-known crates.

## Working with the maintainer

- Open design decisions are put to the maintainer **one at a time**, each with a recommended option and its trade-off, not as a batch.
- Report results plainly: what changed, what was verified and how, what is still open. If a check was not run, say so.

## Current state and next steps

Built and tested: runtime seam, record format, segmented log, the Raft core (elections with optional pre-vote and mandatory check-quorum, snapshots, learners, joint consensus, leader transfer, ReadIndex and optional leases), the simulation harness, and the management console against a built-in demo cluster.

`lotse-server` has `Replica`, a durable Raft group on `lotse-log`, and `Node`, which runs many groups over one transport with Multi-Raft batching ([ADR 0025](docs/adr/0025-frame-oriented-network-seam-and-raft-wire-format.md)) and serves clients over the first cut of the native protocol: `ApiVersions`, `Metadata`, `Produce`, `Fetch` ([ADR 0026](docs/adr/0026-native-protocol-first-cut.md)). `lotse-client` speaks it. `lotse-rt-tokio` runs everything on real disks and TCP. Produce is idempotent: producer ids from a coordinator group, sequences checked against the whole log, producer state in snapshots ([ADR 0028](docs/adr/0028-idempotent-producers.md)); `lotse-client` uses it by default. Transactions work end to end: coordinator records in the coordinator group, markers through the internal `WriteTxnMarkers`, an epoch raised per transaction, last stable offset and aborted ranges in the replica, read_committed in the client ([ADR 0029](docs/adr/0029-transactions.md)). Queues work: acks and delivery counts as batches in the queue's log, applied into an ack index in every replica; the leader pushes deliveries within each subscription's credit, after it has applied an entry of its own term and below the last stable offset; redelivery after nack, disconnect, failover and the ack timeout; consumers in the client ([ADR 0031](docs/adr/0031-queues-first-cut.md)). Durability, transport, protocol, idempotence, transaction and queue rules are mutation-checked. Next: `Publish` with exchanges that route into queues (maintainer's choice), then consumer groups for streams. Known gaps are listed at the end of [README.md](README.md).

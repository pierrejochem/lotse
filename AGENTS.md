# AGENTS.md

Project memory for everyone who changes this repository: people and AI coding agents alike, whatever the tool. Tool-specific files (`CLAUDE.md`, `.claude/`) only point here. When something here is wrong or missing, fix it here, in the same change.

## What Lotse is

A message broker in Rust: RabbitMQ-style reliable queues and Kafka-style persistent streams on one replicated log, with exactly-once delivery. Read [docs/design.md](docs/design.md) (German) for the design and [docs/adr/](docs/adr/) for why each decision was made. The design document describes the current system; ADRs keep the reasons.

## Repository map

| Path | What |
| --- | --- |
| `crates/lotse-rt` | The only seam to the outside world: `Clock`, `Disk`, `SegmentFile`, `Spawn`, `Rng`, `Net`, `Host`, `Http` traits |
| `crates/lotse-rt-tokio` | Production runtime: Tokio, file system, frames over TLS 1.3 (`tls.rs`, `net.rs`) or plain TCP on loopback; the only crate that reads the real clock; `dev-certs` feature: a throwaway CA for tests |
| `crates/lotse-record` | Batch format: 65-byte header, ZigZag-varint records, CRC32C, transaction and Raft control batches, queue acks and delivery counts |
| `crates/lotse-log` | Segmented partition log, which is also the Raft log |
| `crates/lotse-raft` | Sans-I/O Raft core and Multi-Raft; `testing::MemNode` is the in-memory driver |
| `crates/lotse-queue` | Queue semantics free of I/O: `QueueLog` (ack index every replica applies) and `Dispatcher` (the leader's subscriptions, credit, in-flight messages, redelivery, ack timeout) |
| `crates/lotse-auth` | Authentication and authorization free of I/O: `scram` (SCRAM-SHA-512 both ends), `oauth` (OAUTHBEARER, OIDC token validation), `roles` (who may do what), `testing` (a fake issuer, feature `testing`) |
| `crates/lotse-route` | Exchanges and bindings, pure routing (direct, topic, fanout, headers); the client routes with it |
| `crates/lotse-proto` | Native protocol: frames, codec conventions, error codes, request and response types |
| `crates/lotse-server` | Broker node: `Replica` (a Raft group on `lotse-log`, with producer, transaction and coordinator state), `Node` (many groups, one transport), `wire` (node-to-node format), `client` (serving the native protocol, `client/coord.rs` the transaction coordinator, `client/queues.rs` queue serving, `client/groups.rs` the consumer group coordinator, `client/admin.rs` the admin requests, `client/stats.rs` the counters behind `DescribeMetrics`, `client/session.rs` a connection's authentication; `auth.rs` the node's authentication settings and token keys, `access.rs` which request needs which role; group state in `groups.rs`, runtime resources in `metadata.rs`); simulations in `tests/cluster.rs`, `tests/nodes.rs`, `tests/node_links.rs` (who may speak on a node link), `tests/auth.rs` (logging in), `tests/access.rs` (roles and tenants), `tests/access_sim.rs` (the access simulation), `tests/clients.rs`, `tests/queues.rs`, `tests/groups.rs`, `tests/metadata.rs` and `tests/metrics.rs`, real TCP in `tests/tokio_cluster.rs` |
| `crates/lotse-client` | Rust client over the native protocol, on `lotse-rt` (runs in the simulation too) |
| `crates/lotse-sim` | Deterministic simulation: seeded executor, virtual time, crashing disks, a faulty network; Raft fault simulation in `tests/raft.rs` |
| `crates/lotse-console` | Management console backend (axum), serves `console/web`; `cluster/native.rs` a real cluster through `lotse-client`, `cluster/demo.rs` sample data; `auth.rs` sign-in (OIDC, dev mode, the sampler's client credentials), `auth/sealed.rs` sessions sealed into the cookie, `metrics.rs` chart history per resource; `examples/local_cluster.rs` three nodes on localhost with the console; `tests/native.rs` the console against real nodes, `tests/sign_in.rs` signing in against a node that requires authentication |
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

CI (`.github/workflows/ci.yml`) runs format, clippy, tests, the web app build with an OpenAPI drift check, and the simulations at 5,000 seeds; the client and queue simulations run in jobs of their own, 2,000 seeds per push and 5,000 nightly, the group simulation 1,000 per push and 5,000 nightly, the metadata simulation (creating, changing and deleting) 2,000 per push and 5,000 nightly, the access simulation (role bindings changing under load) 1,000 per push and 5,000 nightly ([ADR 0027](docs/adr/0027-client-simulation-depth-in-ci.md), [ADR 0030](docs/adr/0030-client-simulation-in-its-own-ci-job.md), [ADR 0031](docs/adr/0031-queues-first-cut.md), [ADR 0033](docs/adr/0033-consumer-groups-first-cut.md), [ADR 0034](docs/adr/0034-metadata-group.md), [ADR 0035](docs/adr/0035-deleting-resources-and-changing-settings.md), [ADR 0039](docs/adr/0039-tenants-and-roles.md)). Keep it green, and check the nightly run.

## Rules that are not negotiable

1. **Nothing above `lotse-rt` touches the real world directly.** No `Instant::now`, `SystemTime::now`, `std::thread`, OS randomness or ambient Tokio APIs; take a `Clock`, `Disk`, `Spawn` or `Rng`. No `HashMap`/`HashSet`: iteration order would make simulations unrepeatable; use `BTreeMap`/`BTreeSet`. `clippy.toml` enforces most of this. ([ADR 0006](docs/adr/0006-tokio-behind-a-runtime-seam.md))
2. **`unsafe` is forbidden** (workspace lint).
3. **Raft core stays sans-I/O.** `lotse-raft` never does I/O; drivers persist a `Ready` in its documented order: snapshot, truncate, entries (synced), hard state, apply, send ([ADR 0023](docs/adr/0023-persist-the-hard-state-after-the-log.md)).
4. **Never weaken a check to make a test pass.** A failing simulation seed is a bug until proven otherwise; find the cause, then decide whether the code or the check is wrong, and say which in the commit.
5. **Safety rules are mutation-checked.** When you add or change a rule that safety depends on, break it on purpose and confirm a test fails; if none does, add one (simulation, or a deterministic unit test for timing-dependent rules). ([ADR 0010](docs/adr/0010-deterministic-simulation-and-mutation-checking.md))
6. **A changed decision gets an ADR.** New or superseded decisions go to `docs/adr/` (copy `template.md`, next number, add it to the index) in the same change, and the design document is updated to match.
7. **No plaintext by accident.** A `TokioRuntime` uses the network only after `with_tls` or `plaintext_on_loopback`; plain TCP is refused off loopback. When the network verifies peers, a node hears only members of its configuration, each only for itself ([ADR 0037](docs/adr/0037-tls-and-node-identity.md)).
8. **The user acts as themselves.** The console never gets its own admin rights; every action uses the signed-in user's identity ([ADR 0019](docs/adr/0019-management-console-on-a-dedicated-node-with-the-user-s-identity.md)).

## Conventions

- **Language:** code, comments, docs and commit messages in English. The design document is German; keep it German when editing it.
- **Comments** explain why, not what. Public items have doc comments (`missing_docs` is on in library crates).
- **Errors** use `thiserror`; messages say what happened and, where useful, what to do.
- **Tests:** unit tests next to the code (`mod tests;` in `tests.rs`), simulations in `crates/lotse-sim/tests/` or a crate's `tests/`. Simulations take seeds from `lotse_sim::check_seeds` and print the seed on failure; scenario tests whose value does not grow with seeds use `check_few_seeds`. A simulation's oracle is written in the test, never borrowed from the code it checks.
- **UI copy and docs** follow the brand voice in [docs/brand](docs/brand/README.md): calm, exact, short; units always named; no hype, no exclamation marks, no emoji. UI colours, type and spacing come from [docs/brand/tokens.json](docs/brand/tokens.json).
- **Commits:** imperative subject line, a body that explains why. The author is the human maintainer; do not add AI co-author or session trailers.
- **Dependencies** go through `[workspace.dependencies]`. Prefer few, well-known crates.

## Working with the maintainer

- Open design decisions are put to the maintainer **one at a time**, each with a recommended option and its trade-off, not as a batch.
- Report results plainly: what changed, what was verified and how, what is still open. If a check was not run, say so.

## Current state and next steps

Built and tested: runtime seam, record format, segmented log, the Raft core (elections with optional pre-vote and mandatory check-quorum, snapshots, learners, joint consensus, leader transfer, ReadIndex and optional leases), the simulation harness, and the management console against real clusters and a built-in demo cluster.

`lotse-server` has `Replica`, a durable Raft group on `lotse-log`, and `Node`, which runs many groups over one transport with Multi-Raft batching ([ADR 0025](docs/adr/0025-frame-oriented-network-seam-and-raft-wire-format.md)) and serves clients over the first cut of the native protocol: `ApiVersions`, `Metadata`, `Produce`, `Fetch` ([ADR 0026](docs/adr/0026-native-protocol-first-cut.md)). `lotse-client` speaks it. `lotse-rt-tokio` runs everything on real disks and TCP. Produce is idempotent: producer ids from a coordinator group, sequences checked against the whole log, producer state in snapshots ([ADR 0028](docs/adr/0028-idempotent-producers.md)); `lotse-client` uses it by default. Transactions work end to end: coordinator records in the coordinator group, markers through the internal `WriteTxnMarkers`, an epoch raised per transaction, last stable offset and aborted ranges in the replica, read_committed in the client ([ADR 0029](docs/adr/0029-transactions.md)). Queues work: acks and delivery counts as batches in the queue's log, applied into an ack index in every replica; the leader pushes deliveries within each subscription's credit, after it has applied an entry of its own term and below the last stable offset; redelivery after nack, disconnect, failover and the ack timeout; consumers in the client ([ADR 0031](docs/adr/0031-queues-first-cut.md)). Durability, transport, protocol, idempotence, transaction, queue, routing, group, metadata, node-link, authentication, authorization and console sign-in rules are mutation-checked. Publishing goes through exchanges defined in the node configuration and is routed by the client, per queue or atomically in a transaction ([ADR 0032](docs/adr/0032-publish-routed-by-the-client.md)). Consumer groups work: membership and assignments persisted in the coordinator group, cooperative rebalancing so no partition has two owners, offsets committed plainly or within a transaction for exactly-once consume-transform-produce ([ADR 0033](docs/adr/0033-consumer-groups-first-cut.md)). A metadata group creates streams, queues and exchanges and binds queues at runtime (`CreateStream`, `CreateQueue`, `DeclareExchange`, `Bind`, `Unbind`), placing each new group on as many nodes as asked; configured resources stay ([ADR 0034](docs/adr/0034-metadata-group.md)). Runtime resources can be deleted (`DeleteResource`: a tombstone in the metadata state; every replica closes the group and removes its files) and a queue's ack timeout changed and read (`AlterConfigs`, `DescribeConfigs`) ([ADR 0035](docs/adr/0035-deleting-resources-and-changing-settings.md)). Every node answers `DescribeMetrics` for itself: running totals (traffic, produces, fetches, deliveries, acks), a commit latency histogram, its groups, led queues and consumer groups, and the host through the `Host` seam; the console (`kind = "native"`) asks every node and turns counters into rates ([ADR 0036](docs/adr/0036-describemetrics-and-the-console-against-a-real-cluster.md)). Every connection speaks TLS 1.3 with certificates from the cluster's CA; node links need node certificates (`lotse://node/<id>`) on both ends, and plain TCP is allowed only on loopback when asked for ([ADR 0037](docs/adr/0037-tls-and-node-identity.md)). Clients authenticate with a certificate, SASL SCRAM-SHA-512 (credentials in the metadata group, `AlterUser`, `DescribeUsers`) or SASL OAUTHBEARER (tokens checked against the issuer's keys, fetched through the `Http` seam); sessions end and clients renew them ([ADR 0038](docs/adr/0038-authentication.md)). Tenants are created explicitly and every resource belongs to one; six roles (cluster-admin, cluster-viewer since [ADR 0043](docs/adr/0043-cluster-viewer-role.md), tenant-admin, producer, consumer, viewer) are bound to users or IdP groups per tenant and name prefix in the metadata group, with bootstrap admins from the configuration; every request is checked, and what a principal may not see is unknown to it ([ADR 0039](docs/adr/0039-tenants-and-roles.md)). A simulation in its own CI job checks that nothing is allowed that no binding allowed while bindings change under faults. The console signs users in with OpenID Connect (Authorization Code with PKCE, sessions on the console, the access token passed on with OAUTHBEARER) or in dev mode with a SCRAM password, and keeps one client per principal, so each user sees what their roles allow ([ADR 0040](docs/adr/0040-console-sign-in.md)); sessions are sealed into the cookie (AES-256-GCM, a key the console makes once: [ADR 0042](docs/adr/0042-the-console-keeps-its-session-key.md)) and survive restarts, as do sign-ins in progress ([ADR 0044](docs/adr/0044-sign-ins-in-progress-in-a-sealed-cookie.md)), the metrics sampler signs in with client credentials, and charts sum only what the user may see, from history that gets coarser with age ([ADR 0045](docs/adr/0045-chart-history-coarser-with-age.md), [ADR 0041](docs/adr/0041-console-sessions-sampler-sign-in-and-charts-per-user.md)). Next, candidates: retention, a node binary. Known gaps are listed at the end of [README.md](README.md).

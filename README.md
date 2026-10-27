# Lotse

<p><img src="docs/brand/logos/lotse-wordmark.svg" alt="Lotse" height="56"></p>

Lotse is a message broker in Rust: reliable queues and persistent streams on one replicated log, with exactly-once delivery.

Status: foundation. The runtime seam, the record format, the segmented log, the Raft core and the simulation harness exist and are tested, and so does the management console, which runs against a built-in demo cluster. Transactions, queue and stream semantics and the broker server come next.

## Documentation

- [Design document](docs/design.md): architecture and storage design with the reasons behind each decision (German).
- [Architecture decisions](docs/adr/): one record per decision, with its reasons.
- [Architecture overview](docs/architecture.png): the whole system on one page.
- [Brand](docs/brand/): the Fairway mark, logos, colours, type and design tokens.
- [Console](docs/console/): screenshots and mockups of the management console.

## Contributing

[AGENTS.md](AGENTS.md) holds the rules, commands and conventions for everyone who changes this repository, people and AI coding agents alike; [agents/lotse-engineer.md](agents/lotse-engineer.md) describes the project's dedicated coding agent.

## Crates

| Crate | What it does |
| --- | --- |
| `lotse-rt` | The only seam to the outside world: `Clock`, `Disk`, `SegmentFile`, `Spawn`, `Rng` as traits |
| `lotse-record` | The batch format used on disk, on the wire and in Raft: 65-byte header, ZigZag-varint records, CRC32C, transaction markers |
| `lotse-log` | Segmented partition log (also the Raft log): append, read, roll, truncate, crash recovery |
| `lotse-raft` | Sans-I/O Raft core and Multi-Raft: optional pre-vote, check-quorum with leader stickiness, snapshots, learners, joint consensus, leader transfer, ReadIndex and optional leader leases; entries are batches covering offset ranges; messages batched per node pair |
| `lotse-server` | Broker node. So far `Replica`: one Raft group persisted on its own log (atomic hard state and snapshot files, fsync before any acknowledgement, recovery on open) |
| `lotse-sim` | Deterministic simulation: seeded executor with virtual time, a disk that loses unsynced data and tears writes on crash |
| `lotse-console` | Management console for a dedicated node: serves the web app, REST/JSON (OpenAPI) and one WebSocket for live updates |

## Management console

`lotse-console` is one binary. It embeds the React app from `console/web` and talks to the clusters on behalf of the signed-in user. Until `lotse-client` exists it serves a **demo cluster** with sample data that moves over time.

```sh
cd console/web && npm ci && npm run build && cd ../..      # build the web app first
cargo run -p lotse-console -- --config console/lotse-console.example.toml
# open http://127.0.0.1:8080
```

For frontend work, run `npm run dev` in `console/web` next to a running `lotse-console`; Vite proxies `/api` and the WebSocket to port 8080. After changing an API type in Rust, run `npm run api` to regenerate `openapi.json` and the TypeScript client; CI fails if they drift.

Frontend stack: React 18, TypeScript, Radix primitives, React Router, TanStack Query with `openapi-fetch`. Styles use the Lotse tokens, light and dark.

Everything above `lotse-rt` is written against its traits, never against Tokio or `std::time` directly. `clippy.toml` enforces that, and also bans `HashMap`/`HashSet`, whose iteration order would make simulation runs unrepeatable.

## Testing

```sh
cargo test --workspace
```

The crash test in `crates/lotse-sim/tests/log_crash.rs` appends, syncs, truncates and pulls the plug across hundreds of seeded runs, then checks that every synced batch survives and nothing is invented. To run more seeds, or reproduce a failure:

```sh
LOTSE_SIM_SEEDS=5000 cargo test -p lotse-sim --release   # many seeds
LOTSE_SIM_SEED=1234 cargo test -p lotse-sim               # one seed, exactly
```

A failing run prints the seed that reproduces it.

The Raft simulation in `crates/lotse-sim/tests/raft.rs` runs 3- and 5-node groups through lost, delayed, duplicated and reordered messages, partitions, clock drift and crashes (before and after persisting, some with an immediate restart), with fault intensity, pre-vote and leader leases drawn per seed. Meanwhile nodes compact their logs at random, followers catch up through snapshots, the leader is asked to change members at random (add, promote and remove learners, remove voters, itself included, replace several voters or drop two at once in one joint change) and to hand leadership over, and clients send linearizable reads to random nodes. It checks vote safety, election safety, state machine safety, durability of committed entries, leader completeness and that every confirmed read is served at an index no older than anything committed before it was asked, on every step, and liveness after healing.

The replica simulation in `crates/lotse-server/tests/cluster.rs` runs 3- and 5-node groups of real `Replica`s, each on its own simulated disk, through the same message faults and through power loss: between rounds and in the middle of one (the disk stops after a random number of writes), keeping only synced data plus a possibly torn prefix of the rest. Nodes restart from whatever their disk holds and snapshot at random. It checks election safety and that every node applies the same batches in the same order across restarts, and liveness after healing.

Together with the unit tests in `lotse-raft` the Raft simulation was mutation-checked: each of these 18 bugs makes a test fail. Committing older-term entries by counting (Raft's "figure 8"), voting for out-of-date candidates, forgetting a vote on restart, keeping conflicting follower entries, committing past what a follower verified, committing with one ack too few, counting only the new voters of a joint configuration (for commits or for elections), changing voters without a joint configuration, installing a snapshot over a matching log, ignoring votes from nodes outside the membership, never letting a just-dropped voter campaign, serving a read without confirming leadership, before the new leader's first commit or on a round sent before the read, running a lease from an unanswered round, keeping the lease after trying a transfer, and skipping the quiet period after a restart.

## Known gaps

- Compression codecs (zstd, lz4) are declared in the format but not implemented; compressed batches are rejected.
- The sparse offset index is rebuilt by scanning on open; `.index`, `.timeindex` and `.txnindex` files come later.
- Raft: snapshots travel in one message; chunked transfer comes with `lotse-server`. A leader backing up past a conflict resends a whole term's entries rather than probing. Followers do not serve reads themselves; the server forwards them to the leader.
- `Net` joins `lotse-rt` together with `lotse-server`.
- There is no Tokio implementation of `lotse-rt` yet; it lands with `lotse-broker`.
- Console: only the demo cluster exists (`kind = "native"` is refused until `lotse-client` lands), and sign-in is `dev` mode only (OIDC is refused at startup). Purge, requeue and moving leaders have no API yet, so the UI does not offer them. On a phone the navigation sits above the page instead of collapsing into a menu.

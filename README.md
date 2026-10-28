# Lotse

<p><img src="docs/brand/logos/lotse-wordmark.svg" alt="Lotse" height="56"></p>

Lotse is a message broker in Rust: reliable queues and persistent streams on one replicated log, with exactly-once delivery.

Status: foundation. The runtime seam with its Tokio implementation, the record format, the segmented log, the Raft core, the simulation harness, a broker node (many Raft groups over one transport) and the first cut of the native protocol with a Rust client (idempotent produce to and fetch from stream partitions) exist and are tested, and so does the management console, which runs against a built-in demo cluster. Transactions, queues and the remaining API keys come next.

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
| `lotse-rt` | The only seam to the outside world: `Clock`, `Disk`, `SegmentFile`, `Spawn`, `Rng` and `Net` (connections carrying whole frames) as traits |
| `lotse-rt-tokio` | The production runtime: Tokio, the file system with directory fsyncs, TCP with length-prefixed frames |
| `lotse-record` | The batch format used on disk, on the wire and in Raft: 65-byte header, ZigZag-varint records, CRC32C, transaction markers |
| `lotse-log` | Segmented partition log (also the Raft log): append, read, roll, truncate, crash recovery |
| `lotse-raft` | Sans-I/O Raft core and Multi-Raft: optional pre-vote, check-quorum with leader stickiness, snapshots, learners, joint consensus, leader transfer, ReadIndex and optional leader leases; entries are batches covering offset ranges; messages batched per node pair |
| `lotse-proto` | The native protocol: frames, field conventions, error codes; `ApiVersions`, `Metadata`, `FindCoordinator`, `Produce`, `Fetch` and `InitProducer` |
| `lotse-server` | Broker node. `Replica`: one Raft group persisted on its own log (atomic hard state and snapshot files, fsync before any acknowledgement, recovery on open). `Node`: many replicas behind one transport, all messages for a peer bundled into one frame per round (`wire`), and clients served over the native protocol: produce answered once committed and deduplicated per producer, fetch cut at the high watermark, producer ids from a coordinator group |
| `lotse-client` | Rust client on `lotse-rt`: metadata, produce and fetch, following leader changes; idempotent by default, so it retries unknown outcomes without duplicates |
| `lotse-sim` | Deterministic simulation: seeded executor with virtual time, a disk that loses unsynced data and tears writes on crash, a network with latency, partitions and broken connections |
| `lotse-console` | Management console for a dedicated node: serves the web app, REST/JSON (OpenAPI) and one WebSocket for live updates |

## Management console

`lotse-console` is one binary. It embeds the React app from `console/web` and talks to the clusters on behalf of the signed-in user. Until the admin API keys exist it serves a **demo cluster** with sample data that moves over time.

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

The replica simulation in `crates/lotse-server/tests/cluster.rs` runs 3- and 5-node groups of real `Replica`s, each on its own simulated disk, through the same message faults and through power loss: between rounds and in the middle of one (the disk stops after a random number of writes), keeping only synced data plus a possibly torn prefix of the rest. Nodes restart from whatever their disk holds and snapshot at random. It checks election safety and that every node applies the same batches in the same order across restarts, and liveness after healing. Together with the replica's unit tests it catches all eight seeded durability bugs: no fsync of the log or of the temporary file behind an atomic write, the hard state written before the log, keeping a log that does not continue the snapshot, not restarting the log on a snapshot install, not replaying committed entries after a restart, no term clamp on restart, and keeping a torn tail in log recovery.

Together with the unit tests in `lotse-raft` the Raft simulation was mutation-checked: each of these 18 bugs makes a test fail. Committing older-term entries by counting (Raft's "figure 8"), voting for out-of-date candidates, forgetting a vote on restart, keeping conflicting follower entries, committing past what a follower verified, committing with one ack too few, counting only the new voters of a joint configuration (for commits or for elections), changing voters without a joint configuration, installing a snapshot over a matching log, ignoring votes from nodes outside the membership, never letting a just-dropped voter campaign, serving a read without confirming leadership, before the new leader's first commit or on a round sent before the read, running a lease from an unanswered round, keeping the lease after trying a transfer, and skipping the quiet period after a restart.

The node simulation in `crates/lotse-server/tests/nodes.rs` runs three whole `Node`s, each with its own disk and host name, hosting three Raft groups over the simulated network: the real main loop, senders, readers and wire format. Clients propose through node handles while pairs of hosts are partitioned and nodes lose power, between rounds or in the middle of a write, and restart from their disk. It checks that in every group all nodes apply the same batches in the same order and there is at most one leader per term, and that after healing a new batch commits in every group and reaches every node. Together with the codec's unit tests it catches all nine seeded transport bugs: dropping incoming messages, stepping them into the wrong group, queueing them for the wrong peer, a sender that stops after a failed connect or after a broken connection, a codec that swaps fields, accepts trailing bytes or trusts any message count, and passing on a client batch stamped with a term other than its entry's. `crates/lotse-server/tests/tokio_cluster.rs` runs three nodes for real on localhost TCP: a leader is stopped, the group goes on under a new one, and the old node catches up after its restart.

The client simulation in `crates/lotse-server/tests/clients.rs` runs three or five nodes hosting a stream with two partitions and a coordinator group. Two idempotent producers and two consumers use `lotse-client` over the simulated network while pairs of hosts, clients included, are partitioned and nodes lose power. It checks that every acknowledged batch is in its partition at the acknowledged offset, that everything a consumer read is committed at that offset, that no batch reported as not written is there, that no batch is there twice although producers retry unknown outcomes, and that a new batch commits after healing. Together with a deterministic five-node unit test it catches all eight seeded protocol bugs: acknowledging a batch once appended or without checking its term at commit, settling without waiting, reporting "not written" as soon as the entry is replaced on the proposing node (only five voters allow the history that disproves it), returning uncommitted batches, treating offsets between the high watermark and the log end as out of range, and a client that retries a produce that may have arrived or that timed out (checked without idempotence). Eleven seeded idempotence bugs are caught as well, by the simulation and by unit tests of the producer state, the replica and two deterministic five-node histories: a check that ignores the log tail or recognises no duplicates, a tail that is not cut when Raft truncates or not rebuilt on restart, producer state left out of snapshots, no fencing of older epochs, accepted gaps, evicting the newest producer instead of the oldest, answering a retry of a batch in flight before the original settles, and a client that reuses a sequence or keeps its producer id after an unknown outcome. The Tokio test also produces and fetches through `lotse-client`, across the leader's restart.

## Known gaps

- Compression codecs (zstd, lz4) are declared in the format but not implemented; compressed batches are rejected.
- The sparse offset index is rebuilt by scanning on open; `.index`, `.timeindex` and `.txnindex` files come later.
- Raft: snapshots travel in one message and must fit in one 16 MiB frame; chunked transfer comes later. A leader backing up past a conflict resends a whole term's entries rather than probing. Followers do not serve reads themselves; the server forwards them to the leader.
- Connections have no TLS or authentication yet, between nodes or from clients. Streams, their partitions' groups and group membership are fixed by the node configuration until the metadata group exists.
- Native protocol: only `ApiVersions`, `Metadata`, `FindCoordinator`, `Produce`, `Fetch` and `InitProducer`. Producer ids do not survive a client restart (that needs transactional ids), and a producer evicted from a full partition loses its deduplication. `Fetch` copies through the log rather than using `sendfile`, and `read_committed` equals the high watermark until transactions exist.
- Console: only the demo cluster exists (`kind = "native"` is refused until the admin API keys land), and sign-in is `dev` mode only (OIDC is refused at startup). Purge, requeue and moving leaders have no API yet, so the UI does not offer them. On a phone the navigation sits above the page instead of collapsing into a menu.

# Lotse

Lotse is a message broker in Rust: reliable queues and persistent streams on one replicated log, with exactly-once delivery.

Status: foundation. The runtime seam, the record format, the segmented log and the simulation harness exist and are tested. Raft, transactions, queue and stream semantics and the server come next.

## Crates

| Crate | What it does |
| --- | --- |
| `lotse-rt` | The only seam to the outside world: `Clock`, `Disk`, `SegmentFile`, `Spawn`, `Rng` as traits |
| `lotse-record` | The batch format used on disk, on the wire and in Raft: 65-byte header, ZigZag-varint records, CRC32C, transaction markers |
| `lotse-log` | Segmented partition log (also the Raft log): append, read, roll, truncate, crash recovery |
| `lotse-sim` | Deterministic simulation: seeded executor with virtual time, a disk that loses unsynced data and tears writes on crash |

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

## Known gaps

- Compression codecs (zstd, lz4) are declared in the format but not implemented; compressed batches are rejected.
- The sparse offset index is rebuilt by scanning on open; `.index`, `.timeindex` and `.txnindex` files come later.
- `Net` joins `lotse-rt` together with `lotse-server`.
- There is no Tokio implementation of `lotse-rt` yet; it lands with `lotse-broker`.

# 6. Tokio behind a runtime seam

- Status: Accepted
- Date: 2026-10-27

## Context

Tokio is the pragmatic runtime for Rust networking, but code written against ambient Tokio APIs and the system clock cannot be tested deterministically.

## Decision

Everything above `lotse-rt` uses its traits (`Clock`, `Disk`, `SegmentFile`, `Spawn`, `Rng`, later `Net`), never Tokio or `std::time` directly. Production plugs in Tokio; tests plug in `lotse-sim`. `clippy.toml` bans `Instant::now`, `SystemTime::now`, `thread::spawn`/`sleep`, `HashMap` and `HashSet` (whose iteration order would make runs unrepeatable).

## Consequences
- The same code runs in production and in seeded simulation.
- Tokio-specific conveniences are only available behind the seam.

See also: [design document](../design.md#crate-struktur).

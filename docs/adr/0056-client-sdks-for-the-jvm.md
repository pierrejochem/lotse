# 56. Client SDKs for the JVM

- Status: Accepted
- Date: 2026-11-01

## Context

Lotse has one client, `lotse-client` in Rust. Applications on the JVM have none, and the native protocol has no schema: `crates/lotse-proto` and `crates/lotse-record` are the specification, `docs/design.md` had drifted from them (the push layout, error codes 27 and 28, batch kinds 4 and 5), and nothing gives a second implementation bytes to check itself against. The tag blocks are not uniform across messages, the frame length is big-endian while every other field is little-endian, and the unsigned types have no Java counterpart. Each of those is an easy mistake that only shows against a real broker.

## Decision

**A directory `sdk/` holds the SDKs.** `sdk/java` is the core, a Gradle project (Kotlin DSL, pinned wrapper) for Java 17 with `CompletableFuture`, `SSLSocket` and one reader thread per connection, and no runtime dependencies. JUnit is for tests only. `sdk/kotlin` follows as a thin layer on it (`suspend`, `Flow`, builders). The package name `dev.lotse` is a placeholder until a first release; publishing is out of scope.

**The scope is what `lotse-client` does:** streams (idempotent produce, transactions, read_committed fetch), queues, consumer groups, exchanges routed in the client, admin calls, TLS, SCRAM-SHA-512 and OAUTHBEARER. It is built in phases, each with green tests: the protocol codec and record batch first, then connections and authentication, streams, queues, transactions and groups, exchanges and admin, the Kotlin layer, examples and CI.

**The Rust code is the authority, and the vectors enforce it.** `crates/lotse-proto/examples/vectors.rs` writes golden bytes to `sdk/conformance`: every sample request, response and push of the Rust round-trip tests, record batches built with `BatchBuilder` (plain, idempotent, transactional, control, queue kinds, edited in place), and fixed cases (CRC32C check values, ZigZag edge cases, error codes). The samples moved out of `tests.rs` into a `samples` module behind a `samples` feature. Each SDK decodes every vector and writes it again to identical bytes, and refuses each vector cut at every length. `make sdk-vectors-check` regenerates the vectors into a temporary directory and compares, so a layout change in Rust fails until the vectors are regenerated and the SDKs pass.

**The codec is written per message, not generically.** The messages end their structures with tagged fields in different places (the leader hint is sometimes the trailing block, sometimes in the middle, sometimes inside each partition, and a few responses have none), so a generic walker would be wrong. The vectors cover each layout. Unsigned integers are `long` and `int` with explicit helpers; `NO_OFFSET` is `-1` as a `long`.

**The node-to-node keys 43, 44 and 1000 are constants only.** Clients never send them.

**`sdk/PROTOCOL.md` is the implementer's guide** and records what the code shows where the design document or the plan was wrong, such as offsets being the Raft index minus one.

## Consequences

A second protocol implementation has bytes to check against from its first line, and a change to the protocol in Rust cannot silently leave the SDKs behind. The cost is a feature flag on `lotse-proto` and a second place to regenerate after changing a message. Java 17 as the floor leaves out records-pattern syntax in the SDK's own code; the build checks this with `options.release = 17`. The Kotlin layer and the end-to-end tests against real nodes (phases 7 and 8) depend on the Java core and cannot start before its connection and client phases. CI for the SDK is added with those phases; until then `make sdk-test` is the check.

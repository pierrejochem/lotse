# Clients and the native protocol

Lotse speaks one binary protocol, the native protocol, over TLS 1.3. Every client is a program that speaks it. This chapter says which clients exist, how they relate and how they are kept correct. The rest of the developer guide covers each client and the recipes for exactly-once processing.

## The clients

| Client | Where | Notes |
| --- | --- | --- |
| Rust | `crates/lotse-client` | The reference. Written against the `lotse-rt` traits, so it runs on Tokio and in the deterministic simulation. |
| Java | `sdk/java` | Java 17, Gradle. No runtime dependencies; zstd and lz4 batches need two optional ones. |
| Kotlin | `sdk/kotlin` | A thin layer on the Java client: `suspend` functions on `Dispatchers.IO`, flows and `transaction { }`. |

The Rust code is the authority. `lotse-proto` and `lotse-record` define every byte on the wire; `lotse-client` defines how a client behaves: routing to leaders, retries, idempotent sequences, transactions, groups. The Java client follows it rule for rule. The reasons are in [ADR 0056](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0056-client-sdks-for-the-jvm.md); the Kotlin layer is in [ADR 0080](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0080-kotlin-layer-on-dispatchers-io.md).

Pages in this guide:

- [The native protocol](protocol.md): byte layouts and client behaviour, for anyone writing a client in another language.
- [Rust client](rust-client.md)
- [Java and Kotlin](java-and-kotlin.md)
- [Exactly-once recipes](exactly-once.md)
- [Contributing](contributing.md)

## What a client does

A client does four things on top of the frame format:

1. **Finds leaders.** It asks any broker for `Metadata`, then sends `Produce` and `Fetch` to the partition's leader and follows `NOT_LEADER` hints.
2. **Retries safely.** Idempotent produce stamps every batch with a producer id and a sequence per partition, so a retry after a lost answer cannot write a batch twice.
3. **Keeps sessions.** It authenticates each connection (certificate, SCRAM-SHA-512 or OAUTHBEARER) and renews the session before it ends.
4. **Owns the protocols above the log.** Queue credit and acks, transactions, consumer group heartbeats and cooperative rebalancing are all client behaviour driven by broker requests.

A client chooses the partition itself; it does not hash keys for you.

## Golden vectors

Two implementations of a byte format agree only if something checks it. The Rust codec writes the check files, and every SDK reads them.

- `sdk/conformance/` holds the vectors: encoded requests, responses and pushes, record batches (plain, compressed, compacted), varints, CRC32C values, error codes, exchange routing cases and SCRAM exchanges.
- They are written by `crates/lotse-proto/examples/vectors.rs`. Never edit them by hand.
- A decoder passes when it reads every vector and writes it again to the same bytes, refuses each vector cut at every length and with one byte added, and agrees with the field files. [The protocol page](protocol.md#conformance-vectors) lists the files.

Regenerate the vectors after you change a message or the batch format:

```sh
make sdk-vectors
```

`make sdk-test` first checks that `sdk/conformance` still matches what the Rust codec writes, then runs the Java SDK and Kotlin layer tests and compiles their examples (`./gradlew check` in `sdk/java` and `sdk/kotlin`):

```sh
make sdk-test
```

You need a JDK 17 or later. Set `LOTSE_BIN` to a `lotse` executable to add the TLS and real-node tests. CI runs the same target with `LOTSE_BIN` set to the binary it builds.

## Writing a client in another language

1. Read [the protocol page](protocol.md) and the vectors.
2. Pass the vectors first: field codec, frames, record batch.
3. Add connections, authentication and metadata routing.
4. Add idempotent produce and read_committed fetch.
5. Add queues, transactions and groups, in that order of need.
6. Follow `lotse-client` for behaviour. Where the protocol page and the Rust code differ, the Rust code wins and the page is a bug: fix it in the same change.

# Lotse client SDKs

Client libraries for the Lotse native protocol. The Rust code is the authority: [`crates/lotse-proto`](../crates/lotse-proto) and [`crates/lotse-record`](../crates/lotse-record) define every byte, and [`lotse-client`](../crates/lotse-client) defines the client's behaviour. The reasons are in [ADR 0056](../docs/adr/0056-client-sdks-for-the-jvm.md).

| Path | What |
| --- | --- |
| [`PROTOCOL.md`](PROTOCOL.md) | The protocol as an implementer needs it: byte layouts, the traps, what the vectors pin down |
| [`conformance/`](conformance/) | Golden vectors, written by Rust and read by every SDK |
| [`java/`](java/) | Java 17, no runtime dependencies, Gradle |

## Status

Phase 1 of the JVM client is done: `dev.lotse.proto` (field codec, frames, every client-facing request and response, error codes) and `dev.lotse.record` (record batch, builder, in-place edits, control batches). Connections, authentication, the client itself and the Kotlin layer follow.

## Build and test

```sh
make sdk-test            # check the vectors against Rust, then run the Java tests
make sdk-vectors         # regenerate sdk/conformance after changing the protocol
```

`make sdk-test` needs a JDK 17 or newer. The build targets Java 17 whatever JDK runs it. Tests ran on JDK 25 and 21.

## Conformance

`make sdk-vectors-check` regenerates the vectors into a temporary directory and compares them with `sdk/conformance`; a change to a message layout fails it until the vectors are regenerated and committed. Each SDK decodes every vector and writes it again to identical bytes.

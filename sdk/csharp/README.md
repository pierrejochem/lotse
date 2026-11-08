# Lotse client for .NET

A C# client for the Lotse native protocol. It follows the Java SDK rule for rule ([ADR 0083](../../docs/adr/0083-client-sdks-for-csharp-and-nodejs.md), [ADR 0056](../../docs/adr/0056-client-sdks-for-the-jvm.md)). The Rust code is the authority; the golden vectors in [`sdk/conformance`](../conformance) enforce it. The protocol as an implementer needs it is in [`sdk/PROTOCOL.md`](../PROTOCOL.md).

## Status

Phase 1 of 8 is done: the protocol codec and the record batch. There is no connection and no client yet.

| Namespace | What |
| --- | --- |
| `Lotse.Proto` | Field reader and writer, frames, every client-facing request and response (one codec per message), error codes |
| `Lotse.Record` | Record batch parser and builder, in-place edits (producer, transactional bit, base timestamp), control batches, varints, CRC32C |

Not yet: connections, TLS, authentication, streams, queues, transactions, consumer groups, admin calls, examples.

## Types

- .NET 10, nullable reference types, warnings as errors, no runtime dependencies.
- Unsigned wire types keep their width: `u64` is `ulong`, `u32` is `uint`, `u16` is `ushort`, `u8` is `byte`. Timestamps and `i64` fields are `long`.
- Offsets are `ulong`. The offset of "no committed offset" is `Messages.NoOffset` (`ulong.MaxValue`); `Messages.NoOffsetSigned` is the same bits as `-1L`.
- Fixed-width fields are little-endian. The frame length prefix is big-endian (`Frames.WithLengthPrefix`, `Frames.ReadLength`).
- A response with the error flag decodes to `Response.Failed` or `Response.Refused`, whatever request it answers.

## Compressed batches

The SDK has no zstd or LZ4 library yet. A compressed batch still parses: its header and checksum are checked. Reading its records, or building a compressed batch, throws `RecordFormatException` with kind `UnsupportedCompression`, naming the codec. Which library will serve the codecs is settled with the stream client; per ADR 0083 it stays optional.

## Build and test

```sh
export DOTNET_ROOT=$HOME/.dotnet PATH=$HOME/.dotnet:$PATH   # wherever the .NET 10 SDK is
make sdk-csharp-test                                         # or: cd sdk/csharp && dotnet test
```

`global.json` pins the SDK. The tests read the vectors from `sdk/conformance` in place and find them from the test binary's location; `LOTSE_CONFORMANCE` names another directory. Every vector is decoded and written again to identical bytes, and refused when cut at any length or with a byte added.

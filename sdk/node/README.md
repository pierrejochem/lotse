# Lotse client for Node.js

A client for the Lotse native protocol in TypeScript, for Node.js 20 and later. It follows [ADR 0083](../../docs/adr/0083-client-sdks-for-csharp-and-nodejs.md) and [ADR 0056](../../docs/adr/0056-client-sdks-for-the-jvm.md): the Rust code is the authority, the golden vectors in [`../conformance`](../conformance) enforce it, and the [Java client](../java) is the reference for structure. The package name `@lotse/client` is a placeholder; nothing is published.

## Status

Phase 1 of 8 is done: the protocol codec and the record batch. There are no connections yet.

| Module | What |
| --- | --- |
| `src/proto` | Field codec (`Reader`, `Writer`), frames and pushes, every client request and response, one codec per message, error codes |
| `src/record` | Record batch: parse, build, in-place edits (`stamp`, `setProducer`, `setTransactional`, `setBaseTimestamp`), control batches, CRC32C, varints |

## Conventions

- No runtime dependencies. `typescript` and `@types/node` are development dependencies only.
- A 64-bit wire value is a `bigint`. A `u64` is held as its signed 64-bit bit pattern, as in the Java client, so `NO_OFFSET` (`u64::MAX`) is `-1n`. Compare, add and print such values with `compareU64`, `addU64Exact` and `u64ToString`; do not use `<` or `String` when a value may be that large.
- Narrower integers (`u8`, `u16`, `u32`, `i32`) are `number`. A writer refuses a value out of range with a `RangeError`.
- Fixed-width fields are little-endian. The frame length prefix is big-endian.
- A frame that does not decode throws `DecodeError`; a batch that does not parse throws `RecordFormatError`. Both carry a `kind`.
- Keys, values and header values are `Uint8Array`, `null` where the protocol has a null. Null and empty differ.

## Compression

zstd and lz4 are optional (ADR 0061, ADR 0083). This phase reads and writes uncompressed batches. A compressed batch parses and its header is checked, but reading or building its record block throws `RecordFormatError` of kind `UNSUPPORTED_COMPRESSION`, naming the codec. A compacted batch with no records reads as empty whatever its codec, because compaction writes no block for it.

## Tests

```sh
npm ci
npm test        # compiles to dist/, then runs node:test on dist/test
```

or `make sdk-node-test` from the repository root. The tests decode every vector in `sdk/conformance` and write it again to identical bytes, refuse each vector cut at every length and with one byte added, and check the fixed cases for CRC32C, varints, error codes and in-place batch edits. Set `LOTSE_CONFORMANCE` to read the vectors from another directory.

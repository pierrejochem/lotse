# 85. Codecs in the Node.js SDK

- Status: Accepted
- Date: 2026-11-08

## Context
Since [ADR 0058](0058-zstd-and-lz4-compression.md) the broker stores and serves batches compressed with zstd or lz4 exactly as producers sent them, so a consumer must read every codec its producers use. [ADR 0061](0061-optional-codecs-in-the-java-sdk.md) settled this for Java with two optional libraries, and [ADR 0083](0083-client-sdks-for-csharp-and-nodejs.md) left the choice for Node.js to the phase that needs it: phase 3, the stream client, which fetches batches and produces them.

The Node.js SDK has no runtime dependencies. Node.js has no lz4. Its `node:zlib` has zstd (`zstdCompressSync`, `zstdDecompressSync`) since 22.15 and 23.8, and in 24; Node.js 20, the oldest the SDK supports, has none. A zstd decoder is too large and too risky to write inside the SDK. An lz4 frame decoder and a simple encoder are small.

## Decision
**lz4 is written in the SDK** (`src/record/lz4.ts`, `xxhash.ts`), so it works on every supported Node.js and needs no dependency.
- The decoder reads the LZ4 frame format as lz4_flex and lz4-java write it: block sizes 64 KiB to 4 MiB, independent or linked blocks, an optional content size, block checksums and content checksum (all verified with xxHash32). It refuses a dictionary, reserved bits, a legacy frame, a frame that ends early or has bytes after its end, and output past 16 MiB (`TOO_LARGE`; exactly 16 MiB is allowed). Any other frame that does not decode is `MALFORMED`.
- The encoder writes one frame of independent 64 KiB blocks with no checksums, as the Java SDK does, using a greedy hash-table match finder. A block that does not shrink is stored as it is.

**zstd comes from `node:zlib` when it has it.** The functions are looked up at every call, never at import time, so a Node.js without them fails with `RecordFormatError` of kind `UNSUPPORTED_COMPRESSION`, not with a `TypeError`. The message names the codec, says that `node:zlib` has no zstd in this Node.js version, names Node.js 22.15, 23.8 or later (24 is tested), and names `registerCodec`. Compression is level 3, as in the broker. Decompression stops one byte past 16 MiB (`maxOutputLength`) and the batch is refused as `TOO_LARGE`.

**`registerCodec(codec, { compress, decompress })` is the way out for Node.js 20.** An application that must read zstd there passes a function pair of its own (from any npm package it chooses); `undefined` restores the built-in one. The SDK does not depend on or name such a package: an optional peer dependency would pin a native module whose prebuilds we cannot test on every platform, and a function pair is the same seam without that. A codec that throws is reported as damage (`MALFORMED`), not as a missing codec.

**A missing codec is not damage.** `Fetched.committedMessages` and `Batch.records` pass `UNSUPPORTED_COMPRESSION` on as it is. The builder checks its 16 MiB limit before it calls a codec and refuses to compress more.

## Consequences
- A Node.js consumer reads streams and queues whatever codec their producers use on Node.js 22.15 or later, and lz4 on every version. On Node.js 20 zstd needs `registerCodec`; without it the error says so, and everything else works.
- The SDK has no dependency, and lz4 is code we maintain: about 390 lines with its xxHash32. It is tested against the broker's vectors (`compressed.hex`, `compacted.hex`), frames written by hand with every option of the format, cuts at every length, 3 000 mutated frames, and by the real node: a batch this SDK compressed is accepted by the node (which decodes it once on produce and refuses one that does not decode) and served back, for lz4 and zstd; a garbled lz4 frame is refused with `CORRUPT_BATCH`.
- Mutation checks: skipping the decode, the size limit off by one or removed, the header, block or content checksum not verified, a match offset 0 accepted, independent blocks reaching back, the minimum match length off by one, a missing codec reported as damage, and the builder not compressing each broke a test.
- Only Node.js 24 has run the tests. The zstd tests are skipped, with the reason, where `node:zlib` has no zstd; the missing-codec path is tested on every version by removing the functions for the length of one test.
- The encoder is not tuned. It finds matches within 64 KiB with one candidate per hash, so it compresses less than lz4's own `LZ4_compress_default` on some inputs. The format is the same; a better match finder can replace it without a change to anything else.

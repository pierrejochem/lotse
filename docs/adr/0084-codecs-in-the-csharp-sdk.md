# 84. Codecs in the C# SDK

- Status: Proposed
- Date: 2026-11-08

## Context
Since [ADR 0058](0058-zstd-and-lz4-compression.md) the broker stores and serves batches compressed exactly as producers send them, so a consumer must read every codec its producers use. [ADR 0083](0083-client-sdks-for-csharp-and-nodejs.md) gave the C# SDK no runtime dependencies and left the choice of zstd and LZ4 libraries to the phase that builds the stream client. The base class library has neither codec. A zstd decoder is too large and too risky to write and keep inside the SDK, for the reason given in [ADR 0061](0061-optional-codecs-in-the-java-sdk.md). .NET offers two kinds of library: bindings to the reference C libraries, which need a native file for every platform, and ports written in managed code.

## Decision
**A second package, `Lotse.Client.Codecs`, optional.** `Lotse.Client` stays free of dependencies. The project `sdk/csharp/src/Lotse.Client.Codecs` holds the codecs and nothing else, and references two pinned NuGet packages: **ZstdSharp.Port 0.8.6** (zstd, MIT, managed) and **K4os.Compression.LZ4.Streams 1.3.8** (the LZ4 frame format, MIT, managed). Both are managed code, so the package has no native assets per platform, runs where the .NET runtime runs, and needs nothing at publish time for ahead-of-time compilation. A project that never meets a compressed batch references neither.

**The core defines the seam.** `Lotse.Client` declares `IBlockCodec` (decompress one frame up to a limit, compress records into one frame), `IBlockCodecProvider`, and `CompressionCodecs` with `Register(Compression, IBlockCodec)` and `IsAvailable`. The first time a compressed batch is read or built, the SDK looks once for the type `Lotse.Codecs.DefaultCodecProvider` in the assembly `Lotse.Client.Codecs` and asks it for the codec it needs. Adding the package is enough. A program that is trimmed or published ahead of time, where finding a type by name fails, calls `LotseCodecs.Register()` once; the same call shape lets a project register libraries of its own. A registered codec wins over the one found.

**A missing codec is named, not mistaken for damage** (as in ADR 0061). Reading or building a batch whose codec is not available throws `RecordFormatException` of kind `UnsupportedCompression`; the message says to add the package `Lotse.Client.Codecs` and which codec. A package that is found but cannot be used (an assembly it needs is missing) is named the same way, with the reason. The client passes the exception on as it is, not as `CORRUPT_BATCH`, and a batch still parses without a codec: header and checksum are checked, so a consumer can follow offsets through batches it cannot read.

**The same rules as the broker's.** The record block is one zstd frame or one LZ4 frame (the frame format). The SDK, not the codec, enforces the limits: decompression stops one byte past 16 MiB and the batch is refused (`TooLarge`); a block that does not decode, or a codec that throws, is `Malformed`; the builder refuses to compress more than 16 MiB of records. `BatchBuilder` writes zstd at level 3 and LZ4 as independent 64 KiB blocks without checksums, as the broker's builder and the Java SDK do. Nothing compresses by default: the client sends a batch as it was built, with the producer fields written into a copy, and the node serves it compressed.

## Consequences
- A C# consumer reads streams whatever codec their producers used, once the package is referenced. A C# producer can compress with `BatchBuilder` and `ProduceBatchAsync`.
- Users who need compression add one package with two managed dependencies. Without it everything else works, and compressed batches fail with an error that says what to add.
- Both libraries are ports or re-implementations, not the reference libzstd and liblz4. That is the trade-off against native bindings: no platform files and no loader failures, but code that is not Yann Collet's. The tests below bound the risk: the broker's compressed vectors, written by the Rust code, read as written, and a real node, whose leader decodes every compressed batch once when it is produced, accepts the frames this package writes and serves them back, and refuses a block that is not a frame. If a port ever fails on a frame the Rust side writes, the seam allows swapping a library without touching the SDK.
- Tests:
  - The broker's compressed vectors (`compressed.hex`) read as written, and the compacted vector with a codec reads too.
  - Batches compressed here read like their plain twins and are much smaller; blocks of several LZ4 blocks round-trip.
  - Truncated, garbled and plain-but-marked blocks are refused; a block decompressing to one byte past 16 MiB is refused, and one at exactly 16 MiB is not refused for its size; the builder refuses to compress past the limit.
  - A separate test assembly (`Lotse.Client.NoCodecs.Tests`) does not reference the package: compressed batches name the package, plain batches read, a codec registered by hand is used in both directions, the SDK's limit holds against a codec that ignores it, and a provider that fails to load is named.
  - Against three real nodes: zstd and LZ4 batches are produced, fetched and read back compressed, and a block that is not a frame is refused with `CORRUPT_BATCH`.
- Mutation checks, each broken on purpose with a test failing:
  - the size check, removed or set off by one, and the same check in the SDK against a codec that ignores the limit;
  - the builder's limit;
  - decompression skipped, and the builder not compressing;
  - a codec reported as available when it is not;
  - a missing codec reported as damage, and a codec that throws not reported as `Malformed`.
- The libraries are pinned. ZstdSharp.Port and K4os.Compression.LZ4 follow their own release cycles; a Dependabot entry for NuGet under `sdk/csharp` is left to the maintainer.

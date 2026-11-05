# 61. Optional codecs in the Java SDK

- Status: Accepted
- Date: 2026-11-05

## Context

Since ADR 0058 the broker stores and serves batches compressed with zstd or lz4 exactly as producers send them, so a consumer must read every codec its producers use. The Java SDK had no runtime dependencies and refused every compressed batch, so a Java consumer could not read a stream that a Rust producer compressed. The JDK has neither codec. A zstd decoder is too large and too risky to write and maintain inside the SDK.

## Decision

**zstd-jni and lz4-java, both optional** (maintainer's choice). The SDK decompresses with `com.github.luben:zstd-jni` (the reference libzstd through JNI) and `at.yawk.lz4:lz4-java`. These are the libraries Kafka's Java client uses. lz4-java is taken from the maintained fork, at 1.11.0, past the fixes for CVE-2025-12183 and CVE-2025-66566. The Gradle build publishes them as the feature `compression`, and a Maven POM lists them as optional, so a project that never meets a compressed batch pulls in neither. The core stays dependency-free.

**A missing library is named, not mistaken for damage.** Before a codec is used, the SDK checks that its library's class loads. If it does not, reading or building such a batch fails with `RecordFormatException` of kind `UNSUPPORTED_COMPRESSION`, and the message says which artifact to add. The classes that call the libraries are loaded only after that check, so a missing jar never surfaces as a `NoClassDefFoundError`. A native library zstd-jni cannot load on the platform is reported the same way. The client passes this exception on as it is, rather than as `CORRUPT_BATCH`.

**The same rules as the broker's.** The record block is one zstd frame or one LZ4 frame. Decompression stops one byte past 16 MiB and refuses the batch (`TOO_LARGE`). A block that does not decode is `MALFORMED`. `BatchBuilder` compresses when its options name a codec: zstd at level 3, and LZ4 as a frame of independent 64 KiB blocks. It refuses to compress more than 16 MiB of records. As in `lotse-client`, nothing compresses by default.

## Consequences

- A Java consumer reads streams and queues whatever codec their producers use, once the two jars are on its class path. A Java producer can compress.
- Users who need compression add two artifacts, one of them with native code for each platform. Without them, compressed batches are refused with an error that says what to add, and everything else works.
- The Java SDK's `compressed.hex` vectors (ADR 0058) are now read in its tests. A batch compressed by the Java SDK was read by the Rust codec once, by hand: Java writes zstd and LZ4 frames that Rust reads. No vector goes in that direction.
- Tests:
  - The broker's compressed vectors read as written.
  - Batches compressed here are much smaller than and read like their plain twins.
  - Truncated, garbled and plain-but-marked blocks are refused.
  - A block decompressing to one byte past 16 MiB is refused, and one at exactly 16 MiB is not refused for its size.
  - The builder refuses to compress past the limit.
  - A missing class is named.
  - A separate Gradle task (`testWithoutCodecs`, part of `check`) runs with both jars off the class path and checks the error and that plain batches still read.
- Mutation checks: these were broken on purpose, and a test failed for each:
  - the size check, removed or set off by one;
  - the builder's limit;
  - decompression skipped;
  - the library check passing always;
  - the builder not compressing.
- Gradle cannot reach Maven Central from the environment these changes were made in. The record tests ran under a stand-in for JUnit with zstd-jni 1.5.7-7 and lz4-java 1.11.0, with the jars and without them. The build itself, which names zstd-jni 1.5.7-16, and the client tests did not run there.

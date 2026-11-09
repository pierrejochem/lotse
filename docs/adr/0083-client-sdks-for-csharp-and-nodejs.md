# 83. Client SDKs for C# and Node.js

- Status: Accepted
- Date: 2026-11-08

## Context
[ADR 0056](0056-client-sdks-for-the-jvm.md) put the SDKs under `sdk/`, with golden vectors in `sdk/conformance` that every SDK must decode and write again to identical bytes. The JVM SDK covers everything `lotse-client` does. Applications on .NET and Node.js have no client.

## Decision
`sdk/csharp` and `sdk/node` follow ADR 0056 rule for rule: the same scope (streams, queues, publishing, transactions, consumer groups, admin, TLS, SCRAM-SHA-512 and OAUTHBEARER), the same eight phases (codec and record batch, connections and authentication, streams, queues, transactions and groups, admin, examples and CI; the Kotlin layer has no counterpart), the same rule that the Rust code is the authority and the vectors enforce it, and a codec written per message. The Java SDK is the reference for structure and for the behaviours that the vectors cannot show.

**C#:** .NET 10 (LTS), nullable reference types and warnings as errors, no runtime dependencies beyond the base class library (`SslStream`, `System.Security.Cryptography`, `System.IO.Pipelines` is not used). Asynchronous with `Task` and `IAsyncEnumerable`; xUnit for tests only. Unsigned wire types map to `ulong` and `uint` where the API shows them. The package id `Lotse.Client` is a placeholder until a first release; publishing is out of scope.

**Node.js:** TypeScript for Node.js 20 and later, ES modules, no runtime dependencies (`node:tls`, `node:crypto`, `node:zlib`). 64-bit values are `bigint` on the wire and in the API wherever they can exceed 2^53 (offsets, ids, timestamps). Promises and async iterators; `node:test` for tests. The package name `@lotse/client` is a placeholder; publishing is out of scope.

**Compression:** zstd and lz4 are optional in both SDKs, as in ADR 0061: the SDK names the missing library when a batch needs a codec it does not have. Each phase 1 settles which library; a runtime dependency is allowed only behind an optional switch.

**Verification:** each SDK has a `make` target (`sdk-csharp-test`, `sdk-node-test`) that `sdk-test` calls, and a CI job in phase 8. Each SDK's behaviour is mutation-checked as in ADR 0010 where a safety rule depends on it.

## Consequences
The vectors check three SDKs and Kotlin through Java. A change to a message needs `make sdk-vectors` and three codecs updated. The CI gets two jobs and needs the .NET 10 SDK and Node.js 20. 64-bit unsigned values need care in both languages (`bigint`, `ulong`); the vectors cover the edges.

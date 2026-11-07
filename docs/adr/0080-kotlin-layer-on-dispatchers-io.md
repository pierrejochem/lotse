# 80. Kotlin layer: suspend functions on Dispatchers.IO

- Status: Accepted
- Date: 2026-11-07
- Refines: [ADR 0056](0056-client-sdks-for-the-jvm.md), phase 7

## Context

ADR 0056 plans `sdk/kotlin` as a thin layer on the Java core, with `suspend` functions, `Flow` and builders. The Java client is blocking: it waits on sockets and sleeps between retries on the calling thread. Its `...Async` methods run the same blocking calls on threads of its own, and only stream calls have them. A suspend function must not block the thread it runs on, so the layer needs a rule for how it calls the core.

The maintainer chose to run each call on `Dispatchers.IO`. The alternatives were:
- to add an `...Async` form to every Java call and await its future, which adds a large Java API for no gain, since those methods block a thread of the client's anyway;
- to interrupt the calling thread when a coroutine is cancelled. An interrupted produce or commit ends as `Unknown` or `GaveUp`, so cancelling would leave outcomes in doubt more often.

## Decision

**Every suspend function of `sdk/kotlin` runs one call of the Java core in `withContext(Dispatchers.IO)`.** The rules, retries and errors stay the Java client's, which follow the Rust client's. A coroutine cancelled while a call runs lets the call finish, so its outcome is the one the Java client decided, and then drops the result with a `CancellationException`.

**The layer** is the package `dev.lotse.kotlin`:
- `Lotse` wraps `LotseClient`;
- `LotseTransaction` wraps `Transaction`, `LotseQueue` wraps `QueueConsumer` and `LotseGroup` wraps `GroupConsumer`;
- builders `lotse(...) { }`, `groupConfig { }`, `queueOptions { }` and `message(...)`;
- each wrapper keeps the Java object as `java`.

**Flows:**
- `Lotse.records(stream, partition, from)` reads a partition with read_committed for as long as it is collected.
- `LotseQueue.deliveries()` hands out messages without acknowledging them.
- `LotseGroup.records()` polls the member, so the member heartbeats only while the flow is collected. Its records come one partition's poll at a time.

**`Lotse.transaction { }`** begins a transaction, runs the block and commits when the block returns. If the block or the commit throws, or the coroutine is cancelled, it aborts the transaction while it is still open. The abort runs in `NonCancellable`, so cancellation cannot skip it. Then it passes the exception on. `Transaction.isOpen()` is added to the Java core for this.

Its dependencies are the Java core, built from source with `includeBuild("../java")`, and `kotlinx-coroutines-core`. Explicit API mode is on and warnings are errors.

## Consequences

- One rule covers every call, and the Java core needs no new API but `isOpen`.
- Each call in flight takes a thread of `Dispatchers.IO` (64 by default). A program with more concurrent calls waits for threads, or gives the dispatcher a larger limit.
- Cancelling a coroutine does not cut a call short. A long retry (`retryFor`, 30 s by default) keeps its thread until it ends.
- A group member heartbeats only from `poll`, as in Rust and Java. A collector of `records()` that takes longer than the session timeout over one poll's records loses the member's partitions.

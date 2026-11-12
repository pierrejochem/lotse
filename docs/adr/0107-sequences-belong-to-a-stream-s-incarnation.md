# 107. Sequences belong to a stream's incarnation

- Status: Accepted
- Date: 2026-11-12

## Context

An idempotent producer numbers its batches per partition (ADR 0028). All four clients kept that count per stream **name** and partition. A stream deleted and created again under its name is a new resource with new partition groups (ADR 0034, ADR 0035). Until every node has applied the delete, a node behind may still serve the old one.

The metadata simulation found the result (seed 2520):
1. A producer wrote sequences 2 to 7 to the old incarnation of `s3`.
2. It wrote sequence 8 to the new incarnation, which it had learned of from one node.
3. It wrote sequence 9 to the old incarnation again, which another node still named.
4. The old incarnation had never seen 8. It refused 9 with `OUT_OF_ORDER_SEQUENCE`, and the client then started a new producer id for nothing.

The same sharing can also let a new incarnation take a batch for a duplicate of one written to the old incarnation, if the producer is not known there. Today's Raft changes only moved the timing; the counting has been like this from the start.

## Decision

**A client counts sequences per resource id and partition, and a produce stays with the incarnation its sequence was taken for** (maintainer's choice).

- **Taking the sequence.** Before it takes a sequence, the client routes the partition and notes its resource id. The sequence is the next one for that resource id and partition. A new incarnation starts at 0, and an old one, when served again, goes on where it stood.
- **Retries.** If a retry finds the stream under another resource id, the produce ends with `UnknownStream` (the stream it was meant for is gone), and nothing is sent to the new incarnation with a sequence that belongs to the old one. The next produce starts on the new incarnation.
- **In doubt.** If an earlier attempt of that produce may have been written (its outcome was unknown), its sequence is in doubt in the old incarnation. The client then starts over with a new producer id, as it does for any unknown outcome.
- **Scope.** The Rust client, the Java SDK (and the Kotlin layer on it), the C# SDK and the Node.js SDK do this. Transactions are unchanged: each transaction numbers its partitions afresh, and a stream created again during one ends it.

## Consequences

- A stream deleted and created again while producers write to it gives them `UnknownStream` once, where before they could get `OUT_OF_ORDER_SEQUENCE` and lose their producer id.
- A produce never follows a name to a new incarnation on its own. The caller decides whether to send again. A batch whose outcome was unknown in the old incarnation is then sent at most once to each incarnation.
- Tests:
  - Each client has scripted-node tests: incarnations count their own sequences; a refused batch keeps its sequence; a produce in doubt when its stream moves starts a new producer.
  - The metadata simulation at seed 2520.
  - Mutation checks, Rust, Java and Node.js:
    - Keying by name makes a test fail; in Rust it also fails the seed.
    - Leaving out the pin makes a test fail.
    - Leaving out the new producer id when in doubt makes a test fail.
    - C# runs only in CI.

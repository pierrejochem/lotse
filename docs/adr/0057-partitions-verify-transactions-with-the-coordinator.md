# 57. Partitions verify transactions with the coordinator

- Status: Accepted
- Date: 2026-11-05

## Context

A partition opened a transaction for a producer with its first transactional batch, whatever the coordinator knew (ADR 0029). A client that wrote transactional batches without `AddPartitionsToTxn` first, through a bug or on purpose, opened a transaction the coordinator had never heard of. No marker would ever close it, the coordinator's timeout would not abort it, and the partition's last stable offset stayed behind its first batch for good: every read_committed consumer of the partition stopped there, in any tenant. The README listed this as a known gap.

## Decision

**A partition's leader verifies a producer's first transactional batch of an epoch** (maintainer's choice; Kafka does the same, KIP-890). When a transactional batch comes for a producer that has no transaction open in the partition (applied state and log tail), the leader holds it and asks the transaction coordinator with the new internal request `VerifyTxn` (key 45: transactional id, producer id and epoch, resource and partition), sent through the node's own client like `WriteTxnMarkers`. The coordinator answers from its applied state, as its leader once it has applied an entry of its own term:

- `NONE` if the transactional id is in `Ongoing` at that producer id and epoch and holds the partition;
- `PRODUCER_FENCED` for another producer id or an older epoch;
- `CONCURRENT_TRANSACTIONS` (retriable) while a change to that id is being written;
- `INVALID_TXN_STATE` otherwise: unknown id, a partition never added, a transaction ending or none open.

Only after `NONE` is the batch proposed, through the same checks as any batch; any other answer refuses it, and nothing is written. Batches that come for the same partition, producer and epoch while the question is out wait for the same answer. Once a batch is in the log the transaction is open here, and later batches go straight through.

**A late batch is still safe.** If the transaction ends between the answer and the proposal, its marker has raised the producer's epoch in the partition (ADR 0029), and the batch is refused as fenced.

**Nothing is written without an answer.** A batch still held at the produce request's timeout is answered `REQUEST_TIMED_OUT` with "not written"; a coordinator that cannot be reached at all makes it `COORDINATOR_NOT_AVAILABLE`. Both are safe to retry.

## Consequences

- A transaction exists in a partition only if the coordinator holds it, so the coordinator's timeout and its markers always end it: a client cannot hold back a partition's last stable offset beyond its transaction timeout.
- The first batch of each transaction in each partition costs one round trip to the coordinator, which may be on another node; later batches cost nothing more.
- Only nodes may send `VerifyTxn`; the Java SDK knows the key as a constant only. The golden vectors gain one internal request and the key in the `ApiVersions` answer.
- Tests: a client-layer test that a transactional batch waits for the coordinator, is refused and never written on `INVALID_TXN_STATE`, written once confirmed, needs no further question once its transaction is open, and that batches waiting on the same question share it; a unit test of the coordinator's answer (unknown id, partition never added, older epoch, another producer, a change being written, a transaction ending, none open). Every rule was broken on purpose and a test failed: no verification, verifying every batch, the partition, phase and epoch checks, a refusal ignored, questions not shared. The client, queue, group, metadata, retention, membership and access simulations run their transactional producers through the new path.

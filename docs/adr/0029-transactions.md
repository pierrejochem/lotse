# 29. Transactions

- Status: Accepted
- Date: 2026-10-28

## Context

The design document describes transactions: `InitProducer` fences old instances, `AddPartitionsToTxn` records the partitions, `EndTxn` writes `PREPARE_COMMIT` or `PREPARE_ABORT` to `__txn_state`, markers go to every partition, `COMPLETE_COMMIT` ends it, and read_committed consumers read up to the last stable offset and skip aborted batches. It leaves open how markers reach partition leaders on other nodes, where partitions keep transaction state, and how a late marker is kept from closing the producer's next transaction, a gap Kafka closed only later (KIP-890).

## Decision

**Coordinator.** The coordinator group of ADR 0028 is `__txn_state`. Each change to a transactional id is one record holding its whole new state (producer id, epoch, phase, timeout, partitions), stored as an internal control batch so no user state machine or consumer sees it. The coordinator acts only once its leader has applied an entry of its own term, and changes one transactional id at a time; a second request meanwhile gets the new error code 19 `CONCURRENT_TRANSACTIONS` (retriable). `InitProducer` for an id with a transaction still open aborts that transaction (with a raised epoch, which fences the old instance) and answers `CONCURRENT_TRANSACTIONS`; the client retries. A transaction open longer than its timeout, measured by the coordinator leader's clock and restarted when leadership changes, is aborted the same way.

**Ending raises the epoch.** `PrepareCommit`/`PrepareAbort` carry the producer's epoch plus one, the markers are written with it, and `EndTxn` returns it (one field more than the design's layout): the producer's next transaction runs at the new epoch. A partition closes an open transaction only with a marker of a *higher* epoch than its data, so a marker retried late cannot close the next transaction. Each marker also carries the coordinator epoch (the coordinator group's Raft term); a partition refuses markers older than one it has seen for the producer, answering the new error code 20 `COORDINATOR_FENCED`.

**Markers through the protocol.** The coordinator node writes markers with the internal key `WriteTxnMarkers` (1000; internal keys start at 1000), through the same client code as everyone else, so leader routing, `NOT_LEADER` and the commit rule of ADR 0026 apply unchanged. A partition leader appends a marker only if it closes an open transaction or raises the producer's epoch; otherwise it answers that nothing is left to do. The coordinator writes `CompleteCommit`/`CompleteAbort` only after every partition has answered.

**Partition state in the replica.** Open transactions, aborted ranges and coordinator epochs per producer live in the replica's state next to the producer state: rebuilt from the log, checked against applied state plus log tail, saved in the Raft snapshot (instead of `.txnindex` files). The last stable offset is the lowest first offset of an open transaction, and never beyond the applied index, since below it every transaction must be decided. `Fetch` with read_committed stops there and lists the aborted transactions in what it returns; the client drops those batches, from each one's first offset to its producer's ABORT marker.

**Client.** `Client::begin_transaction` returns a `Transaction`: produce through it adds the partition first; `commit` and `abort` end it and keep the raised epoch. After a write with an unknown outcome the transaction can only be aborted. A transaction dropped without either makes the client get a fresh producer id next time, which aborts it.

## Consequences

- Writes to several partitions become visible together or not at all, also across leader changes, crashes and coordinator failover; zombie producers are fenced wherever the markers land, including partitions they never wrote to.
- A producer id runs out of epochs after 65,535 transactions; the client then needs a new one (`InitProducer` gives it).
- Not yet: partitions do not ask the coordinator whether a transaction is open (KIP-890's verification), so a client that skips `AddPartitionsToTxn` can hold back the last stable offset of that partition until the producer's next marker; one coordinator group for all transactional ids; `AddAcksToTxn` and `AddOffsetsToTxn`; aborted ranges are pruned only once log retention exists.
- The client simulation runs a transactional producer next to the plain ones and checks atomicity per transaction; it takes about twice as long as before.

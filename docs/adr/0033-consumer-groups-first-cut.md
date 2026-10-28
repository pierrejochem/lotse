# 33. Consumer groups: the first cut

- Status: Accepted
- Date: 2026-10-28

## Context

ADR 0013 chose cooperative, broker-side group rebalancing (KIP-848 style) with group coordinators on `__groups` partitions and offset commits as records that join transactions through `AddOffsetsToTxn`. Streams exist (ADR 0026), and so do transactions (ADR 0029), but no consumer groups. Building them leaves open where group state lives before the metadata group exists, whether membership survives a coordinator failover, how a partition is kept from having two owners, and how offsets committed in a transaction become visible.

## Decision

**Group state lives in the coordinator group** (maintainer's choice), next to the transaction coordinator's: one Raft group coordinates all consumer groups. `FindCoordinator` answers for groups as for producers. Hashing groups over several `__groups` groups (ADR 0013) comes with the metadata group.

**Membership is persisted** (maintainer's choice). Every change to a group is one `GroupState` record holding the whole group: its epoch and, per member, its subscription, rebalance timeout, member epoch, target, assigned and revoking partitions. Committed offsets are `GroupOffsets` records. Both are internal control batches, applied into the coordinator state and saved in its snapshot (format 5). A coordinator failover keeps members and assignments.

**Protocol.** `GroupHeartbeat` (6) joins (epoch 0; the coordinator names the member `<term>-<n>`, unique across leaders), stays (with the partitions the member still owns) or leaves (epoch -1). `CommitOffsets` (7), `OffsetFetch` (8, with `require_stable`) and `AddOffsetsToTxn` (19) carry offsets. `AddOffsetsToTxn` carries the offsets themselves (no separate `TxnOffsetCommit`). New error codes: 24 `UNSTABLE_OFFSET_COMMIT` and 25 `CONCURRENT_GROUP_CHANGE`, both retriable. The coordinator acts only after applying an entry of its own term and changes one group at a time.

**Assignment is cooperative.** Per stream, the coordinator spreads partitions evenly over subscribed members, each keeping as many of its current partitions as its share allows. A member's assignment moves towards its target: a partition it should give up is revoked (it may still read it); a partition of its target is added only once no other member has it assigned or revoking. A member stops reading a revoked partition as soon as an answer tells it, and the next heartbeat reports it released. No partition has two owners at once. A member's epoch rises with every change of its assignment, and the full assignment goes out whenever the member's epoch differs from the one it sent.

**Liveness is the coordinator leader's, and errs late.** A member is removed when no heartbeat has arrived for the session timeout (`NodeConfig::group_session_timeout`, 45 s by default), or when it has not released a revoked partition within its rebalance timeout, and never sooner than the session timeout after the revocation. Both are measured from when this leader first saw the member or the revocation. The client drops all partitions once no heartbeat has succeeded for the session timeout, counted from when it sent the last one that did. So a member has stopped reading before its partitions go to someone else.

**Exactly-once consume-transform-produce** (maintainer's choice to include it). `Transaction::send_offsets` adds the consumer's positions to the transaction. The coordinator accepts them only from a member that still holds each partition: a zombie is fenced, and its transaction can only be aborted. The offsets wait in the transaction's state and become the group's committed offsets when its `PrepareCommit` is applied. Offsets live in the coordinator group itself, so they need no markers. A `PrepareAbort` drops them. While a transaction holds an offset, `OffsetFetch` with `require_stable` answers `UNSTABLE_OFFSET_COMMIT`, so a new owner waits for the outcome before it reads.

**Client.** `Client::join_group` returns a `GroupConsumer`. It heartbeats from `poll`, reads its partitions with read_committed from the committed offsets (from offset 0 when there are none), commits on request, and also commits revoked partitions when auto-commit is on. After an aborted transaction, `rewind` starts again from the committed offsets.

## Consequences

- A group of processors can read a stream and write results exactly once across crashes, failovers, partitions and stalled members. The group simulation checks that every acknowledged input appears in the output exactly once, while processors crash, leave zombies, stall past their session holding open transactions and commit carelessly.
- One coordinator group serves every consumer group and every transactional id, and each heartbeat that changes a group is a Raft write of the whole group.
- No `ListOffsets` yet: a partition without a committed offset is read from offset 0. No server-side assignor choice, no static membership, no regex subscriptions.
- The group simulation is the slowest per seed (about 0.7 s here). It runs in its own CI job: 1,000 seeds per push and 5,000 nightly (maintainer's choice).

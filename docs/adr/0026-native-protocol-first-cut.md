# 26. Native protocol: the first cut

- Status: Accepted
- Date: 2026-10-28

## Context

The design document specifies the native protocol: frames, field conventions, API keys, error codes and the field layouts of each request. A first implementation needs details it leaves open (API key numbers, the encoding of tagged fields), and rules for the moment a `Produce` may be answered and what a `Fetch` may return, which correctness depends on. The metadata group that will own streams does not exist yet.

## Decision

**Encoding.** A frame is what one `Net` connection carries; its length is the transport's (the `u32` prefix on TCP), so headers start after it. API keys are numbered in the order of the design table: `ApiVersions` 0, `Metadata` 1, `FindCoordinator` 2, `Produce` 3, `Fetch` 4, and so on. Tagged fields are `array{tag varint, bytes}` at the end of every structure; readers skip tags they do not know. A response whose body is only an error code (the request could not be served at all) sets flag `ERROR` (2); a `NOT_LEADER` partition carries `{leader u32, term u64}` as tag 0. All keys start at version 0.

**Produce.** The leader proposes each batch and answers once its outcome is final: written when the batch's index is committed on this node and holds the term it was proposed in; not written (`NOT_LEADER`) when the committed entry there has another term. A batch whose entry is replaced on this node before it commits is *not* reported as failed: another node may still hold it and commit it later. If the commit is not seen within `timeout_ms`, the answer is `REQUEST_TIMED_OUT`, meaning unknown.

**Fetch.** Only the leader serves fetches, and never a batch at or beyond its commit index. `OFFSET_OUT_OF_RANGE` means before the log start or after the leader's log end, not after the high watermark: a new leader learns the commit index only once an entry of its own term commits, so its high watermark can briefly lag offsets a client has already read. Between the two, the fetch waits for data.

**Streams.** Until the metadata group exists, streams and the Raft group of each partition come from the node configuration.

**Client.** `lotse-client` does not retry a produce whose outcome is unknown (connection lost after sending, `REQUEST_TIMED_OUT`) until idempotent producers exist; it retries only what was definitely not written.

## Consequences

- An acknowledged batch is never lost or moved, a definite "not written" is true, and consumers never see uncommitted data; the client simulation checks all three and the rules are mutation-checked.
- Without idempotent producers, an unknown outcome is the caller's problem.
- Not yet: TLS, SASL, transactions, queues and the other API keys; `Fetch` copies segment bytes through the log instead of `sendfile`.

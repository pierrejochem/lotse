# 79. Publish: routed by the node

- Status: Accepted; changed by [ADR 0101](0101-a-publish-refused-by-a-node-behind-is-sent-again.md) (a node behind refuses with `NOT_LEADER`, and the publish is sent again)
- Date: 2026-11-07
- Changes: [ADR 0032](0032-publish-routed-by-the-client.md) for publishes outside a transaction

## Context

Under ADR 0032 the client routes. It reads an exchange's bindings from metadata, picks the queues with `lotse-route` and produces to each of them. Since bindings change at runtime (ADR 0034), a client may route by bindings up to `metadata_max_age` old. Every SDK also has to carry routing code of its own; the Java SDK, for one, has none. Key 9, `Publish`, was left free for this.

The maintainer chose to add `Publish` and let the node route. The alternatives were to keep client routing in every SDK, or to send every publish through the metadata leader as a transaction.

A second question was whose rights a forwarded write needs. The maintainer chose the producer role on the exchange and on every queue the message routes to. The alternatives were the exchange alone, or the exchange with forbidden queues skipped silently.

## Decision

**`Publish` (key 9)** goes to any node. It carries entries of exchange id, routing key, `mandatory` and a batch of exactly one data record. The node routes each entry by the exchange's bindings as its metadata state has them, so bindings are never older than what that node has applied.

Each entry gets one of these answers:
- `UNKNOWN_RESOURCE` when the exchange is unknown;
- `INVALID_REQUEST` when the batch is not one plain data record, or the request names a transaction;
- `UNROUTABLE` (error 32) when the message is mandatory and routes to no queue; a message that is not mandatory and routes nowhere is dropped with `NONE`;
- `UNAUTHORIZED` when the publisher lacks the producer role on any routed queue. Nothing is written in that case, and no queue is named. The producer role on the exchange is checked when the request is admitted.

**Writing.** Otherwise the node writes the message to each routed queue in turn. It does so through the native protocol, with an idempotent producer of its own, like any client. Nodes may now send `Produce` and `InitProducer` outside a transaction, for this alone. The answer lists each routed queue by id and name. A client routing for itself always knew the queues; one that does not may never have seen a queue bound and deleted between two of its lookups. The metadata simulation found such a queue reported only by its id. The name is empty only when the node no longer knows the queue either. Each queue's entry carries:
- `NONE` with the offset when the queue holds the message;
- `REQUEST_TIMED_OUT` when its outcome is unknown;
- any other code when it does not hold it.

The request's `timeout_ms` bounds the node's writing. At the deadline the node drops a write still under way. The queue simulation found that this broke idempotence: a dropped produce had taken a sequence it never released, so the next batch carried the same sequence, and the leader took it for the dropped one and answered with that one's offset. Dropping a produce in the Rust client is now treated like an unknown outcome. Outside a transaction it ends the producer session, so the next batch starts with a new producer id. Within a transaction it leaves the transaction to be aborted. This holds for any caller that drops a produce, not only the node.

**The Rust client** publishes with it outside a transaction: `Client::publish` returns `Published`, `NotAllWritten` or `Unroutable` as before. If a node's answer is lost, the result is `ClientError::Unknown`. The publish is not sent again, since the node may have written it and a second one could write it twice. An exchange the node does not know is looked up once more, in case its name was made again with a new id.

**Transactions** keep client routing: `Transaction::publish` produces to each queue within the transaction, so the message reaches all its queues or none.

**The Java SDK** gets `LotseClient.publish`, with the same answers and the same rule for a lost answer.

## Consequences

- Clients other than the Rust one publish through exchanges without routing code. A binding counts for the next publish once the receiving node has applied it, whatever the publisher has cached.
- A publish to several queues is not atomic outside a transaction, as before.
- The node does the fan-out. Each message is written once more over the network: client to node, then node to each leader.
- A binding made a moment ago may not yet be applied on the node that gets the publish.
- A lost answer leaves the outcome unknown instead of retrying idempotently. Before, each queue's write was retried with the client's own producer. To be sure, read the queues, or publish in a transaction.
- A node's writes are refused only by the queue leader's own checks. The publisher's rights are checked once, at the receiving node.
- The node writes each queue by name. If a queue is deleted and made again under its name while a publish is under way, the message can land in the new queue under the old one's id in the answer. The new queue sits in the same tenant under the same name, so the same roles cover it.

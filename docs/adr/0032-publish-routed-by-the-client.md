# 32. Publish: exchanges routed by the client

- Status: Accepted
- Date: 2026-10-28

## Context

Queues exist (ADR 0031), but messages reach them only by `Produce` to a named queue. RabbitMQ users publish to exchanges, which route by routing key or headers to the queues bound to them. The design document lists `Publish`, `DeclareExchange` and `Bind` and says `Publish` routes through exchanges and writes to the target queues; it does not say where routing happens, how a message routed to several queues (several Raft groups) stays exactly-once, or what a publish guarantees when only some of its queues could be written. Exchanges and bindings also need a home before the metadata group exists.

## Decision

**The client routes.** `Metadata` describes an exchange as a resource of kind `EXCHANGE` with its type (`DIRECT`, `TOPIC`, `FANOUT`, `HEADERS`) and bindings (queue, binding key, header match, all or any), and a request for an exchange by name also returns the queues bound to it. The client routes with the new crate `lotse-route` and produces the message to each queue. Each write is an ordinary produce, idempotent per queue with that queue's own sequence, and inside a transaction an ordinary transactional produce. The `Publish` wire key is not added yet. Routing follows RabbitMQ: direct compares keys, topic keys are `.`-separated words with `*` for one word and `#` for zero or more, fanout takes every binding, headers compares header values (null only equals null) with all or any; a queue bound twice gets one copy.

**Definitions come from the node configuration.** Like streams and queues, exchanges and their bindings are fixed in `NodeConfig::exchanges` until the metadata group exists; `DeclareExchange` and `Bind` come with it.

**Fan-out is per queue, as in RabbitMQ.** `Client::publish` returns once every routed queue holds the message, with each queue's offset. If some could not be written, `ClientError::NotAllWritten` lists the queues that hold it and, per other queue, whether the write is unknown or did not happen. A message no binding matches is dropped, or refused with `ClientError::Unroutable` when `mandatory` is set. For all-or-nothing fan-out, `Transaction::publish` writes within a transaction; once some queues hold the message and another write fails, the transaction can only be aborted.

## Consequences

- Publishing reuses idempotence, transactions and leader routing unchanged, and adds no broker work beyond metadata. Brokers never forward messages.
- Every client has to route. Other-language clients need their own `lotse-route`, and protocol adapters (AMQP) will route in the broker with the same crate.
- Bindings are read from metadata the client cached. With fixed configuration that is exact; once bindings change at runtime, a client may route by stale bindings until it refreshes, which the metadata group step has to settle (for example with a bindings epoch checked by the queue leader).
- One message per publish and one produce per queue: no batching across messages yet, and queues are written one after another rather than in parallel.
- The queue simulation now runs two queues and two exchanges and checks that a published message lands in exactly the queues its key selects, that a mandatory message matching nothing is refused, and that a queue reported as not written does not hold it. At 2,000 seeds it takes about 13 minutes here, in its own CI job.

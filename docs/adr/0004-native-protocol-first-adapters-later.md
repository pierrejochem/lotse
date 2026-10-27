# 4. Native protocol first, adapters later

- Status: Accepted
- Date: 2026-10-27

## Context

Speaking AMQP 0-9-1 and the Kafka wire protocol from the start would tie Lotse's semantics to two foreign models before its own are settled.

## Decision

Lotse ships its own binary protocol first: TCP with TLS 1.3, length-prefixed frames (16 MiB maximum), correlation IDs for pipelining, a version per API key announced at connection time, mTLS for services and SASL (SCRAM-SHA-512, OAUTHBEARER) for users. Smart clients go to the leader; `NOT_LEADER` redirects. Streams are pulled (`Fetch` with long polling), queues are pushed within a credit window. AMQP 0-9-1 and Kafka adapters come later as plugins on the same core API.

## Consequences
- A lean start with exactly the semantics Lotse needs; record batches go from the log to the wire unchanged (zero-copy `Fetch`).
- Existing AMQP/Kafka clients need the adapters, which are not in the MVP.

See also: [design document](../design.md#natives-protokoll).

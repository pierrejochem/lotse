# 71. Named idempotent producers

- Status: Accepted
- Date: 2026-11-06

## Context

A plain idempotent producer gets a fresh producer id from the coordinator each time its client starts (ADR 0028). After a restart, nothing ties the new instance to the old one. If the old instance is still alive somewhere (a process that hung, a network partition), its retries can land after the new instance's batches. Transactional ids already survive restarts (ADR 0029). The maintainer chose an optional, stable producer name, over keeping Kafka's behaviour or storing the id on the client.

## Decision

**A client may give its idempotent producer a name** (`ClientConfig::producer_name` in `lotse-client`, `producerName` in the Java SDK).
- With a name, the client asks for its producer id with `InitProducer`, giving the name where a transactional id would go.
- The coordinator treats the name exactly like a transactional id that never opens a transaction:
  - the first start gets a new producer id;
  - every later start keeps the id and raises its epoch;
  - at the end of the epochs, it gets a new id.
- Partition leaders already refuse batches of an older epoch (`PRODUCER_FENCED`), so an earlier instance is fenced as soon as the new one starts.

**A fenced named producer stays fenced.** On `PRODUCER_FENCED` a client with a name keeps its producer id and reports the error. Without that, it would ask for a new epoch and fence the newer instance in turn, and the two would take turns. A client without a name starts over with a new id as before.

**No broker change.** Names share the namespace of transactional ids, in the request's tenant (ADR 0062). They need the producer role with a matching prefix, live in the tenant's incarnation (ADR 0069), and must not contain NUL. One client instance per name; a name used as a transactional id elsewhere fences that producer too.

## Consequences

- A restarted client fences its earlier instance, whose late retries are refused instead of written after the new instance's batches.
- A batch that the old instance never had acknowledged is still the application's to send again, and it then gets a new sequence. A name gives fencing, not deduplication of a resend by the application.
- Each start writes one coordinator record.

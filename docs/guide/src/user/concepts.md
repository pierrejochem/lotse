# Concepts

Lotse keeps messages in replicated logs. A log can be read like a stream, or worked off like a queue. This chapter names the parts you meet as an application developer.

## Stream and partition

A **stream** is a named, ordered log of messages. A stream has one or more **partitions**; each partition is its own log with its own offsets. Messages in one partition keep the order they were written in. Across partitions there is no order.

Every message in a partition has an **offset**, a number that starts at 0 and only grows. Reading does not remove anything: a reader keeps its own position and may read the same offsets again. What leaves a stream is decided by retention (see [Streams](streams.md)).

Each partition is a Raft group on several nodes, the stream's **replication** factor. A write is acknowledged once a majority of those nodes holds it.

## Queue

A **queue** hands each message to one consumer, then waits for that consumer to say what became of it: **ack** (done), **nack** (give it back, now or after a delay). A message nobody acknowledged is delivered again after a failure, a disconnect or the **ack timeout**. A queue is one partition, so it has one order, and offsets exist but are rarely needed.

A queue can limit deliveries, expire messages and send the ones it gives up on to a **dead-letter queue**. See [Queues](queues.md).

## Exchange and binding

An **exchange** takes a message and a **routing key** and passes it on to the queues **bound** to it. A **binding** says which queues an exchange serves and under which key or headers. There are four kinds: direct, topic, fanout and headers. See [Exchanges and routing](exchanges.md).

You can also write to a queue directly, as to a stream's partition 0, and skip the exchange.

## Tenant

Every stream, queue and exchange belongs to a **tenant**. Names are unique inside a tenant, not across the cluster. A client works in one tenant, set when it is configured; without one it is `default`. What a user may do is decided by roles bound per tenant and name prefix: cluster-admin, cluster-viewer, tenant-admin, producer, consumer and viewer. A resource you may not see is unknown to you, so a missing permission can look like a missing queue. See [ADR 0039](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0039-tenants-and-roles.md).

## Producer and delivery guarantees

The Java and Rust clients are **idempotent producers** by default: a retried write after a lost answer does not create a duplicate. A producer with a name, or with a transactional id, gets its identity from the cluster, so a restarted instance fences the earlier one. See [ADR 0028](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0028-idempotent-producers.md) and [ADR 0071](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0071-named-idempotent-producers.md).

**Transactions** write to several partitions and queues, and commit offsets or acks, all or nothing. See [Transactions and exactly-once](transactions.md).

## Consumer group

A **consumer group** shares the partitions of one or more streams among its members and stores their committed offsets in the cluster. Each partition has one owner at a time. When members join or leave, partitions move without any partition having two owners. A member can keep its place across a restart with a fixed **instance id**, and a group can subscribe to streams by a regular expression. See [ADR 0033](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0033-consumer-groups-first-cut.md), [ADR 0072](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0072-static-group-membership.md) and [ADR 0073](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0073-pattern-subscriptions.md).

## Which one to use

| You want | Use |
| --- | --- |
| Keep history, read it again, several independent readers | A stream |
| Share work among workers, retry failures, dead letters | A queue |
| Send one message to several queues by a rule | An exchange with bindings |
| Spread a stream's partitions over a team of workers | A stream and a consumer group |
| Read, change and write elsewhere with no loss or repeat | A transaction |

## About the examples

The Java examples in this guide are the files under `sdk/java/src/examples`; the build compiles them, so they match the SDK. They read the cluster address and credentials from `Env.java`. The Kotlin layer in `sdk/kotlin` offers the same calls as `suspend` functions and flows. Short snippets that are not from those files are marked as such.

# Rust client

`lotse-client` is the reference client. It is generic over the runtime traits of `lotse-rt` (`Net`, `Clock`, `Spawn`, `Rng`), so the same code runs on Tokio in production and in the deterministic simulation in tests. It is a workspace crate; depend on it by path or Git until a release is published.

```toml
[dependencies]
lotse-client = { git = "https://github.com/pierrejochem/lotse" }
lotse-rt-tokio = { git = "https://github.com/pierrejochem/lotse" }
lotse-record = { git = "https://github.com/pierrejochem/lotse" }
bytes = "1"
```

## Connecting

`ClientConfig::new(bootstrap)` takes brokers as `host:port` strings and fills in defaults. The fields you will change most:

| Field | Default | Meaning |
| --- | --- | --- |
| `request_timeout` | 10 s | How long one request may take before the connection is given up. |
| `retry_for` | 30 s | How long retriable failures are retried before `ClientError::GaveUp`. |
| `produce_timeout` | 5 s | How long the leader waits for the quorum on a produce. |
| `idempotent` | `true` | Idempotent produce. Needs the coordinator group. |
| `transactional_id` | `None` | Needed for `begin_transaction`. One client instance per id. |
| `producer_name` | `None` | A stable name for an idempotent producer ([ADR 0071](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0071-named-idempotent-producers.md)). |
| `txn_timeout` | 60 s | How long a transaction may stay open before the coordinator aborts it. |
| `credentials` | `None` | `Credentials::Scram { user, password }` or `Credentials::OAuthBearer(..)`. |
| `tenant` | `default` | Where created resources, transactional ids and group ids live. |
| `zone` | none | The zone the client runs in. Fetches go to a replica of the partition in that zone, which serves the committed data it has and may lag the leader a little; to the leader if none is there or that replica fails. Everything else goes to the leader. Nodes name their zone in the cluster file ([ADR 0104](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0104-followers-serve-fetches-in-their-zone.md)). |

The runtime decides the transport. A `TokioRuntime` uses the network only after `with_tls` (a TLS 1.3 configuration) or `plaintext_on_loopback`; plain TCP is refused off loopback.

```rust,ignore
use lotse_client::{Client, ClientConfig};
use lotse_rt_tokio::TokioRuntime;

let rt = TokioRuntime::new(std::env::temp_dir()).plaintext_on_loopback();
let client = Client::new(rt, ClientConfig::new(vec!["127.0.0.1:9092".into()]));
```

## Streams

Create a stream, produce a batch and read it back. `produce` takes a record batch built with `lotse_record::BatchBuilder`; it returns the offset of the batch's first record once the batch is committed.

```rust,ignore
use std::time::Duration;
use lotse_record::{BatchBuilder, BatchOptions};

client.create_stream("orders", 2, 3).await?;          // 2 partitions, 3 replicas

let mut batch = BatchBuilder::new(BatchOptions::default());
batch.push(now_ms, Some(b"customer-7"), Some(b"order-1"), &[]);
let offset = client.produce("orders", 0, batch.finish()?).await?;

let fetched = client.fetch("orders", 0, offset, Duration::from_millis(500)).await?;
for b in fetched.committed_batches()? {
    for record in b.records()? {
        // record.offset, record.key, record.value
    }
}
let next = fetched.next_offset(offset);
```

- You choose the partition. The client does not hash keys.
- `fetch` is read_committed. `fetch_uncommitted` also returns open and aborted transactions and their markers; use it to inspect a log, not to process it.
- `Fetched::committed_batches` drops what a read_committed reader must not see. `Fetched::next_offset` moves on even when no message came, because control entries advance the offset.
- `list_offset(stream, partition, at)` takes `OffsetAt::Earliest`, `OffsetAt::Latest` or `OffsetAt::Time(ms)` ([ADR 0059](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0059-list-offsets.md)).
- `ClientError::OffsetOutOfRange` means retention deleted the offset. Read on from the log start it reports.

## Idempotent produce and unknown outcomes

With `idempotent` on, the client gets a producer id from the coordinator, stamps each batch and retries a batch whose outcome is unknown with the same sequence. The leader recognises a batch it already holds and answers with its offset ([ADR 0028](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0028-idempotent-producers.md)).

Only when retrying runs out of `retry_for` with the outcome still unknown does the caller get `ClientError::Unknown`. The client then starts over with a new producer id so later batches do not wait for a gap. Read the partition, or produce again and accept a possible duplicate. Without `idempotent`, an unknown outcome is never retried.

## Queues

`create_queue(name, replication, ack_timeout)` creates a queue; `create_queue_with` and `create_queue_in` take more options and a tenant. `subscribe(queue, credit)` returns a `Consumer`. The queue's leader pushes messages within the credit; the consumer settles each one.

```rust,ignore
client.create_queue("jobs", 3, None).await?;
let mut jobs = client.subscribe("jobs", 16).await?;
while let Some(message) = jobs.next(Duration::from_secs(5)).await? {
    // message.record, message.delivery_count
    jobs.ack(&message).await?;       // or jobs.nack(&message, true) / jobs.nack_after(&message, delay)
}
jobs.close().await;
```

- `ack` settles a message for good. `nack(&message, requeue)` delivers it again with `requeue`, and discards it like an acknowledged one without; `nack_after(&message, delay)` returns it after a delay ([ADR 0077](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0077-delayed-delivery.md)).
- `is_standby()` is true while a subscription waits behind another on a queue with `single.active.consumer`.
- After a leader change or a broken connection the consumer subscribes again by itself, naming the consumer id the leader gave it, and keeps the messages it received; `ack_in` sends a transactional ack again on the new subscription (ADR 0109). If it does not come back within the queue's resume window, its messages are delivered again, possibly to another consumer.
- `Message::delivery_count` is how often the message has been delivered, this time included.

## Publishing through an exchange

`declare_exchange(name, kind)`, `bind(exchange, queue, key)`, `bind_headers` and `unbind` set up routing. `publish` goes to any node, which routes by the bindings it has applied ([ADR 0079](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0079-publish-routed-by-the-node.md)).

```rust,ignore
use lotse_client::OutMessage;

let published = client.publish("orders-x", "eu.created", OutMessage::value("order-1"), false).await?;
// published: each routed queue and the message's offset there
```

With `mandatory` set, a message no queue takes is refused; otherwise it is dropped. If a node's answer is lost the result is `ClientError::Unknown`, not a second publish. `OutMessage` has a timestamp, an optional key and value, and headers; a headers exchange routes by them.

## Transactions

Set `transactional_id`, then `begin_transaction`. Starting one fences any older instance with the same id. See [Exactly-once recipes](exactly-once.md) for the patterns.

| Call | What it does |
| --- | --- |
| `Transaction::produce(stream, partition, batch)` | Writes a batch that becomes visible on commit. |
| `Transaction::publish(exchange, key, message, mandatory)` | Routes in the client and writes to each queue atomically ([ADR 0032](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0032-publish-routed-by-the-client.md)). |
| `Transaction::send_offsets(&group)` | Commits a `GroupConsumer`'s positions with the transaction. |
| `Consumer::ack_in(&mut txn, &message)` | Acknowledges a queue message within the transaction ([ADR 0075](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0075-queue-acks-in-transactions.md)). |
| `Transaction::commit()`, `abort()` | End it. |

A transaction dropped without `commit` or `abort` is left open; the client's next transaction starts with a fresh producer id, which makes the coordinator abort the old one. After a partial failure a transaction reports `ClientError::MustAbort`; only `abort` is left.

## Consumer groups

`join_group(group, &["orders"], GroupConfig::default())` joins; partitions arrive with the first `poll` calls. `join_group_matching(group, pattern, cfg)` subscribes by a regular expression ([ADR 0073](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0073-pattern-subscriptions.md)).

```rust,ignore
use lotse_client::GroupConfig;

let mut group = client.join_group("billing", &["orders"], GroupConfig::default()).await?;
loop {
    for r in group.poll(Duration::from_millis(500)).await? {   // heartbeats too: call it often
        // r: stream, partition, record
    }
    group.commit().await?;     // with auto_commit on, positions are also committed on revoke and close
}
```

`GroupConfig` holds `session_timeout` (45 s), `rebalance_timeout` (60 s), `auto_commit` (on), `start_from` (an `OffsetAt`, `Earliest`) and `instance_id` for static membership ([ADR 0072](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0072-static-group-membership.md)). `rewind()` forgets the read positions so every owned partition starts again from the committed offset; use it after an aborted transaction. `positions()` and `assignment()` show what the member holds.

## Admin

The admin calls are methods of `Client`, in `crates/lotse-client/src/admin.rs`:

- Resources: `create_stream`, `create_stream_with`, `create_queue`, `create_queue_in`, `declare_exchange`, `bind`, `unbind`, `delete`, `delete_if_empty`, `create_partitions`.
- Queues: `purge`, `requeue`, `alter_configs`, `describe_configs`.
- Access: `set_user_password`, `delete_user`, `describe_users`, `create_tenant`, `delete_tenant`, `describe_tenants`, `alter_role_bindings`, `describe_role_bindings`, `who_am_i`, `revoke_sessions`.
- Cluster: `add_node`, `remove_node`, `describe_nodes`, `move_leader`, `node_metrics`.

Each is checked against the signed-in principal's roles on the broker ([ADR 0039](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0039-tenants-and-roles.md)).

## Testing against the simulation

Because the client takes its runtime as a parameter, you can run it on `lotse-sim` with crashing disks and a faulty network. The client simulation in `crates/lotse-server/tests/clients.rs` does this. See [Contributing](contributing.md).

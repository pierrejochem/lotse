# Lotse client for Node.js

A client for the Lotse native protocol in TypeScript, for Node.js 20 and later. It follows [ADR 0083](../../docs/adr/0083-client-sdks-for-csharp-and-nodejs.md) and [ADR 0056](../../docs/adr/0056-client-sdks-for-the-jvm.md): the Rust code is the authority, the golden vectors in [`../conformance`](../conformance) enforce it, and the [Java client](../java) is the reference for structure. The package name `@lotse/client` is a placeholder; nothing is published.

## Status

Phase 7 of 8 is done: the examples, on top of the admin calls, transactions, publishing and consumer groups. The client creates streams and queues, produces idempotently, reads committed records, finds offsets, consumes queues with credit, acks, nacks, delayed requeues, standbys of a single active consumer and dead-letter headers, runs transactions (produce, publish, queue acks, group offsets), publishes through exchanges, declares and binds exchanges, joins consumer groups, and administers a cluster: deletes, settings, purge and requeue, users, tenants, role bindings, nodes, pinned leaders, partitions, revoked sessions and node metrics.

| Module | What |
| --- | --- |
| `src/proto` | Field codec (`Reader`, `Writer`), frames and pushes, every client request and response, one codec per message, error codes |
| `src/record` | Record batch: parse, build, in-place edits (`stamp`, `setProducer`, `setTransactional`, `setBaseTimestamp`), control batches, CRC32C, varints |
| `src/net` | `Connection` (TLS 1.3 or plain TCP on loopback, pipelining, pushes, session state), `ConnectionPool`, `TlsConfig`, `Address`, errors |
| `src/auth` | `ScramClient` (SCRAM-SHA-512), `ScramCredential` (the `AlterUser` credential), OAUTHBEARER messages, `authenticate` |
| `src/client` | `LotseClient`: metadata routing with leader hints, idempotent `produce`, read_committed `fetch`, `listOffset`, `createStream`, `createQueue`, `readRecords`, `subscribe`, `beginTransaction`, `publish`, `declareExchange`, `bind`, `unbind`, `joinGroup`, the admin calls (see "Administration"); `Transaction`, `GroupConsumer`, `QueueConsumer`, `Delivery`, `QueueOptions`, `SubscribeOptions`, `GroupConfig`, `Fetched`, `OffsetAt`, `OutMessage`, `Published`, `NodeMetrics`, the errors; the client-side exchange routing (`exchange.ts`) |

## Connections

```ts
import { Connection, ConnectionPool, TlsConfig, scramCredentials, parseAddress } from "@lotse/client";

const tls = await TlsConfig.fromFiles("ca.pem", "alice.pem", "alice.key"); // client certificate optional
const node = parseAddress("10.0.0.1:9092");

const connection = await Connection.open(node, { tls });
const metadata = await connection.call({ type: "Metadata", names: [] }, "Metadata");
connection.close();

// One authenticated connection per address, renewed and replaced as sessions end.
const pool = new ConnectionPool({ tls, credentials: scramCredentials("bob", "secret") });
const c = await pool.get(node);
pool.close();
```

- **The transport has no default.** Options without `tls` and without `plaintextOnLoopback: true` are refused with `InsecureTransportError` before a socket is opened. Plain TCP is allowed only when every address the host resolves to is loopback, and refused otherwise, again before anything is sent (rule 7 of `AGENTS.md`, [ADR 0037](../../docs/adr/0037-tls-and-node-identity.md)).
- **TLS 1.3 only**, with the CA you give and no other: the roots Node.js ships with are not consulted. The node's certificate must be valid for the host you dialled, including IP entries: dial the address the cluster file lists. The client key is a PKCS#8 PEM, as `lotse certs` writes it.
- **Requests are pipelined.** `send` returns a promise that settles when the response with its correlation id arrives, in any order. A response with the `ERROR` flag rejects with `BrokerError` (code and optional reason) before any typed decoding and leaves the connection open. No answer within `requestTimeoutMs` (default 10 000) rejects with `RequestTimeoutError`, whose outcome is unknown. A broken or closed connection rejects every pending request with `ConnectionError`; `requestSent` tells whether the request may have reached the node. The writer does not wait for the socket to drain, so bound the requests you keep in flight.
- **Pushes** go to the `PushListener` registered with `register(subscriptionId, listener)`, or are read with `for await (const push of connection.pushes(id))`. A push that arrives before its subscription is registered is kept, for at most 64 subscriptions, and delivered on registration.
- **Sessions.** After a SASL login, `sessionState()` is `ACTIVE`, `RENEW_DUE` (past four fifths of the lifetime, counted from before the first request) or `ENDED`; a certificate login is `UNLIMITED`. The pool renews a due session on the same connection and replaces an ended or broken one with a fresh login. Without a pool, call `connection.authenticate(credentials)` before the session ends.
- **A connection ends when the node's certificate expires** ([ADR 0068](../../docs/adr/0068-connections-end-when-their-certificate-expires.md)): pending and later requests fail with `ConnectionError`. The deadline timer does not keep the process alive.

## Streams

```ts
import { LotseClient, OffsetAt, TlsConfig } from "@lotse/client";

const client = new LotseClient({
  bootstrap: ["10.0.0.1:9092", "10.0.0.2:9092"],
  tls: await TlsConfig.fromFiles("ca.pem", "alice.pem", "alice.key"),
});

const id = await client.createStream("orders", 2, 3, { "retention.ms": "86400000" }); // 2 partitions on 3 nodes each
const { baseOffset } = await client.produce("orders", 0, { key: "k1", value: "hello", headers: [{ name: "n", value: "1" }] });

for await (const record of client.readRecords("orders", 0, { from: baseOffset, untilEnd: true })) {
  console.log(record.offset, Buffer.from(record.value ?? []).toString());
}
const end = await client.listOffset("orders", 0, OffsetAt.LATEST);
client.close();
```

The client follows `lotse-client` and the Java client rule for rule; [`../PROTOCOL.md`](../PROTOCOL.md) lists the rules.

- **Options.** Durations are milliseconds: `requestTimeoutMs` 10 000, `retryForMs` 30 000, `backoffMs` 50, `produceTimeoutMs` 5 000, `txnTimeoutMs` 60 000. `tenant` is `default`. Like `Connection`, the client has no default transport: give `tls` or `plaintextOnLoopback: true`, or it refuses to connect with `InsecureTransportError`, which is never retried.
- **Routes.** `Metadata` gives each partition's leader and term. The client uses a route until a request fails. A `NOT_LEADER` or `FENCED_LEADER_TERM` hint is followed only if its term is not older (compared as `u64`) and it names another leader; otherwise the route is dropped and metadata is asked again. A name one broker does not know is asked of all brokers, and the highest resource id wins. A leader of 0 asks the partition's voters.
- **Produce is idempotent by default.** The first produce gets a producer id from the coordinator; every batch is stamped with it and a sequence per partition. A batch whose outcome is unknown (the connection broke after sending, `REQUEST_TIMED_OUT`) is sent again with the same bytes, so the leader recognises it. Produces to one partition go out one at a time, in call order; different partitions run in parallel. If retrying runs out of time with the outcome still unknown, the call rejects with `UnknownOutcomeError` and the client starts over with a new producer id. With `idempotent: false` nothing is stamped and an unknown outcome is never retried. `producerName` keeps a producer id across restarts (ADR 0071): the next instance with the name fences the earlier one.
- **Give up a call with a signal.** `produce`, `fetch`, `listOffset` and `createStream` take `{ signal }`. A produce given up while its answer may still come rejects with `UnknownOutcomeError` and drops the producer id, because reusing the sequence could make the leader take the next batch for the abandoned one. A call that was not sent rejects with `GaveUpError`.
- **Fetch is read_committed.** It stops at the last stable offset. `Fetched.records` holds whole batches as the broker sent them; `committedBatches()` and `committedMessages()` leave out control batches and aborted transactions. Go on from `nextOffset(from)`, not from the number of records: a response may hold only a control batch, and the first record of a fresh partition is not at offset 0. `fetchUncommitted` reads up to the high watermark. An offset the broker no longer has rejects with `OffsetOutOfRangeError`, whose `logStart` says where to go on.
- **`readRecords`** is `fetch` as an async iterator: it yields committed records, goes on by itself, and ends when `untilEnd` is set and a fetch returns nothing, or when its `signal` fires.
- **`listOffset`** with `OffsetAt.EARLIEST`, `OffsetAt.LATEST` or `OffsetAt.time(ms)` (ADR 0059). A time finds the first batch with a record at or after it, so the batch may begin with older records.
- **`createStream`** goes to the metadata leader. A create whose answer was lost is sent again; if the first one did create the stream, the second rejects with `ResourceExistsError`, which carries the id.
- **Messages.** `OutMessage` takes `string` (UTF-8) or `Uint8Array` for keys, values and header values; `null` and empty differ. The timestamp defaults to the time of the call: a message stamped 0 is old at once to the stream's retention.
- **Errors** extend `ClientError`: `BrokerResponseError` (with `code`), `OffsetOutOfRangeError`, `UnknownOutcomeError`, `GaveUpError`, `UnknownStreamError`, `ResourceExistsError`, `ClientAuthenticationError`, `ClientClosedError`, and for transactions and publishing `MustAbortError`, `TransactionEndedError`, `NotTransactionalError`, `UnroutableError` and `NotAllWrittenError`. Only `UnknownOutcomeError` needs a decision: the write may or may not have happened.

## Queues

```ts
const id = await client.createQueue("jobs", { ackTimeoutMs: 30_000, maxDeliveries: 5, deadLetterQueue: "jobs-dead" });
await client.produce("jobs", 0, { value: "work" }); // a queue is a resource with one partition, 0

const consumer = await client.subscribe("jobs", { credit: 16 });
for await (const d of consumer) {
  try {
    await handle(d.text());
    await consumer.ack(d);
  } catch {
    await consumer.nackAfter(d, 5_000); // back in the queue after 5 s
  }
}
consumer.close();
```

The consumer follows `lotse-client` and the Java `QueueConsumer` rule for rule; [`../PROTOCOL.md`](../PROTOCOL.md) lists them under "Client behaviour: queues".

- **Credit.** `credit` (default 64, at least 1) is how many messages the leader may push before they are counted as used. At most that many wait unprocessed in the client. After a message is handed out and half the credit (rounded up) is used, the consumer sends the used messages and bytes back as new credit.
- **Reading.** `next()` waits for a message, `poll(waitMs)` resolves `null` when none came, and `for await (const d of consumer)` yields until the consumer is closed. Leaving the loop does not close the consumer. A signal in the options of `poll`, `next` or `deliveries({ signal })` ends `poll` with `null`, ends that iterator, and makes `next` reject with `GaveUpError`. A signal given to `subscribe` only gives up the subscribing; `for await (const d of consumer)` without options has no signal, so leave that loop with `break` or use `deliveries({ signal })`. Reads called together take turns.
- **A `Delivery`** has `offset`, `deliveryCount` (this delivery included; more than 1 means an earlier one was not acknowledged; a lower bound, since a failover can start it over), `timestamp`, `key`, `value`, `headers`, `text()` and `deadLetter()`, which reads the `x-lotse-dead-letter-*` headers of a message the broker moved from another queue (`reason`, `queue`, `queueId`, `offset`, `deliveries`, `floor`).
- **Settling.** `ack(delivery | deliveries)` names offsets as `[from, to)` ranges, sorted as `u64`, and is retried: settling twice is harmless, and a message that went to someone else meanwhile is still settled. `nack(d, false)` discards (or dead-letters) like an ack. `nack(d, true)` and `nackAfter(d, delayMs)` (ADR 0077) give the message back on the subscription's own connection and are not retried; if the subscription has ended, the message went back with it and the call does nothing.
- **Ending and subscribing again.** A push with an error, a broken connection, a leader change or an ack timeout ends the subscription. The consumer hands out what it holds and subscribes again on its next read, following a leader hint. Messages it held may meanwhile have gone to others, so they can arrive twice, with a higher `deliveryCount`.
- **Single active consumer** (ADR 0076). The consumer asks to be told when its turn comes. `consumer.isStandby` is true while it waits; the broker pushes nothing until then, and the flag clears on the next read that sees the activation or a message.
- **Closing.** `close()` sends `Unsubscribe` and returns the messages received but not handed out, buffered or still unread, with one requeueing `Nack`; it does not wait for answers. Messages already handed out stay with the consumer until acknowledged or until the connection closes. A read that waits rejects with `ConsumerClosedError`.
- **`createQueue(name, options)`** takes `replication` (default 3), `ackTimeoutMs`, `maxDeliveries`, `messageTtlMs`, `deadLetterQueue`, `singleActiveConsumer`, `priorityLevels` (2 to 10, ADR 0078; a message's level is its `x-lotse-priority` header) and `settings`, a map for any other key. The named options become the settings `ack.timeout.ms`, `max.deliveries`, `message.ttl.ms`, `single.active.consumer` and `priority.levels`. A key given twice, or a value out of range, is a `RangeError` before anything is sent. A producer delays one message with the header `x-lotse-delay-ms` (ADR 0077).
- Exclusive subscriptions are not offered: the broker refuses them.

## Transactions

```ts
const client = new LotseClient({ bootstrap, tls, transactionalId: "payments-1" });

{
  await using txn = await client.beginTransaction(); // aborts at the end of the scope unless it is committed or aborted
  await txn.produce("ledger", 0, { key: "acct-7", value: "debit 40" });
  await txn.publish("events", "ledger.debit", { value: "debit 40" }); // routed here, written to every queue it matches
  await txn.commit();
}
```

A client with a `transactionalId` (in the client's tenant, ADR 0062) runs one transaction at a time; a second `beginTransaction` waits for the first. The first one asks the coordinator for a producer id, which fences an older client with the same id and aborts what it left open; later ones go on with the epoch the last `EndTxn` answer gave. The client follows `lotse-client` and the Java `Transaction` rule for rule ([`../PROTOCOL.md`](../PROTOCOL.md): "Client behaviour: transactions").

- **Writing.** `produce` and `produceBatch` add the partition with `AddPartitionsToTxn` once, stamp the batch with the producer id, epoch, a sequence per partition and the transactional flag, and send it with the transactional id. A batch whose outcome is unknown is sent again with the same bytes. Read_committed readers see nothing of the transaction before the commit, and nothing of it ever after an abort.
- **Must abort.** After an error that may have left something written (a write with an unknown outcome, a refusal, a call given up after it was sent, a client closed mid-call) only `abort` is left: every other call rejects with `MustAbortError`, `commit` included, without asking the coordinator. `GaveUpError` and `UnknownStreamError` prove that nothing was written and leave the transaction open.
- **Ending.** `commit` resolves once the decision is durable. A lost request or a retriable error may have been decided already: it is sent again, and `UnknownOutcomeError` follows if time runs out. After a failed end the next transaction asks for a fresh producer id. A transaction that wrote nothing ends without asking the coordinator. After `commit` or `abort` every call rejects with `TransactionEndedError`.
- **`await using`.** Leaving the scope aborts a transaction that was not ended. If the abort does not get through, the transaction is abandoned as by `abandon()` and no error is thrown, so the error that left the scope is not hidden; call `abort()` yourself to see it. `abandon()` ends the handle without a decision: the coordinator aborts the transaction after `txnTimeoutMs`, or at once when the client begins the next one with a fresh producer id. Where `Symbol.asyncDispose` does not exist, `dispose()` does the same.
- **Signals.** `beginTransaction({ signal })` gives up the wait for the turn with `GaveUpError`; once the transaction has begun, the signal aborts it: the call in progress is given up as its own signal would give it up, then the transaction is aborted. Every call also takes its own `{ signal }`. A produce, commit or ack given up after it was sent rejects with `UnknownOutcomeError`.
- **Order.** Calls on one transaction take turns in the order they were made.
- **Publishing in a transaction** is routed by the client (ADR 0032): it reads the exchange's type and bindings from `Metadata`, keeps them for `metadataMaxAgeMs` (30 000), picks the queues by the rules of `lotse-route` (checked against [`../conformance/routing.txt`](../conformance/routing.txt)) and produces the message to each queue in the transaction. A message no queue takes is dropped, or rejects with `UnroutableError` when `mandatory`. If a write fails after some queues hold the message, only an abort is left. A `bind` or `unbind` by the same client drops its cached bindings.
- **Queue acks** (ADR 0075): `consumer.ackIn(txn, delivery)` adds the queue to the transaction and sends `Ack` with the transactional id, producer id and epoch on the subscription's own connection; the commit settles the message and the abort gives it back. A broken connection or an ended subscription is `MESSAGE_NOT_HELD`, and so is a message another transaction holds (of two transactions the first holds it). Any error leaves only an abort.
- **Group offsets:** `txn.sendOffsets(group)` sends the member's read positions with `AddOffsetsToTxn`; they become the group's committed offsets with the commit and change nothing with an abort. A fenced member, a refusal or an unknown outcome leaves only an abort. A transaction that only sent offsets (no partition added) is still ended at the coordinator, which holds the offsets as pending until then; an add that only gave up (`GaveUpError`) does not count, as nothing was written and an `EndTxn` can be refused with `INVALID_TXN_STATE` while the previous transaction is still completing. This is exactly-once consume-transform-produce: read with `autoCommit: false`, write the results and the offsets in one transaction, and after an abort call `group.rewind()` to read again from the committed offsets.

## Publishing through exchanges

```ts
await client.declareExchange("orders", ExchangeType.TOPIC);
await client.bind("orders", "orders-eu", "*.eu");
const { routed } = await client.publish("orders", "orders.eu", { value: "m" }, { mandatory: true });
// routed: [{ queue: "orders-eu", offset: 17n }, ...], in queue name order
```

Outside a transaction `publish` goes to any node, which routes by the bindings it has applied, checks the publisher's producer role on the exchange and every routed queue, and writes each queue with an idempotent producer of its own (ADR 0079). It resolves once every routed queue holds the message. A message no queue takes is dropped, or rejects with `UnroutableError` when `mandatory`. If some queues could not be written, `NotAllWrittenError` lists the queues that hold it and why the others do not. **A lost answer is not sent again**: the node may have written the message, so the call rejects with `UnknownOutcomeError` (also when its signal gave it up after it was sent). A node routes by what it has applied, so a binding made a moment ago may not be seen by the node you publish to yet.

`declareExchange(name, type)` (`ExchangeType.DIRECT`, `TOPIC`, `FANOUT`, `HEADERS`), `bind(exchange, queue, key)`, `bindHeaders(exchange, queue, headers, matchAll)` and `unbind` go to the metadata leader and are retried; a declare whose answer was lost may find its first attempt and reject with `ResourceExistsError`.

## Consumer groups

```ts
const group = await client.joinGroup("billing", ["orders"], { autoCommit: true, startFrom: OffsetAt.EARLIEST });
for await (const { stream, partition, record } of group) {
  await handle(record);
}
await group.close(); // commits the positions and leaves
```

`poll(waitMs, { signal })` heartbeats when one is due, then reads what one owned partition has and resolves the records in offset order (an empty array when there is nothing yet or the signal fired); `for await (const r of group)` or `group.records({ signal, waitMs })` polls without end and yields one record at a time until the consumer is closed or the signal fires. The member heartbeats **only from `poll`**: call it more often than the session timeout, or it loses its partitions. Calls to `poll` take turns; `commit`, `rewind` and `close` may be called at any time. The member follows `lotse-client` and the Java `GroupConsumer` rule for rule ([`../PROTOCOL.md`](../PROTOCOL.md): "Client behaviour: consumer groups"):

- **Joining.** `joinGroup(group, streams, config)` joins with the stream ids; `joinGroupMatching(group, pattern, config)` subscribes by a regular expression over the tenant's streams the member may read, which the coordinator matches with every heartbeat, so streams created later join in (ADR 0073). The next heartbeat is due after `min(the coordinator's interval, sessionTimeoutMs / 3)`.
- **Rebalancing.** A partition missing from a new assignment is not read from that moment on. With `autoCommit` its position is committed, and the next heartbeat goes out at once and reports it released; only then may another member get it. So no partition has two owners, and no record is read twice across a hand-over.
- **Lost.** When no heartbeat has succeeded for `sessionTimeoutMs` (45 000), counted from when the last successful one was sent, every partition is dropped without a commit and the member joins again as a new member; `UNKNOWN_MEMBER_ID` and `FENCED_MEMBER_EPOCH` mean the same. A fetch that outlasts the session hands out nothing.
- **Static members** (ADR 0072): with `instanceId` a member that comes back within the session timeout takes over its partitions without a rebalance, and the earlier instance gets `BrokerResponseError` with `FENCED_INSTANCE_ID` from `poll`, and stays out.
- **Positions.** A partition without a position asks `OffsetFetch` with `requireStable`, so an offset an open transaction holds waits. With no committed offset it starts at 0 (`OffsetAt.EARLIEST`) or at `ListOffsets` for `startFrom` (ADR 0059). A committed offset that retention deleted goes on from the log start. `positions()` and `assignment()` show what the member owns; `rewind()` forgets the positions so that every partition starts again from the committed offset.
- **`close()`** commits the positions when `autoCommit` is on, then leaves with epoch -1. It waits for the poll in progress, which hands out nothing once the consumer is closed.

## Administration

```ts
await client.createTenant("acme");
await client.alterRoleBindings([{ subjectKind: SubjectKind.USER, subject: "bob", role: Role.TENANT_ADMIN, tenant: "acme", prefix: "" }]);
await client.setUserPassword("bob", "secret"); // only a salted credential leaves the client

const bob = new LotseClient({ bootstrap, tls, credentials: scramCredentials("bob", "secret"), tenant: "acme" });
await bob.createStream("orders", 1, 3);
await bob.alterConfigs("orders", { "retention.ms": "86400000" }, ["retention.bytes"]);
const settings = await bob.describeConfigs("orders"); // [{ key, value, source: ConfigSource.DYNAMIC | DEFAULT | STATIC }]
await bob.deleteIfEmpty("orders");
```

Every call takes `{ signal }` last, as the stream calls do, and follows `lotse-client` and the Java client rule for rule ([`../PROTOCOL.md`](../PROTOCOL.md): "Client behaviour: admin").

| Calls | What |
| --- | --- |
| `delete(name)`, `deleteIfEmpty(name)` | A stream, queue or exchange made at runtime, and its id. `deleteIfEmpty` is refused with `RESOURCE_NOT_EMPTY` when a queue holds an unsettled message or a stream a record (ADR 0063); it guards against mistakes, not against a producer. |
| `purge(queue)`, `requeue(queue)` | The count of messages settled, or moved from a dead-letter queue back to their source (ADR 0064), in rounds until the leader says there is no more. A message a consumer holds, its credit included, is not purged. Tenant admin. |
| `alterConfigs(name, set, reset)`, `describeConfigs(name)` | Settings of a resource made at runtime; each setting is read with its source. A key the resource does not take is refused with `INVALID_REQUEST` and the reason. |
| `setUserPassword`, `deleteUser`, `describeUsers` | SCRAM-SHA-512 users kept in the metadata group. Cluster admin. |
| `createTenant`, `deleteTenant`, `describeTenants` | Tenants (ADR 0039). A tenant that holds resources is not deleted. `describeTenants` lists those the caller has a role in. |
| `alterRoleBindings(add, remove)`, `describeRoleBindings`, `whoAmI` | Role bindings of a user or IdP group, in a tenant, for names with a prefix; removals are applied first. `whoAmI` says who the client acts as and which bindings apply. |
| `revokeCertificate(issuer, serial, notAfterMs, reason)` | Revokes one client certificate, named by the SHA-256 of its issuer's name (32 bytes) and its serial: nodes end its connections and refuse it until it expires (ADR 0090). The CRL file stays as it is. Cluster admin. |
| `revokeSessions(subject, group)` | Ends the sessions of a user or IdP group that began before now, and refuses their older tokens (ADR 0070). A SCRAM user can log in again unless the password is changed or deleted. Cluster admin. |
| `addNode`, `removeNode`, `describeNodes` | The node list in the metadata group (ADR 0048): `removeNode` starts a drain, `describeNodes` shows replicas and moves. Ids are never used twice. |
| `moveLeader(name, partition, node)` | Pins a partition's leader to one of its voters; `null` unpins (ADR 0065). Cluster admin. |
| `createPartitions(name, count)` | Raises a runtime stream's partition count (ADR 0074); never lowers it. |
| `nodeMetrics()` | Each node's `DescribeMetrics` answer, asked of the node itself: `{ nodeId, metrics, error }`. A node that does not answer has a `NoAnswerError`; the call does not reject. |

- **Where requests go.** Admin requests go to the metadata group's leader. Until the client knows it, it asks the brokers in turn; a retriable answer with a leader hint sets the next address. A change that was sent and got no answer is sent again, so a delete may find the resource gone and is then done, and a create may find what it made and reject with `ResourceExistsError` and the id. Other refusals are `BrokerResponseError` with the code and the broker's reason in `brokerMessage`. A name a principal may not see is `UnknownStreamError`, as for a name that does not exist.
- **Given up.** A change given up after it was sent rejects with `UnknownOutcomeError`; a read given up, or one that was refused as retriable until the time ran out, rejects with `GaveUpError`.
- **Names and ids.** The calls that take a name ask every known broker for it and take the newest resource, so a name made a moment ago is found on a broker that lags. The node that answers a read has applied what the metadata leader had when it answered; a node you ask for metadata may be behind, so wait for a change you made before you look for it on every node.
- **Arguments** out of range (`createPartitions` with a count below 1 or above `u32`, a partition that is not a `u32`, a node id that is not positive) are a `RangeError` before anything is sent. Node ids are `number` or `bigint`; `describeNodes` gives `bigint`.
- Not offered, as in Java: creating a queue in another tenant than the client's (give the client that `tenant`), and the nodes' own `SettleVoters`.

## Authentication

`scramCredentials(user, password)` and `oauthBearerCredentials(() => token)` (the function is called for every authentication, renewals included, and may be async). A client certificate is part of `TlsConfig`.

SCRAM-SHA-512 follows RFC 5802 and RFC 7677 with the particulars in [`../PROTOCOL.md`](../PROTOCOL.md): the password is its UTF-8 bytes with no SASLprep, the server nonce must strictly extend the client's, iteration counts outside 4096 to 1 048 576 are refused, and the server's final signature is always verified; a node that cannot prove it knows the credential is refused with `ScramError` of kind `SERVER_NOT_VERIFIED`. PBKDF2 runs off the event loop. A wrong password and an unknown user are refused alike by the node (`AUTHENTICATION_FAILED`), and the node closes the connection.

`ScramCredential.forPassword(password)` computes what a node stores (salt, iterations, `StoredKey`, `ServerKey`), and `.alterUser(name)` is the `AlterUser` request that sets it; the password itself is never sent.

## Conventions

- No runtime dependencies. `typescript` and `@types/node` are development dependencies only.
- A 64-bit wire value is a `bigint`. A `u64` is held as its signed 64-bit bit pattern, as in the Java client, so `NO_OFFSET` (`u64::MAX`) is `-1n`. Compare, add and print such values with `compareU64`, `addU64Exact` and `u64ToString`; do not use `<` or `String` when a value may be that large.
- Narrower integers (`u8`, `u16`, `u32`, `i32`) are `number`. A writer refuses a value out of range with a `RangeError`.
- Fixed-width fields are little-endian. The frame length prefix is big-endian.
- A frame that does not decode throws `DecodeError`; a batch that does not parse throws `RecordFormatError`. Both carry a `kind`.
- Keys, values and header values are `Uint8Array`, `null` where the protocol has a null. Null and empty differ.

## Compression

zstd and lz4 are read and written ([ADR 0085](../../docs/adr/0085-codecs-in-the-nodejs-sdk.md)). Set `attributes.compression` on a `BatchBuilder`, or read compressed batches from a fetch; nothing compresses by default.

- **lz4** is implemented in the SDK (frame format with checksums, independent and linked blocks) and works on every Node.js.
- **zstd** is `node:zlib`'s, which has it from Node.js 22.15 and 23.8 and in 24. On Node.js 20 and earlier 22 versions, reading or building a zstd batch rejects with `RecordFormatError` of kind `UNSUPPORTED_COMPRESSION`, naming the codec and what to do. To read zstd there anyway, give the SDK a codec of your own: `registerCodec(Compression.ZSTD, { compress, decompress })` with functions from a package you choose; `registerCodec(Compression.ZSTD, undefined)` removes it.
- A block is at most 16 MiB once decompressed; one byte more is `TOO_LARGE`. A block that does not decode is `MALFORMED`. A compacted batch with no records reads as empty whatever its codec, because compaction writes no block for it.

## Examples

`examples/` holds four programs and a helper, written to run against a cluster. They are compiled by the same `tsc` run as the library, so `npm test` fails when one no longer fits the API. They mirror the Java examples ([`../java/src/examples`](../java/src/examples)) with the same scenarios and the same output.

| Script | What |
| --- | --- |
| `npm run example:stream` | Creates the stream `example-orders` (2 partitions), writes ten messages by customer key and reads them back. |
| `npm run example:queue` | Work queue `example-jobs` with the dead-letter queue `example-jobs-dead`: `job-3` fails every time and is requeued with `nackAfter` after 1 s; after three deliveries it arrives in the dead-letter queue with the reason `max-deliveries`. |
| `npm run example:exactly-once` | A group member reads `example-orders` (run the stream example first), writes an invoice for each order to `example-invoices`, and commits the invoices and the group's offsets in one transaction with `sendOffsets`. A failed transaction is aborted and the member reads the same orders again. It stops after 20 s. |
| `npm run example:idiomatic` | The Node.js shape: `await using` for a transaction (one committed, one left without `commit` and aborted at the end of its scope), `for await` over `readRecords` and over a queue consumer. Needs nothing from the other examples. |

Each script compiles with `tsc` and then runs `node dist/examples/<name>.js`, so after `npm run build` you can run that file directly. Every example closes its client and ends by itself. The resources are created if they are missing and kept, so the examples can run again; the idiomatic example adds a committed ledger record on each run.

The connection settings come from the environment, with the same names as the Java examples (`examples/env.ts`):

| Variable | Meaning |
| --- | --- |
| `LOTSE_BOOTSTRAP` | Brokers, `host:port` separated by commas. Default `127.0.0.1:9092`. |
| `LOTSE_CA` | PEM file of the cluster's CA. Without it the examples use plain TCP, which the client allows on loopback only. |
| `LOTSE_CERT`, `LOTSE_KEY` | A client certificate and its PKCS#8 key, with `LOTSE_CA`. |
| `LOTSE_USER`, `LOTSE_PASSWORD` | A SCRAM-SHA-512 login, if the cluster asks for one. |

## Tests

```sh
npm ci
npm test        # compiles the library, tests and examples to dist/, then runs node:test on dist/test
```

or `make sdk-node-test` from the repository root. The tests decode every vector in `sdk/conformance` and write it again to identical bytes, refuse each vector cut at every length and with one byte added, and check the fixed cases for CRC32C, varints, error codes and in-place batch edits. Set `LOTSE_CONFORMANCE` to read the vectors from another directory.

The admin tests (`admin.test.ts`) run against scripted brokers and check every field of every admin request and every refusal; `admin-real.test.ts` runs them against three real nodes (deletes seen on every node, `deleteIfEmpty`, purge in rounds, requeue of a dead letter, settings, partitions, a pinned leader, metrics, nodes) and one TLS node with authentication (users and their logins, tenants, a tenant admin inside and outside the tenant, prefixes, revoked sessions). The client tests run against scripted brokers in the test process (routing, hints, idempotent retries, read_committed, signals, `createStream`, the queue consumer: credit, settling, standbys, ending and subscribing again, closing, `createQueue`; transactions: must-abort, ending, `await using`, signals, routed publish, queue acks, offsets; publishing, `declareExchange` and `bind`; a group member: heartbeats, rebalancing, a lost session, a fenced instance, positions, patterns) and, with `LOTSE_BIN`, against three real nodes on loopback (create, produce, read back, `ListOffsets`, compressed batches, a leader killed in the middle of a produce, a failover, a named producer fenced; queues: consuming, ack, requeue, `nackAfter`, ack timeout, a vanished consumer, dead letters, a single active consumer taking over, priorities, a leader killed mid-consumption; commit and abort as read_committed readers see them, a producer fenced by a newer epoch, exactly-once consume-transform-produce from a stream and from a queue, `MESSAGE_NOT_HELD` between two transactions, two group members that never share a partition, a static member taken over, a pattern, a publish through a topic exchange to two queues, and a publish whose answer never comes, made by pausing the nodes) and one TLS node. The client-side exchange routing is checked against `conformance/routing.txt`. The lz4 codec has its own tests (`lz4.test.ts`); the zstd tests are skipped, with the reason, on a Node.js without zstd in `node:zlib`.

The connection tests run against a fake broker in the test process (plain TCP and TLS), with a fake clock for sessions; SCRAM is checked against `conformance/scram.txt` and a server written for the tests from the RFC. The tests that need real certificates or a real node (`tls`, `real-node`, `client-real`) use the `lotse` executable: set `LOTSE_BIN`, or build `lotse-node` so that `/tmp/cargo-target/debug/lotse` (or `$CARGO_TARGET_DIR/debug/lotse`) exists. Without one they are skipped and say so. They start one TLS node with sessions limited to 4 s and one plain-TCP node with authentication off, and the stream client tests three more, so the suite takes about 80 s with `LOTSE_BIN` set. The certificate expiry test needs `openssl` 3.4 or later and is skipped without it.

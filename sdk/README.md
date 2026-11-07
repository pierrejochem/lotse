# Lotse client SDKs

Client libraries for the Lotse native protocol. The Rust code is the authority: [`crates/lotse-proto`](../crates/lotse-proto) and [`crates/lotse-record`](../crates/lotse-record) define every byte, and [`lotse-client`](../crates/lotse-client) defines the client's behaviour. The reasons are in [ADR 0056](../docs/adr/0056-client-sdks-for-the-jvm.md).

| Path | What |
| --- | --- |
| [`PROTOCOL.md`](PROTOCOL.md) | The protocol as an implementer needs it: byte layouts, the traps, what the vectors pin down |
| [`conformance/`](conformance/) | Golden vectors, written by Rust and read by every SDK |
| [`java/`](java/) | Java 17, Gradle; no runtime dependencies, two optional ones for compressed batches |

## Status

Done in the Java client: `dev.lotse.proto` (field codec, frames, every client-facing request and response, error codes), `dev.lotse.record` (record batch, builder, in-place edits, control batches), `dev.lotse.net` (connections, TLS 1.3, pipelining, pushes, session renewal) and `dev.lotse.auth` (SCRAM-SHA-512, OAUTHBEARER, the `AlterUser` credential), and `dev.lotse.client` for streams (metadata and leader routing, idempotent produce, read_committed fetch, `listOffset`), queues (subscribe with credit, ack, nack, resubscribing, dead-letter headers), publishing through exchanges (routed by a node, or by the client within a transaction, with the routing checked against `routing.txt`), transactions (produce, publish, queue acks and group offsets in one transaction), consumer groups and the admin calls of `lotse-client`. The Kotlin layer follows.

### Producing and reading a stream

```java
var config = ClientConfig.builder("127.0.0.1:9092", "127.0.0.1:9093")
        .plaintextOnLoopback()        // or .tls(TlsConfig.fromFiles(ca, cert, key)); there is no default
        .build();
try (var client = new LotseClient(config)) {
    client.createStream("orders", 2, 3);                       // 2 partitions, 3 replicas each

    // Idempotent by default: a retry after a lost answer cannot write the message twice.
    ProduceResult sent = client.produce("orders", 0, OutMessage.value("order-1").withKey("customer-7"));
    System.out.println("committed at offset " + Long.toUnsignedString(sent.baseOffset()));

    long offset = 0;
    while (true) {
        Fetched fetched = client.fetch("orders", 0, offset, Duration.ofMillis(500));   // read_committed
        for (BatchRecord record : fetched.committedMessages()) {
            System.out.println(record.offset() + " " + record.value().toUtf8());
        }
        offset = fetched.nextOffset(offset);    // also when no message came: control entries move the offset
    }
}
```

- Every call has a `...Async` form that returns a `CompletableFuture`. Two async produces to one partition run one after the other, in no promised order: await the first if the order matters.
- The partition is yours to choose; the client does not hash keys. Produces to one partition are serialised, produces to different partitions run in parallel.
- A message without a timestamp is stamped with the time of the call. Retention judges a stream's data by producers' timestamps, so a message stamped 0 would be deleted at the next pass.
- `ClientException.Unknown` is the one error that needs a decision: after retrying until `retryFor` the client cannot tell whether the batch was written. It has dropped its producer id, so later produces are not blocked. Read the partition, or produce again and accept a possible duplicate. Without `idempotent(true)` an unknown outcome is never retried.
- `ClientException.OffsetOutOfRange` carries `logStart()`: retention deleted the offset; read on from there.
- Admin calls are under "Administering" below.

### Consuming a queue

```java
try (var client = new LotseClient(config)) {
    client.createQueue("jobs", QueueOptions.builder()
            .replication(3)
            .ackTimeout(Duration.ofSeconds(30))
            .maxDeliveries(5)
            .deadLetterQueue("jobs-dead")      // must exist; without one, retired messages are dropped
            .build());
    client.produce("jobs", 0, OutMessage.value("resize image 42"));   // queues are written like partition 0

    try (QueueConsumer consumer = client.subscribe("jobs", SubscribeOptions.withCredit(16))) {
        while (true) {
            Delivery job = consumer.poll(Duration.ofSeconds(1));      // null if nothing came
            if (job == null) {
                continue;
            }
            try {
                work(job.value().toUtf8());
                consumer.ack(job);
            } catch (RuntimeException e) {
                consumer.nack(job, true);      // true: deliver again, to any consumer
            }
        }
    }
}
```

- Credit is how many messages the leader may have pushed to this consumer and not yet counted as used. It is also the most that wait unprocessed in the client. The consumer adds credit by itself once half of it is used up.
- A message can arrive more than once: after a `nack` with requeue, after the ack timeout, or after a failover. `deliveryCount()` says how often it has been delivered, and after a failover it can start over. Process messages so that a second run does no harm.
- `ack` and `nack` name an offset, not a delivery, so they are safe to repeat and are retried when an answer is lost. `ack(Collection)` sends neighbouring offsets as one range.
- `nack(job, true)` only works for a message of the current subscription, on its connection. After the subscription ended the broker has already taken the message back, and the call does nothing.
- `poll` and `next` are for one thread; `ack`, `nack` and `close` may be called from any thread. `close` hands back the messages the consumer received but did not hand out. Messages it handed out stay with it until acknowledged or until the client's connection closes.
- A dead-lettered message carries where it came from: `delivery.deadLetter()` has the reason (`max-deliveries` or `expired`), the source queue, its id, the offset there, the delivery count and the ack floor.
- `ackIn(txn, delivery)` acknowledges within a transaction; see below. `nackAfter(job, Duration.ofSeconds(30))` gives a message back to be delivered again once the delay has passed (ADR 0077); a producer delays a message with the header `x-lotse-delay-ms`.
- On a queue with `single.active.consumer` (ADR 0076) one subscription gets messages and the others wait as standbys in the order they subscribed: `isStandby()` says which this one is.

### Transactions

```java
var config = ClientConfig.builder("127.0.0.1:9092")
        .plaintextOnLoopback()
        .transactionalId("billing-1")       // one running client per id; a new one fences the old
        .build();
try (var client = new LotseClient(config);
        var group = client.joinGroup("billing", List.of("orders"), GroupConfig.builder().autoCommit(false).build())) {
    while (true) {
        List<GroupRecord> records = group.poll(Duration.ofMillis(500));   // heartbeats too: call it often
        if (records.isEmpty()) {
            continue;
        }
        try (Transaction txn = client.beginTransaction()) {
            for (GroupRecord r : records) {
                txn.produce("invoices", 0, OutMessage.value(invoiceFor(r.record())));
                txn.publish("events", "invoice.created", OutMessage.value("..."), false);
            }
            txn.sendOffsets(group);           // the group's offsets commit with the output, or not at all
            txn.commit();
        } catch (ClientException e) {
            group.rewind();                   // read again from the committed offsets
        }
    }
}
```

- What a transaction writes becomes visible to read_committed readers together when it commits, and never if it aborts. One transaction at a time per client: `beginTransaction` waits for the open one.
- After an error that may have left something written, the transaction only aborts: every other call throws `ClientException.MustAbort`. `GaveUp` and `UnknownStream` prove nothing was written and leave it open.
- A transaction closed without `commit` or `abort` (as above, after an exception) is aborted by the coordinator; the client's next transaction starts with a fresh producer id.
- `publish` in a transaction is routed by this client, by the bindings metadata gave within `metadataMaxAge`, and reaches every queue or none.
- `consumer.ackIn(txn, delivery)` acknowledges a queue message within the transaction: the commit settles it, an abort gives it back.

### Consumer groups

- `joinGroup(group, streams, config)` or `joinGroupMatching(group, regex, config)`. Partitions arrive with the first `poll` calls, and `poll` returns the records of one owned partition at a time, in offset order.
- **The member heartbeats only from `poll`.** Call it more often than the session timeout (default 45 s), or the member loses its partitions and joins again. The rules are the Rust client's, listed in `PROTOCOL.md`.
- With auto-commit (default) a revoked partition's position is committed before it is released, and `close` commits and leaves. Without it, commit with `commit()` or with a transaction (`sendOffsets`).
- `GroupConfig.startFrom` says where a partition without a committed offset starts: `OffsetAt.EARLIEST` (default), `LATEST` or `time(ms)`. `instanceId` keeps a member's partitions across a restart within the session timeout; a second instance with the same id fences the first, whose `poll` then throws `FENCED_INSTANCE_ID`.
- A `GroupConsumer` is for one thread.

### Administering

```java
client.declareExchange("events", Messages.ExchangeType.TOPIC);
client.createQueue("audit");
client.bind("events", "audit", "#");
client.alterConfigs("audit", Map.of("max.deliveries", "5"), List.of());
long purged = client.purge("audit");
client.deleteIfEmpty("audit");
```

- Admin requests go to the metadata group's leader; a broker that is not the leader names it, and the client follows. Each call needs the role the broker asks for (tenant-admin for most, cluster-admin for nodes, pins and revoking sessions).
- A create whose answer was lost is retried. If the first attempt did create the resource, the call fails with `ClientException.ResourceExists`, which carries the id to compare with what you expected. A delete is done once the resource is gone, whoever deleted it.
- `setUserPassword` sends only a salted SCRAM credential, with a new salt each time; the password does not leave the client.
- `purge` and `requeue` work through the queue in rounds until its leader says there is no more; messages produced meanwhile are left alone.
- Also there: `describeConfigs`, `unbind`, `bindHeaders`, `delete`, `createPartitions`, `moveLeader`, `createTenant`, `deleteTenant`, `describeTenants`, `alterRoleBindings`, `describeRoleBindings`, `whoAmI`, `describeUsers`, `deleteUser`, `revokeSessions`, `addNode`, `removeNode`, `describeNodes` and `nodeMetrics`, each as in `lotse-client`.

### Connecting

```java
var tls = TlsConfig.fromFiles(Path.of("ca.pem"), Path.of("alice.pem"), Path.of("alice.key")); // certificate login
var options = ConnectionOptions.builder().tls(tls).build();
try (var connection = Connection.open(Address.parse("127.0.0.1:9092"), options)) {
    var versions = connection.apiVersions().get();                 // optional; the Rust client skips it
    var metadata = connection.call(new Request.Metadata(List.of()), Response.Metadata.class);
}
```

- TLS 1.3 only, the cluster's CA only, the certificate checked against the host you dial (IP addresses against the certificate's IP entries). `lotse certs` writes ECDSA P-256 keys in PKCS#8 PEM, which `Pem` reads; other PKCS#8 RSA and EC keys load too.
- Nothing connects until the options say `tls(...)` or `plaintextOnLoopback()`. Plain TCP to an address that is not loopback is refused before a byte is sent.
- `credentials(Credentials.scram(user, password))` or `Credentials.oauthBearer(supplier)` log in on connecting. The supplier is called for every authentication, renewals included. Use a `ConnectionPool` to have sessions renewed at four fifths of their lifetime and replaced when they end.
- Failures: `ConnectionException` (`requestSent()` tells whether the outcome is unknown), `RequestTimeoutException`, `BrokerException` (the node refused the request as a whole), `AuthenticationException`, `InsecureTransportException`.
- Listeners for pushes run on the connection's reader thread: hand work to another thread. A write to a node that has stopped reading can block the writing thread until the operating system gives up; there is no write timeout in `Socket`.

### Compressed batches

Producers may compress a batch with zstd or lz4, and the broker serves it as sent (ADR 0058). To read such batches, or to write them, put both codec libraries on the class path ([ADR 0061](../docs/adr/0061-optional-codecs-in-the-java-sdk.md)):

```kotlin
// Gradle
implementation("dev.lotse:lotse-client:0.0.1") {
    capabilities { requireCapability("dev.lotse:lotse-client-compression") }
}
// or name them yourself, also in Maven
implementation("com.github.luben:zstd-jni:1.5.7-16")
implementation("at.yawk.lz4:lz4-java:1.11.0")
```

- Without them everything else works, and a compressed batch fails with `RecordFormatException` of kind `UNSUPPORTED_COMPRESSION`, naming the library to add. `Fetched.committedMessages()` and a queue consumer pass that exception on as it is.
- zstd-jni ships native libraries for the common platforms; on one it has none for, zstd batches fail the same way.
- To compress, name the codec in the batch's options: `new BatchBuilder(new BatchOptions(new Attributes(Compression.ZSTD, false, false, BatchKind.DATA), 0, 0, -1))`. A compressed batch holds at most 16 MiB of records and is at most 1 MiB itself. Nothing compresses by default.

## Build and test

```sh
make sdk-test            # check the vectors against Rust, then run the Java tests, with and without the codec libraries
LOTSE_BIN=/path/to/lotse make sdk-test   # also the tests that need real certificates and a real node
make sdk-vectors         # regenerate sdk/conformance after changing the protocol
```

`make sdk-test` needs a JDK 17 or newer. The TLS and real-node tests use the `lotse` executable (`cargo build -p lotse-node`): they look at `LOTSE_BIN`, then at `$CARGO_TARGET_DIR/debug/lotse` (default `/tmp/cargo-target`), and skip with a message when there is none. The build targets Java 17 whatever JDK runs it. Tests ran on JDK 25 and 21. The client's routing, produce, fetch, transaction, group and admin rules are tested against scripted brokers in the test process; `RealClusterTest` starts three `lotse` processes and kills the leader of a partition while a client produces.

## Conformance

`make sdk-vectors-check` regenerates the vectors into a temporary directory and compares them with `sdk/conformance`; a change to a message layout fails it until the vectors are regenerated and committed. Each SDK decodes every vector and writes it again to identical bytes.

# Lotse

<p><img src="docs/brand/logos/lotse-wordmark.svg" alt="Lotse" height="56"></p>

Lotse is a message broker in Rust: reliable queues and persistent streams on one replicated log, with exactly-once delivery.

- **Queues and streams on one log:** RabbitMQ-style queues (acks, redelivery, dead-letter queues, delayed delivery, priorities, exchanges and bindings) and Kafka-style streams (partitions, consumer groups, retention, log compaction) share one storage engine.
- **Replication with Raft:** every partition is a Raft group, batched per node pair; nodes can be added to and drained from a running cluster.
- **Exactly-once:** idempotent producers, transactions and consumer group offsets committed in the same transaction.
- **Secure by default:** TLS 1.3 on every connection, SCRAM-SHA-512, OAUTHBEARER and client certificates, tenants and roles.
- **Tested by simulation:** deterministic simulation with crashing disks and a faulty network, 5,000 seeds per simulation before every release.
- **Clients and console:** clients for Rust, Java (with a Kotlin layer), C# and Node.js, and a web console.

Status: foundation. The runtime seam with its Tokio implementation, the record format, the segmented log, the Raft core, the simulation harness, a broker node (many Raft groups over one transport) and the first cut of the native protocol with a Rust client (idempotent and transactional produce, read_committed fetch, queues with acks, credit and redelivery, publishing through exchanges, consumer groups with exactly-once consume-transform-produce, a metadata group that creates, changes and deletes streams, queues and exchanges at runtime), TLS 1.3 on every connection with node certificates, authentication (client certificates, SCRAM-SHA-512, OAUTHBEARER), tenants and six roles checked on every request exist and are tested, and so does the management console, with OpenID Connect sign-in, against a real cluster (numbers from `DescribeMetrics`, which every node answers for itself) or a built-in demo cluster. Retention deletes whole segments by age and size (streams) or below the ack floor (queues), and streams can be compacted to the newest record per key. The `lotse` binary runs a node from a cluster file, as a systemd unit, a Debian package or a container image, and nodes can be added to and drained from a running cluster.

## Documentation

- [Design document](docs/design.md): architecture and storage design with the reasons behind each decision (German).
- [Architecture decisions](docs/adr/): one record per decision, with its reasons.
- [Architecture overview](docs/architecture.png): the whole system on one page.
- [Client SDKs](sdk/): the protocol for implementers, golden test vectors and the Java, C# and Node.js clients (the protocol codec, record batch, connections, authentication, streams, queues, publishing, transactions, consumer groups and admin), with a Kotlin layer of `suspend` functions and flows on the Java client.
- [Brand](docs/brand/): the Fairway mark, logos, colours, type and design tokens.
- [Console](docs/console/): screenshots and mockups of the management console.

## Contributing

[AGENTS.md](AGENTS.md) holds the rules, commands and conventions for everyone who changes this repository, people and AI coding agents alike; [agents/lotse-engineer.md](agents/lotse-engineer.md) describes the project's dedicated coding agent.

## Crates

| Crate | What it does |
| --- | --- |
| `lotse-rt` | The only seam to the outside world: `Clock` (with the wall clock, for token expiry), `Disk`, `SegmentFile`, `Spawn`, `Rng` (with secrets from the operating system), `Net` (connections carrying whole frames, node or client links, and who the other end proved to be), `Host` (CPU, memory and disk, for metrics) and `Http` (an identity provider's documents) as traits |
| `lotse-auth` | Authentication and authorization free of I/O: SCRAM-SHA-512 for both ends, OAUTHBEARER messages and OpenID Connect token validation against the issuer's keys, and roles (cluster-admin, tenant-admin, producer, consumer, viewer) bound per tenant and name prefix; a fake issuer for tests |
| `lotse-rt-tokio` | The production runtime: Tokio, the file system with directory fsyncs, length-prefixed frames over TLS 1.3 (rustls) with node and client certificates from the cluster's CA, or plain TCP on loopback when asked for; a throwaway CA for tests (`dev-certs`) |
| `lotse-record` | The batch format used on disk, on the wire and in Raft: 65-byte header, ZigZag-varint records, CRC32C, transaction markers, queue acks and delivery counts |
| `lotse-queue` | Queue semantics on the log, free of I/O: the ack index every replica applies (settled offsets as ranges, delivery counts) and the leader's dispatcher (subscriptions, credit, messages in flight, redelivery, ack timeout) |
| `lotse-log` | Segmented partition log (also the Raft log): append, read, roll, truncate, crash recovery |
| `lotse-raft` | Sans-I/O Raft core and Multi-Raft: optional pre-vote, check-quorum with leader stickiness, snapshots, learners, joint consensus, leader transfer, ReadIndex and optional leader leases; entries are batches covering offset ranges; messages batched per node pair |
| `lotse-route` | Exchanges and bindings: direct, topic (`*`, `#`), fanout and headers routing, shared by the client and later protocol adapters |
| `lotse-proto` | The native protocol: frames, field conventions, error codes; `ApiVersions`, `Metadata`, `FindCoordinator`, `Produce`, `Fetch`, `InitProducer`, `AddPartitionsToTxn`, `EndTxn`, the internal `WriteTxnMarkers`, for queues `Subscribe`, `Deliver` (a push), `Credit`, `Ack`, `Nack` and `Unsubscribe`, for consumer groups `GroupHeartbeat`, `CommitOffsets`, `OffsetFetch` and `AddOffsetsToTxn`, for admins `CreateStream`, `CreateQueue`, `DeclareExchange`, `Bind`, `Unbind`, `DeleteResource`, `DescribeConfigs`, `AlterConfigs`, `DescribeMetrics`, `AlterUser`, `DescribeUsers`, `CreateTenant`, `DeleteTenant`, `DescribeTenants`, `AlterRoleBindings` and `DescribeRoleBindings`, `SaslHandshake` and `SaslAuthenticate` to log in, and `WhoAmI`; refusals carry a reason |
| `lotse-server` | Broker node. `Replica`: one Raft group persisted on its own log (atomic hard state and snapshot files, fsync before any acknowledgement, recovery on open). `Node`: many replicas behind one transport, all messages for a peer bundled into one frame per round (`wire`), and clients served over the native protocol: produce answered once committed and deduplicated per producer, fetch cut at the high watermark (read_committed: the last stable offset, with aborted ranges), streams, queues, exchanges and bindings created, changed and deleted at runtime through a metadata group, each new group placed on as many nodes as asked and each deleted one closed and removed from every replica, producer ids, transactions and consumer groups from a coordinator group that writes markers through the protocol, and queues: the leader pushes unsettled messages within each subscription's credit, acks are batches in the queue's log, and whatever is not acknowledged goes out again after a nack, a disconnect, a leader change or the ack timeout; every node counts its traffic, produces, fetches, deliveries, acks and commit latencies and answers `DescribeMetrics` for itself |
| `lotse-client` | Rust client on `lotse-rt`: logs in with SCRAM or a bearer token (or a client certificate) and renews its sessions; metadata, produce and fetch, following leader changes; idempotent by default, so it retries unknown outcomes without duplicates; transactions across partitions; read_committed filtering; consumer groups that follow cooperative rebalances and commit offsets plainly or within a transaction; admin calls to create, describe, change and delete streams, queues and exchanges, to bind queues and to set and remove users; queue consumers that refill credit and subscribe again on their own; publishing through exchanges, routed in the client, per queue or atomically in a transaction |
| `lotse-sim` | Deterministic simulation: seeded executor with virtual time, a disk that loses unsynced data and tears writes on crash, a network with latency, partitions and broken connections |
| `lotse-node` | The `lotse` binary: reads the cluster file, makes the CA and the node and client certificates, runs one node until Ctrl-C or SIGTERM, adds, drains and lists nodes |
| `lotse-console` | Management console for a dedicated node: serves the web app, REST/JSON (OpenAPI) and one WebSocket for live updates, against real clusters (through `lotse-client`) or a demo cluster |

## Running a cluster

`lotse` runs one node from a cluster file that is the same on every node (ADR 0047). Streams, queues and exchanges are not in the file; clients create them.

```toml
# cluster.toml
[cluster]
data_dir = "/var/lib/lotse"

[[node]]
id = 1
peer = "10.0.0.1:7000"     # where the other nodes reach it
client = "10.0.0.1:9000"   # where clients reach it
[[node]]
id = 2
peer = "10.0.0.2:7000"
client = "10.0.0.2:9000"
[[node]]
id = 3
peer = "10.0.0.3:7000"
client = "10.0.0.3:9000"

[tls]
ca = "/etc/lotse/ca.pem"
cert = "/etc/lotse/node-{id}.pem"
key = "/etc/lotse/node-{id}.key"
crl = "/etc/lotse/crl.pem"  # revoked certificates, written by `lotse certs --revoke`

[retention]
ms = 604800000             # 7 days, the default; -1 keeps everything

[auth]
admins = ["alice"]         # signs in with the certificate below
```

```sh
cargo build --release -p lotse-node                               # target/release/lotse
lotse check --config cluster.toml                                 # what the file describes
lotse certs --config cluster.toml                                 # a CA, a certificate per node (one year) and an empty revocation list
lotse certs --config cluster.toml --client alice --out ./alice    # the first admin's client certificate
lotse run --config cluster.toml --node 1                          # on each machine, its own id
```

`lotse certs` keeps what exists, so it can run again after a node is added to the file; keep `ca.key` off the nodes. Certificates last a year (`--days` for another length), the CA ten; `lotse check` warns 30 days before one ends. A running node reads its certificate files every 10 s and takes up a renewed certificate or a new revocation list without a restart (ADR 0060):

```sh
rm /etc/lotse/node-2.pem /etc/lotse/node-2.key && lotse certs --config cluster.toml   # renew node 2's certificate
lotse certs --config cluster.toml --revoke alice/alice.pem         # then copy crl.pem to every node
```

To add a node, give it a `[[node]]` entry, make its certificate, announce it and start it with `--join`; to remove one, drain it. Its replicas move to other nodes, then it leaves the list and stops by itself (ADR 0048, ADR 0051). A new node fills up by itself: replicas and leadership move onto it until the nodes are even (ADR 0050).

```sh
lotse certs --config cluster.toml
lotse node add --config cluster.toml --node 4 --cert alice/alice.pem --key alice/alice.key
lotse run --config cluster.toml --node 4 --join                         # on the new machine
lotse node remove --config cluster.toml --node 2 --cert alice/alice.pem --key alice/alice.key
lotse node list --config cluster.toml --cert alice/alice.pem --key alice/alice.key
``` Larger setups put certificates from their own CA at the same paths. Clients authenticate with a certificate, SCRAM-SHA-512 (users set at runtime) or OAUTHBEARER (`[auth.oidc]`); `auth.required = false` with `[tls] plaintext_on_loopback = true` runs a cluster on one machine without either. `LOTSE_LOG` sets the log filter; the node stops cleanly on Ctrl-C or SIGTERM.

### Packages, images and systemd

Each release (ADR 0049) has static binaries for x86_64 and aarch64, Debian packages and the image `ghcr.io/pierrejochem/lotse`. A node takes its id from `--node` or `LOTSE_NODE`, its cluster file from `--config` or `LOTSE_CONFIG`, and joins with `--join` or `LOTSE_JOIN=1`.

```sh
# Debian or Ubuntu: the binary, lotse@.service, the lotse user
sudo apt install ./lotse_<version>_amd64.deb
sudo install -d /etc/lotse
sudo cp /usr/share/doc/lotse/cluster.toml.example /etc/lotse/cluster.toml   # then edit it
sudo lotse certs --config /etc/lotse/cluster.toml && sudo chown -R lotse: /etc/lotse
sudo systemctl enable --now lotse@1

# A container: the cluster file and certificates read-only, the data in a volume
docker run -e LOTSE_NODE=1 -v /etc/lotse:/etc/lotse:ro -v lotse-1:/var/lib/lotse \
  -p 7000:7000 -p 9000:9000 ghcr.io/pierrejochem/lotse
```

`packaging/` holds the Dockerfile, the unit and the package scripts; `docker build -f packaging/Dockerfile .` and `cargo deb -p lotse-node` build them from the repository. In the image and under the unit the data directory is `/var/lib/lotse`, so the cluster file's `data_dir` should be that path or one below it.

## Management console

`lotse-console` is one binary. It embeds the React app from `console/web` and talks to the clusters on behalf of the signed-in user. A cluster with `kind = "native"` is a real one, reached through `lotse-client` at its `bootstrap` addresses; the console asks every node for its metrics and turns counters into rates (ADR 0036). A cluster with `kind = "demo"` serves sample data that moves over time.

```sh
cd console/web && npm ci && npm run build && cd ../..      # build the web app first
cargo run -p lotse-console --example local_cluster          # three real nodes on localhost, with the console
cargo run -p lotse-console -- --config console/lotse-console.example.toml   # or: the console alone, as configured
# open http://127.0.0.1:8080
```

Users sign in with OpenID Connect at the issuer the brokers trust (`[auth] mode = "oidc"`), or, on a machine only you can reach, in `dev` mode as one configured user with an optional SCRAM password. The console passes the user's access token (or password) to the cluster, one client per principal, so every screen shows what that user's roles allow (ADR 0040). Sessions are sealed into the cookie with a key the console makes once in its data directory, so a restart keeps users signed in (ADR 0042); the metrics sampler signs in with client credentials; charts sum only the streams and queues the user may see (ADR 0041). What a real cluster does not report yet (zones, disk limits, quotas, an operator) shows as `—` or is left out.

For frontend work, run `npm run dev` in `console/web` next to a running `lotse-console`; Vite proxies `/api` and the WebSocket to port 8080. After changing an API type in Rust, run `npm run api` to regenerate `openapi.json` and the TypeScript client; CI fails if they drift.

Frontend stack: React 19, TypeScript, Radix primitives, React Router, TanStack Query with `openapi-fetch`. Styles use the Lotse tokens, light and dark.

Everything above `lotse-rt` is written against its traits, never against Tokio or `std::time` directly. `clippy.toml` enforces that, and also bans `HashMap`/`HashSet`, whose iteration order would make simulation runs unrepeatable.

## Testing

```sh
cargo test --workspace
```

The crash test in `crates/lotse-sim/tests/log_crash.rs` appends, syncs, truncates and pulls the plug across hundreds of seeded runs, then checks that every synced batch survives and nothing is invented. To run more seeds, or reproduce a failure:

```sh
LOTSE_SIM_SEEDS=5000 cargo test -p lotse-sim --release   # many seeds
LOTSE_SIM_SEED=1234 cargo test -p lotse-sim               # one seed, exactly
```

A failing run prints the seed that reproduces it.

The Raft simulation in `crates/lotse-sim/tests/raft.rs` runs 3- and 5-node groups through lost, delayed, duplicated and reordered messages, partitions, clock drift and crashes (before and after persisting, some with an immediate restart), with fault intensity, pre-vote and leader leases drawn per seed. Meanwhile nodes compact their logs at random, followers catch up through snapshots, the leader is asked to change members at random (add, promote and remove learners, remove voters, itself included, replace several voters or drop two at once in one joint change) and to hand leadership over, and clients send linearizable reads to random nodes. It checks vote safety, election safety, state machine safety, durability of committed entries, leader completeness and that every confirmed read is served at an index no older than anything committed before it was asked, on every step, and liveness after healing.

The replica simulation in `crates/lotse-server/tests/cluster.rs` runs 3- and 5-node groups of real `Replica`s, each on its own simulated disk, through the same message faults and through power loss: between rounds and in the middle of one (the disk stops after a random number of writes), keeping only synced data plus a possibly torn prefix of the rest. Nodes restart from whatever their disk holds and snapshot at random. It checks election safety and that every node applies the same batches in the same order across restarts, and liveness after healing. Together with the replica's unit tests it catches all eight seeded durability bugs: no fsync of the log or of the temporary file behind an atomic write, the hard state written before the log, keeping a log that does not continue the snapshot, not restarting the log on a snapshot install, not replaying committed entries after a restart, no term clamp on restart, and keeping a torn tail in log recovery.

Together with the unit tests in `lotse-raft` the Raft simulation was mutation-checked: each of these 18 bugs makes a test fail. Committing older-term entries by counting (Raft's "figure 8"), voting for out-of-date candidates, forgetting a vote on restart, keeping conflicting follower entries, committing past what a follower verified, committing with one ack too few, counting only the new voters of a joint configuration (for commits or for elections), changing voters without a joint configuration, installing a snapshot over a matching log, ignoring votes from nodes outside the membership, never letting a just-dropped voter campaign, serving a read without confirming leadership, before the new leader's first commit or on a round sent before the read, running a lease from an unanswered round, keeping the lease after trying a transfer, and skipping the quiet period after a restart. Six more concern chunked snapshots (ADR 0053), also caught: taking a chunk out of order, installing before the last chunk, not starting over on another snapshot, a leader that ignores where the follower stands, a leader that abandons a transfer when it takes a newer snapshot, and a driver that keeps an offset when it replaces the snapshot; the simulation cuts snapshots into chunks of 32 to 1,500 bytes and counts that transfers span several chunks, resume, restart and are interrupted by crashes.

The node simulation in `crates/lotse-server/tests/nodes.rs` runs three whole `Node`s, each with its own disk and host name, hosting three Raft groups over the simulated network: the real main loop, senders, readers and wire format. Clients propose through node handles while pairs of hosts are partitioned and nodes lose power, between rounds or in the middle of a write, and restart from their disk. It checks that in every group all nodes apply the same batches in the same order and there is at most one leader per term, and that after healing a new batch commits in every group and reaches every node. Together with the codec's unit tests it catches all nine seeded transport bugs: dropping incoming messages, stepping them into the wrong group, queueing them for the wrong peer, a sender that stops after a failed connect or after a broken connection, a codec that swaps fields, accepts trailing bytes or trusts any message count, and passing on a client batch stamped with a term other than its entry's. `crates/lotse-server/tests/tokio_cluster.rs` runs three nodes for real on localhost TCP: a leader is stopped, the group goes on under a new one, and the old node catches up after its restart.

The client simulation in `crates/lotse-server/tests/clients.rs` runs three or five nodes hosting a stream with two partitions and a coordinator group. Two plain producers (one idempotent, one not), a transactional producer and two read_committed consumers use `lotse-client` over the simulated network while pairs of hosts, clients included, are partitioned and nodes lose power. It checks that every acknowledged batch is in its partition at the acknowledged offset, that everything a consumer read is committed at that offset, that no batch reported as not written is there, that no batch is there twice although producers retry unknown outcomes, that every committed transaction is visible in full, no aborted one at all and one with an unknown end either way, that the last stable offset catches up after healing, and that a new batch commits after healing. Together with a deterministic five-node unit test it catches all eight seeded protocol bugs: acknowledging a batch once appended or without checking its term at commit, settling without waiting, reporting "not written" as soon as the entry is replaced on the proposing node (only five voters allow the history that disproves it), returning uncommitted batches, treating offsets between the high watermark and the log end as out of range, and a client that retries a produce that may have arrived or that timed out (checked without idempotence). Eleven seeded idempotence bugs are caught as well, by the simulation and by unit tests of the producer state, the replica and two deterministic five-node histories: a check that ignores the log tail or recognises no duplicates, a tail that is not cut when Raft truncates or not rebuilt on restart, producer state left out of snapshots, no fencing of older epochs, accepted gaps, evicting the newest producer instead of the oldest, answering a retry of a batch in flight before the original settles, and a client that reuses a sequence or keeps its producer id after an unknown outcome. Fifteen seeded transaction bugs are caught by the simulation and by unit tests: a last stable offset that ignores open transactions or runs ahead of the applied index, no aborted ranges in fetches, markers that close a transaction of the same epoch or come from an older coordinator, ending or timing out a transaction without raising the epoch, a coordinator acting before applying its own term or taking two changes to one transaction at once, completing before the markers are written, read_committed reading to the high watermark or keeping aborted batches, and a client that commits after an unknown write or continues an abandoned transaction. The Tokio test also produces and fetches through `lotse-client`, across the leader's restart.

The queue simulation in `crates/lotse-server/tests/queues.rs` runs three or five nodes hosting two queues, a coordinator group, a topic and a direct exchange. A producer and a transactional producer write to the queues directly and through the exchanges while two consumers subscribe with small credit windows, acknowledge most messages, requeue and discard some, hold some past the ack timeout and drop their subscriptions, under the same partitions and power losses. After healing a last consumer drains the queue. It checks that nothing is lost (every message whose produce was acknowledged was settled or is drained), that no message whose ack or discard was confirmed comes back, that every message carries what was produced at its offset, and that no message of an aborted transaction is ever delivered while every message of a committed one is. Together with unit tests of the ack index, the dispatcher, the replica and a deterministic five-node failover it catches all eleven seeded queue bugs: a new leader delivering before it has applied its own term, delivering up to the log end or ignoring the last stable offset, accepting acks beyond the log end, acks that settle nothing, aborted or pending transactional messages treated as deliverable, produced batches of other kinds than data, the ack index left out of snapshots, and a dispatcher that keeps settled messages for redelivery or in flight. It also checks that a published message lands in exactly the queues its key selects, that a mandatory message matching nothing is refused and that a queue reported as not written does not hold it; with unit tests of `lotse-route` and a scripted broker it catches all six seeded routing bugs (topic `#` that needs a word, `*` that matches anything, headers that ignore `any`, a publish that hides failed queues, `mandatory` ignored, and a transaction committed after a partial publish). A third run moves messages from one queue to another, the write and the ack of the original in one transaction that transformers commit, abort or abandon, some after holding the message past the ack timeout, under the same faults; it checks that every acknowledged message arrives exactly once and nothing else does (ADR 0075). A fourth gives a queue a single active consumer and three consumers that ack, hold a message past the ack timeout, unsubscribe and disconnect under the same faults; it checks that each subscription receives offsets in increasing order, which a handover while another consumer still holds older messages would break, and that nothing is lost (ADR 0076). A fifth publishes messages with delays and priority levels to a queue with three levels and gives some back with a delay; it checks that none arrives before its time and that nothing is lost (ADR 0077, 0078).

The group simulation in `crates/lotse-server/tests/groups.rs` runs three or five nodes hosting an input stream with three partitions, an output stream and the coordinator group. Three processors in one consumer group read the input and write every message to the output, committing their read offsets in the same transaction, while they crash, leave zombies behind, stall past their session holding read messages or an open transaction, and commit carelessly after a refusal, under partitions and power losses. It checks that every acknowledged input reaches the output exactly once. Together with unit tests of the assignor (no partition ever has two holders, members converge) and two deterministic coordinator tests, it catches all eight seeded group bugs: commits accepted from an unknown member or for a partition the member no longer holds, `require_stable` ignored, transactional offsets visible before the commit decision, a partition handed to a member while another still holds it, a revocation timeout shorter than a session, a client that commits after its offsets were refused, and members never expiring.

The metadata simulation in `crates/lotse-server/tests/metadata.rs` runs four or five nodes that start with only a metadata group, a coordinator group and one configured stream. Two admin clients race to create the same streams, queues and an exchange, bind queues to it, change queues' ack timeouts, and delete and create again a stream, a queue and the exchange, while a producer writes to the streams, a publisher publishes through the exchange and a transactional producer writes to a stream that is deleted under it, under partitions and power losses. It checks that every node reports the same resources with one id per name, that acknowledged creates of names never deleted exist with their partitions and as many voters as asked, that no older resource outlives a newer one of the same name, that a deleted resource runs nowhere, has no files left on any disk and is unknown to every node, that no binding names a deleted queue, that each node runs exactly the groups it is a voter of, that configured names can be neither created again nor deleted, that ack timeouts are the default or a value an admin set, that acknowledged produces to streams never deleted are where they were acknowledged, that the producer can still write to a stream created again under it, that published messages reach only bound queues, and that a transaction whose partition was deleted still ends. With the unit tests it catches the seeded metadata bugs (creating: six of seven, the seventh equivalent; deleting and settings: all 13). It found a real bug in the codec: `Metadata` refused bindings with an empty key.

The authentication tests in `crates/lotse-server/tests/auth.rs` run three nodes that require it, with a fake identity provider served over simulated HTTP: every way in and out (certificates, SCRAM users from the configuration and from the metadata group, bearer tokens with rotated keys), sessions that end and are renewed, and nodes authenticating to each other by their certificates; `tokio_auth.rs` does the same on real sockets over TLS. `access.rs` checks each role, prefix and tenant request by request. The access simulation in `access_sim.rs` keeps changing role bindings and passwords while four users create, write, read and list on a faulty cluster, and checks that nothing succeeded that no binding allowed and that after healing each user can do exactly what the bindings allow; its idea of the roles is written in the test, independent of `lotse-auth`. The release workflow runs it in a job of its own at 5,000 seeds.

## Known gaps

Three kinds, most urgent first: weaknesses that can bite today, things not built yet, and limits that were decided (the ADR says why).

### Weaknesses

- **Leadership is not settled when a group is new.** The node that wins a new group's first election hands leadership to the group's preferred node (ADR 0050), usually within 400 ms and, on a node short of CPU, sometimes seconds later. Clients meet `NOT_LEADER`: a transactional ack goes only on the connection the message came on (ADR 0075), so after a move the transaction is aborted and the message comes again. A deposed leader goes on reporting its queues in `DescribeMetrics` until it hears of the new term: read the leader `Metadata` names.
- **Queue state that only the leader holds is lost in a failover.** Delivery counts are written only on requeue and ack timeout, so after a failover they may start over, and `max.deliveries` counts again. A failover may expire a message a consumer still holds (ADR 0055). A deposed leader may still feed a single active consumer until check-quorum (ADR 0076), and a new leader orders priority levels by offset until it has read the backlog, 1 MiB a round (ADR 0078).
- **The deep simulations did not cover the newest certificate code.** The 5,000-seed access, access-simulation and authentication runs covered revocation (ADR 0090). The inventory (ADR 0091) and certificate requests (ADR 0092) have unit tests, short simulations and mutation checks; the deep runs happen at the next release (ADR 0088).
- **Certificate records are the CA holder's word.** `RecordCertificate` does not parse the certificate, so only the handshake checks a signature (ADR 0092). The inventory tells issuers apart by a hash of their name, so two CAs with one name count as one (ADR 0090).
- **Memory grows with data in three places.** Compaction holds every key of a partition in memory and rebuilds the map from the log after a restart, with no dirty ratio, minimum lag or bound, and a tombstone dropped while a replica that left the group still holds an older value is not covered by a test (ADR 0067). A snapshot is collected in memory before it is installed. Tombstones of deleted resources are kept for ever.
- **A producer evicted from a full partition loses its deduplication**, and a plain idempotent producer's id survives a client restart only with a producer name, which fences the earlier instance but does not deduplicate a batch the application sends again (ADR 0071).
- **Tests that start real nodes depend on timing.** The SDK tests against three real nodes assume a quiet cluster, and one can fail on a slow runner when a leader moves while it runs.

### Not built yet

- **Raft:** pipelined snapshot chunks and chunks spilled to disk; probing instead of resending a whole term's entries after a conflict; followers serving reads themselves (the server forwards them to the leader).
- **Certificates and sessions:** a user asking for a certificate in their own name (cluster admins ask, ADR 0092); anything that tells the CA holder a request is waiting; OCSP; revoking a node certificate other than through the CRL file; revoking a session per token (they go per user or IdP group, ADR 0070).
- **Metadata:** replication and queue length limits cannot change after creation; a stream's partitions only go up (ADR 0074); streams from the node configuration take the node's defaults and cannot be changed at runtime, and only runtime streams can be compacted.
- **`DescribeMetrics`:** which subscription of a single active consumer queue is active, and counts per priority level.
- **Console:** open sessions cannot be listed, a pinned partition is not shown, and the oldest record's age, open transactions per stream, bytes in flight per consumer, zones and disk limits are not reported. On a phone the navigation sits above the page instead of collapsing into a menu.
- **Packages:** releases and the image are built only by GitHub Actions; the package targets systemd systems.

### Limits by design

- **Retention** keeps whole segments, so a partition holds up to a segment more than its limits; age goes by the timestamps producers write (ADR 0046).
- **Storage and reads** (ADR 0094): one `.index` file per sealed segment serves offsets and times; there is no `.timeindex` or `.txnindex`. A node trusts a matching index file on start, so damage inside a sealed segment shows when the batch is read, not at start-up. `ListOffsets` by time answers with a batch, not a record. `Fetch` has no `sendfile`: TLS encrypts in the process, and the response frame copies the data once.
- **Compression** is per batch, zstd and lz4, and a decompressed block may hold at most 16 MiB (ADR 0058). The Java SDK needs zstd-jni and lz4-java on its class path (ADR 0061), the C# SDK the `Lotse.Client.Codecs` package (ADR 0084), and the Node.js SDK brings lz4 itself and uses `node:zlib` for zstd, which needs Node.js 22.15 or later (ADR 0085). `lotse-client` compresses only what the caller builds compressed.
- **Transactions and groups:** one coordinator group serves all transactional ids and all consumer groups, and every heartbeat that changes a group writes the whole group. A transactional queue ack names at most 10,000 messages. Settled queue offsets stay in the ack index as ranges, even below the log start; aborted ranges are dropped once their marker is below it (ADR 0094). Ids live in a tenant (ADR 0062); a tenant deleted and created again is a new incarnation whose ids start afresh (ADR 0069).
- **Publishing** outside a transaction routes by the bindings the node has applied and writes each queue in turn: not atomic across queues, and a publish whose answer is lost is reported as unknown, not sent again (ADR 0079). Inside one the client routes, with bindings it may have cached for up to `metadata_max_age` (ADR 0032). One message per publish.
- **Queues:** dead-letter queues and message TTL are per-queue settings, and a retired message is dropped when no dead-letter queue is set (ADR 0055). Delayed delivery counts whole milliseconds of the leader's clock, and a waiting message holds the ack floor (ADR 0077).
- **Changes reach brokers one by one:** right after a change some brokers do not know it yet; a removed role binding takes effect on each node once it applies the metadata group; a conditional delete (`if_empty`) checks first and deletes after, so a message produced in between goes with the resource (ADR 0063).
- **Certificates:** the CRL file is copied to every node by the operator (ADR 0060), and revoking a client certificate by issuer and serial (ADR 0090) does not cover node certificates. The inventory lists only the client certificates nodes accepted, its "last seen" is up to an hour late, and it does not show certificates revoked only in the CRL file (ADR 0091). A request lapses after 7 days, and at most 256 are held (ADR 0092).
- **Nodes:** four groups move at a time, each as a full copy through Raft; rebalancing moves by replica count and uses size only to choose which group goes, so nodes with equal counts and very different sizes stay as they are, and traffic is not weighed (ADR 0052). A drained node exits by itself and must join under a new id to come back (ADR 0051); one that cannot reach any node it lists keeps running until it can.
- **Console:** chart history (about 10 KiB per stream or queue for 24 h) covers only what the sampler may see (ADR 0045).
- **Native protocol:** keys 0 to 57 except 18, which is free because queue acks join a transaction through `Ack` (ADR 0075), plus the internal `WriteTxnMarkers`.

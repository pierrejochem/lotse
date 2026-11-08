# A first message

This chapter writes ten records to a stream and reads them back, then looks at them in the console. It assumes the [one-machine trial](one-machine-trial.md) is running. Against a TLS cluster you set the CA and a client certificate instead; see below.

## With the Java client

The Java 17 client is in `sdk/java` of the repository. The example `StreamExample` creates the stream `example-orders` with 2 partitions and 3 replicas, writes ten records and reads them back. Its source is compiled by `./gradlew check`, so it keeps up with the API:

```java
{{#include ../../../../sdk/java/src/examples/java/dev/lotse/examples/StreamExample.java}}
```

The examples read their connection settings from the environment (`sdk/java/src/examples/java/dev/lotse/examples/Env.java`):

| Variable | Meaning |
| --- | --- |
| `LOTSE_BOOTSTRAP` | brokers as `host:port`, separated by commas. The default is `127.0.0.1:9092`, so set it. |
| `LOTSE_CA` | the CA certificate, to use TLS. Without it the example uses plain TCP, which the client allows on loopback only. |
| `LOTSE_CERT`, `LOTSE_KEY` | a client certificate and its key, with `LOTSE_CA`. |
| `LOTSE_USER`, `LOTSE_PASSWORD` | a SCRAM-SHA-512 login, if the cluster asks for one. |

For the trial:

```sh
export LOTSE_BOOTSTRAP=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
```

For the TLS cluster of [A first cluster](first-cluster.md):

```sh
export LOTSE_BOOTSTRAP=10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000
export LOTSE_CA=/etc/lotse/ca.pem LOTSE_CERT=admin/admin.pem LOTSE_KEY=admin/admin.key
```

The expected output has ten lines of the form `order-3 at partition 1, offset 0`, then ten lines of the form `read order-3 from partition 1`. The offsets start at 0 on a new stream and the order across partitions is not defined; within a partition it is the order written.

This guide has not run the example end to end. The repository's Gradle build has no task that starts an example; it only compiles them. Put the compiled classes of the `examples` source set and the main classes on the class path and run `dev.lotse.examples.StreamExample`. The [developer guide](../developer/overview.md) covers the clients in full.

## With the Rust client

`lotse-client` is the Rust client over the native protocol. `Client::new(rt, ClientConfig::new(bootstrap))` connects, `create_stream(name, partitions, replication)` creates a stream, `produce(stream, partition, batch)` writes a record batch and returns its offset, and `fetch` reads from an offset. The client is generic over the runtime in `lotse-rt`; on a real machine that is `TokioRuntime` from `lotse-rt-tokio` with `.plaintext_on_loopback()` for the trial or `.with_tls(...)` for a TLS cluster. The working code to start from is `crates/lotse-console/examples/local_cluster.rs`, which produces to a stream and a queue in a loop.

## Check it in the console

The console shows what the cluster holds. For the trial cluster, write a console file `lotse-console.toml`:

```toml
listen = "127.0.0.1:8080"

[auth]
mode = "dev"
user = "trial"

[[clusters]]
id = "trial"
kind = "native"
bootstrap = ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"]
```

Build the web app once, then start the console:

```sh
cd console/web && npm ci && npm run build && cd ../..
cargo run -p lotse-console -- --config lotse-console.toml
```

Open <http://127.0.0.1:8080>. The stream `example-orders` is listed with its partitions, replicas and offsets, and the charts show produce and fetch rates. This guide has not run this exact console file; the keys follow `console/lotse-console.example.toml` and the comments in `crates/lotse-console/src/config.rs`, where a `native` cluster's `ca_file` is optional and plain TCP is allowed on loopback only.

Against a TLS cluster, add `ca_file = "/etc/lotse/ca.pem"` to the cluster entry. The console passes the signed-in user's identity on to the cluster, so each user sees only what their roles allow ([ADR 0040](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0040-console-sign-in.md)). In the `dev` mode used here the console acts as the one configured user; use it only on a machine only you can reach.

With `make dev` you can skip all this: the console is already running at <http://127.0.0.1:8080> with the cluster `local`, and your Java client can connect to the client addresses in the log line `three nodes running`, because that cluster picks free ports at random.

# A one-machine trial

For a first look you do not need three machines, certificates or logins. Two settings switch them off, and only on loopback.

## The cluster file

Three nodes on 127.0.0.1, each with its own ports and data directory:

```toml
[cluster]
data_dir = "/tmp/lotse-trial/node-{id}"

[[node]]
id = 1
peer = "127.0.0.1:7001"
client = "127.0.0.1:9001"

[[node]]
id = 2
peer = "127.0.0.1:7002"
client = "127.0.0.1:9002"

[[node]]
id = 3
peer = "127.0.0.1:7003"
client = "127.0.0.1:9003"

[tls]
plaintext_on_loopback = true

[auth]
required = false
```

Save it as `trial.toml`. `plaintext_on_loopback = true` runs the node links and the client port as plain TCP. `auth.required = false` lets every client do everything. Neither is a setting for production, and the node enforces the limits:

- `[tls]` may not name certificates together with `plaintext_on_loopback`.
- Every address in the file must be on loopback.
- `auth.required` with `plaintext_on_loopback` is refused, because nodes prove who they are by their certificates.

```sh
lotse check --config trial.toml
```

```text
3 nodes: 1, 2, 3
TLS: none, loopback only
retention: 604800000 ms by age, no limit per partition
clients: no authentication, everyone may do everything
```

## Run the nodes

Start each node in its own terminal:

```sh
lotse run --config trial.toml --node 1
lotse run --config trial.toml --node 2
lotse run --config trial.toml --node 3
```

Each node logs a warning that it listens with plain TCP. That is expected here. Clients connect to `127.0.0.1:9001`, `127.0.0.1:9002` and `127.0.0.1:9003`. Stop a node with Ctrl-C and start it again to see it rejoin from its data directory.

## Or: `make dev`

From a checkout of the repository, `make dev` starts three real nodes and the console with sample traffic:

```sh
make dev                    # web app, three nodes, console on http://127.0.0.1:8080
make dev ADDR=127.0.0.1:8081
```

It builds the web app first if `console/web/dist` is missing, which needs Node.js and npm. The nodes keep their data in a fresh temporary directory that is removed when you stop with Ctrl-C. A client writes to a stream `orders` (3 partitions) and a queue `jobs` every 250 ms, so the console has something to show. This cluster also runs without TLS and authentication, and you sign in to the console as the user `local`.

## When you are done

Delete `/tmp/lotse-trial`. The next chapter, [A first message](first-message.md), works against either cluster.

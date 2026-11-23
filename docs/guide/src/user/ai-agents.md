# Using Lotse with AI agents

`lotse mcp` lets an AI agent work with a cluster through the Model Context Protocol (MCP) ([ADR 0134](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0134-lotse-mcp-a-separate-command-for-ai-agents.md)). An operator can ask it how the cluster is doing; a developer can ask it to create a stream, produce a few records and read them back.

The agent acts as the person who signed in. Every tool call is a request of that person, and the cluster checks it with the same roles and tenants as over the native protocol. `lotse mcp` has no rights of its own, so an agent can do nothing its user cannot. What it may not see is unknown to it.

It speaks MCP 2026-07-28, and 2025-11-25 for clients that still open a session. Tools only: no resources or prompts. It talks to the cluster through the native protocol, always over TLS; plain TCP only with `--plaintext-on-loopback`, to a loopback address.

## Choose a credential

| Credential | For | How |
| --- | --- | --- |
| An access token | an agent on your machine, or an HTTP client | `LOTSE_TOKEN`, or `Authorization: Bearer` over HTTP. Make one in the console under *Users and access*, *Tokens*, or with a client or `lotse`, or ask a cluster admin; see [access tokens](../admin/access-tokens.md). It stops by itself, within 7 to 365 days |
| A user and password | the same, where no token exists | `LOTSE_USER` and `LOTSE_PASSWORD` (SCRAM-SHA-512), or `Authorization: Basic` over HTTP |
| A client certificate | stdio only | `--cert` and `--key` |

A token is the better choice for an agent: it can be revoked alone, and it grants only the roles bound to its owner's own name, never those that come from IdP groups. A token cannot make other tokens.

## On your machine: stdio

The agent starts `lotse mcp` and talks to it on standard input and output. One principal for the whole process.

```sh
LOTSE_TOKEN=lotse_tok_... lotse mcp --config cluster.toml
```

`--config` names the cluster file, from which `lotse mcp` takes the node addresses and the CA. Without it, name the nodes and the CA: `--bootstrap node1:9092,node2:9092 --ca ca.pem`.

A desktop agent that reads a list of MCP servers takes an entry like this one (the file and its keys differ by agent; the shape is the usual one):

```json
{
  "mcpServers": {
    "lotse": {
      "command": "lotse",
      "args": ["mcp", "--config", "/etc/lotse/cluster.toml"],
      "env": { "LOTSE_TOKEN": "lotse_tok_00000000000001f4_BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY" }
    }
  }
}
```

Keep the token out of files you share. Revoke it if it leaks.

`lotse mcp` signs in at start, for up to 15 s. A refused credential or an unreachable cluster ends it with exit 1 before any tool is offered; a missing credential or a wrong flag ends it with exit 2 and a message that names what is missing. Logs go to standard error only (`LOTSE_LOG`, `info` by default), so standard output stays clean for MCP.

## For several people: Streamable HTTP

One shared process that agents reach over the network.

```sh
lotse mcp --config cluster.toml --listen 0.0.0.0:8443 \
  --tls-cert mcp.pem --tls-key mcp.key
```

- **Endpoint:** `POST /mcp`.
- **TLS:** with `--tls-cert` and `--tls-key` (PEM) it serves HTTPS. Without them it starts only on a loopback address, over plain HTTP; on any other address it exits 2. The certificate is read at start and not reloaded: restart to change it.
- **A credential on every request:** `Authorization: Bearer lotse_tok_...` or `Authorization: Basic` with the user and password. Without one, or with one the cluster refuses, the answer is 401 and no tool runs. The process holds no credential of its own, and refuses to start with any of `LOTSE_TOKEN`, `LOTSE_USER`, `LOTSE_PASSWORD`, `--cert` or `--key` given.
- **Revocation:** before a request is served, the process asks the cluster that the credential still signs in (one round trip per request), so a revoked or expired token fails on its next request. A changed password is not noticed this way: a cached client keeps working until its session ends. After rotating a password, a cluster admin revokes the user's sessions (`RevokeSessions`, from the console or a client). That also ends every token the user made before it, for good, so new ones must be made; `list_tokens` and the console still list those tokens until they expire.
- **Load:** a signed-in client is kept per credential, by its SHA-256 and never the credential itself, for 15 minutes idle, at most 1,000 at once. At most 256 credentials wait or sign in at once and 32 sign-ins reach the cluster at once. A request that waits longer than 5 s gets 503 with `Retry-After: 1`; so does a request at once when 256 credentials are already waiting or signing in, and a request while the cluster cannot be reached.
- **Browsers:** a request with an `Origin` header is refused with 403 unless `--allow-origin` names it, as `scheme://host[:port]`, repeated for more. There is no CORS preflight support, so in practice a browser cannot use it. `null` is refused at start, and a value with a path or a trailing slash can never match and is warned about. Against DNS rebinding, a plaintext loopback listener checks the `Host` header; under TLS the certificate's name and the `Origin` check protect.

Two processes behind a load balancer need nothing shared, as no session is kept between requests.

## The tools

Each tool takes and returns JSON. A result carries `structuredContent` and a short text. A refusal or a failure comes back as a tool result with `isError` and the broker's message, for example `UNAUTHORIZED: alice may not produce to orders`, so the agent can read it. Where several nodes answer, a part that the principal may not see is shown as not shown, and a part that failed as an `*_error` field (`configs_error`, `cluster_version_error`), so a gap is not a zero.

**Look**

| Tool | What it gives |
| --- | --- |
| `cluster_overview` | Each node with its version, uptime, connections and groups, partitions without a leader, nodes low on disk, the cluster version |
| `list_resources` | Streams, queues and exchanges you may see, with tenant, partitions, leaders, voters and bindings; `kind` narrows it |
| `describe_stream` | Per partition the leader, offsets, size in bytes, replicas behind, open transactions; settings; the groups reading it with their lag |
| `describe_queue` | Messages ready, in flight and held by transactions; consumers and credit; settings; the dead-letter queue |
| `describe_consumer_group` | Members and partitions; per partition the committed offset, last stable offset, high watermark and lag |
| `node_metrics` | Each node's traffic, connections, CPU, memory, disk and commit latency; `node_id` narrows it |
| `list_sessions` | Open sessions with principal, mechanism, peer and the `token_id` of a token sign-in (16 hex digits, as `list_tokens` gives it): all for a cluster admin or viewer, your own for anyone else |
| `describe_upgrade` | The cluster version, the restart turn and each node's release; needs cluster-admin or cluster-viewer |

`lag` is to the last stable offset, which is what a `read_committed` group can read; `lag_against` says so, and `lag_to_high_watermark` sits beside it. Both count offsets.

**Data**

| Tool | What it does |
| --- | --- |
| `read_stream` | Reads committed records of one partition from an offset, a time or the start: at most 100 records or 1 MiB of raw record bytes (as base64 the answer can reach about 1.4 MiB). `next_offset` says where to go on; `limited_by` says whether records or bytes stopped it. A key, value or header that is not UTF-8 comes back in base64, with `encoding` saying so |
| `produce` | Appends up to 100 records to a stream partition or a queue, idempotently; returns the first offset |
| `publish` | Publishes one message to an exchange with a routing key; returns each queue it reached with the offset; `mandatory` refuses a message no queue takes |
| `list_offsets` | A partition's earliest and latest offsets, and with `time_ms` the first offset at or after that time |

No tool takes messages from a queue. A consumer that reads takes them out of flight for others, which an agent should not do by accident; `describe_queue` shows what waits.

**Change**

`create_stream`, `create_queue`, `declare_exchange`, `bind`, `unbind`, `alter_configs` and `create_partitions`. They work on resources created at runtime, as the same requests do over the native protocol. `alter_configs` is marked `destructiveHint` too. A change to `retention.ms`, `retention.bytes`, `delete.retention.ms`, `cleanup.policy`, `max.length`, `max.length.bytes`, `overflow`, `message.ttl.ms` or `max.deliveries` can delete data (the last two drop messages when the queue has no dead-letter queue), so it runs only when `confirm` is the resource's name; without it the call changes nothing, not even the other settings it names. Other settings change without `confirm`.

**Destroy**

`delete_resource`, `purge_queue` and `requeue_dead_letters`. Each needs a `confirm` argument equal to the resource's name, so an agent names what it destroys; nothing happens otherwise. They are marked `destructiveHint`, which a client may use to ask you first. `purge_queue` settles messages that are ready now; those held by consumers stay.

**Tokens**

`list_tokens` lists your own access tokens, or with `owner` another user's, which only a cluster admin or viewer may. There is no tool to make or revoke a token: that stays in the console (*Users and access*, *Tokens*) and the clients.

## Limits

- No OAuth authorization flow: an agent signs in with a token or a password.
- No tool takes queue messages or manages tokens.
- The tool list grows with the features. Every new feature ships its tools in the same change, so what is listed here is what exists.

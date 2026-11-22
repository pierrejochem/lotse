# `lotse mcp` and access tokens: design

- Date: 2026-11-22
- Status: for the maintainer's approval
- Becomes: two ADRs (0133 access tokens, 0134 `lotse mcp`), the design document (German), the guides, AGENTS.md and status.md, in the changes that build it

## Intent

AI agents should be able to work with a Lotse cluster through the Model Context Protocol (MCP). There are two kinds of users, and both matter from the start:

- **Operators** look at a cluster and act on it: health, lag, metrics, nodes and upgrades; create, change, purge and delete resources.
- **Developers** work on their applications against it: create streams and queues, produce, read records, look at what a consumer group or a queue holds.

An agent signs in as a person, with a username and password or with an **access token** whose lifetime is chosen from fixed steps. Users manage their tokens in the console. MCP becomes a standing part of the product: whenever a new feature is planned, extending the MCP tools for it is part of the plan (an AGENTS.md rule).

## Goals

1. A command `lotse mcp` that speaks MCP over **stdio** (the agent starts it on the user's machine) and over **Streamable HTTP** (one shared process that several people and agents reach over the network).
2. Every tool call runs as the principal who signed in and is checked by the cluster exactly like the same request over the native protocol. `lotse mcp` holds no rights of its own (in the spirit of rule 8).
3. The connection to the cluster is always TLS (the native protocol as today). The HTTP listener is TLS too, and plain HTTP is allowed only on loopback (rule 7).
4. Sign-in with a username and password (SCRAM-SHA-512, as today) or with an access token.
5. Access tokens as a new credential for the whole cluster, with a lifetime of exactly one of: 1 week, 1 month (30 days), 3 months (90 days), 6 months (180 days), 1 year (365 days). Usable on the native protocol too, so SDKs and scripts can use them.
6. A *Tokens* page in the console: create (name and lifetime, the secret shown once), list and revoke.
7. Tools for what the broker does, in the first release the set below, growing with every feature.
8. The AGENTS.md rule: a new feature's plan includes its MCP tools.

## Non-goals

- MCP's OAuth authorization flow (an authorization server, dynamic client registration). Agents sign in with a token or a password in a header or an environment variable.
- The MCP server inside the node or the console (approaches A and B, below).
- Tokens that carry IdP groups, scopes narrower than the owner's roles, or lifetimes other than the five steps.
- Tools for everything on day one: the first release has the set below.
- MCP resources, prompts, sampling and the tasks extension. Tools only.

## Anti-goals: it would fail, even if it works, if

- an agent could do anything its user could not do over the native protocol, or `lotse mcp` held a credential of its own that grants more;
- a token's secret were stored anywhere in the cluster or the console, or could be shown again after creation;
- a token could live longer than a year, or past its owner's revocation;
- credentials or tokens travelled in plain text off loopback;
- a destructive tool (delete, purge) ran on a vague instruction without naming what it destroys.

## Constraints

- Nothing above `lotse-rt` touches the real world (rule 1); `lotse mcp` is a binary on Tokio, so its tool logic sits on `lotse-client` and can be tested in the simulation, and only the transports touch Tokio.
- New metadata changes, a snapshot section and protocol messages follow the cluster version: written only once the cluster version that introduced them is in force, read either way, tested with the previous version in force (ADR 0127).
- Safety rules are mutation-checked (rule 5): a token's expiry, revocation, the hash check, the owner check, the lifetime steps and the plaintext refusal.
- Brand voice in UI copy and docs: calm, exact, units named.
- Dependencies through `[workspace.dependencies]`, few and well known.
- Next free numbers today: request keys 66 and up, metadata change tags 35 and up, metadata snapshot section 19, session mechanism 4.

## Approaches

Three were considered; the maintainer chose **C**.

- **A. In every node:** a node with `mcp = "host:port"` serves MCP over HTTP. Integrated and highly available, but the broker gains an HTTP and JSON-RPC surface.
- **B. In the console:** least new code, as the console already holds a client per user; but the console is optional and runs on one machine.
- **C. A separate command, `lotse mcp` (chosen):** speaks MCP over stdio or HTTP and talks to the cluster through `lotse-client`. Not inside the broker, but simple, it keeps the broker's surface as it is, stdio suits developers, and the HTTP mode serves a shared deployment. Asked for: stdio **and** HTTP in the first release.

## Decided with the maintainer

- `lotse mcp`, a separate command (C), with stdio and Streamable HTTP.
- For operators and developers alike; tools for what the broker can do, each run as the signed-in user.
- Sign-in with a username and password or an access token, over TLS.
- Token lifetimes: 1 week, 1 month, 3 months, 6 months, at most 1 year.
- Tokens are managed in the console.
- AGENTS.md: when a new feature is planned, the plan includes extending the MCP tools.
- The Java, Kotlin, C# and Node.js SDKs get token sign-in and the token calls in this project.
- A cluster admin may create tokens in another principal's name.

## Design

### Access tokens

**What a token is.** A random secret of 32 bytes, drawn by the client that asks (below). Its text form is `lotse_tok_<id>_<secret>`, where `<id>` is 16 hex digits naming the token and `<secret>` is 43 characters of base64url. The prefix lets secret scanners recognise one.

**Who makes the secret.** The client that asks (`lotse-client`, the console's backend for the signed-in user) draws the secret through its runtime's `fill_secret`, sends only its SHA-256 hash in `CreateToken`, and shows the secret to the user once. The cluster never sees the secret, as it never sees a password the console sets (ADR 0089). *Builder's call.*

**What the cluster keeps** (metadata change `CreateToken`, tag 35; snapshot section 19): token id (assigned by the metadata leader: the change's index), name (at most 64 bytes), owner principal, SHA-256 of the secret, created at, expires at, created by (the principal who asked). `RevokeToken` (tag 36) removes it. Expired tokens are dropped when the next token change applies, as expired renewed certificates are (ADR 0123).

**Lifetimes.** The request names one of five steps (`1` week = 7 days, `2` 30 days, `3` 90 days, `4` 180 days, `5` 365 days); anything else is `INVALID_REQUEST`. Expiry is the metadata leader's clock at proposal plus the step, written into the change, so every replica holds the same time.

**Who may.** Any signed-in user makes tokens for themselves; a cluster admin also for any principal, such as a service account or a CI pipeline (decided with the maintainer); the token records who created it. A user lists and revokes their own; a cluster admin and cluster viewer list all, a cluster admin revokes any. A token cannot be used to make another token (a session signed in with a token gets `UNAUTHORIZED` for `CreateToken`), so a stolen token cannot outlive itself.

**Limits.** At most 50 live tokens per owner and 10,000 in all; past either, `CreateToken` is refused with a message that names the limit.

**Signing in with one.** A new SASL mechanism `LOTSE-TOKEN` (session mechanism 4): its one message is the token's text. The node looks the id up in its applied metadata state and compares the SHA-256 of the secret in constant time.
- Unknown id, wrong secret, expired or revoked: authentication fails, with one message for all, so a guesser learns nothing.
- The session acts as the owner, with the roles bound to the owner's own name; IdP groups do not count (as for certificates).
- The session ends when the token expires, and at once when it is revoked (the node closes the connections that signed in with it, as for a revoked certificate, ADR 0090), or when the owner's sessions are revoked (`RevokeSessions`, ADR 0070, by creation time).
- A token sign-in is refused on a connection without TLS off loopback (the node never offers it there).
- `DescribeSessions` names the mechanism and the token's id (never the secret).

**Requests.** `CreateToken` (key 66: name, owner, lifetime step, secret hash → id, expires at), `DescribeTokens` (key 67: owner filter → id, name, owner, created, expires, created by), `RevokeToken` (key 68: id). All go to the metadata leader. The Rust client gets `create_token` (which draws the secret and returns the token text with its id and expiry), `describe_tokens`, `revoke_token`, and `Credentials::Token`.

**SDKs.** The Java SDK, the Kotlin layer, the C# SDK and the Node.js SDK get the same, rule for rule (decided with the maintainer): signing in with a token (`LOTSE-TOKEN`) and the calls to create, list and revoke tokens, the secret drawn in the SDK. The golden vectors in `sdk/conformance` gain the three messages, `sdk/PROTOCOL.md` the mechanism and the requests.

**Metrics.** Every node reports `lotse_tokens` (live tokens) and `lotse_token_sign_ins_total` (by result) under `/metrics`.

### The console

A *Tokens* tab under *Users & access* (*builder's call*):

```
Tokens                                              [ Create token ]

Name            Owner     Created      Expires       Last 4
ci-pipeline     alice     2026-11-22   2027-02-20    …Qx7f   [ Revoke ]
claude-desktop  alice     2026-11-20   2026-11-27    …a91B   [ Revoke ]
```

- *Create token* asks for a name and a lifetime (a choice of the five steps, *1 month* selected), then shows the token once, with a copy button and the sentence "This token is shown once. Store it now; it cannot be shown again."
- The dialog says, as the certificate request does (ADR 0119), that a token carries no IdP groups and grants only roles bound to the user's own name; a user whose roles all come from IdP groups is told the token would grant nothing.
- A user sees their own; a cluster admin sees everyone's, can filter by owner, and may create one for another user.
- Revoking asks for confirmation and says that connections signed in with the token end at once.
- Every action is the signed-in user's own request (rule 8). The secret is drawn in the console's backend for that request only, returned to the browser once and never stored or logged. (The token's last four characters are shown from the id, not the secret: *builder's call*, the id's last four.)

### `lotse mcp`

**Crate and binary.** A new crate `lotse-mcp` holds the tools, generic over the `lotse-rt` traits, so they run in the simulation against real nodes. The `lotse` binary gains the subcommand `mcp`, which adds the transports through `rmcp` (the official Rust MCP SDK, features for the server, stdio and the Streamable HTTP server).

**Protocol version.** MCP 2026-07-28 (stateless, `server/discover`), and 2025-11-25 for clients that still speak it, as far as `rmcp` serves both. No sessions are kept; nothing a tool needs is held between calls.

**Reaching the cluster.** `--config <cluster file>` (addresses and the CA from it) or `--bootstrap host:port,…` with `--ca <file>`. Always TLS; plain TCP only with `--plaintext-on-loopback` to a loopback address, as the runtime enforces today.

**Stdio mode** (`lotse mcp --config cluster.toml`): one principal for the process, from `LOTSE_TOKEN`, or `LOTSE_USER` and `LOTSE_PASSWORD`, or a client certificate (`--cert`, `--key`). No credential: it refuses to start, naming the variables. Logs go to stderr only.

**HTTP mode** (`lotse mcp --config cluster.toml --listen host:port`):
- One endpoint, `POST /mcp` (Streamable HTTP). TLS with `--tls-cert` and `--tls-key`; without them it refuses to start unless the address is loopback (rule 7). The `Origin` header is checked against `--allow-origin` (none by default, which refuses browser requests), against DNS rebinding.
- Every request carries its own credential: `Authorization: Bearer lotse_tok_…` or `Authorization: Basic` (username and password). None or a failed sign-in: HTTP 401 with `WWW-Authenticate`, and no tool runs.
- `lotse mcp` keeps one signed-in `lotse-client` per credential, keyed by a hash of the credential (never the credential itself), closed after 15 minutes unused and at most 1,000 at once (the least recently used goes first). A revoked or expired token fails at the cluster, and the client is dropped.
- It holds no credential of its own. A request is only ever served by the client of the credential it carried.

**Tools in the first release.** Names, inputs and outputs are JSON with schemas; results carry `structuredContent` and a short text. Each tool is one or a few `lotse-client` calls.

| Group | Tools |
| --- | --- |
| Look | `cluster_overview` (nodes, groups led, cluster version, health from `DescribeMetrics`), `list_resources` (streams, queues, exchanges the principal may see, with partitions and leaders), `describe_stream`, `describe_queue` (ready, in flight, consumers, dead letters), `describe_consumer_group` (members, lag per partition), `node_metrics`, `list_sessions`, `describe_upgrade` |
| Data | `read_stream` (from an offset or a time, at most 100 records or 1 MiB, read_committed), `produce` (records to a stream partition or a queue, idempotent), `publish` (to an exchange with a routing key), `list_offsets` |
| Change | `create_stream`, `create_queue`, `declare_exchange`, `bind`, `unbind`, `alter_configs`, `create_partitions` |
| Destroy | `delete_resource`, `purge_queue`, `requeue_dead_letters` — each needs a `confirm` argument equal to the resource's name, and is marked `destructiveHint` |
| Tokens | `list_tokens` (the principal's own; a cluster admin may filter by owner). Creating and revoking tokens stays in the console and the clients |

Taking messages from a queue is left out of the first release: a consumer that reads takes them out of flight for others, which an agent should not do by accident; `describe_queue` shows what waits. *Builder's call.*

Errors from the cluster come back as tool results with `isError` and the broker's message (for example `UNAUTHORIZED: alice may not produce to orders`), not as protocol errors, so the agent can read them.

**Tests.** The tools in the simulation (a cluster, principals with different roles, a refused tool, the `confirm` rule, the read limits); the HTTP mode on real sockets (TLS, 401, Basic and Bearer, a revoked token failing at once, the plaintext refusal off loopback, the `Origin` check); stdio with a scripted MCP client; tokens in the simulation (expiry, revocation ending connections, the owner check, the five steps, limits, no token from a token session) and with the previous cluster version in force (nothing written, requests refused with a message).

### AGENTS.md

Under *Conventions*, a rule in these words or close: "**MCP follows the features.** The plan for a new feature or a changed request includes its tools in `lotse mcp` (`crates/lotse-mcp`): what an operator or a developer would ask an agent to do with it, each as the signed-in principal. They ship in the same change, with tests; a feature without tools says why in its ADR." The repository map gains `crates/lotse-mcp` and the `mcp` subcommand.

### Docs

ADR 0133 (access tokens) and ADR 0134 (`lotse mcp`), the design document (German), a guide page *Using Lotse with AI agents* (stdio setup for a desktop agent, the shared HTTP mode, tokens), the console guide, status.md (known gaps: no OAuth flow, tools so far), AGENTS.md.

## Left to the builder

- `rmcp`'s exact features and version (3.10 or newer), and whether it serves 2025-11-25 alongside 2026-07-28.
- Tool input and output schemas in detail, and the text of each description.
- How the per-credential client cache is built, within the limits above.
- The order of the work (see the plan): tokens first (cluster, client, console), then `lotse mcp`, then docs.

## Calls made by the builder, for the maintainer to check

- The token secret is drawn by the client that asks; the cluster keeps only its SHA-256.
- A token session cannot create tokens.
- Limits: 50 live tokens per owner, 10,000 in all.
- One failure message for every failed token sign-in.
- Tokens carry no IdP groups; the console says so when one is made.
- The *Tokens* tab sits under *Users & access*; *1 month* is the preselected lifetime.
- Destructive tools need `confirm` equal to the resource's name.
- No tool takes messages from a queue in the first release.
- HTTP mode: 15 minutes idle, at most 1,000 signed-in clients; browser origins refused unless allowed.
- Creating and revoking tokens is not an MCP tool in the first release.

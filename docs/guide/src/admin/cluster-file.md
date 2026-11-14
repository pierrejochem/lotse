# The cluster file

One TOML file describes the whole cluster. It is the same file on every node; a node learns which entry is its own from `--node` (or `LOTSE_NODE`). Streams, queues and exchanges are not in the file: clients create them at runtime ([ADR 0047](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0047-the-lotse-binary.md)).

Unknown keys are refused. So is a file that could not run: no `[[node]]`, a duplicate id or address, missing certificates, plain TCP off loopback, authentication without TLS, or authentication required with no admin. `lotse check --config cluster.toml` reads the file and says what it describes.

## Example

```toml
[cluster]
data_dir = "/var/lib/lotse"

[[node]]
id = 1
peer = "10.0.0.1:7000"
client = "10.0.0.1:9000"

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
crl = "/etc/lotse/crl.pem"

[retention]
ms = 604800000

[auth]
admins = ["admin"]
```

The Debian package installs the same example as `/usr/share/doc/lotse/cluster.toml.example`.

## `{id}` in paths

In `cluster.data_dir`, `tls.cert` and `tls.key`, the text `{id}` becomes the node's id. Several nodes can then share one machine and one file. `tls.ca` and `tls.crl` are taken as written.

## `[cluster]`

| Key | Type, unit | Default | Meaning |
| --- | --- | --- | --- |
| `data_dir` | string, required | none | Where a node keeps its data. |
| `tick_ms` | integer, ms | 10 | The Raft tick. Must be at least 1. |
| `segment_bytes` | integer, bytes | 134217728 (128 MiB) | Log segment size. Retention deletes whole segments. Must be at least 4096. |
| `group_session_timeout_ms` | integer, ms | 45000 | A consumer group member without a heartbeat for this long is removed. |
| `group_heartbeat_ms` | integer, ms | 5000 | How often group members send heartbeats. |

## `[[node]]`

One entry per node, at least one.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `id` | integer, required | none | The node's id, 1 or more, unique. |
| `peer` | `host:port`, required | none | The address other nodes dial. |
| `client` | `host:port`, required | none | The address clients dial. |
| `peer_bind` | `host:port` | the `peer` address | Where the node listens for nodes, if not on `peer`, such as `0.0.0.0:7000`. |
| `client_bind` | `host:port` | the `client` address | Where the node listens for clients, if not on `client`. |
| `zone` | string | none | The node's zone (a data centre or availability zone). Clients that name the same zone fetch from its replicas (ADR 0104). A node added later takes it with `lotse node add --zone`. |

Every `peer` and `client` address must be unique in the file.

## `[tls]`

Either name `ca`, `cert` and `key`, or set `plaintext_on_loopback`. Naming both is refused. See [Certificates](certificates.md).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `ca` | path | none | The CA certificate, PEM. |
| `cert` | path with `{id}` | none | The node's certificate chain, PEM. |
| `key` | path with `{id}` | none | The node's private key, PEM. |
| `crl` | path | none | The certificate revocation list, PEM. Optional. A certificate it names is refused in every handshake ([ADR 0060](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0060-certificate-renewal-and-revocation.md)). |
| `plaintext_on_loopback` | boolean | false | No TLS. Allowed only when every address, including `peer_bind` and `client_bind`, is on loopback. Needs `auth.required = false`. For trying a cluster on one machine. |

Plain TCP is refused off loopback ([ADR 0037](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0037-tls-and-node-identity.md)).

## `[retention]`

What streams keep unless they say otherwise. A stream created at runtime can override them; see [Retention and compaction](retention-and-compaction.md).

| Key | Type, unit | Default | Meaning |
| --- | --- | --- | --- |
| `ms` | integer, ms | 604800000 (7 days) | Records older than this go. `-1` for no limit. |
| `bytes` | integer, bytes | no limit | Each partition keeps at least this much. `-1` for no limit. |

Any other value must be positive.

## `[compaction]`

How compacted streams are compacted; see [Retention and compaction](retention-and-compaction.md).

| Key | Type, unit | Default | Meaning |
| --- | --- | --- | --- |
| `map_bytes` | integer, bytes | 67108864 (64 MiB) | Memory the key map of the node's one compaction sweep may take, 48 bytes per key. |

## `[producers]`

How each partition keeps idempotent producers ([ADR 0110](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0110-idle-producers-expire-and-a-full-partition-refuses-new-ones.md)). Use the same values on every node.

| Key | Type, unit | Default | Meaning |
| --- | --- | --- | --- |
| `max` | integer | 10000 | Producers a partition admits. While it knows this many, a new producer is refused with `PRODUCER_LIMIT` and its client retries. A known producer is never refused or forgotten to make room. |
| `expiration_ms` | integer, ms | 86400000 (a day) | How long a producer may write nothing to a partition before its leader forgets it. A retry after that is taken as a new batch. |

Both must be at least 1.

## `[auth]`

| Key | Type, unit | Default | Meaning |
| --- | --- | --- | --- |
| `required` | boolean | true | Clients must authenticate. With `false`, everyone may do everything. |
| `mtls` | boolean | true | A client certificate names its principal. |
| `scram` | boolean | true | SASL SCRAM-SHA-512, with users set at runtime. |
| `admins` | list of strings | empty | Principals that are cluster admins from the start. Must not be empty while `required` is true. |
| `tenants` | list of strings | empty | Tenants besides `default` that exist from the start. |
| `max_session_s` | integer, s | 3600 | A session ends after this at the latest. |

With `required` true, at least one of `mtls`, `scram` and `oidc` must be on, and TLS is required: nodes prove who they are by their certificates.

### `[auth.oidc]`

Present only when tokens from an identity provider are accepted (SASL OAUTHBEARER).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `issuer` | string, required | none | The issuer, as in the tokens' `iss`. |
| `audience` | string, required | none | The audience tokens must be for. |
| `principal_claim` | string | `sub` | The claim that names the principal. |
| `groups_claim` | string | `groups` | The claim that lists the user's groups. |
| `jwks_url` | URL | the issuer's discovery document | The key set's URL. |

See [Authentication and roles](authentication-and-roles.md).

## Environment variables

| Variable | Used by | Meaning |
| --- | --- | --- |
| `LOTSE_CONFIG` | every command that reads the file | The cluster file, when `--config` is not given. |
| `LOTSE_NODE` | `lotse run` | The node's id, when `--node` is not given. |
| `LOTSE_JOIN` | `lotse run` | `1` or `true` makes the node join a running cluster, like `--join`. |
| `LOTSE_LOG` | `lotse run` | The log filter, in the syntax of the `tracing` env filter. Default `info`. |
| `LOTSE_PASSWORD` | `lotse node` | The password for `--user`. |

A flag wins over the variable. The systemd unit and the container image use these variables ([ADR 0049](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0049-packaging.md)).

## Commands

```text
lotse run   --config <cluster.toml> --node <id> [--join]
lotse certs --config <cluster.toml> [--days <n>]
lotse certs --config <cluster.toml> --client <principal> --out <dir> [--days <n>]
lotse certs --config <cluster.toml> --revoke <cert.pem>
lotse check --config <cluster.toml>
lotse node add    --config <cluster.toml> --node <id> [--peer <host:port> --client-addr <host:port>] <admin>
lotse node remove --config <cluster.toml> --node <id> <admin>
lotse node list   --config <cluster.toml> <admin>
lotse --version
```

`<admin>` is `--cert <file> --key <file>` (a client certificate) or `--user <name>` with the password in `LOTSE_PASSWORD`.

`lotse run` stops cleanly on Ctrl-C or SIGTERM. `lotse check` prints the node count, the TLS mode, the certificates on this machine that expire within 30 days, the retention defaults and the sign-in methods.

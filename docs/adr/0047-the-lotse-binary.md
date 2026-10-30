# 47. The `lotse` binary: one cluster file, resources at runtime

- Status: Accepted
- Date: 2026-10-30

## Context

A node could only run from Rust: tests and the console's example built a `NodeConfig` by hand, with hand-picked Raft group ids, and started `Node` themselves. Deploying a cluster meant writing a program. The pieces a binary needs were all there: the metadata group creates resources at runtime (ADR 0034), the coordinator group serves producers, transactions and consumer groups, TLS with node certificates is mandatory off loopback (ADR 0037), and authentication and roles are in place (ADRs 0038, 0039).

## Decision

**A new crate `lotse-node` builds the `lotse` executable**; its library reads the cluster file, makes certificates and runs a node, so tests start nodes the same way.

**One cluster file, the same on every node** (maintainer's choice). A TOML file names every node (`[[node]]` with `id`, the `peer` and `client` addresses others dial, optional `peer_bind` and `client_bind`), the data directory (`{id}` becomes the node's id), the Raft tick and segment size, TLS (`ca`, `cert`, `key`, `{id}` in paths), retention defaults (`-1`: no limit) and authentication (`required`, `mtls`, `scram` on by default, `admins`, `tenants`, `oidc`). A node is started with `lotse run --config cluster.toml --node <id>`. Unknown keys are refused, and so is a file that could not run: duplicate ids or addresses, missing certificates, plain TCP off loopback, authentication without TLS, no admin while authentication is required.

**Resources only at runtime** (maintainer's choice). The file has no streams, queues or exchanges. Every node hosts the metadata group (Raft group 1) and the coordinator group (2), with every node a voter; clients create everything else, which the metadata group places on as many nodes as asked. Configured resources stay a library feature for tests.

**Certificate paths in the file and a command to make them** (maintainer's choice). `lotse certs --config cluster.toml` makes a CA (`ca.pem`, its key beside it as `ca.key`) and for each node a certificate for `lotse://node/<id>` and the hosts of its addresses, at the paths `[tls]` names, keeping what exists, so it can run again after a node is added. `lotse certs --config cluster.toml --client <principal> --out <dir>` signs a client certificate whose common name is the principal: how the first admin signs in. Keys are written readable by their owner only. A larger setup brings certificates from its own PKI to the same paths.

`lotse check` reads a file and says what it describes. `lotse run` logs through `LOTSE_LOG` (default `info`) and stops cleanly on Ctrl-C or SIGTERM.

## Consequences

- A cluster is a file, certificates and one command per machine; there is nothing to compile.
- Every node is a voter of the metadata and coordinator groups. Up to five or so nodes this is what one would choose; beyond that those groups get slower than they need to be. Membership is fixed by the file: adding a node to a running cluster is not possible yet, as Raft group membership is fixed when a group is created.
- The file must be the same on every node; nothing checks that it is. A node whose peer list differs refuses links from nodes it does not know (ADR 0037), which shows up as a node that never joins.
- Certificates made by `lotse certs` do not expire and cannot be revoked; nodes read them once at start.
- Tests: unit tests of the file (each node's configuration, `-1`, every refusal), and a test that runs `lotse check` and `lotse certs`, starts two nodes in the process and one as the executable, has the admin create a stream and write to it while a client without a certificate may not, stops the executable with SIGTERM, restarts all three and reads everything back.

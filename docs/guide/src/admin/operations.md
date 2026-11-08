# Operations

This chapter says what the code supports: where the data is, what a restart does, and how to read common failures. The repository documents no backup or upgrade procedure. Those are listed as open at the end, not invented.

## What is in the data directory

`cluster.data_dir` (with `{id}` replaced by the node's id) holds everything a node keeps. The node creates it if it is missing. Under the systemd unit and in the image it is `/var/lib/lotse`, the only place the unit lets the node write.

| Path | Content |
| --- | --- |
| `first-voters` | The voters the configured groups started with, written at the node's first start. |
| `groups/<group id>/` | One directory per Raft group the node hosts. Group 1 is the metadata group, group 2 the coordinator group; every stream partition and queue has a group of its own. |
| `groups/<id>/log/<base offset>.log` | The segments of the group's log. The name is the first offset, 20 digits. The Raft log and the partition log are the same log. |
| `groups/<id>/raft.state` | The group's term and vote. |
| `groups/<id>/snapshot` | The group's latest snapshot. |
| `groups/<id>/moved` | A record of an earlier move of the replica off this node. |

The node writes these files itself. The code does not offer a supported way to edit or copy them while the node runs. Resources, users, tenants, role bindings and the node list are in the metadata group, so they are in these files too; the cluster file does not hold them.

## Restart and crash

- A node stops cleanly on Ctrl-C or SIGTERM. The systemd unit sends SIGTERM and allows 60 s (`TimeoutStopSec`), and restarts the node after a failure (`Restart=on-failure`, `RestartSec=5`).
- After a crash, opening a log scans the active segment, cuts a torn or corrupt tail and syncs the result. Appended data survives a crash only once the log has been synced.
- Raft state is written in a fixed order: snapshot, truncation, entries (synced), hard state, then apply and send ([ADR 0023](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0023-persist-the-hard-state-after-the-log.md)).
- The sparse offset index is rebuilt by scanning on open, and so is the key map of a compacted stream. A restart of a node with large logs takes longer for it.
- After a restart a node comes back with an old view of the metadata group if it was down for a while. It asks for the node list and catches up ([ADR 0050](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0050-rebalancing-groups-onto-new-nodes.md)).
- Consumer group members time out after `group_session_timeout_ms` (default 45 s) without a heartbeat. Queue messages in flight to a disconnected consumer are redelivered.

A cluster of three nodes needs two to commit. The code does not state how many restarts at once a given cluster survives beyond what Raft's majority rule says.

## A drained node

`lotse run` exits with status 0 when its node is drained, so a supervisor does not start it again. A drained node's data directory is no longer used. The id cannot be used again ([Nodes](nodes.md#drain-and-remove-a-node)).

## Troubleshooting

Messages below are those the code prints.

| Symptom | Cause and action |
| --- | --- |
| `lotse run` or `check` fails with `--config (or LOTSE_CONFIG) is missing` or `--node (or LOTSE_NODE) is missing` | Give the flag or set the variable. |
| `there is no node N in the file; it names ...` | The node id is not in `[[node]]`. For a new node, add its entry first. |
| `[tls] needs ca, cert and key ... or plaintext_on_loopback = true` | The file names no certificates. Run `lotse certs`, or set `plaintext_on_loopback` on a machine you use alone. |
| `auth.admins is empty: nobody could create tenants or grant roles` | Name at least one principal in `auth.admins`, then make its certificate with `lotse certs --client`. |
| `auth.required needs TLS ...` | Authentication needs certificates. Use TLS, or set `auth.required = false` with `plaintext_on_loopback` to try a cluster without either. |
| `... is not on loopback; without TLS every address must be` | `plaintext_on_loopback` allows only loopback addresses, `peer_bind` and `client_bind` included. |
| `TLS: ...` at start | A certificate, key or list could not be read or does not match. Check the paths and that the `lotse` user can read them. |
| Log: `TLS files do not load; keeping the material in use` | New certificate files are broken. The node keeps the old ones. Fix the files; the node tries again within 10 s. |
| Connections drop at a fixed time and reconnect fails | A certificate expired ([ADR 0068](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0068-connections-end-when-their-certificate-expires.md)). Renew it and let the node pick it up. `lotse check` warns 30 days before. |
| A client gets `UNAUTHORIZED` | The principal lacks a role for that request, or has not authenticated. Use `WhoAmI` to see its bindings ([Authentication and roles](authentication-and-roles.md)). |
| `AUTHENTICATION_FAILED` for SCRAM | The answer says no more, so that names cannot be probed. Check user and password. |
| `OFFSET_OUT_OF_RANGE` on a fetch | The position is below the log start; retention deleted it. Read the log start from the answer and go on from there. |
| `lotse node ...` with `--user` fails with `--user needs LOTSE_PASSWORD` | Put the password in `LOTSE_PASSWORD`. |
| `lotse node add` says `node N is not in the file: give --peer and --client-addr` | Add the `[[node]]` entry or pass both addresses. |
| A node sits in `draining` in `lotse node list` | Its groups are still moving; look at `moving`. At most four groups move at a time. |
| A consumer does not get queue messages | `DescribeMetrics` shows the queue's `ready`, `in_flight` and each subscription's `credit`. A queue with `single.active.consumer` serves one subscription at a time. |

For more detail raise the log level, for instance `LOTSE_LOG=debug`. The filter uses the `tracing` env filter syntax.

## Open: backup

The repository documents no backup procedure, and the code offers no snapshot or export command. These questions are open:

- Whether copying a stopped node's data directory gives a usable backup, and how it relates to the other replicas.
- Whether a backup of one node is enough, since each partition is replicated.
- How to restore a whole cluster, and whether a restored node may rejoin with its old id.
- How to back up the CA key, the certificates and the cluster file. These are ordinary files and you hold them yourself; `ca.key` must stay off the nodes.

## Open: upgrade

The repository documents no upgrade procedure. These questions are open:

- Whether nodes of different versions can run together, and so whether a rolling restart is supported.
- Which versions can read each other's data directory, snapshots and node-to-node format.
- Whether the native protocol versions across releases are compatible; `ApiVersions` lets a client ask a node which versions it speaks.
- The order in which to restart nodes, and how to know a restarted node has caught up before the next goes down. `DescribeMetrics` reports every replica's `log_end` on the leader, which is a starting point but is not documented as a procedure.

Until these are answered, test an upgrade on a copy of your cluster first.

## Open: other gaps

- Releases and the image are built only by GitHub Actions. The package targets systemd systems.
- A drained node must join under a new id to come back.
- Certificate revocation is a file you copy to every node; there is no OCSP.

# A first cluster

This chapter sets up three nodes with TLS 1.3. Every node reads the same cluster file. Streams, queues and exchanges are not in the file; clients create them.

## The cluster file

Copy the example to `/etc/lotse/cluster.toml` and change the addresses. The Debian package ships it as `/usr/share/doc/lotse/cluster.toml.example`.

```toml
{{#include ../../../../packaging/deb/cluster.toml.example}}
```

What the keys mean:

| Key | Meaning |
| --- | --- |
| `[cluster] data_dir` | where a node keeps its data. `{id}` becomes the node's id, so `/var/lib/lotse/node-{id}` gives each node its own directory on a shared machine. |
| `[[node]] id` | a number from 1 up, unique. |
| `[[node]] peer` | `host:port` where the other nodes reach this node. |
| `[[node]] client` | `host:port` where clients reach this node. |
| `[tls] ca`, `cert`, `key` | the CA certificate and the node's certificate and key. `{id}` is replaced as in `data_dir`. |
| `[tls] crl` | the revocation list. Optional. |
| `[retention] ms` | how long streams keep records, in milliseconds. `-1` keeps everything. The default is 7 days. |
| `[auth] admins` | the principals that may create tenants and grant roles. Required while `auth.required` is on, which is the default. |

The file is checked strictly: an unknown key is an error. [Administration](../admin/cluster-file.md) will list every key; this guide uses only those above.

## Check the file

```sh
lotse check --config /etc/lotse/cluster.toml
```

For the example it prints:

```text
3 nodes: 1, 2, 3
TLS: certificates from [tls]
retention: 604800000 ms by age, no limit per partition
clients sign in with: certificate, SCRAM-SHA-512; admins: admin
```

Once certificates exist, `lotse check` also names any that expire within 30 days.

## Make the certificates

```sh
lotse certs --config /etc/lotse/cluster.toml
lotse certs --config /etc/lotse/cluster.toml --client admin --out ./admin
```

The first command writes a CA (`ca.pem`, `ca.key`), a certificate and key per node, and an empty revocation list where `[tls]` names them. Node certificates last one year, the CA ten; `--days` sets another length. `lotse certs` keeps files that exist, so you can run it again after you add a node to the file.

The second command writes a client certificate `./admin/admin.pem` and key `./admin/admin.key` for the principal `admin`, the name `auth.admins` lists. Use this certificate to sign in as the first administrator.

`ca.key` signs every certificate of the cluster. Move it off the nodes after you have made the certificates. Copy `ca.pem`, `crl.pem` and each node's own `node-<id>.pem` and `node-<id>.key` to its machine, readable by the user that runs `lotse`.

## Start the nodes

On each machine, with its own id:

```sh
lotse run --config /etc/lotse/cluster.toml --node 1
```

A node stops cleanly on Ctrl-C or SIGTERM. `LOTSE_LOG` sets the log filter, for example `LOTSE_LOG=debug`; the default is `info`.

## Run under systemd

The Debian package installs `lotse@.service`. The text after `@` is the node's id. The unit reads `/etc/lotse/cluster.toml`, runs as the user `lotse` and restarts the node on failure.

```sh
sudo install -d /etc/lotse
sudo cp /usr/share/doc/lotse/cluster.toml.example /etc/lotse/cluster.toml   # then edit it
sudo lotse certs --config /etc/lotse/cluster.toml
sudo chown -R lotse: /etc/lotse
sudo systemctl enable --now lotse@1
```

Run the last command on each machine with its own id (`lotse@2`, `lotse@3`). Keep `data_dir` at `/var/lib/lotse` or below: the unit lets the node write nowhere else. Follow a node with `journalctl -u lotse@1 -f`.

This guide ran `lotse check` and `lotse certs` on the example file's shape and started three nodes on one machine ([the trial](one-machine-trial.md)). It did not run the systemd unit or a cluster across three machines.

## Next

[A first message](first-message.md) sends and reads a record, and shows the cluster in the console. Certificates, renewal, revocation, authentication, roles, and adding and draining nodes belong to the administration guide.

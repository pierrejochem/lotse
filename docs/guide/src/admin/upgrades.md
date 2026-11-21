# Upgrades

A cluster upgrades without stopping: one node after another restarts on the new release, and every group keeps its quorum. This chapter shows how on machines with packages, on cloud machines and on Kubernetes ([ADR 0127](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0127-rolling-upgrades-with-a-cluster-version-and-restart-turns.md)).

## The rules

- **One minor release at a time.** A release runs side by side with the previous minor release: 0.5 with 0.4, not with 0.3. A release more than one minor release ahead of the cluster refuses to start and says which releases fit. To go from 0.3 to 0.5, upgrade every node to 0.4 first, then to 0.5.
- **The cluster version.** The cluster keeps a version, the `major.minor` it writes its formats for. It stays where it is during an upgrade, so each node can still go back to the previous release. Once every node runs the new release, a cluster admin raises it, and only then does the new release use what is new in its formats.
- **One restart at a time.** Before a node restarts for an upgrade, it takes the cluster's restart turn. There is one turn per cluster, and a node gets it only while no other node holds it and no group it votes in would lose its quorum without it. The restarted node gives the turn back once every group it is in has a leader with a quorum or has caught up.

Where an upgrade stands:

```sh
lotse upgrade status --config /etc/lotse/cluster.toml --node 1
```

```
cluster version: 0.4
restart turn:    node 2
turns:           running
node   release      cluster versions
1      0.5.0        0.4 to 0.5
2      0.4.3        0.3 to 0.4
3      0.4.3        0.3 to 0.4
```

`--node` asks with the node's own certificate, on the node's machine; elsewhere give an admin's certificate (`--cert`, `--key`) or SCRAM user (`--user`, `LOTSE_PASSWORD`). The console's *Nodes* page shows the same.

## Machines with packages

Install the new package the usual way, on one machine or on all of them at once: `apt upgrade`, `dnf upgrade`, `apk upgrade`, or unattended updates. After it installs the binary, the package's script restarts each node it finds running on the machine, under systemd every active `lotse@<id>`, under OpenRC every started `lotse.<id>`, under sysvinit the node `LOTSE_NODE` names, each only once it holds the restart turn. The package manager waits meanwhile, so an update started on every machine at once restarts one node after another.

| Setting in `/etc/default/lotse`, `/etc/sysconfig/lotse` or `/etc/conf.d/lotse` | Default | Meaning |
| --- | --- | --- |
| `LOTSE_RESTART_ON_UPGRADE` | `yes` | `no` leaves the nodes on the old binary until you restart them |
| `LOTSE_TURN_WAIT` | `30m` | how long to wait for each node's turn |
| `LOTSE_CONFIG` | `/etc/lotse/cluster.toml` | the cluster file |

If a node's turn does not come within the wait, the script says so, leaves that node on the old binary and does not fail the upgrade. Restart it later the same way:

```sh
lotse turn take --node 2 && systemctl restart lotse@2
```

From a tarball, or with your own supervisor, do the same for each node: install the binary, take the turn, restart.

## Cloud machines

A provider's patching that updates packages in place (AWS Systems Manager Patch Manager, Google Cloud OS Config, Azure Update Manager) goes through the package scripts above. Schedule it on every machine; the turns order the restarts.

A machine that is replaced by a new image keeps its node id and data volume. Run this in its termination hook, before the instance stops (an Auto Scaling lifecycle hook, a shutdown script, Azure Scheduled Events), with the hook's timeout above the wait:

```sh
lotse turn take --config /etc/lotse/cluster.toml --node "$LOTSE_NODE" --wait 20m
```

The replacement starts with the same id and volume and gives the turn back once it is ready. Replace one machine group at a time; with a rolling replacement, keep at most one instance out of service.

## Kubernetes

The Helm chart in `packaging/helm/lotse` runs the nodes as a StatefulSet ([ADR 0128](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0128-a-helm-chart-whose-pods-take-the-restart-turn.md)); its README covers the install. An upgrade is a `helm upgrade` with the new image tag:

```sh
helm upgrade lotse packaging/helm/lotse --reuse-values --set image.tag=<release>
kubectl exec lotse-0 -- lotse upgrade status
```

The StatefulSet replaces one pod at a time. Each stopping pod takes the turn in its preStop hook, and the new pod is ready, so the next one goes, only once its groups have caught up. Kubernetes 1.28 or later is needed.

## Finishing

Once `lotse upgrade status` shows every node on the new release, raise the cluster version as a cluster admin:

```sh
lotse upgrade finalize --config cluster.toml --cert admin.pem --key admin.key
```

Without `--version` it raises to the newest version every node supports. The cluster refuses a version some node does not support, and names the node. After this, the nodes cannot go back to the previous release. The console's *Nodes* page has the same as *Raise cluster version*.

## Pausing, and a stuck turn

```sh
lotse upgrade hold   --config cluster.toml --cert admin.pem --key admin.key
lotse upgrade resume --config cluster.toml --cert admin.pem --key admin.key
```

While paused, the node that holds the turn finishes and no node gets the next one. On Kubernetes this makes the next pod wait in its preStop hook up to `turnWait`; to hold a rollout for longer, set the StatefulSet's `updateStrategy.rollingUpdate.partition`.

A turn waits for its node to come back ready, for as long as that takes: it does not expire, because the next restart could cost a group its quorum. A node that went down and stays down keeps the turn. When it will not come back, give the turn back as a cluster admin:

```sh
lotse upgrade release --node 2 --config cluster.toml --cert admin.pem --key admin.key
```

On the node's own machine, `lotse turn release --node 2` does the same after a restart that failed.

## Probes

`lotse probe healthz`, `readyz` and `safe-to-stop` ask a node's operator endpoint on loopback and exit 0 when it answers 200. Use them in scripts and in containers without a shell. The node's `[[node]]` entry must name `metrics` on a loopback address.

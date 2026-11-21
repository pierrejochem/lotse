# 127. Rolling upgrades with a cluster version and restart turns

- Status: Accepted
- Date: 2026-11-21
- Changes [ADR 0123](0123-client-certificates-are-renewed-over-a-connection-made-with-them.md) and [ADR 0125](0125-a-group-change-writes-only-the-members-it-changes.md) in part: their formats needed every node on the new version; from the next format change on, a change is written only once the cluster version allows it

## Context

Nothing said whether nodes of two Lotse versions may run side by side. Every format carries a version byte, and readers accept older versions, but every writer writes the newest: a new node's log records, snapshots or node frames could be unreadable to a node not yet upgraded. ADR 0123 and ADR 0125 therefore said that every node must run the new version, which made every upgrade a stop of the whole cluster.

The parts for a rolling restart existed. A stopping node hands its groups over first (ADR 0114). `/safe-to-stop` judges whether a group would lose its quorum without a node, and `/readyz` says when a restarted node has caught up (ADR 0117). Nothing used them to restart one node after another, and nothing stopped two machines from restarting at once: an unattended package update on every machine, a cloud provider's patching, or two operators.

The console has shown a rolling upgrade since its mockups, filled only by the demo cluster (ADR 0019). The design document's Kubernetes operator was meant to drive it, and does not exist.

## Decision

**Nodes of two adjacent minor releases run side by side. New formats are written only once a cluster admin raises the cluster version, which the metadata group keeps. Before a node restarts, it takes the cluster's single restart turn, and it gives the turn back once the restarted node is ready.** The maintainer chose these: an admin raises the version, never the cluster on its own; the window is one minor release; bare metal and cloud machines take turns in the cluster rather than through an outside tool.

### Versions

- **A release's cluster version** is its `major.minor`. A release supports two cluster versions: its own, and the previous minor release's (`lotse_server::version`, `CLUSTER_VERSION` and `OLDEST_CLUSTER_VERSION`; a test holds the window to one minor release). A release keeps reading and writing the previous release's formats; from the release after, the code for them may go.
- **Each node reports** its release and the cluster versions it supports to the metadata leader (the internal `ReportVersion`, key 63), at start and again whenever the metadata state says otherwise, a node only for itself, as with `ReportDisk` (ADR 0124). The metadata state keeps them (snapshot section 18).
- **The cluster version** is in the metadata state. A cluster that has none yet takes the lowest of its nodes' newest versions, once every listed node has reported; the metadata leader proposes it then.
- **Raising it** is `AlterUpgrade` (key 64) with a cluster version, by a cluster admin, in the console or with `lotse upgrade finalize`. The metadata leader takes a version newer than the current one only if every listed node supports it, and names the nodes that do not. The version never goes down. Until it is raised, every node writes the formats of the version in force, so a node can still go back to the previous release.
- **Writers.** A format change from now on is written only while `MetadataState::cluster_version` is at least the version that introduced it, and read either way. AGENTS.md says so.
- **A node outside the window** stops: one whose newest version is older than the cluster's, or whose oldest version is newer (a release was skipped). `lotse run` exits with a message that names the cluster's version and the releases that support it.

### Restart turns

- **One turn per cluster**, in the metadata state: the node holding it, the instance of its process that took it (a random number each process draws at start), and when.
- **Taking a turn.** `lotse turn take --node <id>` runs on the node's machine before the node stops for an upgrade. It connects to that node with the node's own certificate and sends the internal `Turn` (key 62).
  - The node first judges, as `/safe-to-stop` does, whether a group it votes in would lose its quorum without it (ADR 0117), from every node's answer.
  - Only then does it ask the metadata leader to give it the turn. The leader gives it if no node holds the turn, if this node already holds it, and if turns are not paused.
  - Otherwise the answer is `TURN_UNAVAILABLE` (error 35), with the reason: the groups at risk, the node holding the turn, or the pause. The code is not retriable, since sending the same request at once would only be refused again; the command waits and asks again, every 2 s, up to `--wait` (30 min by default).
  - It exits 0 once the node holds the turn.
- **Giving it back.** A node gives the turn back itself (`Turn` with `release`) once it holds the turn under another instance than the one that took it, meaning it restarted, and every one of its groups is ready, as `/readyz` judges. The process that took the turn never gives it back, so a restart that did not happen keeps the turn. `lotse turn release` gives it back from the machine when the restart failed; a cluster admin gives back any node's turn with `AlterUpgrade`. A node removed from the cluster gives its turn back with it.
- **Pausing.** A cluster admin pauses turns with `AlterUpgrade` (the console's *Pause upgrade*, `lotse upgrade hold`): the node holding the turn finishes, and no node gets the next one until `lotse upgrade resume`.
- The judgement rests on what each group's leader heard lately, as `/safe-to-stop` does: a node that failed within the last election timeout still counts as answering, so a turn taken in that moment is not refused.
- A turn guards voluntary restarts. A crash, or a `systemctl restart` without a turn, still happens, and the cluster handles it as before (ADR 0114, Raft). A turn only waits for one when it judges the next restart.

### Where it is used

- **Packages.** On an upgrade, the `.deb`, `.rpm` and `.apk` restart the node they had running: take the turn, then restart. Under systemd that is every active `lotse@<id>`, under OpenRC every started `lotse.<id>`, and under sysvinit the node `LOTSE_NODE` names. The package manager waits meanwhile. `LOTSE_RESTART_ON_UPGRADE=no` in `/etc/default/lotse` or `/etc/conf.d/lotse` turns it off. If the turn does not come within the wait, the script says so, leaves the node on the old binary and does not fail. An update started on every machine at once therefore restarts one node after another.
- **Cloud machines.** In-place updates by a provider's patching (AWS Systems Manager, Google OS Config, Azure Update Manager) go through the packages above. A machine replaced by an image runs `lotse turn take` in its termination hook. The replacement, with the same node id and data volume, gives the turn back once it is ready. The guide shows both.
- **Kubernetes.** A Helm chart runs the nodes as a StatefulSet. Its preStop hook takes the turn, and its readiness probe is `/readyz` ([ADR 0128](0128-a-helm-chart-whose-pods-take-the-restart-turn.md)); an operator follows ([ADR 0129](0129-a-kubernetes-operator-after-the-chart.md)).
- **Status.** `DescribeUpgrade` (key 65, a cluster admin or viewer) gives the cluster version, the turn, the pause, and each node's release and the cluster versions it supports. It is a request of its own because `DescribeNodes` has no tag block to grow by: new fields there would break clients that read it today. `lotse upgrade status` prints it. The console's *Nodes* page shows them as the rolling upgrade from its mockups, against a real cluster, and pauses, resumes and raises the version as the signed-in user (ADR 0019).
- **Probes.** `lotse probe healthz|readyz|safe-to-stop` asks the node's operator endpoint on loopback and exits 0 for 200. Containers without a shell, such as the distroless image, run it as an exec probe.

Rejected options:

- **The cluster raises its version on its own** once every node runs the new release, as CockroachDB does. It saves a step, but after that no node can go back. The maintainer chose an admin's command.
- **A whole major version, or two minor releases, side by side.** Operators could skip releases, but every format would stay readable and tested for much longer.
- **An outside tool that restarts the nodes in order** (`lotse upgrade` over SSH or a cloud API). It is simpler in the cluster, but nothing stops an unattended update on every machine at once.
- **Taking the turn on SIGTERM.** Every stop would wait for a turn, including the stop of a whole cluster or a machine's shutdown.
- **A turn that expires.** It would let the next node restart while the last one is still down, and that can cost a group its quorum. A stuck turn waits for an admin instead.

## Consequences

- A cluster upgrades without stopping, node by node, on bare metal, on cloud machines and on Kubernetes.
- A release reads and writes the previous minor release's formats. A format change therefore costs a gate, a test with the old version in force, and code that goes one release later. A release that skips a minor release has to be installed in two steps.
- Until a cluster admin raises the cluster version, a new release runs with the previous release's formats, and features that need new formats stay off.
- A package upgrade now restarts the running node. It used to leave that to the operator. An update can take as long as every node before it takes to restart and catch up.
- A node that went down and does not come back holds the turn until an admin gives it back.
- The cluster version and turns change the metadata state: changes 30 to 34 and snapshot section 18. An older release cannot read them, which is the last time an upgrade needs every node at once: releases before this one have no cluster version.
- Tests:
  - Unit tests check the metadata state's rules: one turn, the same node again, the pause, a removed node, raising the version, the default once every node reported, and the window.
  - A simulation upgrades every node of a cluster in turn under faults. It restarts a node only with the turn, checks that no two nodes are ever down for an upgrade at once and that every group keeps its quorum, and checks that each turn comes back after the restarted node is ready.
  - A test with real nodes runs `lotse turn take`, restarts the node and sees the turn come back.
  - The CI package checks reinstall the `.deb` and see the node restart through a turn.
- Mutation-checked: the metadata leader giving a second turn while one is held; a node giving back a turn its own process took; a node taking the turn while a group would lose its quorum; raising the version past a node's newest.

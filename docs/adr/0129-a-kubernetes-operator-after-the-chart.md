# 129. A Kubernetes operator after the chart

- Status: Proposed
- Date: 2026-11-21

## Context

The Helm chart (ADR 0128) runs a fixed set of nodes. It cannot add or remove nodes, since that takes `lotse node add` or `remove` in the cluster and a node started with `--join`. Every pod mounts every node's key. It does not raise the cluster version after an upgrade, and it cannot hold a rollout while turns are paused. The design document has named an operator since its first version, and the console's rolling-upgrade view says "managed by the Lotse operator" when it reports events.

The maintainer chose the chart now and the operator as the next step.

## Decision

**A Lotse operator follows the chart: a controller with a `LotseCluster` resource that owns the StatefulSet, adds and drains nodes, gives each pod only its own certificate, and drives upgrades through the cluster's restart turn and version.** This ADR records the direction; the operator's own ADR decides its details once work on it starts.

What the operator is to do:

- **Scale.** A raised node count adds each new node to the cluster (`AddNode`) before its pod starts with `--join`. A lowered count drains the highest node (`RemoveNode`) and removes its pod and volume once it stopped by itself (ADR 0051).
- **Certificates.** Each pod gets a Secret with only its own key. The operator signs it with the renewal CA (ADR 0123), or with a CA it is given, and never holds a client's key.
- **Upgrades.** The operator replaces pods itself, in the order the turn allows, and stops while turns are paused. Once every node runs a new release, it raises the cluster version, but only when the resource asks for that: an admin's choice, as in ADR 0127.
- **Status.** The resource's status carries `DescribeUpgrade` and `DescribeNodes`, and Kubernetes events feed the console's upgrade view.
- **Identity.** It acts with a certificate whose principal holds only the roles it needs, bound in the cluster like a user's (ADR 0039).

## Consequences

- Until it exists, the chart and the guide cover Kubernetes, and growing a cluster on Kubernetes is a manual procedure.
- The operator is a separate program with its own release, and probably its own language and client (a Kubernetes client in Rust, or Go), to decide in its ADR.

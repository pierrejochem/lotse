# 128. A Helm chart whose pods take the restart turn

- Status: Accepted
- Date: 2026-11-21

## Context

Lotse ran on Kubernetes only as an image: no manifests, no chart, and the design document's operator did not exist. ADR 0127 gave the cluster restart turns, so that a node restarts only while no other node does and no group loses its quorum. Kubernetes restarts pods on its own schedule: a StatefulSet rollout, a node drain, a changed ConfigMap. Without a turn, a rollout and a package update on a machine outside Kubernetes, or a rollout and a node drain, could stop two nodes at once.

The maintainer chose a Helm chart first and an operator after it (ADR 0129).

The image is distroless: no shell. A pod cannot compute its node id from its name in a command, and probes and hooks must be programs that take their arguments as given.

## Decision

**`packaging/helm/lotse` runs a cluster as a StatefulSet. Pod index i is node i + 1. Each pod's preStop hook takes the restart turn, its readiness probe is `/readyz`, and both run the `lotse` executable in the pod.**

- **Node ids.** Kubernetes 1.28 and later label each StatefulSet pod with its index (`apps.kubernetes.io/pod-index`). The chart passes it to the container as `LOTSE_NODE_INDEX`. `lotse` takes the node id from `--node`, else `LOTSE_NODE`, else `LOTSE_NODE_INDEX` plus one, for every command that needs the node: `run`, `turn`, `probe`, `upgrade status`.
- **The cluster file** is a ConfigMap rendered from the values, the same for every pod, as ADR 0047 has it. Node i's `peer` and `client` addresses are the pod's name under a headless Service, which publishes names before pods are ready so that the nodes of a new cluster find each other. Nodes bind `0.0.0.0`. Each node's operator endpoint listens on `127.0.0.1` at a base port plus its id: the cluster file must give every node its own address, and loopback keeps the endpoint in the pod.
- **TLS.** One Secret holds the CA certificate and every node's certificate and key, made with `lotse certs` against the rendered cluster file, so the certificates name the pods' names. The CA key stays outside the cluster.
- **Probes.** Startup and liveness run `lotse probe healthz`, readiness `lotse probe readyz`. A rollout replaces the next pod only once the last one is ready, which is when every group it is in has a quorum or has caught up.
- **preStop** runs `lotse turn take --wait <turnWait>` (10 min by default). The pod then gets SIGTERM and hands its groups over (ADR 0114). The restarted node gives the turn back once ready (ADR 0127). If the turn does not come, the pod stops when the wait ends; Kubernetes stops it at the end of `terminationGracePeriodSeconds` (15 min) in any case.
- **The StatefulSet** starts pods in parallel, since no group of a new cluster has a quorum until a majority runs, and updates them one at a time. A PodDisruptionBudget lets a node drain take one pod at a time. A changed cluster file changes a checksum annotation, so the pods roll.
- **CI** lints and renders the chart, makes certificates for the rendered file with the `lotse` the job built and checks the file with `lotse check`. It installs the chart in a kind cluster with the image the job built, waits for three ready nodes, rolls them, and checks that the turn is free again and every node ready.

Rejected options:

- **A shell in the image, to compute the id from the hostname.** The image would grow and gain a shell for one subtraction.
- **Node ids from 0.** Ids start at 1 everywhere else (ADR 0047). 0 means "none" in several requests.
- **One Secret per node.** A StatefulSet mounts the same volumes in every pod. Per-pod certificates need an operator or a CSI driver that signs with the Lotse CA.
- **The operator endpoint on the pod's address.** Prometheus could scrape it, but over TLS only with a certificate that sees the whole cluster (ADR 0117), and `lotse probe` reads only loopback.

## Consequences

- A Kubernetes rollout restarts one node at a time, in turn with every other restart in the cluster, and waits for each node to catch up before the next.
- Every pod has every node's key. A pod that is broken into can speak as any node. The operator removes this (ADR 0129).
- `replicas` is the node list of the cluster file. Adding or removing nodes needs `lotse node add` or `remove` and a node started with `--join`, which the chart does not do. The operator will.
- Stopping the whole cluster, with `helm uninstall` or by scaling to zero, makes every pod but one wait for the turn up to `turnWait`.
- Prometheus cannot scrape the nodes from outside their pods.
- Clients must run in the cluster or resolve the pods' names: metadata names each leader by its pod's address.
- `lotse` reads `LOTSE_NODE_INDEX` (unit test in `crates/lotse-node`).

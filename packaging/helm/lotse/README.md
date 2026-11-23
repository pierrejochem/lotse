# Lotse Helm chart

Runs a Lotse cluster as a StatefulSet ([ADR 0128](../../../docs/adr/0128-a-helm-chart-whose-pods-take-the-restart-turn.md)). Each pod is one node: `lotse-0` is node 1, `lotse-1` node 2, and so on. The cluster file every node reads is rendered from the values. Rolling upgrades take the cluster's restart turn ([ADR 0127](../../../docs/adr/0127-rolling-upgrades-with-a-cluster-version-and-restart-turns.md)), so a Kubernetes rollout and a node restarted elsewhere never overlap.

Needs Kubernetes 1.28 or later: the pod index label gives each pod its node id.

## Install

1. Render the cluster file and make the certificates for it. `lotse certs` writes them where `[tls]` says, so point it at a local directory first:

   ```sh
   helm template lotse packaging/helm/lotse --set 'auth.admins={admin}' \
     --show-only templates/configmap.yaml | yq '.data["cluster.toml"]' \
     | sed 's#/etc/lotse/tls#./tls#' > cluster.toml
   lotse certs --config cluster.toml
   lotse certs --config cluster.toml --client admin --out clients
   ```

   Render with the release name, namespace and `replicas` you install with: the certificates name the pods' DNS names.

2. Store them in a Secret, and keep `tls/ca.key` somewhere safe, not in the cluster:

   ```sh
   kubectl create secret generic lotse-tls --from-file=tls/ca.pem \
     $(for f in tls/node-*; do printf -- '--from-file=%s ' "$f"; done)
   ```

3. Install:

   ```sh
   helm install lotse packaging/helm/lotse --set 'auth.admins={admin}' --wait
   ```

Clients bootstrap at the pods' names, such as `lotse-0.lotse-nodes.<namespace>.svc.cluster.local:9000`, and then reach each partition's leader by its pod name, so they run in the cluster. The node certificates name the pods, not the Service `lotse`, so a client that bootstraps through the Service fails the TLS name check.

## Upgrade

```sh
helm upgrade lotse packaging/helm/lotse --set image.tag=<new release> --reuse-values
kubectl exec lotse-0 -- lotse upgrade status
```

The StatefulSet replaces one pod at a time, highest index first. Each stopping pod waits in its preStop hook for the restart turn (at most `turnWait`), hands over the groups it leads, and stops; the new pod gives the turn back once every group it is in has a quorum or has caught up, and only then is it ready, so the next pod goes after it. A changed cluster file rolls the nodes the same way.

Once every node runs the new release, an admin raises the cluster version, which lets the new release use what is new in it:

```sh
lotse upgrade finalize --config cluster.toml --cert clients/admin.pem --key clients/admin.key
```

To hold a rollout, set `updateStrategy` partition with `kubectl patch statefulset lotse -p '{"spec":{"updateStrategy":{"rollingUpdate":{"partition":3}}}}'`; `lotse upgrade hold` only makes the next pod wait up to `turnWait`. A release more than one minor version ahead of the cluster version refuses to start: upgrade one minor release at a time.

## Console

With `console.enabled=true` the chart also runs the management console ([ADR 0137](../../../docs/adr/0137-the-console-ships-in-the-release-with-https-of-its-own.md)): one pod from `ghcr.io/pierrejochem/lotse-console` and a Service `lotse-console:8443`. It serves HTTPS with the certificate in the `kubernetes.io/tls` Secret `console.tlsSecretName` and reaches this cluster at its pods' names with the cluster's CA, the only file it mounts from `tls.secretName`. `console.config` holds `[auth]` and `[metrics]`; files it names go in the Secret `console.secretName`, mounted at `/etc/lotse-console/secrets`.

```sh
kubectl create secret tls lotse-console-tls --cert=console.pem --key=console.key
helm upgrade lotse packaging/helm/lotse --reuse-values \
  --set console.enabled=true --set-file console.config=console-auth.toml
```

Expose the Service with an Ingress or a load balancer that passes TLS through or speaks HTTPS to it. The pod keeps its session key and chart history in the pod, so a restart signs users out and starts the charts afresh.

## Values

See [values.yaml](values.yaml); every value is described there. The ones you will set: `auth.admins`, `replicas` (odd, at install), `image.tag`, `persistence.size` and `persistence.storageClassName`, `resources`, and for the console `console.enabled`, `console.tlsSecretName` and `console.config`.

## Not yet

- Adding or removing nodes is not a Helm upgrade: `replicas` is the node list of the cluster file. A Kubernetes operator ([ADR 0129](../../../docs/adr/0129-a-kubernetes-operator-after-the-chart.md)) will do it.
- Every pod mounts every node's key from the one Secret. The operator will give each pod only its own.
- The operator endpoint listens on loopback in each pod, for the probes; Prometheus cannot scrape it from outside the pod yet.

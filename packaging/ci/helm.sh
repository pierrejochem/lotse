#!/bin/sh
# The Helm chart (ADR 0128) on a kind cluster: lint it, render it, make the
# certificates for the cluster file it renders with the lotse executable,
# install it with the image CI built (lotse:ci), wait for three ready nodes,
# then roll them and check that the restart turn came back. Needs helm, kind,
# kubectl, yq and lotse on the PATH (the .deb installed it), and docker.
set -eu

chart=packaging/helm/lotse
work=$(mktemp -d)
# On failure, everything needed to see why: the pods, the upgrade state and
# each node's log.
diagnose() {
    status=$?
    if [ "$status" -ne 0 ] && kind get clusters 2>/dev/null | grep -q lotse-ci; then
        kubectl get pods -o wide || true
        kubectl describe statefulset lotse || true
        kubectl exec lotse-0 -- lotse upgrade status || true
        for pod in lotse-0 lotse-1 lotse-2; do
            echo "== $pod"
            kubectl exec "$pod" -- lotse probe readyz || true
            kubectl logs "$pod" --tail 300 || true
            kubectl logs "$pod" --previous --tail 300 || true
        done
    fi
    rm -rf "$work"
    exit "$status"
}
trap diagnose EXIT
set -- --set image.repository=lotse --set image.tag=ci --set image.pullPolicy=Never \
    --set 'auth.admins={ci-admin}' --set turnWait=2m --set persistence.size=1Gi

helm lint --strict "$chart" "$@"
helm template lotse "$chart" "$@" >"$work/all.yaml"
helm template lotse "$chart" "$@" --show-only templates/configmap.yaml |
    yq '.data["cluster.toml"]' >"$work/cluster.toml"
cat "$work/cluster.toml"

# The certificates name the pods' DNS names; lotse certs writes them where the
# file says, so point it at a local directory first.
sed "s#/etc/lotse/tls#$work/tls#" "$work/cluster.toml" >"$work/local.toml"
lotse certs --config "$work/local.toml"
lotse certs --config "$work/local.toml" --client ci-admin --out "$work/admin"
lotse check --config "$work/local.toml"

kind create cluster --name lotse-ci --wait 120s
kind load docker-image lotse:ci --name lotse-ci
kubectl create secret generic lotse-tls --from-file="$work/tls"
helm install lotse "$chart" "$@" --wait --timeout 5m

status() { kubectl exec lotse-0 -- lotse upgrade status; }
status
status | grep -q 'restart turn:    free'

# A rolling restart: each pod takes the turn in its preStop hook, and the
# restarted node gives it back once ready, before the next pod goes.
kubectl rollout restart statefulset/lotse
kubectl rollout status statefulset/lotse --timeout 10m
for i in $(seq 60); do
    status | grep -q 'restart turn:    free' && break
    sleep 2
done
status
status | grep -q 'restart turn:    free'
test "$(kubectl get statefulset lotse -o jsonpath='{.status.readyReplicas}')" = 3
kind delete cluster --name lotse-ci

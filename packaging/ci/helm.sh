#!/bin/sh
# The Helm chart (ADR 0128) on a kind cluster: lint it, render it, make the
# certificates for the cluster file it renders with the lotse executable,
# install it with the images CI built (lotse:ci, lotse-console:ci), wait for
# three ready nodes and the console (ADR 0137), check the console serves
# HTTPS and lists the cluster, then roll the nodes and check that the restart
# turn came back. Needs helm, kind, kubectl, yq, openssl and lotse on the
# PATH (the .deb installed it), and docker.
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
        kubectl describe deployment lotse-console || true
        kubectl logs deployment/lotse-console --tail 300 || true
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
# The console signs every request in as ci-admin in dev mode: enough to see
# it serve and reach the cluster, not a setup for real use.
printf '[auth]\nmode = "dev"\nuser = "ci-admin"\n' >"$work/console.toml"
set -- --set image.repository=lotse --set image.tag=ci --set image.pullPolicy=Never \
    --set 'auth.admins={ci-admin}' --set turnWait=2m --set persistence.size=1Gi \
    --set console.enabled=true --set console.image.repository=lotse-console --set console.image.tag=ci \
    --set console.image.pullPolicy=Never --set-file console.config="$work/console.toml"

helm lint --strict "$chart" "$@"
helm template lotse "$chart" "$@" >"$work/all.yaml"
helm template lotse "$chart" "$@" --show-only templates/configmap.yaml |
    yq '.data["cluster.toml"]' >"$work/cluster.toml"
cat "$work/cluster.toml"
helm template lotse "$chart" "$@" --show-only templates/console.yaml | yq 'select(.kind == "ConfigMap") | .data["lotse-console.toml"]'

# The certificates name the pods' DNS names; lotse certs writes them where the
# file says, so point it at a local directory first.
sed "s#/etc/lotse/tls#$work/tls#" "$work/cluster.toml" >"$work/local.toml"
lotse certs --config "$work/local.toml"
lotse certs --config "$work/local.toml" --client ci-admin --out "$work/admin"
lotse check --config "$work/local.toml"

kind create cluster --name lotse-ci --wait 120s
kind load docker-image lotse:ci lotse-console:ci --name lotse-ci
kubectl create secret generic lotse-tls --from-file="$work/tls"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 -subj /CN=lotse-console \
    -addext subjectAltName=DNS:lotse-console,DNS:localhost -keyout "$work/console.key" -out "$work/console.pem"
kubectl create secret tls lotse-console-tls --cert="$work/console.pem" --key="$work/console.key"
helm install lotse "$chart" "$@" --wait --timeout 5m

# The console: ready (it serves its web app over HTTPS), and its API lists
# the cluster the chart configured.
kubectl rollout status deployment/lotse-console --timeout 2m
kubectl port-forward service/lotse-console 18443:8443 >/dev/null 2>&1 &
forward=$!
for i in $(seq 30); do
    curl -fsS --cacert "$work/console.pem" --resolve localhost:18443:127.0.0.1 https://localhost:18443/api/v1/clusters \
        >"$work/clusters.json" 2>/dev/null && break
    sleep 1
done
kill "$forward" || true
cat "$work/clusters.json"
grep -q '"lotse"' "$work/clusters.json"

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

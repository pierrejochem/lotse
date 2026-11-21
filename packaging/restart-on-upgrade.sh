#!/bin/sh
# Restarts the Lotse nodes running on this machine after a package upgrade,
# each only with the cluster's restart turn (ADR 0127): an update started on
# every machine at once restarts one node after another. Installed as
# /usr/lib/lotse/restart-on-upgrade; the .deb, .rpm and .apk call it after
# an upgrade.
#
# Settings, from /etc/default/lotse, /etc/sysconfig/lotse or /etc/conf.d/lotse:
#   LOTSE_RESTART_ON_UPGRADE=no   leave the nodes on the old binary
#   LOTSE_TURN_WAIT=30m           how long to wait for each node's turn
#   LOTSE_CONFIG=...              the cluster file (/etc/lotse/cluster.toml)
#
# It never fails the upgrade: a node whose turn does not come keeps running
# the old binary, and the script says how to restart it later.
set -u

for f in /etc/default/lotse /etc/sysconfig/lotse /etc/conf.d/lotse; do
    # shellcheck disable=SC1090
    [ -r "$f" ] && . "$f"
done
[ "${LOTSE_RESTART_ON_UPGRADE:-yes}" = no ] && exit 0
config=${LOTSE_CONFIG:-/etc/lotse/cluster.toml}
wait=${LOTSE_TURN_WAIT:-30m}
lotse=${LOTSE_BIN:-/usr/bin/lotse}

# restart <node id> <how to restart> <how to restart later, for the message>
restart() {
    echo "lotse: node $1: waiting for the cluster's restart turn (at most $wait)"
    if "$lotse" turn take --config "$config" --node "$1" --wait "$wait"; then
        # The restarted node gives the turn back once it is ready.
        if ! sh -c "$2"; then
            echo "lotse: node $1 did not restart; giving the turn back" >&2
            "$lotse" turn release --config "$config" --node "$1" || true
        fi
    else
        echo "lotse: node $1 keeps running the previous release; restart it later with: $3" >&2
    fi
}

if [ -d /run/systemd/system ] && command -v systemctl >/dev/null; then
    for unit in $(systemctl list-units --type=service --state=active --no-legend --plain 'lotse@*.service' 2>/dev/null | awk '{print $1}'); do
        id=${unit#lotse@}
        id=${id%.service}
        restart "$id" "systemctl restart $unit" "lotse turn take --node $id && systemctl restart $unit"
    done
elif command -v rc-service >/dev/null && [ -d /run/openrc ]; then
    for svc in /etc/init.d/lotse.*; do
        [ -e "$svc" ] || continue
        name=${svc##*/}
        rc-service "$name" status >/dev/null 2>&1 || continue
        id=${name#lotse.}
        restart "$id" "rc-service $name restart" "lotse turn take --node $id && rc-service $name restart"
    done
elif [ -x /etc/init.d/lotse ] && [ -n "${LOTSE_NODE:-}" ]; then
    if /etc/init.d/lotse status >/dev/null 2>&1; then
        restart "$LOTSE_NODE" "/etc/init.d/lotse restart" "lotse turn take --node $LOTSE_NODE && /etc/init.d/lotse restart"
    fi
fi
exit 0

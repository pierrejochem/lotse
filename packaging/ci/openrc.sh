#!/bin/sh
# In an Alpine container (ADR 0126): the .apk installs, OpenRC starts node 1
# as lotse.1 under supervise-daemon, the node answers, the upgrade script
# restarts it with the cluster's restart turn (ADR 0127), OpenRC stops it.
#
#   docker run --rm -v "$PWD:/src" -w /src alpine:3.22 sh packaging/ci/openrc.sh
set -eux
apk add --no-cache openrc >/dev/null
apk add --no-cache --allow-untrusted ./dist/lotse_*_x86_64.apk
lotse --version
getent passwd lotse
test -d /var/lib/lotse

# OpenRC in a container: no init ran, so mark the runlevel as started.
mkdir -p /run/openrc
touch /run/openrc/softlevel
sed -i 's/^#\?rc_sys=.*/rc_sys="docker"/' /etc/rc.conf

install -d /etc/lotse
install -m 0644 packaging/ci/one-node.toml /etc/lotse/cluster.toml
# The bare service refuses to start: a node needs its id.
! rc-service lotse start
ln -s lotse /etc/init.d/lotse.1
rc-service lotse.1 start
sh packaging/ci/healthy.sh /var/log/lotse/lotse.1.log
rc-service lotse.1 status
lotse probe readyz --config /etc/lotse/cluster.toml --node 1

# What the .apk's post-upgrade runs: the node restarts with the cluster's
# restart turn (ADR 0127) and gives it back once ready.
/usr/lib/lotse/restart-on-upgrade
sh packaging/ci/healthy.sh /var/log/lotse/lotse.1.log
i=0
until lotse upgrade status --config /etc/lotse/cluster.toml --node 1 | grep -q 'restart turn: *free'; do
    i=$((i + 1))
    test $i -lt 30
    sleep 1
done
rc-service lotse.1 stop
! rc-service lotse.1 status

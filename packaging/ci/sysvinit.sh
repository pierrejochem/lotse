#!/bin/sh
# In a Debian container without systemd (ADR 0126): the .deb installs, its
# init script starts the node LOTSE_NODE names, the node answers, the script
# stops it, and removing the package leaves the data.
#
#   docker run --rm -v "$PWD:/src" -w /src debian:12 sh packaging/ci/sysvinit.sh
set -eux
apt-get update -qq
apt-get install -y -qq curl >/dev/null
apt-get install -y -qq ./dist/lotse_*_amd64.deb
lotse --version
getent passwd lotse
test -d /var/lib/lotse
test -L /etc/rc2.d/S*lotse || ls /etc/rc2.d | grep -q lotse

# Nothing starts before LOTSE_NODE is set.
/etc/init.d/lotse start
! /etc/init.d/lotse status

install -d /etc/lotse
install -m 0644 packaging/ci/one-node.toml /etc/lotse/cluster.toml
sed -i 's/^LOTSE_NODE=.*/LOTSE_NODE=1/' /etc/default/lotse
/etc/init.d/lotse start
sh packaging/ci/healthy.sh /var/log/lotse/lotse.log
/etc/init.d/lotse status
test "$(stat -c %U "/proc/$(cat /run/lotse.pid)")" = lotse
/etc/init.d/lotse stop
! /etc/init.d/lotse status

apt-get remove -y -qq lotse
test -d /var/lib/lotse

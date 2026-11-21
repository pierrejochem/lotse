#!/bin/sh
# In a Fedora container (ADR 0126): the .rpm installs with its user, data
# directory and unit, the binary runs a node, and removing the package
# leaves the data.
#
#   docker run --rm -v "$PWD:/src" -w /src fedora:42 sh packaging/ci/rpm.sh
set -eux
dnf install -y -q util-linux   # setpriv; the minimal image lacks it
dnf install -y -q ./dist/lotse-*.x86_64.rpm
lotse --version
getent passwd lotse
test -d /var/lib/lotse
test -f /usr/lib/systemd/system/lotse@.service
test -f /usr/lib/sysusers.d/lotse.conf

# No systemd in the container: start the node as the unit would.
install -d /etc/lotse
install -m 0644 packaging/ci/one-node.toml /etc/lotse/cluster.toml
setpriv --reuid=lotse --regid=lotse --init-groups env LOTSE_NODE=1 LOTSE_CONFIG=/etc/lotse/cluster.toml \
    /usr/bin/lotse run >/tmp/lotse.log 2>&1 &
sh packaging/ci/healthy.sh /tmp/lotse.log
kill -TERM $!
wait $! || true

dnf remove -y -q lotse
test -d /var/lib/lotse

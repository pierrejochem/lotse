#!/bin/sh
# In a Fedora container (ADR 0137): the console's .rpm installs with its
# user, directories and unit, the binary serves the console as the unit
# would start it, and removing the package leaves config and data.
#
#   docker run --rm -v "$PWD:/src" -w /src fedora:42 sh packaging/ci/console-rpm.sh
set -eux
dnf install -y -q util-linux   # setpriv; the minimal image lacks it
dnf install -y -q ./dist/lotse-console-*.x86_64.rpm
lotse-console --version
getent passwd lotse-console
test -d /var/lib/lotse-console
test -f /usr/lib/systemd/system/lotse-console.service
test -f /usr/lib/sysusers.d/lotse-console.conf

install -m 0640 -g lotse-console packaging/ci/console.toml /etc/lotse-console/lotse-console.toml
setpriv --reuid=lotse-console --regid=lotse-console --init-groups \
    /usr/bin/lotse-console --config /etc/lotse-console/lotse-console.toml >/tmp/console.log 2>&1 &
sh packaging/ci/console-up.sh /tmp/console.log
kill -TERM $!
wait $! || true

dnf remove -y -q lotse-console
test -f /etc/lotse-console/lotse-console.toml
test -d /var/lib/lotse-console

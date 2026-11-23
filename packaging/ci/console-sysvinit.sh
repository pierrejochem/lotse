#!/bin/sh
# In a Debian container without systemd (ADR 0137): the console's .deb
# installs, its init script starts nothing without a config, then starts the
# console, which serves its web app and API; an upgrade restarts it, the
# script stops it, and removing the package leaves config and data.
#
#   docker run --rm -v "$PWD:/src" -w /src debian:12 sh packaging/ci/console-sysvinit.sh
set -eux
apt-get update -qq
apt-get install -y -qq curl >/dev/null
apt-get install -y -qq ./dist/lotse-console_*_amd64.deb
lotse-console --version
getent passwd lotse-console
test -d /var/lib/lotse-console
test -f /usr/share/doc/lotse-console/lotse-console.toml.example
ls /etc/rc2.d | grep -q lotse-console

# Nothing starts without a config.
/etc/init.d/lotse-console start
! /etc/init.d/lotse-console status

install -m 0640 -g lotse-console packaging/ci/console.toml /etc/lotse-console/lotse-console.toml
/etc/init.d/lotse-console start
sh packaging/ci/console-up.sh /var/log/lotse-console/lotse-console.log
test "$(stat -c %U "/proc/$(cat /run/lotse-console.pid)")" = lotse-console
curl -fsS http://127.0.0.1:8080/ | grep -qi '<!doctype html>'

# An upgrade restarts a running console.
before=$(cat /run/lotse-console.pid)
apt-get install -y -qq --reinstall ./dist/lotse-console_*_amd64.deb
test "$(cat /run/lotse-console.pid)" != "$before"
sh packaging/ci/console-up.sh /var/log/lotse-console/lotse-console.log
/etc/init.d/lotse-console stop
! /etc/init.d/lotse-console status

apt-get remove -y -qq lotse-console
test -f /etc/lotse-console/lotse-console.toml
test -d /var/lib/lotse-console

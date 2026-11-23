#!/bin/sh
# In an Alpine container (ADR 0137): the console's .apk installs, OpenRC
# refuses to start it without a config, then starts it under
# supervise-daemon; it serves its web app and API, and OpenRC stops it.
#
#   docker run --rm -v "$PWD:/src" -w /src alpine:3.22 sh packaging/ci/console-openrc.sh
set -eux
apk add --no-cache openrc >/dev/null
apk add --no-cache --allow-untrusted ./dist/lotse-console_*_x86_64.apk
lotse-console --version
getent passwd lotse-console
test -d /var/lib/lotse-console

mkdir -p /run/openrc
touch /run/openrc/softlevel
sed -i 's/^#\?rc_sys=.*/rc_sys="docker"/' /etc/rc.conf

! rc-service lotse-console start
install -m 0640 -g lotse-console packaging/ci/console.toml /etc/lotse-console/lotse-console.toml
rc-service lotse-console start
sh packaging/ci/console-up.sh /var/log/lotse-console/lotse-console.log
rc-service lotse-console status
rc-service lotse-console stop
! rc-service lotse-console status

#!/bin/sh
# Waits up to 30 s for the node on 127.0.0.1:9464 to answer /healthz, with
# whichever of curl and wget the image has; prints the log given on failure.
log=$1
i=0
while [ $i -lt 30 ]; do
    if command -v curl >/dev/null; then
        curl -fsS http://127.0.0.1:9464/healthz >/dev/null 2>&1 && exit 0
    else
        wget -q -O /dev/null http://127.0.0.1:9464/healthz 2>/dev/null && exit 0
    fi
    i=$((i + 1))
    sleep 1
done
echo "no answer on /healthz in 30 s" >&2
[ -n "$log" ] && cat "$log" >&2
exit 1

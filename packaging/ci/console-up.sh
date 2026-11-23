#!/bin/sh
# Waits up to 30 s for the console on 127.0.0.1:8080 to list its clusters,
# with whichever of curl and wget the image has; prints the log given on
# failure.
log=$1
i=0
while [ $i -lt 30 ]; do
    if command -v curl >/dev/null; then
        out=$(curl -fsS http://127.0.0.1:8080/api/v1/clusters 2>/dev/null) && echo "$out" | grep -q '"demo"' && exit 0
    else
        out=$(wget -q -O - http://127.0.0.1:8080/api/v1/clusters 2>/dev/null) && echo "$out" | grep -q '"demo"' && exit 0
    fi
    i=$((i + 1))
    sleep 1
done
echo "the console did not list its clusters in 30 s" >&2
[ -n "$log" ] && cat "$log" >&2
exit 1

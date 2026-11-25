#!/bin/sh
# Compares two builds of the SDK packages (ADR 0142): exits 0 only when both
# directories hold the same file names with the same bytes, and names every
# file that is missing on one side or differs.
set -eu

[ $# -eq 2 ] || { echo "usage: $0 DIR_A DIR_B" >&2; exit 2; }
a=$1
b=$2
status=0

for f in $( (cd "$a" && ls; cd "$b" && ls) | sort -u); do
    if [ ! -f "$a/$f" ]; then
        echo "only in $b: $f" >&2
        status=1
    elif [ ! -f "$b/$f" ]; then
        echo "only in $a: $f" >&2
        status=1
    elif ! cmp -s "$a/$f" "$b/$f"; then
        echo "differs: $f" >&2
        status=1
    fi
done

[ $status -eq 0 ] && echo "identical: $(ls "$a" | wc -l) files"
exit $status

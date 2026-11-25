#!/bin/sh
# Prints the version each SDK package will carry, one line per package:
# "<sdk> <version>". Given a version, it also checks every line against it
# and exits 1 naming those that differ. The release workflow runs it with the
# tag's version, so a tag whose SDKs were not bumped fails before anything is
# built (ADR 0142).
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)

gradle_version() {
    sed -n 's/^version = "\(.*\)"$/\1/p' "$1" | head -1
}

csproj_version() {
    sed -n 's|.*<Version>\(.*\)</Version>.*|\1|p' "$1" | head -1
}

{
    echo "java $(gradle_version "$root/sdk/java/build.gradle.kts")"
    echo "kotlin $(gradle_version "$root/sdk/kotlin/build.gradle.kts")"
    echo "kotlin-core-dependency $(sed -n 's/.*"dev\.lotse:lotse-client:\([^"]*\)".*/\1/p' "$root/sdk/kotlin/build.gradle.kts" | head -1)"
    echo "csharp $(csproj_version "$root/sdk/csharp/src/Lotse.Client/Lotse.Client.csproj")"
    echo "csharp-codecs $(csproj_version "$root/sdk/csharp/src/Lotse.Client.Codecs/Lotse.Client.Codecs.csproj")"
    echo "node $(sed -n 's/^  "version": "\(.*\)",$/\1/p' "$root/sdk/node/package.json" | head -1)"
} > "${TMPDIR:-/tmp}/sdk-versions.$$"

cat "${TMPDIR:-/tmp}/sdk-versions.$$"
status=0
if [ $# -gt 0 ]; then
    while read -r sdk version; do
        if [ "$version" != "$1" ]; then
            echo "$sdk is at '${version}', not $1" >&2
            status=1
        fi
    done < "${TMPDIR:-/tmp}/sdk-versions.$$"
fi
rm -f "${TMPDIR:-/tmp}/sdk-versions.$$"
exit $status

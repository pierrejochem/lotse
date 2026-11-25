#!/bin/sh
# Builds one SDK's release packages into a directory, flat (ADR 0142):
#
#   packaging/sdk-package.sh java   OUT   lotse-client-<v>.jar, -sources.jar, -javadoc.jar, .pom, .module
#   packaging/sdk-package.sh kotlin OUT   the same for lotse-client-kotlin
#   packaging/sdk-package.sh csharp OUT   Lotse.Client and Lotse.Client.Codecs .nupkg and .snupkg
#   packaging/sdk-package.sh node   OUT   pierrejochem-lotse-client-<v>.tgz
#
# The same commit gives the same bytes: the release workflow runs it twice
# and compares the two directories with sdk-compare.sh. It builds; it does
# not test (the workflow runs each SDK's tests first) and does not publish.
set -eu

usage() {
    echo "usage: $0 java|kotlin|csharp|node OUTDIR" >&2
    exit 2
}
[ $# -eq 2 ] || usage
sdk=$1
mkdir -p "$2"
out=$(cd "$2" && pwd)
root=$(cd "$(dirname "$0")/.." && pwd)

# Gradle publishes into a throwaway Maven repository; its files, without the
# local repository's own metadata, are the release files.
gradle_package() {
    dir=$1
    artifact=$2
    repo=$(mktemp -d)
    (cd "$root/sdk/$dir" && ./gradlew --quiet --no-daemon publishToMavenLocal -Dmaven.repo.local="$repo")
    version=$("$root/packaging/sdk-versions.sh" | sed -n "s/^$dir //p")
    cp "$repo/dev/lotse/$artifact/$version/$artifact-$version"* "$out/"
    rm -rf "$repo"
}

# Zip entry times and the like come from the commit, not the clock.
SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(git -C "$root" log -1 --pretty=%ct)}
export SOURCE_DATE_EPOCH

csharp_package() {
    version=$("$root/packaging/sdk-versions.sh" | sed -n 's/^csharp //p')
    for project in Lotse.Client Lotse.Client.Codecs; do
        dotnet pack "$root/sdk/csharp/src/$project/$project.csproj" -c Release -o "$out" --nologo -v quiet \
            -p:ContinuousIntegrationBuild=true -p:DeterministicTimestamp="$SOURCE_DATE_EPOCH"
    done
    python3 "$root/packaging/nupkg-normalise.py" "$out"/*.nupkg "$out"/*.snupkg
    # The codecs are built against one release of the client and must say so exactly.
    dependency=$(unzip -p "$out/Lotse.Client.Codecs.$version.nupkg" Lotse.Client.Codecs.nuspec |
        sed -n 's/.*<dependency id="Lotse.Client" version="\([^"]*\)".*/\1/p')
    if [ "$dependency" != "[$version]" ]; then
        echo "Lotse.Client.Codecs depends on Lotse.Client '$dependency', not [$version]" >&2
        exit 1
    fi
}

# npm pack writes fixed times and a sorted file list by itself; the build must
# start clean so no stale output from another commit lands in the tarball.
node_package() {
    rm -rf "$root/sdk/node/dist"
    (cd "$root/sdk/node" && npm ci --no-audit --no-fund --loglevel=error && npm run --silent build &&
        npm pack --silent --pack-destination "$out" > /dev/null)
}

case "$sdk" in
java) gradle_package java lotse-client ;;
kotlin) gradle_package kotlin lotse-client-kotlin ;;
csharp) csharp_package ;;
node) node_package ;;
*) usage ;;
esac

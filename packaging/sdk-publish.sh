#!/usr/bin/env bash
# Publishes the SDK packages of a release (ADR 0142):
#
#   packaging/sdk-publish.sh [--dry-run] TAG DIR
#
# DIR holds every package the build jobs made and compared (sdk-package.sh,
# sdk-compare.sh). In order:
#
# 1. The release assets are the record. A package the release already holds
#    (a re-run) must have the same bytes, since the builds are reproducible;
#    one it lacks is uploaded. SHA256SUMS-sdk goes up last, so a release that
#    has it has every package, and an existing one must match too.
# 2. GitHub Packages: Java, then Kotlin (its POM names the Java package), C#,
#    Node.js. A registry that already holds the version counts as done: the
#    checksums above matched, so it holds these bytes.
# 3. Maven Central, nuget.org and npm, each only when its repository variable
#    (PUBLISH_MAVEN_CENTRAL, PUBLISH_NUGET, PUBLISH_NPM) is "true" and its
#    secret is set.
#
# Needs gh, a JDK, the .NET SDK and npm on PATH, GITHUB_TOKEN and
# GITHUB_ACTOR, and runs from a checkout of TAG. --dry-run prints what it
# would upload and publish and changes nothing.
set -euo pipefail

dry=
if [ "${1:-}" = "--dry-run" ]; then
    dry=1
    shift
fi
[ $# -eq 2 ] || { echo "usage: $0 [--dry-run] TAG DIR" >&2; exit 2; }
tag=$1
dir=$(cd "$2" && pwd)
version=${tag#v}
root=$(cd "$(dirname "$0")/.." && pwd)
repo=${GITHUB_REPOSITORY:-pierrejochem/lotse}
owner=${repo%%/*}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

run() {
    if [ -n "$dry" ]; then
        echo "would run: $*"
    else
        "$@"
    fi
}

# A publish whose output says the version is already there is done; any
# other failure is a failure.
already_there() {
    # Only the registries' own words for it: a bare "409" also turns up in log
    # file names and line numbers.
    grep -qE 'status code 409|409 Conflict|code E409|EPUBLISHCONFLICT|[Cc]annot publish over|already exists' "$1"
}

publish_or_exists() {
    local what=$1
    shift
    if [ -n "$dry" ]; then
        echo "would run: $*"
        return
    fi
    if "$@" > "$work/out" 2>&1; then
        echo "published $what"
    elif already_there "$work/out"; then
        echo "$what $version is already there"
    else
        cat "$work/out" >&2
        exit 1
    fi
}

# 1. The release assets.
(cd "$dir" && sha256sum -- * | LC_ALL=C sort -k2) > "$work/SHA256SUMS-sdk"
gh release view "$tag" --repo "$repo" --json assets --jq '.assets[].name' > "$work/assets"
for package in "$dir"/* "$work/SHA256SUMS-sdk"; do
    name=$(basename "$package")
    if grep -qxF "$name" "$work/assets"; then
        rm -rf "$work/existing"
        gh release download "$tag" --repo "$repo" --pattern "$name" --dir "$work/existing"
        if ! cmp -s "$package" "$work/existing/$name"; then
            echo "the release already holds $name, and it differs from this build; a re-run must build what the release holds" >&2
            exit 1
        fi
        echo "the release already holds $name"
    else
        run gh release upload "$tag" --repo "$repo" "$package"
    fi
done

# 2. GitHub Packages.
#
# Gradle cannot upload files it did not build, so it builds again into a
# local repository, the result is checked against the compared files, and
# only then does Gradle upload what it just built: the same bytes.
gradle_publish() {
    local project=$1 artifact=$2 local_repo="$work/m2-$1"
    (cd "$root/sdk/$project" && ${GRADLEW:-./gradlew} --quiet --no-daemon publishToMavenLocal -Dmaven.repo.local="$local_repo")
    for f in "$local_repo/dev/lotse/$artifact/$version/$artifact-$version"*; do
        cmp -s "$f" "$dir/$(basename "$f")" || { echo "rebuilt $(basename "$f") differs from the compared one" >&2; exit 1; }
    done
    publish_or_exists "$artifact (GitHub Packages)" \
        sh -c "cd '$root/sdk/$project' && ${GRADLEW:-./gradlew} --quiet --no-daemon publishMavenPublicationToGitHubPackagesRepository"
    # Gradle uploads a version file by file, and the first one already there
    # stops it. A version an earlier run left without its POM would count as
    # done, so the POM, uploaded after the jars, must be there.
    if [ -z "$dry" ] && ! curl -fsS -o /dev/null -u "${GITHUB_ACTOR:-x}:${GITHUB_TOKEN:-}" \
        "https://maven.pkg.github.com/$repo/dev/lotse/$artifact/$version/$artifact-$version.pom"; then
        echo "GitHub Packages holds $artifact $version without its POM: delete that version and run again" >&2
        exit 1
    fi
}
gradle_publish java lotse-client
gradle_publish kotlin lotse-client-kotlin

for package in "$dir"/*.nupkg; do
    # GitHub Packages takes no symbol packages; the .snupkg files are release assets only.
    publish_or_exists "$(basename "$package") (GitHub Packages)" \
        dotnet nuget push "$package" --source "https://nuget.pkg.github.com/$owner/index.json" \
        --api-key "${GITHUB_TOKEN:-}" --skip-duplicate --no-symbols
done

printf '//npm.pkg.github.com/:_authToken=%s\n' "${GITHUB_TOKEN:-}" > "$work/npmrc"
for package in "$dir"/*.tgz; do
    publish_or_exists "$(basename "$package") (GitHub Packages)" \
        npm publish "$package" --userconfig "$work/npmrc" --registry https://npm.pkg.github.com
done

# 3. The public registries, switched off until their variable and secret exist.
if [ "${PUBLISH_MAVEN_CENTRAL:-}" = "true" ] && [ -n "${MAVEN_CENTRAL_TOKEN:-}" ] && [ -n "${MAVEN_GPG_KEY:-}" ]; then
    # A bundle for the Central Portal: every file signed and summed, laid out
    # as a Maven repository.
    export GNUPGHOME="$work/gnupg"
    mkdir -m 700 "$GNUPGHOME"
    printf '%s\n' "$MAVEN_GPG_KEY" | gpg --batch --import
    for artifact in lotse-client lotse-client-kotlin; do
        bundle="$work/bundle-$artifact"
        path="$bundle/dev/lotse/$artifact/$version"
        mkdir -p "$path"
        cp "$dir/$artifact-$version"* "$path/"
        rm -f "$path/$artifact-$version"*.nupkg
        for f in "$path"/*; do
            gpg --batch --yes --pinentry-mode loopback --passphrase "${MAVEN_GPG_PASSPHRASE:-}" -ab "$f"
            md5sum "$f" | cut -d' ' -f1 > "$f.md5"
            sha1sum "$f" | cut -d' ' -f1 > "$f.sha1"
        done
        (cd "$bundle" && zip -qr "$work/$artifact.zip" dev)
        publish_or_exists "$artifact (Maven Central)" \
            curl --fail-with-body -sS -H "Authorization: Bearer $MAVEN_CENTRAL_TOKEN" \
            -F "bundle=@$work/$artifact.zip" \
            "https://central.sonatype.com/api/v1/publisher/upload?name=$artifact-$version&publishingType=AUTOMATIC"
    done
fi

if [ "${PUBLISH_NUGET:-}" = "true" ] && [ -n "${NUGET_API_KEY:-}" ]; then
    for package in "$dir"/*.nupkg; do
        publish_or_exists "$(basename "$package") (nuget.org)" \
            dotnet nuget push "$package" --source https://api.nuget.org/v3/index.json \
            --api-key "$NUGET_API_KEY" --skip-duplicate
    done
fi

if [ "${PUBLISH_NPM:-}" = "true" ] && [ -n "${NPM_TOKEN:-}" ]; then
    printf '//registry.npmjs.org/:_authToken=%s\n' "$NPM_TOKEN" > "$work/npmrc-public"
    for package in "$dir"/*.tgz; do
        publish_or_exists "$(basename "$package") (npm)" \
            npm publish "$package" --userconfig "$work/npmrc-public" --registry https://registry.npmjs.org --access public
    done
fi

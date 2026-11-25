#!/usr/bin/env bash
# Tests sdk-publish.sh against stand-ins for gh, Gradle, dotnet and npm
# (ADR 0142): a first publish, a re-run with the same packages, a re-run
# with different ones, a registry that already holds the version, and a
# registry that fails. Run by the SDK release workflow on pull requests.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT
v=9.8.7

mkdir -p "$t/bin" "$t/pkg" "$t/release"
for f in lotse-client-$v.jar lotse-client-$v.pom lotse-client-kotlin-$v.jar lotse-client-kotlin-$v.pom \
    Lotse.Client.$v.nupkg Lotse.Client.$v.snupkg pierrejochem-lotse-client-$v.tgz; do
    echo "$f" > "$t/pkg/$f"
done

# gh: a release whose assets live in $t/release.
cat > "$t/bin/gh" <<EOF
#!/usr/bin/env bash
echo "gh \$*" >> "$t/log"
case "\$1 \$2" in
"release view") ls "$t/release" ;;
"release download") mkdir -p "\${@: -1}"; cp "$t/release/SHA256SUMS-sdk" "\${@: -1}/" ;;
"release upload") shift 4; for f in "\$@"; do [ "\$f" = --repo ] || cp "\$f" "$t/release/" 2>/dev/null || true; done ;;
esac
EOF
# Gradle: "builds" by copying the compared files into the local repository,
# and uploads as the scenario says.
cat > "$t/bin/gradlew" <<EOF
#!/usr/bin/env bash
echo "gradlew in \$(basename "\$PWD"): \$*" >> "$t/log"
for a in "\$@"; do
    case "\$a" in
    -Dmaven.repo.local=*)
        r=\${a#-Dmaven.repo.local=}
        for art in lotse-client lotse-client-kotlin; do
            mkdir -p "\$r/dev/lotse/\$art/$v"
            cp "$t/pkg/\$art-$v".* "\$r/dev/lotse/\$art/$v/"
        done ;;
    publishMavenPublicationToGitHubPackagesRepository)
        [ "\${SCENARIO:-}" = exists ] && { echo "Received status code 409 from server: Conflict"; exit 1; } ;;
    esac
done
exit 0
EOF
cat > "$t/bin/dotnet" <<EOF
#!/usr/bin/env bash
echo "dotnet \$*" >> "$t/log"
EOF
cat > "$t/bin/npm" <<EOF
#!/usr/bin/env bash
echo "npm \$*" >> "$t/log"
case "\${SCENARIO:-}" in
exists) echo "npm error code E409"; exit 1 ;;
broken) echo "npm error code E500 Internal Server Error"; exit 1 ;;
esac
EOF
chmod +x "$t/bin"/*
export PATH="$t/bin:$PATH" GRADLEW="$t/bin/gradlew" GITHUB_TOKEN=token GITHUB_ACTOR=someone

fail() { echo "FAIL: $*" >&2; exit 1; }
publish() { : > "$t/log"; "$here/sdk-publish.sh" "v$v" "$t/pkg" > "$t/out" 2>&1; }

# (a) No SDK assets yet: upload all, then publish to every registry.
publish || { cat "$t/out"; fail "a first publish failed"; }
grep -q "gh release upload" "$t/log" || fail "a first publish uploaded nothing"
[ -f "$t/release/SHA256SUMS-sdk" ] || fail "no SHA256SUMS-sdk on the release"
grep -q "publishMavenPublicationToGitHubPackagesRepository" "$t/log" || fail "no Maven publish"
[ "$(grep -c 'dotnet nuget push' "$t/log")" = 1 ] || fail "expected one nupkg push (no snupkg)"
grep -q "npm publish" "$t/log" || fail "no npm publish"
[ "$(grep "publishMavenPublicationToGitHubPackagesRepository" "$t/log" | sed 's/gradlew in \([a-z]*\):.*/\1/' | tr '\n' ' ')" = "java kotlin " ] ||
    fail "the Maven packages were not published Java first, then Kotlin"

# (b) A re-run with the same packages, and the registries already hold them.
SCENARIO=exists publish || { cat "$t/out"; fail "a re-run with the same packages failed"; }
grep -q "gh release upload" "$t/log" && fail "a re-run uploaded again"
grep -q "already there" "$t/out" || fail "an existing version was not counted as done"

# (c) A re-run with different packages: refused before anything is published.
echo changed > "$t/pkg/Lotse.Client.$v.nupkg"
if publish; then fail "different packages were accepted"; fi
grep -q "differ" "$t/out" || fail "the refusal does not say the packages differ"
grep -q "dotnet nuget push" "$t/log" && fail "published after the checksums differed"
echo "Lotse.Client.$v.nupkg" > "$t/pkg/Lotse.Client.$v.nupkg"

# (d) A registry that fails for another reason fails the run.
if SCENARIO=broken publish; then fail "a broken registry was counted as done"; fi
grep -q "E500" "$t/out" || fail "the registry's error is not shown"

echo "sdk-publish.sh: all cases pass"

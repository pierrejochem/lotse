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

# gh: a release whose assets live in $t/release. Like the real one, an upload
# of a name the release already holds fails.
cat > "$t/bin/gh" <<EOF
#!/usr/bin/env bash
echo "gh \$*" >> "$t/log"
case "\$1 \$2" in
"release view") ls "$t/release" ;;
"release download")
    pattern=; dir=
    while [ \$# -gt 0 ]; do
        case "\$1" in --pattern) pattern=\$2; shift ;; --dir) dir=\$2; shift ;; esac
        shift
    done
    mkdir -p "\$dir"; cp "$t/release/\$pattern" "\$dir/" ;;
"release upload")
    shift 2
    for f in "\$@"; do
        [ -f "\$f" ] || continue
        if [ -e "$t/release/\$(basename "\$f")" ]; then echo "asset already exists: \$(basename "\$f")" >&2; exit 1; fi
        [ "\${SCENARIO:-}" = dies ] && case "\$f" in *.tgz) echo "connection reset" >&2; exit 1 ;; esac
        cp "\$f" "$t/release/"
    done ;;
esac
EOF
# curl: GitHub Packages answers for the POM unless the scenario lost it.
cat > "$t/bin/curl" <<EOF
#!/usr/bin/env bash
echo "curl \$*" >> "$t/log"
[ "\${SCENARIO:-}" = pom-missing ] && exit 22
exit 0
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
        case "\${SCENARIO:-}" in exists|pom-missing) echo "Received status code 409 from server: Conflict"; exit 1 ;; esac ;;
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
noisy) echo "npm error code E401 Unauthorized"; echo "npm error A complete log is in /home/runner/.npm/_logs/2026-11-25T10_31_02_409Z-debug-0.log"; exit 1 ;;
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

# (e) An earlier run uploaded some packages and died before the sums: the
# re-run checks those, uploads the rest and the sums, and goes on.
rm -f "$t/release"/*
cp "$t/pkg/lotse-client-$v.jar" "$t/release/"
publish || { cat "$t/out"; fail "a re-run after a partial upload failed"; }
for f in "$t/pkg"/*; do [ -f "$t/release/$(basename "$f")" ] || fail "$(basename "$f") is missing from the release"; done
[ -f "$t/release/SHA256SUMS-sdk" ] || fail "no SHA256SUMS-sdk after the re-run"

# (e2) A run that dies while uploading leaves no SHA256SUMS-sdk behind, so a
# release that has the sums has every package.
rm -f "$t/release"/*
if SCENARIO=dies publish; then fail "an upload that died was counted as done"; fi
[ -f "$t/release/SHA256SUMS-sdk" ] && fail "SHA256SUMS-sdk went up before every package"
publish || { cat "$t/out"; fail "the re-run after a died upload failed"; }

# (f) The sums are there but a package is not: the re-run uploads it.
rm -f "$t/release/Lotse.Client.$v.snupkg"
publish || { cat "$t/out"; fail "a re-run with a package missing from the release failed"; }
[ -f "$t/release/Lotse.Client.$v.snupkg" ] || fail "a package missing from the release was not uploaded again"

# (g) A package on the release whose bytes differ is refused.
echo tampered > "$t/release/Lotse.Client.$v.snupkg"
if publish; then fail "a release asset with other bytes was accepted"; fi
cp "$t/pkg/Lotse.Client.$v.snupkg" "$t/release/"

# (h) A failure whose output happens to contain 409 (a log file's time) is a failure.
if SCENARIO=noisy publish; then fail "an npm failure with 409 in a log path was counted as done"; fi

# (i) GitHub Packages says the Maven version exists but holds no POM: a partial
# upload from an earlier run, which must not count as done.
if SCENARIO=pom-missing publish; then fail "a Maven version without its POM was counted as done"; fi
grep -q "POM" "$t/out" || fail "the refusal does not name the missing POM"

echo "sdk-publish.sh: all cases pass"

#!/bin/sh
# Builds a release (ADR 0126): for x86_64 and aarch64, the static `lotse`
# binary, a tarball with the binary, the systemd, OpenRC and sysvinit
# scripts, and the .deb, .rpm and .apk packages; then BUILDINFO and
# SHA256SUMS. The build is reproducible: the same commit with the tools of
# packaging/release.env gives the same bytes, wherever it runs.
#
#   packaging/release.sh 0.4.0                  # both architectures into dist/
#   packaging/release.sh --arch arm64 0.4.0     # one
#   packaging/release.sh --out /tmp/r 0.4.0     # elsewhere
#   packaging/release.sh --verify SHA256SUMS 0.4.0
#
# --verify builds into a temporary directory and compares with the given
# SHA256SUMS of a published release, file by file; check out the release's
# tag first. Install the tools with packaging/tools.sh.
set -eu

usage() {
    echo "usage: $0 [--arch amd64|arm64|all] [--out DIR] [--verify SHA256SUMS] VERSION" >&2
    exit 2
}

arch=all
out=
verify=
while [ $# -gt 1 ]; do
    case $1 in
        --arch) arch=$2; shift 2 ;;
        --out) out=$2; shift 2 ;;
        --verify) verify=$2; shift 2 ;;
        *) usage ;;
    esac
done
[ $# -eq 1 ] || usage
version=${1#v}
case $version in
    [0-9]*.[0-9]*.[0-9]*) ;;
    *) echo "VERSION must look like 1.2.3 or 1.2.3-rc.1, not '$1'" >&2; exit 2 ;;
esac
case $arch in amd64 | arm64 | all) ;; *) usage ;; esac

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
. packaging/release.env

if [ -n "$verify" ]; then
    verify=$(cd "$(dirname "$verify")" && pwd)/$(basename "$verify")
    [ -f "$verify" ] || { echo "no file $verify" >&2; exit 2; }
    out=$(mktemp -d)
    trap 'rm -rf "$out"' EXIT
fi
out=${out:-$root/dist}
mkdir -p "$out"
out=$(cd "$out" && pwd)

# The tools. Other versions build too, but not the published bytes.
fail=
need() { # name, wanted, found
    if [ "$2" != "$3" ]; then
        echo "$1 $3 found, release.env pins $2" >&2
        fail=1
    fi
}
export RUSTUP_TOOLCHAIN="$RUST_VERSION"
need rustc "$RUST_VERSION" "$(rustc --version | cut -d' ' -f2)"
zig=$(command -v zig || python3 -c 'import os, ziglang; print(os.path.join(os.path.dirname(ziglang.__file__), "zig"))')
need zig "$ZIG_VERSION" "$("$zig" version)"
need cargo-zigbuild "$CARGO_ZIGBUILD_VERSION" "$(cargo-zigbuild --version | cut -d' ' -f2)"
need nfpm "$NFPM_VERSION" "$(nfpm --version 2>&1 | sed -n 's/^GitVersion: *//p')"
if [ -n "$fail" ]; then
    if [ "${LOTSE_RELEASE_UNPINNED:-}" = 1 ]; then
        echo "LOTSE_RELEASE_UNPINNED=1: building anyway; the bytes will differ from a release" >&2
    else
        echo "install the pinned tools with packaging/tools.sh, or set LOTSE_RELEASE_UNPINNED=1" >&2
        exit 1
    fi
fi
export CARGO_ZIGBUILD_ZIG_PATH="$zig"

# Every timestamp in every file is the commit's time.
commit=$(git rev-parse HEAD)
SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct)}
export SOURCE_DATE_EPOCH
dirty=no
[ -z "$(git status --porcelain --untracked-files=no)" ] || dirty=yes
if [ "$dirty" = yes ]; then echo "warning: the working tree has changes; the build is not of $commit" >&2; fi

# No path of this machine ends up in a binary: sources map to /lotse and
# /cargo, debug info is stripped (the symbols stay, for backtraces).
cargo_home=${CARGO_HOME:-$HOME/.cargo}
export CARGO_TARGET_DIR="$root/target/release-dist"
export RUSTFLAGS="--remap-path-prefix=$root=/lotse --remap-path-prefix=$cargo_home=/cargo"
export CFLAGS="-ffile-prefix-map=$root=/lotse -ffile-prefix-map=$cargo_home=/cargo"
export CARGO_PROFILE_RELEASE_STRIP=debuginfo
export CARGO_PROFILE_RELEASE_DEBUG=0

case $arch in
    all) arches="amd64 arm64" ;;
    *) arches=$arch ;;
esac

for a in $arches; do
    case $a in
        amd64) target=x86_64-unknown-linux-musl ;;
        arm64) target=aarch64-unknown-linux-musl ;;
    esac
    cargo zigbuild --release --locked -p lotse-node --target "$target"
    bin="$out/lotse-$version-linux-$a"
    install -m 0755 "$CARGO_TARGET_DIR/$target/release/lotse" "$bin"
    # A static binary has no program interpreter.
    if LC_ALL=C readelf -l "$bin" 2>/dev/null | grep -q 'Requesting program interpreter'; then
        echo "$bin is not statically linked" >&2
        exit 1
    fi
    python3 packaging/tarball.py "$bin" "$version" "$a" "$out"
    for format in deb rpm apk; do
        VERSION=$version ARCH=$a BINARY=$bin nfpm package --config packaging/nfpm.yaml \
            --packager "$format" --target "$out/"
    done
done

{
    echo "version: $version"
    echo "commit: $commit"
    echo "dirty: $dirty"
    echo "source_date_epoch: $SOURCE_DATE_EPOCH"
    echo "rust: $(rustc --version | cut -d' ' -f2)"
    echo "zig: $("$zig" version)"
    echo "cargo-zigbuild: $(cargo-zigbuild --version | cut -d' ' -f2)"
    echo "nfpm: $(nfpm --version 2>&1 | sed -n 's/^GitVersion: *//p')"
    echo "zlib: $(python3 -c 'import zlib; print(zlib.ZLIB_RUNTIME_VERSION)')"
} >"$out/BUILDINFO"

(cd "$out" && find . -maxdepth 1 -type f ! -name SHA256SUMS -printf '%f\n' | LC_ALL=C sort | xargs sha256sum >SHA256SUMS)

if [ -n "$verify" ]; then
    # Compare only the files this build made: a release of one architecture
    # is checked against those lines of the published sums.
    status=0
    while read -r sum name; do
        published=$(awk -v n="$name" '$2 == n { print $1 }' "$verify")
        if [ -z "$published" ]; then
            echo "not in $verify: $name"
            status=1
        elif [ "$published" = "$sum" ]; then
            echo "same: $name"
        else
            echo "differs: $name"
            status=1
        fi
    done <"$out/SHA256SUMS"
    exit $status
fi
echo "release $version in $out:"
cat "$out/SHA256SUMS"

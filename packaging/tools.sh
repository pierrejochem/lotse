#!/bin/sh
# Installs the tools of packaging/release.env (ADR 0126): the Rust toolchain
# with both musl targets, zig (from PyPI), cargo-zigbuild and nfpm (checked
# against its checksum). Needs rustup, cargo, pip and curl. Node.js
# (NODE_VERSION, for the console's web app, ADR 0137) is not installed here:
# take it from nodejs.org or a version manager; CI uses actions/setup-node.
#
#   packaging/tools.sh            # nfpm goes to ~/.local/bin
#   NFPM_DIR=/usr/local/bin packaging/tools.sh
set -eu

here=$(cd "$(dirname "$0")" && pwd)
. "$here/release.env"

rustup toolchain install "$RUST_VERSION" --profile minimal \
    --target x86_64-unknown-linux-musl --target aarch64-unknown-linux-musl

# --break-system-packages: Debian and Ubuntu refuse a user install otherwise.
pip install --user --break-system-packages "ziglang==$ZIG_VERSION" 2>/dev/null \
    || pip install --user "ziglang==$ZIG_VERSION"

cargo install --locked "cargo-zigbuild@$CARGO_ZIGBUILD_VERSION"

case $(uname -m) in
    x86_64) nfpm_arch=x86_64 nfpm_sum=$NFPM_SHA256_X86_64 ;;
    aarch64 | arm64) nfpm_arch=arm64 nfpm_sum=$NFPM_SHA256_ARM64 ;;
    *) echo "no nfpm build for $(uname -m)" >&2; exit 1 ;;
esac
dir=${NFPM_DIR:-$HOME/.local/bin}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fsSL -o "$tmp/nfpm.tar.gz" \
    "https://github.com/goreleaser/nfpm/releases/download/v$NFPM_VERSION/nfpm_${NFPM_VERSION}_Linux_$nfpm_arch.tar.gz"
echo "$nfpm_sum  $tmp/nfpm.tar.gz" | sha256sum -c --quiet
tar -xzf "$tmp/nfpm.tar.gz" -C "$tmp" nfpm
mkdir -p "$dir"
install -m 0755 "$tmp/nfpm" "$dir/nfpm"
node_found=$(node --version 2>/dev/null || echo none)
[ "$node_found" = "v$NODE_VERSION" ] || echo "note: Node.js $NODE_VERSION is needed for the console, found $node_found" >&2
echo "installed: Rust $RUST_VERSION, zig $ZIG_VERSION, cargo-zigbuild $CARGO_ZIGBUILD_VERSION, nfpm $NFPM_VERSION in $dir"

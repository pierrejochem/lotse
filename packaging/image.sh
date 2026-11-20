#!/bin/sh
# Builds the release image for linux/amd64 and linux/arm64 into an OCI
# archive (ADR 0126), from the binaries packaging/release.sh wrote to DIST and
# on BASE, a distroless image named by its digest. Every timestamp is the
# commit's and no attestation is added, so the same binaries on the same base
# give the same index digest; it is printed, and written to $GITHUB_OUTPUT
# as `digest` when that is set. Needs docker buildx with a builder that
# exports OCI archives (docker buildx create --use).
#
#   packaging/image.sh dist gcr.io/distroless/static-debian12@sha256:... image.tar
set -eu
[ $# -eq 3 ] || { echo "usage: $0 DIST BASE ARCHIVE" >&2; exit 2; }
dist=$1 base=$2 archive=$3
case $base in
    *@sha256:*) ;;
    *) echo "BASE must name a digest (…@sha256:…), not a tag: $base" >&2; exit 2 ;;
esac
version=$(sed -n 's/^version: //p' "$dist/BUILDINFO")
SOURCE_DATE_EPOCH=$(sed -n 's/^source_date_epoch: //p' "$dist/BUILDINFO")
export SOURCE_DATE_EPOCH

docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --file packaging/Dockerfile.prebuilt \
    --build-arg BASE="$base" \
    --build-arg VERSION="$version" \
    --build-arg DIST="$dist" \
    --provenance=false --sbom=false \
    --output "type=oci,dest=$archive,rewrite-timestamp=true" \
    .

digest=$(tar -xOf "$archive" index.json | python3 -c 'import json, sys; print(json.load(sys.stdin)["manifests"][0]["digest"])')
echo "image index digest: $digest"
if [ -n "${GITHUB_OUTPUT:-}" ]; then echo "digest=$digest" >>"$GITHUB_OUTPUT"; fi

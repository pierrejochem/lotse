#!/bin/sh
# Builds a release image for linux/amd64 and linux/arm64 into an OCI archive
# (ADR 0126), from the binaries packaging/release.sh wrote to DIST and on
# BASE, a distroless image named by its digest: the node's image, or with
# PROGRAM lotse-console the console's (ADR 0137). Every timestamp is the
# commit's and no attestation is added, so the same binaries on the same base
# give the same index digest; it is printed, and written to $GITHUB_OUTPUT
# when that is set, as `digest` for the node's image and `console_digest`
# for the console's. Needs docker buildx with a builder that
# exports OCI archives (docker buildx create --use).
#
#   packaging/image.sh dist gcr.io/distroless/static-debian12@sha256:... image.tar
#   packaging/image.sh dist gcr.io/distroless/static-debian12@sha256:... console.tar lotse-console
set -eu
[ $# -eq 3 ] || [ $# -eq 4 ] || { echo "usage: $0 DIST BASE ARCHIVE [lotse|lotse-console]" >&2; exit 2; }
dist=$1 base=$2 archive=$3 program=${4:-lotse}
case $program in
    lotse) dockerfile=packaging/Dockerfile.prebuilt ;;
    lotse-console) dockerfile=packaging/Dockerfile.console.prebuilt ;;
    *) echo "PROGRAM is lotse or lotse-console, not $program" >&2; exit 2 ;;
esac
case $base in
    *@sha256:*) ;;
    *) echo "BASE must name a digest (…@sha256:…), not a tag: $base" >&2; exit 2 ;;
esac
version=$(sed -n 's/^version: //p' "$dist/BUILDINFO")
SOURCE_DATE_EPOCH=$(sed -n 's/^source_date_epoch: //p' "$dist/BUILDINFO")
export SOURCE_DATE_EPOCH

docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --file "$dockerfile" \
    --build-arg BASE="$base" \
    --build-arg VERSION="$version" \
    --build-arg DIST="$dist" \
    --provenance=false --sbom=false \
    --output "type=oci,dest=$archive,rewrite-timestamp=true" \
    .

digest=$(tar -xOf "$archive" index.json | python3 -c 'import json, sys; print(json.load(sys.stdin)["manifests"][0]["digest"])')
echo "$program image index digest: $digest"
key=digest
[ "$program" = lotse ] || key=console_digest
if [ -n "${GITHUB_OUTPUT:-}" ]; then echo "$key=$digest" >>"$GITHUB_OUTPUT"; fi

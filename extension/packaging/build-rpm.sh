#!/usr/bin/env bash
# Package the already-built (dynamic) paimon_heap.so as an RPM with nfpm.
#
# The AWS SDK and libhdfs3 are not available as EL packages, so their shared
# libraries are bundled into /usr/pgsql-18/lib/paimon_heap-deps and found via
# $ORIGIN-relative RPATHs. Every other shared dependency becomes an RPM
# soname requirement computed from the ELF NEEDED entries.
#
# Usage: build-rpm.sh <version> <arch: amd64|arm64> <out-dir>
# Env:   AWS_SDK_ROOT (default /opt/aws-sdk-cpp), HDFS3_ROOT (default /usr/local)
set -euo pipefail

VERSION="${1#v}"
ARCH="$2"
OUT="$(realpath -m "$3")"

AWS_SDK_ROOT="${AWS_SDK_ROOT:-/opt/aws-sdk-cpp}"
HDFS3_ROOT="${HDFS3_ROOT:-/usr/local}"
HERE="$(cd "$(dirname "$0")" && pwd)"
EXT="$(dirname "$HERE")"

STAGE="$(mktemp -d)"
DEPS="$STAGE/deps"
mkdir -p "$DEPS" "$OUT"

cp "$EXT/paimon_heap.so" "$EXT/paimon_heap.control" "$EXT/paimon_heap--1.0.sql" "$STAGE/"

# Bundle shared libs under their SONAME (resolves symlink chains to real files).
bundle() {
  local f soname
  for f in "$@"; do
    [ -e "$f" ] || continue
    soname="$(readelf -d "$f" | sed -n 's/.*Library soname: \[\(.*\)\]/\1/p')"
    cp -L "$f" "$DEPS/${soname:-$(basename "$f")}"
  done
}
bundle "$AWS_SDK_ROOT"/lib/*.so* "$HDFS3_ROOT"/lib/libhdfs3.so*

strip --strip-unneeded "$STAGE/paimon_heap.so" "$DEPS"/*
patchelf --set-rpath '$ORIGIN/paimon_heap-deps' "$STAGE/paimon_heap.so"
patchelf --set-rpath '$ORIGIN' "$DEPS"/*

# Soname requirements: NEEDED of everything we ship, minus what we bundle.
needed="$(for f in "$STAGE/paimon_heap.so" "$DEPS"/*; do
            readelf -d "$f" | sed -n 's/.*Shared library: \[\(.*\)\]/\1/p'
          done | sort -u)"
bundled="$(ls "$DEPS")"
requires="$(comm -23 <(echo "$needed") <(echo "$bundled" | sort -u) | sed 's/$/()(64bit)/')"

# Substitute placeholders ourselves rather than relying on which fields
# nfpm env-expands.
cfg="$STAGE/nfpm.yaml"
sed -e "s|\${ARCH}|$ARCH|g" -e "s|\${VERSION}|$VERSION|g" -e "s|\${STAGE}|$STAGE|g" \
  "$HERE/nfpm.yaml" > "$cfg"
[ -n "$requires" ] && echo "$requires" | sed 's/^/  - /' >> "$cfg"

echo "== nfpm.yaml =="; cat "$cfg"

nfpm package -f "$cfg" -p rpm -t "$OUT/"

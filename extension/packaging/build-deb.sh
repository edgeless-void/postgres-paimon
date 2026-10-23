#!/usr/bin/env bash
# Package the already-built (dynamic) paimon_heap.so as a .deb with nfpm.
#
# Runs inside the target distro (Debian 12/13, Ubuntu 24.04/26.04). The AWS SDK
# and libhdfs3 have no distro packages, so their shared libraries are bundled
# into /usr/lib/postgresql/18/lib/paimon_heap-deps and found via $ORIGIN-relative
# RPATHs. Every other shared dependency becomes a Depends: on the package that
# owns that library on this distro (resolved with ldd + dpkg -S).
#
# Usage: build-deb.sh <version> <arch: amd64|arm64> <out-dir>
# Env:   AWS_SDK_ROOT (default /opt/aws-sdk-cpp), HDFS3_ROOT (default /usr/local)
set -euo pipefail

VERSION="${1#v}"
ARCH="$2"
OUT="$(realpath -m "$3")"

AWS_SDK_ROOT="${AWS_SDK_ROOT:-/opt/aws-sdk-cpp}"
HDFS3_ROOT="${HDFS3_ROOT:-/usr/local}"
HERE="$(cd "$(dirname "$0")" && pwd)"
EXT="$(dirname "$HERE")"

# Distro tag for the Debian revision, e.g. 1.debian12 / 1.ubuntu24.04.
# (Subshell: os-release defines VERSION, which would clobber ours.)
RELEASE="1.$(. /etc/os-release && echo "${ID}${VERSION_ID}")"

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

# NEEDED of everything we ship, minus what we bundle.
needed="$(for f in "$STAGE/paimon_heap.so" "$DEPS"/*; do
            readelf -d "$f" | sed -n 's/.*Shared library: \[\(.*\)\]/\1/p'
          done | sort -u)"
bundled="$(ls "$DEPS" | sort -u)"
# The dynamic loader (ld-linux-*.so) is part of libc6, which libc.so.6 already
# pulls in, and ldd prints it without "=>", so leave it out.
external="$(comm -23 <(echo "$needed") <(echo "$bundled") | grep -v '^ld-linux')"

# Map each external soname to the package that owns it. ldd resolves the path
# (RPATHs above make the bundled libs resolvable too); dpkg may record the
# file under /lib or /usr/lib depending on usrmerge, so try both.
resolved="$( (ldd "$STAGE/paimon_heap.so"; for f in "$DEPS"/*; do ldd "$f"; done) \
             | awk '$2 == "=>" && $3 ~ /^\// {print $1, $3}' | sort -u)"
owner() {
  local p="$1" c
  for c in "$p" "$(realpath "$p")" "${p#/usr}" "/usr$p"; do
    dpkg -S "$c" 2>/dev/null | head -1 | cut -d: -f1 && return 0
  done
  return 1
}
depends=""
for so in $external; do
  path="$(awk -v so="$so" '$1 == so {print $2; exit}' <<<"$resolved")"
  [ -n "$path" ] || { echo "error: cannot resolve $so" >&2; exit 1; }
  pkg="$(owner "$path")" || { echo "error: no package owns $path ($so)" >&2; exit 1; }
  depends+="$pkg"$'\n'
done

# Substitute placeholders ourselves rather than relying on which fields
# nfpm env-expands.
cfg="$STAGE/nfpm.yaml"
sed -e "s|\${ARCH}|$ARCH|g" -e "s|\${VERSION}|$VERSION|g" \
    -e "s|\${RELEASE}|$RELEASE|g" -e "s|\${STAGE}|$STAGE|g" \
  "$HERE/nfpm-deb.yaml" > "$cfg"
printf '%s' "$depends" | sort -u | sed '/^$/d; s/^/  - /' >> "$cfg"

echo "== nfpm.yaml =="; cat "$cfg"

nfpm package -f "$cfg" -p deb -t "$OUT/"

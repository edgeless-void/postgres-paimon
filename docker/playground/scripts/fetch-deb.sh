#!/usr/bin/env bash
# Download the Debian 13 paimon_heap .deb for this machine's architecture from
# the latest successful run of the release workflow, into postgres/debs/.
# Needs the GitHub CLI (gh), logged in.
# Usage: scripts/fetch-deb.sh [owner/repo]   (default: the repo gh resolves here)
set -euo pipefail
cd "$(dirname "$0")/.."

repo=(); [ $# -ge 1 ] && repo=(-R "$1")
case "$(uname -m)" in
  x86_64|amd64)  arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

run="$(gh run list ${repo[@]+"${repo[@]}"} --workflow release.yml --status success --limit 1 \
         --json databaseId --jq '.[0].databaseId')"
[ -n "$run" ] || { echo "no successful release.yml run found" >&2; exit 1; }

rm -f postgres/debs/*.deb
gh run download ${repo[@]+"${repo[@]}"} "$run" -n "deb-debian13-$arch" -D postgres/debs
ls -l postgres/debs/*.deb

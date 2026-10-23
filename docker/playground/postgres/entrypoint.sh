#!/usr/bin/env bash
# First start: initdb, create the S3 bucket and the demo database.
# Every start: point paimon_heap at S3 and run postgres.
set -euo pipefail

: "${S3_ENDPOINT:?}" "${S3_BUCKET:?}" "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}"

# Create the bucket (idempotent). 409 = already exists.
for i in $(seq 1 60); do
  code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
            --aws-sigv4 "aws:amz:${AWS_REGION:-us-east-1}:s3" \
            --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
            "$S3_ENDPOINT/$S3_BUCKET" || true)"
  case "$code" in
    200|409) break ;;
  esac
  [ "$i" = 60 ] && { echo "[entrypoint] S3 not reachable at $S3_ENDPOINT (HTTP $code)" >&2; exit 1; }
  sleep 1
done
echo "[entrypoint] S3 bucket $S3_BUCKET ready at $S3_ENDPOINT"

# S3 settings come from the environment so compose.yaml is the one place to change them.
s3_conf=/var/lib/postgresql/paimon-s3.conf
cat > "$s3_conf" <<CONF
paimon_heap.s3_endpoint = '$S3_ENDPOINT'
paimon_heap.s3_bucket = '$S3_BUCKET'
CONF

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "[entrypoint] Initialising cluster in $PGDATA"
  initdb --auth=trust --encoding=UTF8 --locale=C.UTF-8 -U postgres "$PGDATA" >/dev/null
  cat >> "$PGDATA/postgresql.conf" <<CONF

include '/etc/paimon/paimon.conf'
include '$s3_conf'
CONF
  # Playground only: no passwords. compose.yaml publishes the port on 127.0.0.1.
  echo 'host all all all trust' >> "$PGDATA/pg_hba.conf"

  pg_ctl -D "$PGDATA" -o "-c listen_addresses=''" -w start >/dev/null
  psql -v ON_ERROR_STOP=1 -U postgres -q <<'SQL'
CREATE ROLE demo LOGIN;
CREATE DATABASE demo OWNER demo;
\connect demo
CREATE EXTENSION paimon_heap;
SQL
  for f in /docker-initdb.d/*.sql; do
    [ -e "$f" ] || continue
    echo "[entrypoint] Running $(basename "$f")"
    psql -v ON_ERROR_STOP=1 -U demo -d demo -q -f "$f"
  done
  pg_ctl -D "$PGDATA" -m fast -w stop >/dev/null
fi

echo "[entrypoint] Starting PostgreSQL 18 with paimon_heap"
exec postgres -D "$PGDATA"

#!/usr/bin/env bash
# Smoke test for a running playground: change rows in Postgres, then check the
# Paimon table (data files + snapshot) shows up in S3.
# Usage: scripts/smoke.sh   (COMPOSE overrides the compose command)
set -euo pipefail
cd "$(dirname "$0")/.."
read -ra compose <<<"${COMPOSE:-docker compose}"

psql() { "${compose[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U demo -d demo -qAt "$@"; }

echo "== Postgres"
psql <<'SQL'
DELETE FROM orders WHERE order_id = 100;
INSERT INTO orders (order_id, customer, product, quantity, unit_price, status)
VALUES (100, 'Smoke', 'Test', 1, 1.00, 'pending');
UPDATE orders SET status = 'shipped' WHERE order_id = 100;
SQL
[ "$(psql -c "SELECT status FROM orders WHERE order_id = 100")" = shipped ]
echo "orders: $(psql -c 'SELECT count(*) FROM orders') rows"

echo "== S3 (bucket paimon-warehouse, prefix orders/)"
list_keys() {
  "${compose[@]}" exec -T postgres sh -c '
    curl -fsS --aws-sigv4 "aws:amz:${AWS_REGION}:s3" \
         --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
         "$S3_ENDPOINT/$S3_BUCKET?list-type=2&prefix=orders/"' \
    | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g'
}
for i in $(seq 1 30); do
  keys="$(list_keys || true)"
  if grep -q '\.parquet$' <<<"$keys" && grep -q '/snapshot/snapshot-' <<<"$keys"; then
    echo "$keys"
    echo "OK: Paimon table offloaded to S3"
    exit 0
  fi
  sleep 2
done
echo "FAIL: no Parquet data file and snapshot under orders/ after 60s. Keys seen:" >&2
echo "${keys:-<none>}" >&2
exit 1

# paimon_heap playground

PostgreSQL 18 with the `paimon_heap` extension, offloading its Paimon tables to
a local S3 store ([SeaweedFS](https://github.com/seaweedfs/seaweedfs)). Nothing
is compiled: the Postgres image installs the prebuilt Debian 13 package, so a
cold start takes about a minute plus downloads, on amd64 and arm64.

| Service     | What                                   | Host port        |
|-------------|----------------------------------------|------------------|
| `postgres`  | PostgreSQL 18 + `paimon_heap`          | `127.0.0.1:5432` |
| `seaweedfs` | S3 API, bucket `paimon-warehouse`      | `127.0.0.1:8333` |

S3 credentials: `playground` / `playground-secret`. Postgres uses trust
auth (no passwords). Both ports are published on localhost only. This is a
playground; don't expose it.

## 1. Get the package

The image needs `postgresql-18-paimon-heap_*.debian13_<arch>.deb`. Either:

- **From a release:** `export PAIMON_VERSION=v0.1.0` (the release tag).
  `PAIMON_REPO` (default `VadimSurpin/postgres-paimon`) selects the repo.
- **From CI** (before there is a release): download it from the latest
  successful `release.yml` run with the GitHub CLI:

  ```sh
  scripts/fetch-deb.sh                    # or: scripts/fetch-deb.sh owner/repo
  ```

- **Locally built:** copy the `.deb` into `postgres/debs/`.

A `.deb` in `postgres/debs/` takes precedence over `PAIMON_VERSION`.

## 2. Start

```sh
docker compose up -d --build --wait
```

On first start the `postgres` container creates the bucket, the cluster, the
`demo` database with the extension, and an `orders` table
(`postgres/initdb/01-orders.sql`). Podman works too: `podman compose ...`.

## 3. Play

```sh
psql -h 127.0.0.1 -U demo          # or: docker compose exec postgres psql -U demo
```

```sql
SELECT * FROM orders ORDER BY order_id;

INSERT INTO orders (order_id, customer, product, quantity, unit_price, status)
VALUES (6, 'Frank', 'USB-C Hub', 2, 49.95, 'pending');
UPDATE orders SET status = 'shipped' WHERE order_id = 2;
DELETE FROM orders WHERE order_id = 5;

CREATE TABLE events (id bigint PRIMARY KEY, kind text) USING paimon_heap;
```

Within a few seconds (`paimon_heap.flush_interval_secs = 5`) every change is
written as a Paimon table and copied to S3 under `<table>/`: Parquet data in
`bucket-0/`, plus `manifest/`, `schema/` and `snapshot/`. List it with any S3
client, e.g. the AWS CLI:

```sh
AWS_ACCESS_KEY_ID=playground AWS_SECRET_ACCESS_KEY=playground-secret \
  aws --endpoint-url http://127.0.0.1:8333 --region us-east-1 \
  s3 ls --recursive s3://paimon-warehouse/orders/
```

or with plain curl:

```sh
curl --aws-sigv4 aws:amz:us-east-1:s3 --user playground:playground-secret \
  'http://127.0.0.1:8333/paimon-warehouse?list-type=2&prefix=orders/'
```

`scripts/smoke.sh` does a round trip and checks the files arrive (CI runs it).

## 4. Configure

- Extension settings: `postgres/paimon.conf` (rebuild to apply).
  S3 endpoint/bucket/keys: `compose.yaml`.
- Extra SQL to run on first start: add `postgres/initdb/*.sql` (run as `demo`).
- Different host port: `PG_PORT=15432 docker compose up -d --wait`.

## 5. Reset / stop

```sh
docker compose down        # stop, keep data
docker compose down -v     # stop and delete Postgres and S3 data
```

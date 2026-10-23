# PostgreSQL 18 + paimon_heap from the prebuilt Debian 13 package.
# Nothing is compiled here.
#
# The package comes from postgres/debs/ when a matching .deb is there
# (see scripts/fetch-deb.sh), otherwise from the GitHub release PAIMON_VERSION.
FROM debian:13-slim

ARG PAIMON_VERSION=
ARG PAIMON_REPO=VadimSurpin/postgres-paimon
ENV DEBIAN_FRONTEND=noninteractive

# PGDG + Apache Arrow apt repos. The cluster is created at first start (on the
# volume), so stop the package from creating one in the image.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && . /etc/os-release \
 && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && curl -fsSL -o /tmp/arrow.deb \
      "https://packages.apache.org/artifactory/arrow/${ID}/apache-arrow-apt-source-latest-${VERSION_CODENAME}.deb" \
 && apt-get install -y /tmp/arrow.deb \
 && rm /tmp/arrow.deb \
 && mkdir -p /etc/postgresql-common/createcluster.d \
 && echo 'create_main_cluster = false' > /etc/postgresql-common/createcluster.d/playground.conf \
 && apt-get update \
 && apt-get install -y --no-install-recommends postgresql-18 \
 && rm -rf /var/lib/apt/lists/*

COPY debs/ /tmp/debs/
RUN set -e; \
    arch="$(dpkg --print-architecture)"; \
    deb="$(ls /tmp/debs/postgresql-18-paimon-heap_*.debian13_"$arch".deb 2>/dev/null | sort -V | tail -1)"; \
    if [ -z "$deb" ]; then \
      if [ -z "$PAIMON_VERSION" ]; then \
        echo "error: no postgresql-18-paimon-heap_*.debian13_$arch.deb in postgres/debs/" >&2; \
        echo "       and PAIMON_VERSION is not set. Run scripts/fetch-deb.sh," >&2; \
        echo "       or set PAIMON_VERSION to a release tag (e.g. v0.1.0)." >&2; \
        exit 1; \
      fi; \
      deb="/tmp/debs/postgresql-18-paimon-heap_${PAIMON_VERSION#v}-1.debian13_$arch.deb"; \
      curl -fL -o "$deb" \
        "https://github.com/$PAIMON_REPO/releases/download/$PAIMON_VERSION/$(basename "$deb")"; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends "$deb"; \
    rm -rf /tmp/debs /var/lib/apt/lists/*; \
    ! ldd /usr/lib/postgresql/18/lib/paimon_heap.so | grep 'not found'

COPY paimon.conf /etc/paimon/paimon.conf
COPY initdb/ /docker-initdb.d/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

ENV PATH=/usr/lib/postgresql/18/bin:$PATH \
    PGDATA=/var/lib/postgresql/data
RUN install -d -o postgres -g postgres -m 700 "$PGDATA" \
 && install -d -o postgres -g postgres /var/run/postgresql
USER postgres
EXPOSE 5432
ENTRYPOINT ["entrypoint.sh"]

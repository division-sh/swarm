#!/usr/bin/env bash
set -euo pipefail

readonly container_name="swarm-ci-postgres"

docker rm --force "$container_name" >/dev/null 2>&1 || true
docker run --detach --rm \
  --name "$container_name" \
  --tmpfs /var/lib/postgresql/data:rw \
  --env POSTGRES_PASSWORD=postgres \
  --env POSTGRES_DB=postgres \
  --publish 127.0.0.1:5432:5432 \
  postgres:16 \
  -c max_connections=300 \
  -c fsync=off \
  -c synchronous_commit=off \
  -c full_page_writes=off \
  >/dev/null

ready=false
for _ in $(seq 1 60); do
  if docker exec "$container_name" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != "true" ]]; then
  docker logs "$container_name"
  echo "disposable Postgres did not become ready" >&2
  exit 1
fi

declare -A expected=(
  [max_connections]=300
  [fsync]=off
  [synchronous_commit]=off
  [full_page_writes]=off
)
for setting in max_connections fsync synchronous_commit full_page_writes; do
  value="$(docker exec "$container_name" psql -U postgres -d postgres -tAc "SHOW $setting")"
  if [[ "$value" != "${expected[$setting]}" ]]; then
    echo "$setting=$value, want ${expected[$setting]}" >&2
    exit 1
  fi
done

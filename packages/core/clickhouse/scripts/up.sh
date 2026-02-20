#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ROOT_DIR="$(cd "$CORE_DIR/../.." && pwd)"

_SAVED_CLUSTER_ENABLED="${CLICKHOUSE_CLUSTER_ENABLED:-}"
_SAVED_DB="${CLICKHOUSE_DB:-}"
if [ -f "$ROOT_DIR/.env" ]; then
  set -a
  eval "$(grep -v '^#' "$ROOT_DIR/.env" | grep -E '^[A-Za-z_][A-Za-z_0-9]*=' | sed 's/[[:space:]]*#.*//')"
  set +a
fi
[ -n "$_SAVED_CLUSTER_ENABLED" ] && CLICKHOUSE_CLUSTER_ENABLED="$_SAVED_CLUSTER_ENABLED"
[ -n "$_SAVED_DB" ] && CLICKHOUSE_DB="$_SAVED_DB"

if [ -z "${CLICKHOUSE_URL}" ]; then
  echo "Info: CLICKHOUSE_URL not configured, skipping ClickHouse migrations."
  exit 0
fi

if [ -z "${CLICKHOUSE_MIGRATION_URL}" ]; then
  echo "Info: CLICKHOUSE_MIGRATION_URL not configured, skipping ClickHouse migrations."
  exit 0
fi

if [ -z "${CLICKHOUSE_USER}" ]; then
  echo "Info: CLICKHOUSE_USER not set, skipping ClickHouse migrations."
  exit 0
fi

if [ -z "${CLICKHOUSE_PASSWORD}" ]; then
  echo "Info: CLICKHOUSE_PASSWORD not set, skipping ClickHouse migrations."
  exit 0
fi

if ! command -v migrate &>/dev/null; then
  echo "Info: golang-migrate is not installed, skipping ClickHouse migrations."
  echo "Install via 'brew install golang-migrate' or visit https://github.com/golang-migrate/migrate"
  exit 0
fi

if [ -z "${CLICKHOUSE_DB}" ]; then
  export CLICKHOUSE_DB="default"
fi

if [ -z "${CLICKHOUSE_CLUSTER_NAME}" ]; then
  export CLICKHOUSE_CLUSTER_NAME="default"
fi

if [ "$CLICKHOUSE_CLUSTER_ENABLED" == "true" ]; then
  echo "Error: Cluster mode is not yet supported."
  echo "ClickHouse cluster infrastructure is under development."
  echo "Please set CLICKHOUSE_CLUSTER_ENABLED=false or leave it unset."
  exit 1
fi

if [ "$CLICKHOUSE_CLUSTER_ENABLED" == "false" ] || [ -z "$CLICKHOUSE_CLUSTER_ENABLED" ]; then
  MIGRATIONS_DIR="$CORE_DIR/clickhouse/migrations/unclustered"

  if [ "$CLICKHOUSE_MIGRATION_SSL" = true ]; then
    DATABASE_URL="${CLICKHOUSE_MIGRATION_URL}?username=${CLICKHOUSE_USER}&password=${CLICKHOUSE_PASSWORD}&database=${CLICKHOUSE_DB}&x-multi-statement=true&secure=true&skip_verify=true&x-migrations-table-engine=MergeTree"
  else
    DATABASE_URL="${CLICKHOUSE_MIGRATION_URL}?username=${CLICKHOUSE_USER}&password=${CLICKHOUSE_PASSWORD}&database=${CLICKHOUSE_DB}&x-multi-statement=true&x-migrations-table-engine=MergeTree"
  fi
else
  MIGRATIONS_DIR="$CORE_DIR/clickhouse/migrations/clustered"

  if [ "$CLICKHOUSE_MIGRATION_SSL" = true ]; then
    DATABASE_URL="${CLICKHOUSE_MIGRATION_URL}?username=${CLICKHOUSE_USER}&password=${CLICKHOUSE_PASSWORD}&database=${CLICKHOUSE_DB}&x-multi-statement=true&secure=true&skip_verify=true&x-cluster-name=${CLICKHOUSE_CLUSTER_NAME}&x-migrations-table-engine=ReplicatedMergeTree"
  else
    DATABASE_URL="${CLICKHOUSE_MIGRATION_URL}?username=${CLICKHOUSE_USER}&password=${CLICKHOUSE_PASSWORD}&database=${CLICKHOUSE_DB}&x-multi-statement=true&x-cluster-name=${CLICKHOUSE_CLUSTER_NAME}&x-migrations-table-engine=ReplicatedMergeTree"
  fi
fi

MIGRATION_FILES=$(find "$MIGRATIONS_DIR" -name "*.sql" 2>/dev/null | head -1)
if [ -z "$MIGRATION_FILES" ]; then
  echo "No migrations found in $MIGRATIONS_DIR. Skipping."
  exit 0
fi

migrate -source "file://$MIGRATIONS_DIR" -database "$DATABASE_URL" up

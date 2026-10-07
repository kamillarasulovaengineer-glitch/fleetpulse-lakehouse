#!/usr/bin/env bash
# One-time setup: create the catalog that all three targets share. No target owns it, so it isn't a
# bundle resource. On Free Edition `databricks catalogs create` is rejected (the metastore has no
# storage root), while CREATE CATALOG through a SQL warehouse uses the account's default storage.
#
#   bash scripts/create_catalog.sh [catalog]        default: telematics
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash on Windows would turn /api/... into a file path

catalog="${1:-telematics}"
warehouse_name="${WAREHOUSE_NAME:-Serverless Starter Warehouse}"

command -v databricks >/dev/null || { echo "databricks CLI not found on PATH" >&2; exit 1; }
warehouse_id=$(databricks warehouses list | awk -v name="$warehouse_name" 'index($0, name) { print $1; exit }')
if [ -z "$warehouse_id" ]; then
  echo "SQL warehouse '$warehouse_name' not found; set WAREHOUSE_NAME" >&2
  exit 1
fi

response=$(databricks api post /api/2.0/sql/statements --json "{
  \"warehouse_id\": \"$warehouse_id\",
  \"statement\": \"CREATE CATALOG IF NOT EXISTS \`$catalog\`\",
  \"wait_timeout\": \"50s\"
}")

if grep -q '"state": *"SUCCEEDED"' <<<"$response"; then
  echo "catalog '$catalog' is ready"
else
  echo "$response" >&2
  exit 1
fi

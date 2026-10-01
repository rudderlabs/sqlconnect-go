#!/usr/bin/env bash
# internal/scripts/assert-no-skip.sh <go test -json file>: fails when a ClickHouse test skips, except the two catalog subtests.
set -euo pipefail
allowed='^TestClickHouseDB/https?/catalog_admin/(current_catalog|list_catalogs)$'
skips=$(jq -r 'select(.Action=="skip" and .Test!=null) | .Test' "$1" | grep -Ev "$allowed" || true)
if [ -n "$skips" ]; then echo "ClickHouse tests skipped:"; echo "$skips"; exit 1; fi
for t in 'TestClickHouseDB/https' 'TestClickHouseDB/http' 'TestSQ5_TLSMatrix' 'TestSQ4_FloorMatrix/24.8' 'TestSQ4_FloorMatrix/25.8' 'TestSQ4_FloorMatrix/26.3'; do
  jq -e --arg t "$t" 'select(.Action=="pass" and .Test==$t)' "$1" >/dev/null || { echo "missing pass: $t"; exit 1; }
done

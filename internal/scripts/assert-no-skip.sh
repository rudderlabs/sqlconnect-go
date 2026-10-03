#!/usr/bin/env bash
# internal/scripts/assert-no-skip.sh <go test -json file>: fails when a ClickHouse test fails or skips
# (except the two catalog subtests), or when a required test did not pass.
set -euo pipefail
fails=$(jq -r 'select(.Action=="fail") | (.Test // .Package)' "$1" || true)
if [ -n "$fails" ]; then echo "ClickHouse tests failed:"; echo "$fails"; exit 1; fi
allowed='^TestClickHouseDB/https?/catalog_admin/(current_catalog|list_catalogs)$'
skips=$(jq -r 'select(.Action=="skip" and .Test!=null) | .Test' "$1" | grep -Ev "$allowed" || true)
if [ -n "$skips" ]; then echo "ClickHouse tests skipped:"; echo "$skips"; exit 1; fi
for t in 'TestClickHouseDB/https' 'TestClickHouseDB/http' 'TestSQ5_TLSMatrix' 'TestSQ4_FloorMatrix/24.8' 'TestSQ4_FloorMatrix/25.8' 'TestSQ4_FloorMatrix/26.3'; do
  jq -e --arg t "$t" 'select(.Action=="pass" and .Test==$t)' "$1" >/dev/null || { echo "missing pass: $t"; exit 1; }
done

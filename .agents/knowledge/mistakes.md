# Mistakes

> Post-mortem entries from observed failures: CI failures, reverts on prior PRs,
> prod incidents. Accrues over time — bootstrap leaves this empty.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> (e.g., `<!-- pr:<id> -->`) identifying the writer/PR/run; human-authored
> sections are conventionally left untouched by automated runs.

## ACT2-523 — Redshift late-binding view metadata can break targeted column lookup

- Redshift CI failed in `TestRedshiftDB/.../list_columns/list_columns_for_view` with `querying list columns ... query failed: ERROR: schema "trsources_..." does not exist` because `ListColumns` scanned `SVV_ALL_COLUMNS`, which can touch stale or broken late-binding view metadata outside the requested relation.
- For targeted Redshift column lookup, avoid `SVV_ALL_COLUMNS`; prefer combining `information_schema.columns` for normal tables/schema-bound views with `pg_get_late_binding_view_cols()` for late-binding views.

# Feedback

> Durable human direction and review guidance.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> identifying the writer/PR/run; human-authored sections are conventionally left
> untouched by automated runs.

## ACT2-859 — BigQuery token-source API safeguards

<!-- session: 2026-09-25 -->

- Keep `sqlconnect.WithBigQueryTokenSource` token-source-only and bind it to the
  `bigquery` factory; arbitrary Google client options could discard a customer's
  configured key and silently fall back to the pod identity, while use with another
  option-aware factory would otherwise be silently ignored.
- `sqlconnect.NewDB` must reject nil `DBOption` values, and
  `WithBigQueryTokenSource` must reject both nil and typed-nil token sources before
  factory construction so malformed inputs return errors rather than panic on query.
- Regression tests for `sqlconnect/internal/bigquery/db.go::init` must call the public
  `sqlconnect.NewDB("bigquery", ...)` path and force a query with a sentinel-error
  token source; constructor-only tests can pass when registration accidentally drops
  the token source and Google authentication falls back to ambient credentials.

## INT-7271 — Live Fabric SQL compatibility

<!-- session: 2026-10-06 -->

- `sqlconnect/internal/fabric/db.go::fabricSQLCommands` must spell
  `INFORMATION_SCHEMA` views and their column names in uppercase because Fabric
  warehouses default to the case-sensitive `Latin1_General_100_BIN2_UTF8`
  collation; `catalogMetadataQuery` must qualify the same uppercase spelling,
  and `ListCatalogs` must exclude the system `master` database.
- `sqlconnect/internal/fabric/dialect.go::newDialect` must use the shared
  `base.NewGoquDialect` with `QuoteIdentifiers=false`. `QueryCondition` callers
  supply identifiers and expressions already formatted for the warehouse, so a
  Fabric-specific auto-quoting override changes the established API contract.
- `base.Expressions.CurrentDate` lets Fabric render `inlast` relative to
  `CAST(CURRENT_TIMESTAMP AS DATE)` instead of unsupported `CURRENT_DATE`, while
  `integration_test.Options.DateOf` lets its shared scenarios use
  `CAST(column AS DATE)` instead of unsupported `DATE(column)`.
- Fabric mapping fixtures must not create `TINYINT` columns because Fabric
  Warehouse rejects that type. `REAL` value `1.1` is returned by go-mssqldb as
  `1.100000023841858`, so JSON mapping expectations must preserve that value.

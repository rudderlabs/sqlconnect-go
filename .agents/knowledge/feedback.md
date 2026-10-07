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
  Warehouse rejects that type for stored columns. Query expressions can still
  return `TINYINT`, so `sqlconnect/internal/fabric/mappings.go` must map it to
  `int`; likewise query-only `SMALLMONEY`, `SMALLDATETIME`, and `BINARY` results
  map to `float`, `datetime`, and `string`. `REAL` value `1.1` is returned by
  go-mssqldb as `1.100000023841858`, so JSON mapping expectations must preserve
  that value.
- `sqlconnect/internal/fabric/db.go::fabricSQLCommands.DropSchema` must remove
  views before tables and then drop the schema because Fabric T-SQL has no
  `CASCADE`; live integration coverage must exercise a non-empty schema.
- Fabric integration jobs must fail when `FORCE_RUN_INTEGRATION_TESTS=true` and
  `FABRIC_TEST_ENVIRONMENT_CREDENTIALS` is absent. Wire that secret into both
  `.github/workflows/test.yaml` and `.github/workflows/cleanup-test-schemas.yaml`,
  and register Fabric in `sqlconnect/cmd/cleanup/cleanup.go` so `tsqlcon_*`
  schemas do not accumulate.
- Optional Fabric REST bootstrap in `sqlconnect/internal/fabric/db.go::NewDB`
  must have a finite default context deadline, and the default client created by
  `newBootstrapper` must set its own HTTP timeout so construction cannot block
  indefinitely.
- `sqlconnect/internal/fabric/dialect.go::FormatTableName` deliberately preserves
  identifier case for Fabric; document that exception in `sqlconnect.Dialect`
  and the README because the other dialects fold case.
- Adding a driver to `sqlconnect/config/config.go` also requires adding it to
  every applicable table and warehouse list in `sqlconnect/dialects_test.go`.
  Package-local tests do not prove public `sqlconnect.NewDialect` registration
  or parity for normalization, parsing, quoting, table formatting, conditions,
  and expressions.
- `fabricWorkspaceId` is an opaque Fabric API path value, not a client-validated
  UUID. `sqlconnect/internal/fabric/bootstrap.go` must keep the host fixed to
  `api.fabric.microsoft.com`, escape the workspace path segment, and allow the
  Fabric API to return the existing `spn_token_bootstrap:`-classified error.
- `sqlconnect/internal/fabric.Config` must not expose `SkipHostValidation` because
  Fabric has no local/container endpoint that requires loopback access. Its
  `Parse` method must always use `util.ValidateHost`, while lazy-construction
  tests can use a public IP literal without dialing it.
- Fabric identifiers use the shared `base.Dialect` ISO double-quote behavior;
  `sqlconnect/internal/fabric/dialect.go` overrides only case normalization.
  Quote-aware dot parsing or bracket-delimiter extensions to
  `sqlconnect/internal/base/dialect.go` are separate changes and must not be
  bundled into the Fabric driver PR.
- Empty and unset integration credential environment variables are equivalent:
  forced Fabric integration tests must fail with an explicit credential error,
  while `sqlconnect/cmd/cleanup/cleanup.go` must skip an unconfigured warehouse
  and continue cleaning every configured warehouse.
- `sqlconnect/internal/fabric/db.go::unquoteFabricIdentifier` handles one quoted
  identifier, so it must strip one pair of double quotes and unescape `""`
  directly. Calling `base.ParseRelationRef` incorrectly treats dots inside
  schema and table names as qualification separators.
- Fabric's goqu dialect must retain six fractional-second digits for `datetime2`
  comparisons. Its `ListTables` prefix query must escape T-SQL `LIKE`
  metacharacters `%`, `_`, and `[`, plus the selected escape character, before
  appending the prefix wildcard.

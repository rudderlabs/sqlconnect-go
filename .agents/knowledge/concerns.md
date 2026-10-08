# Concerns

> Technical debt, TODOs, FIXMEs, security concerns, architectural issues.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> (e.g., `<!-- pr:<id> -->`) identifying the writer/PR/run; human-authored
> sections are conventionally left untouched by automated runs.
> Top-5–8 highest-signal items per category, not exhaustive.

## TODO/FIXME/XXX/HACK Density
<!-- ticket:RUD-2789 -->

- Trino mapping has an unresolved TODO that may indicate dead/obsolete conversion logic (`sqlconnect/internal/trino/mappings.go` TODO "is this still needed?").
- BigQuery driver still carries TODO markers around deprecated auth option migration (`sqlconnect/internal/bigquery/driver/driver.go` comments around `WithCredentialsFile`/`WithCredentialsJSON`).
- BigQuery DB constructor repeats auth migration TODO comments, suggesting parallel debt in both core and driver layers (`sqlconnect/internal/bigquery/db.go`).
- Multiple `nolint` suppressions around staticcheck/unparam/rowserrcheck indicate intentional debt pockets that should be periodically revalidated (`sqlconnect/internal/trino/db.go`, `sqlconnect/internal/base/tableadmin.go`, `sqlconnect/internal/redshift/driver/connection.go`).

## Security and Secrets Handling Risks
<!-- ticket:RUD-2789 -->

- DSN serialization supports query-string embedding of AWS credentials (`secretAccessKey`, session token), which increases accidental secret leakage risk through logs/traces (`sqlconnect/internal/redshift/driver/dsn.go::(*RedshiftConfig).DSN`, `sqlconnect/internal/redshift/driver/dsn.go::parseDSN`).
- Cleanup command exits via `log.Fatalf` on missing env/connection errors, which can print operational details in CI logs and reduces graceful recovery options (`sqlconnect/cmd/cleanup/cleanup.go::main`).
- README usage examples include inline plaintext password patterns that can normalize unsafe copy/paste practices if consumers reuse samples directly (`README.md` usage sections).
- SSH tunnel config accepts raw private-key material through JSON/env, so surrounding systems must enforce strict secret redaction and short-lived credentials (`sqlconnect/internal/sshtunnel/config.go::Config`, `.github/workflows/test.yaml` secret env wiring).

## Architectural and Coupling Smells
<!-- ticket:RUD-2789 -->

- Registration by `init()` means behavior depends on import side effects; missing an import can fail at runtime with "unknown client factory" without compile-time signal (`sqlconnect/db_factory.go::NewDB`, `sqlconnect/config/config.go`).
- Legacy and modern mapping paths are duplicated across many dialects, increasing drift risk when adding data-type changes (`sqlconnect/internal/*/{mappings.go,legacy_mappings.go}`).
- Integration-test harness is large and central; broad scenario coupling can make isolated changes expensive to validate and reason about (`sqlconnect/internal/integration_test/db_integration_test_scenario.go`).

## Stale/Drift Signals
<!-- ticket:RUD-2789 -->

- CI matrix comments out Trino package tests while README still presents Trino as a supported warehouse, creating support-coverage ambiguity (`.github/workflows/test.yaml`, `README.md`).
- Cleanup binary also comments out Trino cleanup path, reinforcing potential maintenance skew for Trino environments (`sqlconnect/cmd/cleanup/cleanup.go`).
- Release workflow uses `package-name: rudder-server`, which appears mismatched for `sqlconnect-go` and may cause release metadata confusion if unintentional (`.github/workflows/release-please.yaml`).

## INT-7271 — Fabric live-test credential dependency

- Fabric integration tests require `FABRIC_TEST_ENVIRONMENT_CREDENTIALS`; when
  `FORCE_RUN_INTEGRATION_TESTS=true`, absence of that secret is a hard failure.
  Both `.github/workflows/test.yaml` and
  `.github/workflows/cleanup-test-schemas.yaml` wire the credential, so repository
  environments must keep it provisioned for integration tests and cleanup. The
  secret was not provisioned during PR #581 review, so an owner must add it
  before the Fabric CI scenario can pass and provide live coverage.
- `sqlconnect/internal/base/dialect.go::doNormaliseIdentifier` ranges over byte
  offsets but uses those offsets to index a rune slice during lookahead, so a
  quoted Unicode identifier such as `"日本語""x"` can panic. Fabric admin commands
  no longer call this parser, but its public `ParseRelationRef` remains exposed;
  fix it in a separate shared-parser PR with Unicode regression coverage.
## ACT2-766 — ClickHouse driver residual risks

<!-- session: 2026-10-01 -->

- `clickhousequery.CheckAudienceSQL` in `sqlconnect/clickhousequery/queryguard.go` refuses some valid SQL. A bare keyword alias such as `AS final` fails on the FINAL clause rule. A call to `format(...)` fails on the table function name rule, which refuses `format` in any position. `EXTRACT(... FROM ...)`, `trim(... FROM ...)` and `position(a IN f())` pass. A caller must not emit the refused forms.
- The pinned ch-go String decoder (`ColStr.DecodeColumn` in `github.com/ClickHouse/ch-go@v0.74.0/proto/col_str.go`) allocates the length that the wire claims. `Reader.StrLen` in `proto/reader.go` rejects only a negative length. No client-side cap exists, and the server setting `max_result_bytes` does not bound one malformed value. Only `driverReadSettings` sets `max_result_bytes`. A caller that passes its own settings map for a model read must set its own result bound.
- The fork's `discardAndClose` (`github.com/rudderlabs/clickhouse-go/v2@v2.48.0/conn_http_exec.go`) drains an endless 200 response body on Exec until the context deadline, and a deadline during the drain returns `err == nil`. `drainClose` in `sqlconnect/internal/clickhouse/transport.go` caps its own drain at 4096 bytes. The fork does not.
- `readRoleClosure` in `sqlconnect/internal/clickhouse/validation.go` walks the role closure with a queue and a visited set. Nothing bounds the role count or the accumulated grant rows. Add a total role-count bound if a customer role graph grows large.
- The ClickHouse Cloud test (`TestSQ24_CloudSmoke` in `sqlconnect/internal/clickhouse/cloud_test.go`) has build tag `clickhouse_cloud`. It needs `CLICKHOUSE_CLOUD_CONFIG` and runs only through `make test-clickhouse-cloud`. No CI job runs it.
- A typed decimal path inside a JSON column keeps its value but loses its scale, so trailing zeros disappear (`jsonNumbers` and `normalizeJSONValue` in `sqlconnect/internal/clickhouse/mappings.go`; `TestJSONColumn` in `sqlconnect/internal/clickhouse/mappings_integration_test.go`). No integration case covers a generic `Decimal(P,S)` column yet.

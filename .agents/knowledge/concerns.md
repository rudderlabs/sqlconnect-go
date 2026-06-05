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

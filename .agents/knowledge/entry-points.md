# Entry points

> Key entry-point files: read these first to orient in this repo.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> (e.g., `<!-- pr:<id> -->`) identifying the writer/PR/run; human-authored
> sections are conventionally left untouched by automated runs.

## First Files to Read
<!-- ticket:RUD-2789 -->

- `README.md`: usage model, supported warehouses, and import/load expectations for driver registration.
- `sqlconnect/db.go`: core interfaces (`DB`, admin interfaces, dialect abstractions) that define the library contract.
- `sqlconnect/db_factory.go`: `NewDB` dispatch and registry mechanics (`RegisterDBFactory`) used by all warehouses.
- `sqlconnect/config/config.go`: side-effect import aggregator that pulls in all supported dialect packages.
- `sqlconnect/internal/base/db.go`: shared DB implementation and default SQL command templates before per-dialect overrides.
- `sqlconnect/internal/integration_test/db_integration_test_scenario.go`: canonical behavior matrix used to validate dialect parity.

## Operational and Quality Entry Points
<!-- ticket:RUD-2789 -->

- `Makefile`: authoritative local quality commands (`make test`, `make generate`, `make fmt`, `make lint`).
- `.github/workflows/test.yaml`: integration-heavy CI matrix and coverage flow.
- `.github/workflows/verify.yml`: deterministic pre-merge checks for module tidiness, generated code, and formatting/lint.
- `sqlconnect/cmd/cleanup/cleanup.go`: schema cleanup binary used by scheduled CI workflow.

## Representative Dialect Implementations
<!-- ticket:RUD-2789 -->

- `sqlconnect/internal/postgres/db.go`: minimal path for TCP SQL driver + base composition.
- `sqlconnect/internal/redshift/db.go`: dual-mode backend (Redshift Data API vs Postgres path) and SQL command overrides.
- `sqlconnect/internal/trino/db.go`: SOCKS5 tunnel and custom HTTP client registration pattern.

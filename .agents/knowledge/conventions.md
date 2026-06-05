# Conventions

> Coding conventions and naming schemes — things a linter can't catch.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> (e.g., `<!-- pr:<id> -->`) identifying the writer/PR/run; human-authored
> sections are conventionally left untouched by automated runs.

## Package and Directory Shape
<!-- ticket:RUD-2789 -->

- Public API types live in `sqlconnect/`; warehouse implementations stay under `sqlconnect/internal/<dialect>/` so consumers interact through interfaces rather than concrete dialect packages (`sqlconnect/db.go`, `sqlconnect/internal/postgres/db.go`).
- Dialect packages consistently split concerns into `config.go`, `db.go`, `dialect.go`, mapping files, and integration tests, which makes cross-dialect comparisons predictable (`sqlconnect/internal/{bigquery,databricks,mysql,postgres,redshift,snowflake,trino}/`).
- Cross-dialect utility concerns are isolated to shared internal packages (`sqlconnect/internal/base`, `sqlconnect/internal/sshtunnel`, `sqlconnect/internal/integration_test`) instead of being duplicated per backend.

## Naming and Compatibility Conventions
<!-- ticket:RUD-2789 -->

- Each dialect exposes `DatabaseType` string constants that must match registry keys accepted by `sqlconnect.NewDB` (`sqlconnect/internal/postgres/db.go::DatabaseType`, `sqlconnect/internal/redshift/db.go::DatabaseType`).
- Legacy behavior remains opt-in and explicit: configuration fields and files are named around `UseLegacyMappings`, with paired `mappings.go` and `legacy_mappings.go` implementations (`README.md`, `sqlconnect/internal/postgres/db.go::getColumnTypeMappings`).
- Option constructors (`WithCatalog`, `WithPrefix`, relation/schema refs) are favored over ad-hoc string concatenation in call sites (`sqlconnect/db.go::SchemaAdmin`, `sqlconnect/db.go::TableAdmin`, `sqlconnect/relationref.go`).

## Testing and Operational Conventions
<!-- ticket:RUD-2789 -->

- Integration tests read credentials from environment variables with warehouse-specific keys and short-circuit if missing (`sqlconnect/internal/*/integration_test.go`, `.github/workflows/test.yaml`).
- Cleanup workflow uses the same secret names as tests and drops only schemas matching `tsqlcon_`, preserving non-test schemas in shared environments (`sqlconnect/cmd/cleanup/cleanup.go::main`, `.github/workflows/cleanup-test-schemas.yaml`).
- Verification conventions are enforced in CI as "clean diff" checks after `go mod tidy`, `make generate`, and `make fmt`; contributors are expected to run those before pushing (`.github/workflows/verify.yml`, `Makefile`).

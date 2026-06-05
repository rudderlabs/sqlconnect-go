# Architecture

> Component layout, internal relationships, data flow.
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> (e.g., `<!-- pr:<id> -->`) identifying the writer/PR/run; human-authored
> sections are conventionally left untouched by automated runs.

## Public API to Adapter Flow
<!-- ticket:RUD-2789 -->

- `sqlconnect.DB` is the stable contract for callers, combining SQL compatibility (`sqlDB`) with catalog/schema/table admin and dialect expression capabilities (`sqlconnect/db.go::DB`, `sqlconnect/db.go::CatalogAdmin`, `sqlconnect/db.go::Dialect`).
- `sqlconnect.NewDB` resolves a warehouse key through a runtime registry map and delegates construction to a dialect-specific factory (`sqlconnect/db_factory.go::NewDB`, `sqlconnect/db_factory.go::RegisterDBFactory`).
- Driver package loading is side-effect based: importing `sqlconnect/config` brings all dialect packages into the process so their `init()` hooks register factories (`sqlconnect/config/config.go`, `sqlconnect/internal/postgres/db.go::init`, `sqlconnect/internal/redshift/db.go::init`).
- The top-level read path is query-first and mapper-driven: `QueryAsync` executes a SQL query, applies a row mapper, and streams values/errors on a channel (`sqlconnect/async.go::QueryAsync`, `sqlconnect/async.go::JSONRowMapper`).

## Shared Base Layer and Dialect Overrides
<!-- ticket:RUD-2789 -->

- `internal/base.DB` centralizes common behavior (lifecycle, default SQL command templates, row mapping, and identifier utilities), then each warehouse overrides only the pieces that diverge (`sqlconnect/internal/base/db.go::NewDB`, `sqlconnect/internal/base/db.go::SQLCommands`).
- Warehouse implementations compose behavior through `base.With*` options (dialect, column mapping, JSON mapping, SQL command overrides), avoiding per-dialect reimplementation of generic admin methods (`sqlconnect/internal/postgres/db.go::NewDB`, `sqlconnect/internal/redshift/db.go::NewDB`, `sqlconnect/internal/trino/db.go::NewDB`).
- Redshift and Trino explicitly replace catalog/schema/table command templates because their metadata APIs differ from `information_schema` defaults (`sqlconnect/internal/redshift/db.go::NewDB`, `sqlconnect/internal/trino/db.go::NewDB`).

## Connectivity and Runtime Boundaries
<!-- ticket:RUD-2789 -->

- SSH tunnel support is modeled as an optional infra boundary attached to DB lifecycle; base close joins `sql.DB.Close` with tunnel close (`sqlconnect/internal/base/db.go::Close`, `sqlconnect/internal/sshtunnel/tunnel.go::Tunnel`).
- TCP tunnel mode rewrites host/port before creating a SQL connection for engines like Postgres/Redshift, while Trino uses a SOCKS5 HTTP transport and custom Trino client registration (`sqlconnect/internal/postgres/db.go::NewDB`, `sqlconnect/internal/redshift/db.go::newPostgresDB`, `sqlconnect/internal/trino/db.go::sshTunnelling`).
- Integration scenarios are library-owned and dialect-agnostic; shared test harnesses validate a common contract (catalog/schema/table ops, query behavior, cancellation) across warehouse backends (`sqlconnect/internal/integration_test/db_integration_test_scenario.go::TestDatabaseScenarios`, `sqlconnect/internal/integration_test/sshtunnel_integration_test_scenario.go::TestSshTunnelScenarios`).

## Operational and CI Topology
<!-- ticket:RUD-2789 -->

- CI has split responsibilities: `test.yaml` runs matrix integration tests with secrets and coverage upload; `verify.yml` enforces generate/fmt/tidy/lint discipline (`.github/workflows/test.yaml`, `.github/workflows/verify.yml`).
- The cleanup utility is a separate operational entry point that drops test schemas via environment-provided credentials and bounded concurrency (`sqlconnect/cmd/cleanup/cleanup.go::main`).
- Repository support messaging and CI coverage are not identical: README lists Trino support, but Trino is commented out of the main test matrix and cleanup list (`README.md`, `.github/workflows/test.yaml`, `sqlconnect/cmd/cleanup/cleanup.go`).

## Cross-cutting
<!-- ticket:RUD-2789 -->

- Side-effect factory registration is the core extensibility mechanism, so successful usage depends on import discipline (`sqlconnect/config/config.go`) and on test/entry-point code loading the right packages before calling `sqlconnect.NewDB` (`sqlconnect/db_factory.go::NewDB`, `sqlconnect/internal/*/db.go::init`).
- The codebase optimizes for shared behavior plus per-dialect override hooks: base SQL command templates (`sqlconnect/internal/base/db.go::SQLCommands`) and mapper customization (`base.WithColumnTypeMappings`, `base.WithJsonRowMapper`) reduce duplication but make legacy toggles a cross-cutting compatibility surface (`sqlconnect/internal/*/legacy_mappings.go`, `README.md`).
- Integration confidence is secret- and environment-dependent: core behavior is validated mostly through integration-heavy scenarios (`sqlconnect/internal/integration_test/db_integration_test_scenario.go::TestDatabaseScenarios`) and CI secrets wiring (`.github/workflows/test.yaml`), so local/unit-only runs do not exercise most backend contracts.
- The Trino path shows a recurring doc-vs-automation skew: it is part of public API/config surface (`README.md`, `sqlconnect/internal/trino/config.go`) but is partially excluded from routine automation (`.github/workflows/test.yaml`, `sqlconnect/cmd/cleanup/cleanup.go`).
- Toolchain and release automation constraints are also cross-cutting: Go 1.26 is pinned in module/CI/lint (`go.mod`, `.github/workflows/test.yaml`, `.github/workflows/verify.yml`, `.golangci.yml`) while release metadata currently references `package-name: rudder-server`, which should be validated by maintainers for this repo (`.github/workflows/release-please.yaml`).

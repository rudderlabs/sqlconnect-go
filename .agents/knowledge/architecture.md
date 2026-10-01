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

## ACT2-859 — Secure BigQuery caller authentication

- The root `sqlconnect.NewDB` construction path supports sealed, variadic DB options while preserving the existing `DBFactory` and `RegisterDBFactory` API for external custom factories; BigQuery alone uses the parallel option-aware registration path (`sqlconnect/db_factory.go`, `sqlconnect/internal/bigquery/db.go`).
- BigQuery caller-supplied authentication is intentionally exposed as an `oauth2.TokenSource`, not arbitrary Google `option.ClientOption` values. This allows pre-built WIF or impersonation credentials while preventing credential-file/JSON options from bypassing the service-account-only validation on the legacy credentials JSON path (`sqlconnect/db_factory.go`, `sqlconnect/internal/bigquery/db.go`, `sqlconnect/internal/bigquery/credentials.go`).

## ACT2-766 — ClickHouse driver layout

<!-- session: 2026-10-01 -->

- The ClickHouse driver is split across packages. `sqlconnect/internal/clickhouse` holds the driver (registered as `clickhouse` through `sqlconnect/config/config.go`). `sqlconnect/clickhousequery` is the public package that rudder-sources and Lookout call: `CheckAudienceSQL`, `DialPolicy`, `SetDialPolicy`, `ParseBlockedCIDRs`, `WithStatement`, `NewQueryID`, `MaxRunBudget` (2 h) and `Describe`. `internal/chsql` is the SQL lexer, `internal/cherr` is the error code registry (38 `CH_*` codes), and `internal/chpolicy` is the address policy.
- The driver talks HTTPS only, through the fork `github.com/rudderlabs/clickhouse-go/v2` v2.48.0 (upstream v2.48.0 with a new module path and the driver name `clickhouse-v2`). It never uses the native protocol. Do not swap in upstream `ClickHouse/clickhouse-go`, because the driver relies on the fork's deadline handling (see the ACT2-766 section in `patterns.md`).
- `internal/clickhouse/dial.go::guardedDialer.DialContext` resolves the host on every new socket, checks each answer against `chpolicy` and the operator CIDR list, and dials the checked IP literal. The hostname stays in the request URL, so TLS SNI and certificate checks still use it. `util.ValidateHost`, which the other drivers use, checks every answer but does not pin the checked address (follow-up ACT2-840).
- `internal/clickhouse/transport.go::guardedTransport` refuses redirects, uses no proxy, and caps `Retry-After` at 5 minutes (`maxRetryAfter`). `drainClose` drains at most 64 KiB of a body before close.
- `internal/clickhouse/guardconn.go::guardConn` wraps every connection the pool opens. `sanitize` bounds error text, `statementCtx` gives each statement the driver settings map and a fresh `retl-<UUID>` query id, and `Prepare` and `Begin` return unsupported errors.
- `internal/clickhouse/settings.go` owns the settings maps. `driverScratchSettings` is the base for writes (`insert_null_as_default` 0, every overflow mode `throw`, `session_timezone` UTC). `driverReadSettings` adds `readonly` 2 and `max_result_bytes` 256 MiB with `result_overflow_mode` throw. These maps apply to driver reads only (metadata, counts, validation). A caller that streams a model result passes its own map through `clickhousequery.WithStatement`.
- `sqlconnect/clickhousequery/testdata/addresses.json` (56 cases) and `audience-sql.json` (195 rows at 070830c) are shared fixtures. rudder-lookout and rudder-config-backend copy them byte for byte. `internal/clickhouse/testdata/clickhouse-fields.json` is a byte copy of rudder-integrations-config commit 28839ccc, and its test checks the sha256. A change to any of these files needs a matching recopy in the consumer repos. rudder-sources pins this module by pseudo-version until a release tag exists.

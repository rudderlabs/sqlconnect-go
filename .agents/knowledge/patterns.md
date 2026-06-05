# Patterns

> Recurring idioms specific to this repo (error handling, state management,
> retries, logging, DI, request lifecycle).
> Append-only. Agent-authored sections may optionally carry an HTML-comment tag
> (e.g., `<!-- pr:<id> -->`) identifying the writer/PR/run; human-authored
> sections are conventionally left untouched by automated runs.
> Every observed idiom includes a `file:line` reference.

## Factory Registration and Composition
<!-- ticket:RUD-2789 -->

- Dialect plugins self-register in `init()` and expose only a constructor function to the shared registry (`sqlconnect/internal/postgres/db.go::init`, `sqlconnect/internal/redshift/db.go::init`, `sqlconnect/db_factory.go::RegisterDBFactory`).
- Implementations are mostly compositional wrappers around `base.NewDB`; options are layered to install dialect behavior and mapping functions without branching in callers (`sqlconnect/internal/postgres/db.go::NewDB`, `sqlconnect/internal/trino/db.go::NewDB`).
- Configuration parsing is "decode + default + optional tunnel parse" in each backend config object (`sqlconnect/internal/databricks/config.go::Parse`, `sqlconnect/internal/sshtunnel/config.go::ParseInlineConfig`).

## Error Handling and API Contracts
<!-- ticket:RUD-2789 -->

- Public query paths wrap errors with operation context so callers can identify failure stage (`executing query`, `getting column types`, `mapping row`, `iterating rows`) (`sqlconnect/async.go::QueryAsync`, `sqlconnect/async.go::QueryJSONAsync`).
- Feature capability is represented with sentinel semantics (`ErrNotSupported`), and integration tests branch on that instead of failing universally (`sqlconnect/db.go::ErrNotSupported`, `sqlconnect/internal/integration_test/db_integration_test_scenario.go::TestDatabaseScenarios`).
- Some operational code paths still use fail-fast process termination (`log.Fatalf`) rather than returned errors; this pattern appears in cleanup tooling but not in library APIs (`sqlconnect/cmd/cleanup/cleanup.go::main`).

## State and Resource Lifecycle
<!-- ticket:RUD-2789 -->

- DB close semantics explicitly aggregate multiple teardown operations with `errors.Join`, preventing tunnel-close failures from masking DB-close failures (`sqlconnect/internal/base/db.go::Close`).
- Asynchronous query APIs use cooperative cancellation and explicit "leave" support around a single-sender channel primitive (`sqlconnect/async.go::QueryAsync`, `sqlconnect/async.go::QueryJSONAsync`).
- Row mapping copies `[]byte` values before next scan to avoid driver buffer reuse corruption; this is a deliberate portability safeguard (`sqlconnect/async.go::JSONRowMapper`).

## Integration-First Validation Style
<!-- ticket:RUD-2789 -->

- Dialect behavior is validated through broad, shared scenarios that cover admin operations, SQL expressions, mapping, and cancellation in one harness (`sqlconnect/internal/integration_test/db_integration_test_scenario.go::TestDatabaseScenarios`).
- SSH behavior is tested with an in-process SSH server and dynamic credentials mutation, rather than mocking tunnel APIs (`sqlconnect/internal/integration_test/sshtunnel_integration_test_scenario.go::newSshServer`, `sqlconnect/internal/integration_test/sshtunnel_integration_test_scenario.go::TestSshTunnelScenarios`).
- CI toggles integration-heavy coverage by forcing integration execution in matrix jobs and passing many per-warehouse secrets (`.github/workflows/test.yaml`).

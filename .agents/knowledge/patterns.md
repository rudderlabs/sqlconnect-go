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

## ACT2-859 — Compatible factory registry extension

- Legacy and option-aware factory registrations are mutually exclusive for each warehouse key: registering either form removes the previous entry of the other form, preserving the registry's last-registration-wins semantics even though construction uses two maps (`sqlconnect/db_factory.go`).

## ACT2-766 — ClickHouse write, deadline and cleanup idioms

<!-- session: 2026-10-01 -->

- Every ClickHouse driver write runs on one `*sql.Conn` through `internal/clickhouse/exec.go::(*DB).withConn`. `database/sql` replays a failed pool statement, and a replayed write can apply twice. `poolExec.ExecContext` therefore refuses every write with `CH_QUERY_INVALID` wrapping `sqlconnect.ErrNotSupported`. Never send a write through `*sql.DB` directly.
- Every driver INSERT carries `SETTINGS insert_null_as_default = 0` in the statement text (`internal/clickhouse/materialization.go::InsertFromQueryWithOptions`), and `driverScratchSettings` sets the same value. A NULL into a non-nullable column must fail, not become the column default.
- The clickhouse-go fork replaces `max_execution_time` with the remaining context deadline. It adds the setting only when more than 1 s remains. Code that sends its own `max_execution_time` must run without a context deadline: `internal/clickhouse/validation.go` stage 2 uses `withoutDeadline` and sends `MaxRunBudget` itself, and `refusedOpenSetting` bounds the open with `time.AfterFunc(validationOpenTimeout, cancel)` instead of a deadline.
- `internal/clickhouse/validation.go::probeCleanup.run` drops the scratch probe table on a context detached from the caller (`context.WithoutCancel` plus `probeCleanupTimeout`), so a caller cancel still removes it. `step` asks `acquire` again for each step and never replays a statement. `cleanupAcquire` reuses the validation connection while `conn.Raw` succeeds, else it opens a fresh pool connection. A busy pool cannot starve the cleanup this way.
- `probeCleanup.createSettled` polls the query outcome of a CREATE with an unknown outcome until it reads finished or failed. If the outcome stays open, the cleanup still runs the DROP and then returns `CH_OUTCOME_UNKNOWN` inside `CH_SCRATCH_CLEANUP_FAILED`, because the CREATE can land after the DROP. A denied DROP stays fatal.
- `sqlconnect/clickhousequery/queryguard.go::tableFunctionNames` must equal `testdata/table-functions-26.3.txt`, which is `SELECT name FROM system.table_functions` on the pinned 26.3.33.24 image. `queryguard_test.go` checks this. When the pinned image changes, regenerate the file from the new image and update the map in the same commit.
- Each row of `clickhousequery/testdata/audience-sql.json` must parse on the pinned 26.3 server the way the guard assumes. Before a row is added or flipped, run it through `EXPLAIN AST` on 26.3.33.24 (for example with `clickhouse local`). Accepted rows must parse, and refused rows must be refused for the stated reason.

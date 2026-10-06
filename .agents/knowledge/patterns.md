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

- Every ClickHouse driver write runs on one pinned `*sql.Conn`. `(*DB).withConn` in `sqlconnect/internal/clickhouse/exec.go` is the usual path. `ValidateContext` in `sqlconnect/internal/clickhouse/validation.go` owns its own connection, and option-aware materialization in `sqlconnect/internal/clickhouse/materialization.go` receives an executor from the caller, who passes a `*sql.Conn`. `database/sql` replays a failed pool statement, and a replayed write can apply twice. `poolExec.ExecContext` therefore refuses every write with `CH_QUERY_INVALID` wrapping `sqlconnect.ErrNotSupported`. Never send a write through `*sql.DB` directly.
- `InsertFromQueryWithOptions` in `sqlconnect/internal/clickhouse/materialization.go` puts `SETTINGS insert_null_as_default = 0` in the INSERT text. Other driver INSERTs, such as the validation probe INSERT, get the same value from `driverScratchSettings` in `sqlconnect/internal/clickhouse/settings.go`. A NULL into a non-nullable column must fail, not become the column default.
- The clickhouse-go fork replaces `max_execution_time` with the remaining context deadline plus 5 s (`queryOptions` in `github.com/rudderlabs/clickhouse-go/v2@v2.48.0/context.go`). It does this only when more than 1 s remains. Code that sends its own `max_execution_time` must run without a context deadline. Stage 2 in `sqlconnect/internal/clickhouse/validation.go` uses `withoutDeadline` and sends `MaxRunBudget` itself. `refusedOpenSetting` bounds the open with `time.AfterFunc(validationOpenTimeout, cancel)` instead of a deadline. A deadline-bearing diagnostic fallback exists on purpose.
- `probeCleanup.run` in `sqlconnect/internal/clickhouse/validation.go` drops the scratch probe table on a context detached from the caller (`context.WithoutCancel` plus `probeCleanupTimeout`), so a caller cancel still removes it. `step` asks `acquire` again for each step and never replays a statement. `cleanupAcquire` reuses the validation connection while `conn.Raw` succeeds. When that connection is unusable it opens a fresh pool connection, bounded by the cleanup timeout. A busy pool can delay the cleanup only in that fallback case.
- `probeCleanup.createSettled` in `sqlconnect/internal/clickhouse/validation.go` polls the query outcome of a CREATE with an unknown outcome, at most `probeSettlePolls` times. It stops early when less than `probeCleanupReserve` of the deadline remains, or when the outcome read fails. If the outcome stays open, the cleanup still runs the DROP and then returns `CH_OUTCOME_UNKNOWN` inside `CH_SCRATCH_CLEANUP_FAILED`, because the CREATE can land after the DROP. A denied DROP stays fatal.

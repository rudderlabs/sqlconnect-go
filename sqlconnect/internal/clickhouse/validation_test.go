package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

func TestValidation_NamesPinnedConstraints(t *testing.T) {
	for _, setting := range []string{"log_queries", "additional_table_filters", "log_queries_min_type", "log_queries_probability", "log_queries_min_query_duration_ms"} {
		t.Run(setting, func(t *testing.T) {
			var settingsContexts []context.Context
			pool := sql.OpenDB(stubConnector{query: func(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
				settingsContexts = append(settingsContexts, ctx)
				settings, err := p12SettingsAtErr(ctx)
				if err != nil {
					return nil, err
				}
				if _, ok := settings[setting]; ok {
					return nil, &ch.Exception{Code: 452}
				}
				return &tableRows{cols: []string{"1"}, rows: [][]driver.Value{{uint8(1)}}}, nil
			}})
			t.Cleanup(func() { _ = pool.Close() })
			conn, err := pool.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			err = settingFailure(context.Background(), conn, &ch.Exception{Code: 452})
			for _, settingsCtx := range settingsContexts {
				p12SettingsAt(t, settingsCtx)
			}
			details, ok := clickhousequery.Describe(err)
			require.True(t, ok)
			require.Equal(t, "CH_PERMISSION", details.Code)
			require.Equal(t, setting, details.Field)
			require.EqualValues(t, 452, details.ServerCode)
			var stage sqlconnect.ValidationStageError
			require.ErrorAs(t, err, &stage)
			require.Equal(t, 2, stage.Stage)
			require.Equal(t, "settings", stage.Tag)
		})
	}
}

func TestPing_ReachabilityOnly(t *testing.T) {
	db, recorder := unitDBWithRows(t, []string{"1"}, [][]driver.Value{{uint8(1)}})
	ctx := sqlconnect.WithValidationOptions(context.Background(), sqlconnect.ValidationOptions{WorkingDatabase: "invalid!"})
	require.NoError(t, db.Ping())
	require.NoError(t, db.PingContext(ctx))
	for _, call := range recorder.calls() {
		require.Equal(t, "SELECT 1", call.q)
		require.Equal(t, helloSettings(), p12SettingsAt(t, call.ctx))
	}
	require.Len(t, recorder.calls(), 2)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, db.PingContext(cancelled), context.Canceled)
	_, err := db.ValidateContext(ctx)
	require.Error(t, err, "explicit validation still checks the working database")
}

func TestValidation_RoleClosure(t *testing.T) {
	res := &sqlconnect.ValidationResult{} // current role a; a->b, b->c, c->a (cycle), a->c (duplicate path)
	// z is granted to the user but not current: it stays out of the closure.
	w := unitDB(t).diagnostics(context.Background(), scriptedRoles(t, map[string][]string{"a": {"b", "c"}, "b": {"c"}, "c": {"a"}, "z": {"a"}}, nil), res)
	require.True(t, res.GrantsChecked)
	require.Empty(t, filterOp(w, "inspect_grants"))
	require.Equal(t, []string{"a", "b", "c"}, scriptedRolesVisited(t), "each role read once; the cycle stops")
	res = &sqlconnect.ValidationResult{}
	w = unitDB(t).diagnostics(context.Background(), scriptedRoles(t, nil, &ch.Exception{Code: 497}), res)
	require.False(t, res.GrantsChecked, "a denied system.role_grants is a warning, never an error")
	require.Equal(t, []sqlconnect.ValidationWarning{{Info: sqlconnect.ErrorInfo{Category: "permission", Code: "CH_PERMISSION", ServerCode: 497}, Operation: "inspect_grants"}},
		filterOp(w, "inspect_grants"))
}

func TestValidation_EngineRules(t *testing.T) {
	engineSQL := "SELECT engine FROM system.databases WHERE name = ?"
	queries, err := runValidationScriptErr(t, map[string]any{engineSQL: "Replicated"}) // Replicated needs Keeper, so it is scripted
	requireCode(t, err, "CH_CONFIG_INVALID")
	require.Contains(t, err.Error(), "Replicated")
	require.Contains(t, err.Error(), "the working database engine")
	for _, q := range queries {
		require.False(t, strings.HasPrefix(q, "CREATE TABLE"), "no probe after an engine refusal")
	}
	require.NotContains(t, runValidationScript(t, map[string]any{engineSQL: "Shared"}), "SELECT hostName()", "Shared skips the host comparison")
	_, err = runValidationScriptErr(t, map[string]any{engineSQL: "pw_Retl_123"}) // a hostile server echoes a credential as the engine
	requireCode(t, err, "CH_CONFIG_INVALID")
	for e := err; e != nil; e = errors.Unwrap(e) {
		require.NotContains(t, fmt.Sprintf("%v %+v %#v", e, e, e), "pw_Retl_123")
	}
}

func TestValidation_RudderSchemaMissing(t *testing.T) {
	var stub validationStub
	db := stub.db(t, nil, nil)
	working := "custom_working_db"
	ctx := sqlconnect.WithValidationOptions(context.Background(), sqlconnect.ValidationOptions{WorkingDatabase: working})
	var calls []stubCall
	pool := answerDB(t, func(query string, args []any) ([]string, [][]driver.Value, error) {
		calls = append(calls, stubCall{q: query, args: args})
		return []string{"engine"}, nil, nil
	}, nil)
	err := db.checkEngine(ctx, pool, driverReadSettings)
	for _, call := range calls {
		require.Equal(t, engineSQL, call.q)
		require.Equal(t, []any{working}, call.args)
	}
	requireCode(t, err, "CH_CONFIG_INVALID")
	requireStage(t, err, 3, "engine")
	var configErr *cherr.Error
	require.ErrorAs(t, err, &configErr)
	require.Equal(t, "workingDatabase", configErr.Field)
	require.Contains(t, err.Error(), "the working database does not exist")
	require.NotContains(t, err.Error(), working)
}

// fixed returns an acquire function that hands out ex for every step.
func fixed(ex sqlconnect.QueryExecutor) func(context.Context) (sqlconnect.QueryExecutor, func(), error) {
	return func(context.Context) (sqlconnect.QueryExecutor, func(), error) { return ex, func() {}, nil }
}

func TestProbeCleanup_Codes(t *testing.T) {
	ref := sqlconnect.NewRelationRef("_rudder_probe_0123456789abcdef", sqlconnect.WithSchema("_rudderstack"))
	db := unitDB(t)
	for name, c := range map[string]probeCleanup{
		"acquisition fails": {
			db: db, ref: ref, created: true,
			acquire: func(context.Context) (sqlconnect.QueryExecutor, func(), error) {
				return nil, nil, errors.New("pool exhausted")
			},
		},
		"absence times out": {
			db: db, ref: ref, created: true, acquire: fixed(scripted(t, execOK, row("still-there"))),
			policy: sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, Deadline: 50 * time.Millisecond},
		},
		"drop refused": {db: db, ref: ref, created: true, acquire: fixed(scripted(t, execErr(&ch.Exception{Code: 497})))},
	} {
		err := c.run(context.Background())
		requireCode(t, err, "CH_SCRATCH_CLEANUP_FAILED")
		requireStage(t, err, 4, "scratch_cleanup")
		_ = name
	}
	require.NoError(t, probeCleanup{db: db, ref: ref, created: false, acquire: fixed(scripted(t, execOK, noRow))}.run(context.Background()))

	// Each step takes its own connection and releases it.
	good := scripted(t, execOK, noRow)
	var acquired, released int
	require.NoError(t, probeCleanup{
		db: db, ref: ref, created: true,
		acquire: func(context.Context) (sqlconnect.QueryExecutor, func(), error) {
			acquired++
			return good, func() { released++ }, nil
		},
	}.run(context.Background()))
	require.Equal(t, []int{2, 2}, []int{acquired, released}, "the DROP and the absence check each take and release a connection")
	require.True(t, strings.HasPrefix(good.lastExec, "DROP TABLE `_rudderstack`.`_rudder_probe_"), good.lastExec)
	require.Contains(t, unknownLastExec(t), "DROP TABLE IF EXISTS", "an unknown CREATE outcome drops only if the table exists")
}

// lateCreate is a server where the probe CREATE is still in flight when the
// cleanup starts. The CREATE runs for running polls of system.processes and
// then lands, which creates the table. It records every statement in order.
type lateCreate struct {
	mu      sync.Mutex
	running int // polls left that still see the CREATE in system.processes
	logRow  string
	exists  bool
	log     []string
}

func (l *lateCreate) exec(t *testing.T) *sql.DB {
	var outcomeCalls []stubCall
	t.Cleanup(func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		for _, call := range outcomeCalls {
			want := []any{"retl-create"}
			if call.q == queryLogSQL {
				want = append(want, "Create")
			}
			require.Equal(t, want, call.args, "%s", call.q)
		}
	})
	return answerDB(t, func(q string, args []any) ([]string, [][]driver.Value, error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.log = append(l.log, q)
		switch q {
		case processesSQL:
			outcomeCalls = append(outcomeCalls, stubCall{q: q, args: args})
			if l.running > 0 {
				l.running--
				if l.running == 0 {
					l.exists = true // the delayed CREATE lands
				}
				return []string{"count()"}, [][]driver.Value{{int64(1)}}, nil
			}
			return []string{"count()"}, [][]driver.Value{{int64(0)}}, nil
		case queryLogSQL:
			outcomeCalls = append(outcomeCalls, stubCall{q: q, args: args})
			if l.logRow == "" {
				return []string{"type"}, nil, nil
			}
			return []string{"type"}, [][]driver.Value{{l.logRow}}, nil
		case visibilitySQL:
			if l.exists {
				return []string{"v"}, [][]driver.Value{{"0000-uuid"}}, nil
			}
			return []string{"v"}, nil, nil
		}
		return nil, nil, fmt.Errorf("unexpected query %q", q)
	}, func(q string) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.log = append(l.log, q)
		if strings.HasPrefix(q, "DROP TABLE IF EXISTS ") {
			l.exists = false
		}
		return nil
	})
}

func TestProbeCleanup_WaitsForUnknownCreate(t *testing.T) {
	ref := sqlconnect.NewRelationRef("_rudder_probe_0123456789abcdef", sqlconnect.WithSchema("_rudderstack"))
	db := unitDB(t)
	orig := sleep
	sleep = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { sleep = orig })
	cleanup := func(l *lateCreate) probeCleanup {
		return probeCleanup{
			db: db, ref: ref, createID: "retl-create", acquire: fixed(l.exec(t)),
			policy: sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, Deadline: 50 * time.Millisecond},
		}
	}

	l := &lateCreate{running: 3, logRow: "QueryFinish"}
	require.NoError(t, cleanup(l).run(context.Background()))
	require.False(t, l.exists, "a CREATE that lands during the cleanup is dropped")
	drop := slices.IndexFunc(l.log, func(q string) bool { return strings.HasPrefix(q, "DROP TABLE IF EXISTS ") })
	finished := slices.Index(l.log, queryLogSQL)
	require.True(t, finished >= 0 && drop > finished, "the DROP runs after the CREATE is known to be over: %q", l.log)

	// The CREATE ended in failure: that also proves it is over.
	l = &lateCreate{logRow: "ExceptionBeforeStart"}
	require.NoError(t, cleanup(l).run(context.Background()))

	// Never seen, or still running after every poll: the CREATE can still
	// land. The wait is bounded, the DROP still runs, and the cleanup fails.
	for name, l := range map[string]*lateCreate{"not found": {}, "running": {running: probeSettlePolls + 5}} {
		err := cleanup(l).run(context.Background())
		requireCode(t, err, "CH_SCRATCH_CLEANUP_FAILED")
		requireStage(t, err, 4, "scratch_cleanup")
		require.Contains(t, l.log, "DROP TABLE IF EXISTS `_rudderstack`.`_rudder_probe_0123456789abcdef` SYNC", name)
		require.LessOrEqual(t, countOf(l.log, processesSQL), probeSettlePolls, name+": a bounded number of outcome reads")
	}

	// The outcome read is refused: the DROP runs at once, and the outcome
	// stays open.
	refused := scripted(t, failWith(&ch.Exception{Code: 497}), execOK, noRow)
	err := probeCleanup{db: db, ref: ref, createID: "retl-create", acquire: fixed(refused)}.run(context.Background())
	requireCode(t, err, "CH_SCRATCH_CLEANUP_FAILED")
	require.True(t, strings.HasPrefix(refused.lastExec, "DROP TABLE IF EXISTS "), refused.lastExec)
}

func TestProbeCleanup_OutcomeReadKeepsDropTime(t *testing.T) {
	ref := sqlconnect.NewRelationRef("_rudder_probe_0123456789abcdef", sqlconnect.WithSchema("_rudderstack"))
	orig := probeLookupTimeout
	probeLookupTimeout = 20 * time.Millisecond
	t.Cleanup(func() { probeLookupTimeout = orig })

	// The outcome read stalls until its context ends. The DROP still runs,
	// on a context with time left.
	var dropped atomic.Bool
	stalled := sql.OpenDB(stubConnector{
		query: func(ctx context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
			if q == processesSQL {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &stubRows{}, nil
		},
		exec: func(ctx context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
			if ctx.Err() == nil && strings.HasPrefix(q, "DROP TABLE IF EXISTS ") {
				dropped.Store(true)
			}
			return driver.RowsAffected(0), nil
		},
	})
	t.Cleanup(func() { _ = stalled.Close() })
	start := time.Now()
	err := probeCleanup{
		db: unitDB(t), ref: ref, createID: "retl-create", acquire: fixed(stalled),
		policy: sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, Deadline: time.Second},
	}.run(context.Background())
	requireCode(t, err, "CH_SCRATCH_CLEANUP_FAILED")
	require.True(t, dropped.Load(), "the DROP reached the server")
	require.Less(t, time.Since(start), 10*time.Second, "the outcome wait has its own bound")
}

func TestProbeCleanup_BadConnOnOutcomeRead(t *testing.T) {
	ref := sqlconnect.NewRelationRef("_rudder_probe_0123456789abcdef", sqlconnect.WithSchema("_rudderstack"))
	orig := sleep
	sleep = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { sleep = orig })
	// The outcome read loses its connection with driver.ErrBadConn. The DROP
	// takes another connection and still runs.
	broken := scripted(t, failWith(driver.ErrBadConn))
	good := scripted(t, execOK, noRow)
	var acquired, released int
	err := probeCleanup{
		db: unitDB(t), ref: ref, createID: "retl-create",
		acquire: func(context.Context) (sqlconnect.QueryExecutor, func(), error) {
			acquired++
			if acquired == 1 {
				return broken, func() { released++ }, nil
			}
			return good, func() { released++ }, nil
		},
	}.run(context.Background())
	requireCode(t, err, "CH_SCRATCH_CLEANUP_FAILED") // the outcome read failed, so the outcome stays open
	require.Equal(t, acquired, released, "each step releases its connection")
	require.True(t, strings.HasPrefix(good.lastExec, "DROP TABLE IF EXISTS "), good.lastExec)
}

func TestProbeCleanup_ReusesValidationConnection(t *testing.T) {
	cfg, err := parseConfig(validJSONInternal(), false)
	require.NoError(t, err)
	var execs atomic.Int32
	pool := sql.OpenDB(stubConnector{
		query: func(context.Context, string, []driver.NamedValue) (driver.Rows, error) { return nil, io.EOF },
		exec: func(context.Context, string, []driver.NamedValue) (driver.Result, error) {
			if execs.Add(1) == 1 {
				return nil, driver.ErrBadConn // a lost response on the validation connection
			}
			return driver.RowsAffected(0), nil
		},
	})
	db := &DB{cfg: cfg}
	db.DB = base.NewDB(pool, func() error { return nil }, base.WithDialect(newDialect()))
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	own := driverExec{conn: conn, settings: driverScratchSettings}
	acquire := db.cleanupAcquire(own)

	ex, done, err := acquire(ctx)
	require.NoError(t, err)
	require.Same(t, conn, ex.(driverExec).conn, "a usable validation connection serves the cleanup")
	_, err = ex.ExecContext(ctx, "DROP TABLE t SYNC")
	require.ErrorIs(t, err, driver.ErrBadConn)
	done()
	require.EqualValues(t, 1, execs.Load(), "the failed statement is not replayed")

	ex, done, err = acquire(ctx)
	require.NoError(t, err)
	defer done()
	require.NotSame(t, conn, ex.(driverExec).conn, "a connection database/sql closed is replaced by a fresh one")
	_, err = ex.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)
}

func TestProbeCleanup_RefusedDropRunsOnce(t *testing.T) {
	ref := sqlconnect.NewRelationRef("_rudder_probe_0123456789abcdef", sqlconnect.WithSchema("_rudderstack"))
	for _, refused := range []error{
		&cherr.Error{Code: cherr.CodeNetwork, Detail: "HTTP 403 from a proxy"},
		&cherr.Error{Code: cherr.CodeRateLimited},
		&cherr.Error{Code: cherr.CodeRedirectRefused},
		&cherr.Error{Code: cherr.CodePermission, ServerCode: 497},
	} {
		once := scripted(t, failWith(refused))
		err := probeCleanup{db: unitDB(t), ref: ref, created: true, acquire: fixed(once)}.run(context.Background())
		requireCode(t, err, "CH_SCRATCH_CLEANUP_FAILED")
		require.Equal(t, 1, once.calls, "%v: the failed DROP ran once and stays fatal", refused)
	}
}

func countOf(list []string, s string) int {
	n := 0
	for _, v := range list {
		if v == s {
			n++
		}
	}
	return n
}

func TestValidation_VersionAndStages(t *testing.T) {
	v, err := parseVersion("26.3.33.24")
	require.NoError(t, err)
	require.Equal(t, [3]int{26, 3, 33}, v)
	for _, bad := range []string{"", "26", "26.3", "26.x.1", "26.3.-1"} {
		_, err := parseVersion(bad)
		require.Error(t, err, bad)
	}
	require.True(t, less([3]int{25, 8, 99}, versionFloor))
	require.False(t, less([3]int{26, 3, 0}, versionFloor))
	require.Equal(t, "25.8.1.2", sanitizeVersion("25.8.1.2"))
	for _, hostile := range []string{"25.8.1.2<script>secret", "25.8.1.2 password=12345678", "12345678", ""} {
		require.Equal(t, "(unrecognized)", sanitizeVersion(hostile), hostile)
	}
	require.Equal(t, []string{"Replicated", "(unrecognized)"}, []string{engineLabel("Replicated"), engineLabel("pw_Retl_123")},
		"an unknown engine answer is server text and is never repeated")

	for _, c := range []struct {
		err   error
		stage int
		tag   string
	}{
		{&ch.Exception{Code: 516}, 1, "authentication"},
		{&ch.Exception{Code: 164}, 1, "settings"},
		{&ch.Exception{Code: 452}, 1, "settings"},
		{&ch.Exception{Code: 81}, 1, "configuration"},
		{cherr.New(cherr.CodeRedirectRefused, "", "x"), 0, "network"},
		{cherr.New(cherr.CodeHostNotAllowed, "", "x"), 0, "network"},
		{cherr.New(cherr.CodeTLS, "", "x"), 1, "tls"},
		{io.ErrUnexpectedEOF, 0, "network"},
	} {
		requireStage(t, openFailure(c.err, func(int32) string { return "max_execution_time" }), c.stage, c.tag)
	}
	var ce *cherr.Error
	var asked int32
	require.ErrorAs(t, openFailure(&ch.Exception{Code: 452}, func(c int32) string { asked = c; return "send_progress_in_http_headers" }), &ce)
	require.Equal(t, []any{cherr.CodePermission, "send_progress_in_http_headers", int32(452), int32(452)}, []any{ce.Code, ce.Field, ce.ServerCode, asked},
		"the field comes from the refused-setting lookup, never a fixed name")
}

func TestValidation_ScriptedPass(t *testing.T) {
	queries := runValidationScript(t, nil)
	require.Contains(t, queries, "SELECT hostName()", "Atomic compares hosts")
	var creates, drops int
	for _, q := range queries {
		if strings.HasPrefix(q, "CREATE TABLE `_rudderstack`.`_rudder_probe_") {
			creates++
			require.Regexp(t, "^CREATE TABLE `_rudderstack`\\.`_rudder_probe_[0-9a-f]{16}` \\(probe UInt8\\)\\nENGINE = MergeTree\\nORDER BY tuple\\(\\)$", q)
		}
		if strings.HasPrefix(q, "DROP TABLE `_rudderstack`.`_rudder_probe_") {
			drops++
			require.True(t, strings.HasSuffix(q, " SYNC"), q)
		}
	}
	require.Equal(t, []int{1, 1}, []int{creates, drops})
	for _, q := range []string{"CHECK GRANT ALTER DELETE ON `_rudderstack`.`sync_log`", "CHECK GRANT SELECT ON `analytics`.*"} {
		require.Contains(t, queries, q)
	}

	_, err := runValidationScriptErr(t, map[string]any{"SELECT version(), currentDatabase()": []driver.Value{"25.8.1.2", "analytics"}})
	requireCode(t, err, "CH_VERSION_BELOW_FLOOR")
	requireStage(t, err, 1, "version")
	require.Contains(t, err.Error(), "25.8.1.2")
	require.Contains(t, err.Error(), "26.3.0")
	_, err = runValidationScriptErr(t, map[string]any{"SELECT version(), currentDatabase()": []driver.Value{"26.3.1.1", "other"}})
	requireCode(t, err, "CH_CONFIG_INVALID")
	requireStage(t, err, 1, "configuration")
	_, err = runValidationScriptErr(t, map[string]any{"CHECK GRANT INSERT ON `_rudderstack`.*": int64(0)})
	requireStage(t, err, 3, "grant_check")
	var ce *cherr.Error
	require.ErrorAs(t, err, &ce)
	require.Equal(t, []string{cherr.CodePermission, "INSERT"}, []string{ce.Code, ce.Field})
	_, err = runValidationScriptErr(t, map[string]any{"SELECT hostName()": hostSeq{"h1", "h2"}})
	requireCode(t, err, "CH_CLUSTER_UNSUPPORTED")
	requireStage(t, err, 3, "engine")
}

func TestValidation_RudderSchemaOverride(t *testing.T) {
	stub := validationStub{rudderSchema: "custom_rudder"}
	db := stub.db(t, nil, nil)
	ctx := sqlconnect.WithValidationOptions(context.Background(), sqlconnect.ValidationOptions{WorkingDatabase: "custom_rudder", SyncLogPruning: true})
	_, err := db.ValidateContext(ctx)
	require.NoError(t, err)
	for _, privilege := range []string{"SELECT", "INSERT", "CREATE TABLE", "DROP TABLE"} {
		require.Contains(t, stub.statement, "CHECK GRANT "+privilege+" ON `custom_rudder`.*")
	}
	require.Contains(t, stub.statement, "CHECK GRANT ALTER DELETE ON `custom_rudder`.`sync_log`")
	var creates, drops int
	for _, query := range stub.statement {
		if strings.HasPrefix(query, "CREATE TABLE `custom_rudder`.`_rudder_probe_") {
			creates++
		}
		if strings.HasPrefix(query, "DROP TABLE `custom_rudder`.`_rudder_probe_") {
			drops++
		}
	}
	require.Equal(t, []int{1, 1}, []int{creates, drops})
}

func TestValidation_WorkingDatabaseRequired(t *testing.T) {
	for _, ctx := range []context.Context{
		context.Background(),
		sqlconnect.WithValidationOptions(context.Background(), sqlconnect.ValidationOptions{}),
		sqlconnect.WithValidationOptions(context.Background(), sqlconnect.ValidationOptions{SyncLogPruning: true}),
	} {
		stub := validationStub{}
		_, err := stub.db(t, nil, nil).ValidateContext(ctx)
		var configErr *cherr.Error
		require.ErrorAs(t, err, &configErr)
		require.Equal(t, [2]string{cherr.CodeConfigInvalid, "workingDatabase"}, [2]string{configErr.Code, configErr.Field})
		requireStage(t, err, 3, "engine")
		require.Empty(t, stub.statement, "no query runs without a working database")
	}
}

func TestValidation_WorkingDatabaseExclusions(t *testing.T) {
	for _, name := range []string{
		"default", "DEFAULT", "Default", "system", "SYSTEM", "System",
		"information_schema", "INFORMATION_SCHEMA", "Information_Schema", "analytics", "ANALYTICS", "my-db",
	} {
		t.Run(name, func(t *testing.T) {
			stub := validationStub{}
			ctx := sqlconnect.WithValidationOptions(context.Background(), sqlconnect.ValidationOptions{WorkingDatabase: name})
			_, err := stub.db(t, nil, nil).ValidateContext(ctx)
			var configErr *cherr.Error
			require.ErrorAs(t, err, &configErr)
			require.Equal(t, cherr.CodeConfigInvalid, configErr.Code)
			require.Equal(t, "workingDatabase", configErr.Field)
			require.Empty(t, stub.statement)
			if namePattern.MatchString(name) {
				require.Contains(t, err.Error(), "the working database must differ")
			}
		})
	}
}

func TestValidation_RefusedCreateSendsNoDrop(t *testing.T) {
	s := &validationStub{}
	_, err := s.db(t, nil, func(q string) error {
		if strings.HasPrefix(q, "CREATE TABLE ") {
			return &ch.Exception{Code: 57} // TABLE_ALREADY_EXISTS: the name belongs to someone else
		}
		return nil
	}).ValidateContext(workingCtx(context.Background()))
	requireStage(t, err, 4, "scratch_write")
	for _, q := range s.execs {
		require.False(t, strings.HasPrefix(q, "DROP"), "a refused CREATE leaves the existing table alone: %s", q)
	}
}

func TestValidation_ProbeValueMismatch(t *testing.T) {
	s := &validationStub{probeValue: int64(2)}
	_, err := s.db(t, nil, nil).ValidateContext(workingCtx(context.Background()))
	requireCode(t, err, "CH_SCHEMA_MISMATCH")
	requireStage(t, err, 4, "scratch_write")
	require.True(t, slices.ContainsFunc(s.execs, func(q string) bool { return strings.HasPrefix(q, "DROP TABLE `_rudderstack`.`_rudder_probe_") }),
		"the cleanup drops the probe after a wrong read")
	require.False(t, s.created)
}

func TestValidation_CancelledBeforeSlot(t *testing.T) {
	db := unitDB(t)
	db.validateSem <- struct{}{}
	db.validateSem <- struct{}{}
	ctx, cancel := context.WithCancel(workingCtx(context.Background()))
	cancel()
	_, err := db.ValidateContext(ctx)
	require.ErrorIs(t, err, context.Canceled)
	requireStage(t, err, 0, "network")
}

func TestValidation_ReadsCarryTheRemainingBudget(t *testing.T) {
	reads := []string{
		"SELECT version(), currentDatabase()", "CHECK GRANT INSERT ON `_rudderstack`.*", engineSQL, hostNameSQL,
		settingsSQL, currentRolesSQL, roleGrantsSQL, userGrantsSQL, roleGrantSQL,
	}
	for name, c := range map[string]struct {
		callerDeadline time.Duration // 0 sets none
		limit, open    int
	}{
		"no caller deadline":  {0, int(validationBudget.Seconds()), int(validationOpenTimeout.Seconds())},
		"run deadline":        {2 * time.Hour, int(validationBudget.Seconds()), int(validationOpenTimeout.Seconds())},
		"short call deadline": {30 * time.Second, 30, 30},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := workingCtx(context.Background())
			if c.callerDeadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, c.callerDeadline)
				defer cancel()
			}
			s := &validationStub{}
			_, err := s.db(t, map[string]any{currentRolesSQL: "r"}, nil).ValidateContext(ctx)
			require.NoError(t, err)
			seen := map[string]int{}
			for _, r := range s.reads {
				if !slices.Contains(reads, r.q) && !strings.HasPrefix(r.q, "CHECK GRANT ") {
					continue
				}
				seen[r.q]++
				require.NoError(t, r.err, r.q)
				require.False(t, r.deadline, "the fork replaces max_execution_time from a deadline: %s", r.q)
				limit, ok := r.settings["max_execution_time"].(int)
				require.True(t, ok, "no time limit: %s", r.q)
				want := c.limit
				if r.q == reads[0] {
					want = c.open
				}
				require.True(t, limit >= want-2 && limit <= want, "%s: max_execution_time %d, want about %d", r.q, limit, want)
			}
			for _, q := range reads {
				require.Positive(t, seen[q], "the validation sent no %s", q)
			}
		})
	}
}

// The probe writes carry the remaining budget as max_execution_time, as the
// reads do. Without a caller deadline, clickhouse-go sends no time limit.
func TestValidation_ProbeCarriesTheRemainingBudget(t *testing.T) {
	for name, c := range map[string]struct {
		callerDeadline time.Duration // 0 sets none
		limit          int
	}{
		"no caller deadline":  {0, int(validationBudget.Seconds())},
		"run deadline":        {2 * time.Hour, int(validationBudget.Seconds())},
		"short call deadline": {30 * time.Second, 30},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := workingCtx(context.Background())
			if c.callerDeadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, c.callerDeadline)
				defer cancel()
			}
			s := &validationStub{}
			_, err := s.db(t, map[string]any{currentRolesSQL: "r"}, nil).ValidateContext(ctx)
			require.NoError(t, err)
			var probe []stubRead
			for _, w := range s.writes {
				if strings.HasPrefix(w.q, "CREATE TABLE ") || strings.HasPrefix(w.q, "INSERT INTO ") {
					probe = append(probe, w)
				}
			}
			require.Len(t, probe, 2, "CREATE and INSERT")
			readBacks := 0
			for _, r := range s.reads {
				if strings.HasPrefix(r.q, "SELECT probe FROM ") {
					readBacks++
					require.True(t, r.deadline, "the visibility poll bounds the read back: %s", r.q)
				}
			}
			require.Positive(t, readBacks)
			for _, r := range probe {
				require.NoError(t, r.err, r.q)
				require.False(t, r.deadline, "the fork replaces max_execution_time from a deadline: %s", r.q)
				limit, ok := r.settings["max_execution_time"].(int)
				require.True(t, ok, "no time limit: %s", r.q)
				require.True(t, limit >= c.limit-2 && limit <= c.limit, "%s: max_execution_time %d, want about %d", r.q, limit, c.limit)
			}
		})
	}
}

func TestValidation_SnapshotIgnoresItsOwnTimeLimit(t *testing.T) {
	snapshot := func(rows [][]driver.Value) []sqlconnect.ValidationWarning {
		ex := answerDB(t, func(q string, _ []any) ([]string, [][]driver.Value, error) {
			if q == settingsSQL {
				return []string{"name", "value", "changed"}, rows, nil
			}
			return []string{"v"}, nil, nil
		}, nil)
		return filterOp(unitDB(t).diagnostics(context.Background(), ex, &sqlconnect.ValidationResult{}), "settings_snapshot")
	}
	require.Empty(t, snapshot([][]driver.Value{{"max_execution_time", "180", int64(1)}}), "the read sets max_execution_time itself")
	require.Len(t, snapshot([][]driver.Value{{"join_use_nulls", "0", int64(1)}}), 1)
}

var execOK = stubResult{}

func execErr(err error) stubResult { return stubResult{err: err} }

// filterOp returns the warnings with the operation op.
func filterOp(w []sqlconnect.ValidationWarning, op string) []sqlconnect.ValidationWarning {
	var out []sqlconnect.ValidationWarning
	for _, x := range w {
		if x.Operation == op {
			out = append(out, x)
		}
	}
	return out
}

// requireStage asserts that err is a validation failure of stage n and tag.
func requireStage(t *testing.T, err error, n int, tag string) {
	t.Helper()
	var se sqlconnect.ValidationStageError
	require.True(t, errors.As(err, &se), "want a stage error, got %v", err)
	require.Equal(t, []any{n, tag}, []any{se.Stage, se.Tag}, "%v", err)
}

// answerFunc answers one stub query with columns and rows, or an error.
type answerFunc func(q string, args []any) ([]string, [][]driver.Value, error)

// answerDB is a pool on the stub driver. Queries go to answer, execs to exec.
func answerDB(t *testing.T, answer answerFunc, exec func(q string) error) *sql.DB {
	t.Helper()
	var execCtx func(context.Context, string) error
	if exec != nil {
		execCtx = func(_ context.Context, q string) error { return exec(q) }
	}
	return answerDBCtx(t, func(_ context.Context, q string, args []any) ([]string, [][]driver.Value, error) {
		return answer(q, args)
	}, execCtx)
}

// answerDBCtx is answerDB with the statement context passed to answer.
func answerDBCtx(t *testing.T, answer func(ctx context.Context, q string, args []any) ([]string, [][]driver.Value, error), exec func(ctx context.Context, q string) error) *sql.DB {
	t.Helper()
	pool := sql.OpenDB(stubConnector{
		query: func(ctx context.Context, q string, nv []driver.NamedValue) (driver.Rows, error) {
			var args []any
			for _, a := range nv {
				args = append(args, a.Value)
			}
			cols, rows, err := answer(ctx, q, args)
			if err != nil {
				return nil, err
			}
			return &tableRows{cols: cols, rows: rows}, nil
		},
		exec: func(ctx context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
			if exec == nil {
				return driver.RowsAffected(0), nil
			}
			if err := exec(ctx, q); err != nil {
				return nil, err
			}
			return driver.RowsAffected(0), nil
		},
	})
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

var rolesVisited sync.Map // *testing.T -> *[]string

// scriptedRoles answers the stage 5 reads. The current role is "a"; graph
// maps a role to the roles granted to it. A non-nil deny refuses every read
// of system.role_grants and system.grants.
func scriptedRoles(t *testing.T, graph map[string][]string, deny error) sqlconnect.QueryExecutor {
	t.Helper()
	visited := &[]string{}
	var mu sync.Mutex
	rolesVisited.Store(t, visited)
	return answerDB(t, func(q string, args []any) ([]string, [][]driver.Value, error) {
		switch {
		case q == currentRolesSQL:
			return []string{"role_name"}, [][]driver.Value{{"a"}}, nil
		case deny != nil && (strings.Contains(q, "system.role_grants") || strings.Contains(q, "system.grants")):
			return nil, nil, deny
		case q == roleGrantsSQL:
			role := args[0].(string)
			mu.Lock()
			*visited = append(*visited, role)
			mu.Unlock()
			var rows [][]driver.Value
			for _, g := range graph[role] {
				rows = append(rows, []driver.Value{g})
			}
			return []string{"granted_role_name"}, rows, nil
		case q == settingsSQL:
			return []string{"name", "value", "changed"}, [][]driver.Value{{"join_use_nulls", "0", int64(0)}}, nil
		default:
			return []string{"access_type"}, nil, nil
		}
	}, nil)
}

func scriptedRolesVisited(t *testing.T) []string {
	t.Helper()
	v, ok := rolesVisited.Load(t)
	require.True(t, ok, "scriptedRoles was not called")
	return *v.(*[]string)
}

// hostSeq answers successive hostName() reads with the given hosts and then
// repeats the last one.
type hostSeq []string

// validationStub is a DB on the stub driver that answers every validation
// statement with a passing value, except the overrides keyed by exact SQL.
type validationStub struct {
	mu           sync.Mutex
	statement    []string
	reads        []stubRead
	execs        []string
	writes       []stubRead // execs with their settings and deadline
	created      bool
	hostReads    int
	rudderSchema string
	probeValue   driver.Value // the probe read answer; nil answers 1
}

func (s *validationStub) db(t *testing.T, overrides map[string]any, execFail func(q string) error) *DB {
	t.Helper()
	cfg, err := parseConfig(validJSONInternal(), false)
	require.NoError(t, err)
	var engineArgs [][]any
	t.Cleanup(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		working := testWorkingDB
		if s.rudderSchema != "" {
			working = s.rudderSchema
		}
		for _, args := range engineArgs {
			require.Equal(t, []any{working}, args)
		}
	})
	one := func(v driver.Value) ([]string, [][]driver.Value, error) {
		return []string{"v"}, [][]driver.Value{{v}}, nil
	}
	pool := answerDBCtx(t, func(ctx context.Context, q string, args []any) ([]string, [][]driver.Value, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.statement = append(s.statement, q)
		settings, err := p12SettingsAtErr(ctx)
		_, deadline := ctx.Deadline()
		s.reads = append(s.reads, stubRead{q: q, settings: settings, err: err, deadline: deadline})
		if v, ok := overrides[q]; ok {
			switch v := v.(type) {
			case []driver.Value:
				return []string{"a", "b"}[:len(v)], [][]driver.Value{v}, nil
			case hostSeq:
				h := v[min(s.hostReads, len(v)-1)]
				s.hostReads++
				return one(h)
			default:
				return one(v)
			}
		}
		switch {
		case q == "SELECT version(), currentDatabase()":
			return []string{"version()", "currentDatabase()"}, [][]driver.Value{{"26.3.33.24", "analytics"}}, nil
		case strings.HasPrefix(q, "SELECT probe FROM ") && s.probeValue != nil:
			return one(s.probeValue)
		case q == "SELECT 1", strings.HasPrefix(q, "CHECK GRANT "), strings.HasPrefix(q, "SELECT probe FROM "):
			return one(int64(1))
		case q == engineSQL:
			engineArgs = append(engineArgs, args)
			return one("Atomic")
		case q == hostNameSQL:
			return one("h1")
		case q == visibilitySQL:
			if s.created {
				return one("0000-uuid")
			}
			return []string{"v"}, nil, nil
		default:
			return []string{"v"}, nil, nil
		}
	}, func(ctx context.Context, q string) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.statement = append(s.statement, q)
		s.execs = append(s.execs, q)
		settings, err := p12SettingsAtErr(ctx)
		_, deadline := ctx.Deadline()
		s.writes = append(s.writes, stubRead{q: q, settings: settings, err: err, deadline: deadline})
		if execFail != nil {
			if err := execFail(q); err != nil {
				return err
			}
		}
		switch {
		case strings.HasPrefix(q, "CREATE TABLE "):
			s.created = true
		case strings.HasPrefix(q, "DROP TABLE "):
			s.created = false
		}
		return nil
	})
	d := &DB{cfg: cfg, validateSem: make(chan struct{}, maxConcurrentValidations)}
	d.DB = base.NewDB(pool, func() error { return nil }, base.WithDialect(newDialect()))
	return d
}

// stubRead is one query the validation stub answered: its text, the
// settings map it carried and whether its context had a deadline.
type stubRead struct {
	q        string
	settings map[string]any
	err      error
	deadline bool
}

func (s *validationStub) queries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.statement...)
}

// runValidationScriptErr runs ValidateContext on the stub and returns every
// statement it sent, in order, and the error.
func runValidationScriptErr(t *testing.T, overrides map[string]any) ([]string, error) {
	t.Helper()
	s := &validationStub{}
	_, err := s.db(t, overrides, nil).ValidateContext(workingCtx(context.Background()))
	return s.queries(), err
}

// runValidationScript is runValidationScriptErr that requires success.
func runValidationScript(t *testing.T, overrides map[string]any) []string {
	t.Helper()
	q, err := runValidationScriptErr(t, overrides)
	require.NoError(t, err)
	return q
}

// unknownLastExec fails the probe CREATE with a transport error and returns
// the last statement the cleanup sent.
func unknownLastExec(t *testing.T) string {
	t.Helper()
	s := &validationStub{}
	_, err := s.db(t, nil, func(q string) error {
		if strings.HasPrefix(q, "CREATE TABLE ") {
			return io.ErrUnexpectedEOF
		}
		return nil
	}).ValidateContext(workingCtx(context.Background()))
	requireStage(t, err, 4, "scratch_cleanup") // the stub answers no outcome read, so the outcome stays open
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execs[len(s.execs)-1]
}

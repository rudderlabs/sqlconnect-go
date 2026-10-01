package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

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
	for _, q := range queries {
		require.False(t, strings.HasPrefix(q, "CREATE TABLE"), "no probe after an engine refusal")
	}
	require.NotContains(t, runValidationScript(t, map[string]any{engineSQL: "Shared"}), "SELECT hostName()", "Shared skips the host comparison")
}

func TestProbeCleanup_Codes(t *testing.T) {
	ref := sqlconnect.NewRelationRef("_rudder_probe_0123456789abcdef", sqlconnect.WithSchema("scratch_db"))
	db := unitDB(t)
	for name, c := range map[string]probeCleanup{
		"acquisition fails": {
			db: db, ref: ref, created: true, broken: true,
			acquire: func(context.Context) (sqlconnect.QueryExecutor, func(), error) {
				return nil, nil, errors.New("pool exhausted")
			},
		},
		"absence times out": {
			db: db, ref: ref, created: true, exec: scripted(t, execOK, row("still-there")),
			policy: sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, Deadline: 50 * time.Millisecond},
		},
		"drop refused": {db: db, ref: ref, created: true, exec: scripted(t, execErr(&ch.Exception{Code: 497}))},
	} {
		err := c.run(context.Background())
		requireCode(t, err, "CH_SCRATCH_CLEANUP_FAILED")
		requireStage(t, err, 4, "scratch_cleanup")
		_ = name
	}
	require.NoError(t, probeCleanup{db: db, ref: ref, created: false, exec: scripted(t, execOK, noRow)}.run(context.Background()))
	require.Contains(t, unknownLastExec(t), "DROP TABLE IF EXISTS", "an unknown CREATE outcome drops only if the table exists")
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
	require.Equal(t, "25.8.1.2", sanitizeVersion("25.8.1.2<script>secret"))

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
		if strings.HasPrefix(q, "CREATE TABLE `_rudderstack_ws`.`_rudder_probe_") {
			creates++
			require.Regexp(t, "^CREATE TABLE `_rudderstack_ws`\\.`_rudder_probe_[0-9a-f]{16}` \\(probe UInt8\\)\\nENGINE = MergeTree\\nORDER BY tuple\\(\\)$", q)
		}
		if strings.HasPrefix(q, "DROP TABLE `_rudderstack_ws`.`_rudder_probe_") {
			drops++
			require.True(t, strings.HasSuffix(q, " SYNC"), q)
		}
	}
	require.Equal(t, []int{1, 1}, []int{creates, drops})
	for _, q := range []string{"CHECK GRANT ALTER DELETE ON `_rudderstack_ws`.`sync_log`", "CHECK GRANT SELECT ON `analytics`.*"} {
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
	_, err = runValidationScriptErr(t, map[string]any{"CHECK GRANT INSERT ON `_rudderstack_ws`.*": int64(0)})
	requireStage(t, err, 3, "grant_check")
	var ce *cherr.Error
	require.ErrorAs(t, err, &ce)
	require.Equal(t, []string{cherr.CodePermission, "INSERT"}, []string{ce.Code, ce.Field})
	_, err = runValidationScriptErr(t, map[string]any{"SELECT hostName()": hostSeq{"h1", "h2"}})
	requireCode(t, err, "CH_CLUSTER_UNSUPPORTED")
	requireStage(t, err, 3, "engine")
}

func TestValidation_RefusedCreateSendsNoDrop(t *testing.T) {
	s := &validationStub{}
	_, err := s.db(t, nil, func(q string) error {
		if strings.HasPrefix(q, "CREATE TABLE ") {
			return &ch.Exception{Code: 57} // TABLE_ALREADY_EXISTS: the name belongs to someone else
		}
		return nil
	}).ValidateContext(context.Background())
	requireStage(t, err, 4, "scratch_write")
	for _, q := range s.execs {
		require.False(t, strings.HasPrefix(q, "DROP"), "a refused CREATE leaves the existing table alone: %s", q)
	}
}

func TestValidation_CancelledBeforeSlot(t *testing.T) {
	db := unitDB(t)
	db.validateSem <- struct{}{}
	db.validateSem <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.ValidateContext(ctx)
	require.ErrorIs(t, err, context.Canceled)
	requireStage(t, err, 0, "network")
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
	pool := sql.OpenDB(stubConnector{
		query: func(_ context.Context, q string, nv []driver.NamedValue) (driver.Rows, error) {
			var args []any
			for _, a := range nv {
				args = append(args, a.Value)
			}
			cols, rows, err := answer(q, args)
			if err != nil {
				return nil, err
			}
			return &tableRows{cols: cols, rows: rows}, nil
		},
		exec: func(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
			if exec == nil {
				return driver.RowsAffected(0), nil
			}
			if err := exec(q); err != nil {
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
	mu        sync.Mutex
	statement []string
	execs     []string
	created   bool
	hostReads int
}

func (s *validationStub) db(t *testing.T, overrides map[string]any, execFail func(q string) error) *DB {
	t.Helper()
	cfg, err := parseConfig(validJSONInternal(nil), false)
	require.NoError(t, err)
	one := func(v driver.Value) ([]string, [][]driver.Value, error) {
		return []string{"v"}, [][]driver.Value{{v}}, nil
	}
	pool := answerDB(t, func(q string, _ []any) ([]string, [][]driver.Value, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.statement = append(s.statement, q)
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
		case q == "SELECT 1", strings.HasPrefix(q, "CHECK GRANT "), strings.HasPrefix(q, "SELECT probe FROM "):
			return one(int64(1))
		case q == engineSQL:
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
	}, func(q string) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.statement = append(s.statement, q)
		s.execs = append(s.execs, q)
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
	_, err := s.db(t, overrides, nil).ValidateContext(context.Background())
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
	}).ValidateContext(context.Background())
	requireStage(t, err, 4, "scratch_write")
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execs[len(s.execs)-1]
}

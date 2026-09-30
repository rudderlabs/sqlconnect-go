package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestVisibility_Await(t *testing.T) {
	p := sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond, Deadline: 200 * time.Millisecond}
	ref := sqlconnect.NewRelationRef("t", sqlconnect.WithSchema("s"))
	db := unitDB(t)
	slept := recordSleep(t)
	ex := scripted(t, noRow, noRow, noRow, row("1111-uuid"))
	uuid, err := db.AwaitTable(context.Background(), ex, ref, "", p)
	require.Equal(t, []any{"1111-uuid", nil}, []any{uuid, err}, "hidden for three reads, visible on the fourth")
	require.Equal(t, `SELECT toString(uuid) FROM system.tables WHERE database = ? AND name = ? SETTINGS select_sequential_consistency = 1`,
		normalizeSpace(ex.lastQuery))
	require.Equal(t, []any{"s", "t"}, ex.lastArgs)
	require.Equal(t, []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond}, *slept, "doubling backoff")
	for want, ex := range map[string]*scriptedExec{"want": scripted(t, row("other")), "": scripted(t, noRow)} { // wrong UUID; never visible
		_, err = db.AwaitTable(context.Background(), ex, ref, want, p)
		requireCode(t, err, "CH_DDL_NOT_VISIBLE")
		require.Contains(t, err.Error(), "`s`.`t`", "the error names the table")
		require.Contains(t, err.Error(), map[string]string{"want": "visible with UUID want", "": "visible"}[want])
	}
	require.NoError(t, db.AwaitTableAbsent(context.Background(), scripted(t, row("x"), noRow), ref, p))
	err = db.AwaitTableAbsent(context.Background(), scripted(t, row("x")), ref, p)
	requireCode(t, err, "CH_DDL_NOT_VISIBLE")
	require.Contains(t, err.Error(), "absent")
	require.Equal(t, sqlconnect.VisibilityPolicy{InitialBackoff: 250 * time.Millisecond, MaxBackoff: 5 * time.Second, Deadline: 60 * time.Second},
		withDefaults(sqlconnect.VisibilityPolicy{}))
}

func TestVisibility_ReadErrorsAndReferences(t *testing.T) {
	db := unitDB(t)
	p := sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, Deadline: time.Second}
	ex := scripted(t, failWith(&ch.Exception{Code: 497}), row("never read"))
	_, err := db.AwaitTable(context.Background(), ex, sqlconnect.NewRelationRef("t"), "", p)
	requireCode(t, err, "CH_PERMISSION")
	require.Equal(t, 1, ex.calls, "a server error other than a miss is not retried")
	err = db.AwaitTableAbsent(context.Background(), scripted(t, failWith(&ch.Exception{Code: 497})), sqlconnect.NewRelationRef("t"), p)
	requireCode(t, err, "CH_PERMISSION")

	ex = scripted(t, row("u"))
	_, err = db.AwaitTable(context.Background(), ex, sqlconnect.NewRelationRef("t"), "", p)
	require.NoError(t, err)
	require.Equal(t, []any{"analytics", "t"}, ex.lastArgs, "an omitted schema is the configured database")

	ex = scripted(t, row("u"))
	_, err = db.AwaitTable(context.Background(), ex, sqlconnect.RelationRef{Schema: "s", Name: "a\x00b"}, "", p)
	requireCode(t, err, "CH_INVALID_REFERENCE")
	requireCode(t, db.AwaitTableAbsent(context.Background(), ex, sqlconnect.RelationRef{Name: ""}, p), "CH_INVALID_REFERENCE")
	require.Zero(t, ex.calls, "a bad reference sends no query")
}

func TestVisibility_DeadlineBoundsABlockedRead(t *testing.T) {
	db := unitDB(t)
	ref := sqlconnect.NewRelationRef("t", sqlconnect.WithSchema("s"))
	start := time.Now()
	_, err := db.AwaitTable(context.Background(), blockingExec{}, ref, "", sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, Deadline: 100 * time.Millisecond})
	require.True(t, time.Since(start) < 500*time.Millisecond && classify(err).Code == "CH_DDL_NOT_VISIBLE", "%v", err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	_, err = db.AwaitTable(ctx, blockingExec{}, ref, "", sqlconnect.VisibilityPolicy{Deadline: time.Minute})
	require.ErrorIs(t, err, context.Canceled, "caller cancellation is not a visibility verdict")

	ctx, cancel = context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	err = db.AwaitTableAbsent(ctx, scripted(t, row("x")), ref, sqlconnect.VisibilityPolicy{InitialBackoff: time.Hour, Deadline: time.Minute})
	require.ErrorIs(t, err, context.Canceled, "a cancellation during the backoff sleep is not a verdict")
}

func TestVisibility_ReadRetryAfterPassedCheck(t *testing.T) {
	calls := 0
	err := retryVisibleRead(context.Background(), sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, Deadline: time.Second}, func(context.Context) error {
		if calls++; calls == 1 {
			return &ch.Exception{Code: 60} // a visibility miss after a passed check: retried in place
		}
		return nil
	})
	require.Equal(t, []any{nil, 2}, []any{err, calls})
	err = retryVisibleRead(context.Background(), sqlconnect.VisibilityPolicy{Deadline: time.Second}, func(context.Context) error { return &ch.Exception{Code: 497} })
	requireCode(t, err, "CH_PERMISSION")

	calls = 0
	err = retryVisibleRead(context.Background(), sqlconnect.VisibilityPolicy{InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, Deadline: 50 * time.Millisecond},
		func(context.Context) error { calls++; return &ch.Exception{Code: 81} })
	requireCode(t, err, "CH_DDL_NOT_VISIBLE")
	require.Greater(t, calls, 1, "a miss repeats until the deadline")
}

// recordSleep replaces sleep with a recorder that still waits, so the
// deadline keeps its meaning. It restores sleep at cleanup.
func recordSleep(t *testing.T) *[]time.Duration {
	t.Helper()
	var (
		mu  sync.Mutex
		got []time.Duration
	)
	orig := sleep
	sleep = func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		got = append(got, d)
		mu.Unlock()
		return orig(ctx, d)
	}
	t.Cleanup(func() { sleep = orig })
	return &got
}

func normalizeSpace(s string) string { return strings.Join(strings.Fields(s), " ") }

// unitDB is a lazy DB that never connects. Visibility tests pass their own
// executor, so the pool stays unused.
func unitDB(t *testing.T) *DB {
	t.Helper()
	return mustDBInternal(t)
}

// stubResult is one scripted answer of the stub driver: rows of one string
// column, or an error.
type stubResult struct {
	rows []string
	err  error
}

var noRow = stubResult{}

func row(v string) stubResult { return stubResult{rows: []string{v}} }

func failWith(err error) stubResult { return stubResult{err: err} }

// scriptedExec answers each query with the next scripted result and repeats
// the last one when the script runs out.
type scriptedExec struct {
	db        *sql.DB
	mu        sync.Mutex
	script    []stubResult
	calls     int
	lastQuery string
	lastArgs  []any
}

var _ sqlconnect.QueryExecutor = (*scriptedExec)(nil)

func scripted(t *testing.T, script ...stubResult) *scriptedExec {
	t.Helper()
	ex := &scriptedExec{script: script}
	ex.db = sql.OpenDB(stubConnector{query: func(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
		ex.mu.Lock()
		defer ex.mu.Unlock()
		ex.lastQuery = q
		ex.lastArgs = nil
		for _, a := range args {
			ex.lastArgs = append(ex.lastArgs, a.Value)
		}
		r := ex.script[min(ex.calls, len(ex.script)-1)]
		ex.calls++
		if r.err != nil {
			return nil, r.err
		}
		return &stubRows{vals: r.rows}, nil
	}})
	t.Cleanup(func() { _ = ex.db.Close() })
	return ex
}

func (e *scriptedExec) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return e.db.ExecContext(ctx, q, args...)
}

func (e *scriptedExec) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return e.db.QueryContext(ctx, q, args...)
}

func (e *scriptedExec) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return e.db.QueryRowContext(ctx, q, args...)
}

// blockingExec answers every query only when its context ends, with the
// context error.
type blockingExec struct{}

var _ sqlconnect.QueryExecutor = blockingExec{}

var blockingDB = sync.OnceValue(func() *sql.DB {
	return sql.OpenDB(stubConnector{query: func(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
})

func (blockingExec) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return blockingDB().ExecContext(ctx, q, args...)
}

func (blockingExec) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return blockingDB().QueryContext(ctx, q, args...)
}

func (blockingExec) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return blockingDB().QueryRowContext(ctx, q, args...)
}

// stubConnector is a minimal database/sql driver. Every query goes to query;
// every exec fails.
type stubConnector struct {
	query func(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error)
}

func (c stubConnector) Connect(context.Context) (driver.Conn, error) { return stubConn(c), nil }
func (c stubConnector) Driver() driver.Driver                        { return stubDriver{} }

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) { return nil, driver.ErrSkip }

type stubConn stubConnector

func (stubConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (stubConn) Close() error                        { return nil }
func (stubConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (c stubConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	return c.query(ctx, q, args)
}

func (stubConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, io.ErrUnexpectedEOF
}

// CheckNamedValue accepts every argument as is.
func (stubConn) CheckNamedValue(*driver.NamedValue) error { return nil }

type stubRows struct {
	vals []string
	i    int
}

func (*stubRows) Columns() []string { return []string{"v"} }
func (*stubRows) Close() error      { return nil }

func (r *stubRows) Next(dest []driver.Value) error {
	if r.i >= len(r.vals) {
		return io.EOF
	}
	dest[0] = r.vals[r.i]
	r.i++
	return nil
}

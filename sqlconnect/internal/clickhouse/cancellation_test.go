package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// controlServer is a stub control pool. answer gives the error for the nth
// statement (from 0) with text q; nil answers one row.
func controlServer(t *testing.T, answer func(n int, q string) error) (*DB, *stubRecorder) {
	t.Helper()
	db, rec := unitDBScripted(t, func(string) ([]string, [][]driver.Value) { return nil, nil })
	var mu sync.Mutex
	n := 0
	db.control = sql.OpenDB(stubConnector{query: func(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
		c := stubCall{ctx: ctx, q: q}
		for _, a := range args {
			c.args = append(c.args, a.Value)
		}
		rec.add(c)
		mu.Lock()
		i := n
		n++
		mu.Unlock()
		if err := answer(i, q); err != nil {
			return nil, err
		}
		return &tableRows{cols: []string{"x"}, rows: [][]driver.Value{{uint8(1)}}}, nil
	}})
	t.Cleanup(func() { _ = db.control.Close() })
	return db, rec
}

var running216 = &ch.Exception{Code: 216, Message: "Query with id = retl-x is already running by user other-user-secret-text"}

func TestSQ28_RunningCheck(t *testing.T) {
	for name, c := range map[string]struct {
		err  error
		want bool
	}{"running": {running216, true}, "not running": {nil, false}} {
		db, rec := controlServer(t, func(int, string) error { return c.err })
		caller := clickhousequery.WithStatement(context.Background(), map[string]any{"max_threads": 8}, "retl-other")
		got, err := db.QueryRunning(caller, "retl-x")
		require.NoError(t, err, name)
		require.Equal(t, c.want, got, name)
		require.Equal(t, []string{runningSQL}, rec.queries(), name)
		call := rec.last()
		require.Empty(t, call.args, "the target id is sent as the query id, never spliced into the SQL")
		require.Equal(t, "retl-x", queryIDOf(call.ctx), "the check runs under the target's id")
		require.Equal(t, map[string]any{"send_progress_in_http_headers": 0, "limit": 0, "offset": 0, "additional_result_filter": "", "replace_running_query": 0}, settingsOf(call.ctx),
			"the check never asks the server to replace the target, and no caller setting reaches it")
	}
	secret := "other-user-secret-text"
	db, _ := controlServer(t, func(int, string) error { return &ch.Exception{Code: 497, Message: secret} })
	_, err := db.QueryRunning(context.Background(), "retl-x")
	requireCode(t, err, "CH_PERMISSION")
	require.NotContains(t, err.Error(), secret)
	_, err = db.QueryRunning(context.Background(), "")
	requireCode(t, err, "CH_CONFIG_INVALID")
}

func TestSQ28_StopQuery(t *testing.T) {
	orig := sleep
	sleep = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { sleep = orig })

	// Running, stopped, then not running.
	db, rec := controlServer(t, func(n int, _ string) error {
		if n == 0 {
			return running216
		}
		return nil
	})
	stopped, err := db.StopQuery(context.Background(), "retl-x")
	require.NoError(t, err)
	require.True(t, stopped)
	require.Equal(t, []string{runningSQL, stopSQL, runningSQL}, rec.queries())
	stop := rec.calls()[1]
	require.Equal(t, "retl-x", queryIDOf(stop.ctx))
	require.Equal(t, map[string]any{"send_progress_in_http_headers": 0, "limit": 0, "offset": 0, "additional_result_filter": "", "replace_running_query": 1}, settingsOf(stop.ctx))
	_, hasDeadline := stop.ctx.Deadline()
	require.True(t, hasDeadline, "a caller without a deadline gets stopTimeout")

	// Not running: no stop statement is sent.
	db, rec = controlServer(t, func(int, string) error { return nil })
	stopped, err = db.StopQuery(context.Background(), "retl-x")
	require.NoError(t, err)
	require.False(t, stopped)
	require.Equal(t, []string{runningSQL}, rec.queries())

	// The server refuses the setting: a readonly profile (164) or a constraint (452).
	for _, code := range []int32{164, 452} {
		db, _ = controlServer(t, func(n int, _ string) error {
			if n == 0 {
				return running216
			}
			return &ch.Exception{Code: code}
		})
		stopped, err = db.StopQuery(context.Background(), "retl-x")
		requireCode(t, err, "CH_PERMISSION")
		require.False(t, stopped)
		require.EqualValues(t, code, classify(err).ServerCode)
	}

	// The server did not stop the target within its wait: the stop itself gets 216.
	db, _ = controlServer(t, func(int, string) error { return running216 })
	_, err = db.StopQuery(context.Background(), "retl-x")
	requireCode(t, err, "CH_OUTCOME_UNKNOWN")
	require.NotContains(t, err.Error(), "other-user-secret-text")

	// The target still runs after a stop that returned: a bounded number of checks.
	db, rec = controlServer(t, func(_ int, q string) error {
		if q == runningSQL {
			return running216
		}
		return nil
	})
	_, err = db.StopQuery(context.Background(), "retl-x")
	requireCode(t, err, "CH_OUTCOME_UNKNOWN")
	require.Len(t, rec.queries(), 2+stopConfirmTries)
}

func TestSQ28_StopQueryContention(t *testing.T) {
	db, rec := controlServer(t, func(_ int, q string) error {
		if q == runningSQL {
			return running216
		}
		return &ch.Exception{Code: 394, Message: "secret-server-text"}
	})
	stopped, err := db.StopQuery(context.Background(), "retl-x")
	require.False(t, stopped)
	requireCode(t, err, "CH_OUTCOME_UNKNOWN")
	require.EqualValues(t, 394, classify(err).ServerCode)
	require.NotContains(t, err.Error(), "secret-server-text")
	require.Equal(t, []string{runningSQL, stopSQL}, rec.queries())
}

func TestSQ28_StopQueryDeadline(t *testing.T) {
	for name, timeout := range map[string]time.Duration{"earlier": time.Second, "later": time.Hour} {
		t.Run(name, func(t *testing.T) {
			db, rec := controlServer(t, func(int, string) error { return nil })
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			before := time.Now()
			_, err := db.StopQuery(ctx, "retl-x")
			require.NoError(t, err)
			deadline, ok := rec.last().ctx.Deadline()
			require.True(t, ok)
			if timeout < stopTimeout {
				callerDeadline, _ := ctx.Deadline()
				require.Equal(t, callerDeadline, deadline)
			} else {
				require.WithinDuration(t, before.Add(stopTimeout), deadline, time.Second)
			}
		})
	}
}

// A stop holds the target id across its initial check, replacement and
// confirmation. A second check must wait until all three have returned.
func TestSQ28_ControlCallsSerializeTarget(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	db, rec := controlServer(t, func(n int, _ string) error {
		if n == 0 {
			close(entered)
			<-release
			return running216
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() {
		_, err := db.StopQuery(ctx, "retl-x")
		stopDone <- err
	}()
	<-entered
	checkStarted, checkDone := make(chan struct{}), make(chan error, 1)
	go func() {
		close(checkStarted)
		_, err := db.QueryRunning(ctx, "retl-x")
		checkDone <- err
	}()
	<-checkStarted
	select {
	case err := <-checkDone:
		close(release)
		require.NoError(t, <-stopDone)
		t.Fatalf("same-id check ran before the stop returned: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-stopDone)
	require.NoError(t, <-checkDone)
	require.Equal(t, []string{runningSQL, stopSQL, runningSQL, runningSQL}, rec.queries())
}

func TestSQ28_ControlLockOtherTargetsAndCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	db, rec := controlServer(t, func(n int, _ string) error {
		if n == 0 {
			close(entered)
			<-release
		}
		return nil
	})
	firstDone := make(chan error, 1)
	go func() {
		_, err := db.QueryRunning(context.Background(), "retl-x")
		firstDone <- err
	}()
	<-entered
	defer func() { close(release); require.NoError(t, <-firstDone) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := db.QueryRunning(ctx, "retl-other")
	require.NoError(t, err, "another id has its own lock")
	waiting, cancelWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelWait()
	_, err = db.StopQuery(waiting, "retl-x")
	requireCode(t, err, "CH_TIMEOUT")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, rec.queries(), 2, "an expired lock waiter sends no control SQL")
}

func TestSQ28_RunningCheckLockTimeout(t *testing.T) {
	orig := stopTimeout
	stopTimeout = 20 * time.Millisecond
	t.Cleanup(func() { stopTimeout = orig })
	unlock, err := controlLocks.lock(context.Background(), "retl-x")
	require.NoError(t, err)
	var once sync.Once
	release := func() { once.Do(unlock) }
	defer release()
	db, rec := controlServer(t, func(int, string) error { return nil })
	type result struct {
		running bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		running, err := db.QueryRunning(context.Background(), "retl-x")
		done <- result{running: running, err: err}
	}()
	select {
	case got := <-done:
		require.False(t, got.running)
		requireCode(t, got.err, "CH_TIMEOUT")
		require.ErrorIs(t, got.err, context.DeadlineExceeded)
		require.Empty(t, rec.queries())
	case <-time.After(time.Second):
		release()
		<-done
		t.Fatal("a running check without a caller deadline waited past stopTimeout for the lock")
	}
}

func TestSQ28_RunningCheckDeadline(t *testing.T) {
	for name, timeout := range map[string]time.Duration{"none": 0, "earlier": time.Second, "later": time.Hour} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			db, rec := controlServer(t, func(int, string) error { return nil })
			before := time.Now()
			running, err := db.QueryRunning(ctx, "retl-x")
			require.NoError(t, err)
			require.False(t, running)
			deadline, ok := rec.last().ctx.Deadline()
			require.True(t, ok)
			if timeout != 0 && timeout < stopTimeout {
				callerDeadline, _ := ctx.Deadline()
				require.Equal(t, callerDeadline, deadline)
			} else {
				require.WithinDuration(t, before.Add(stopTimeout), deadline, time.Second)
			}
		})
	}
}

func TestSQ28_RunningCheckContention(t *testing.T) {
	for name, second := range map[string]error{
		"idle after contention":    nil,
		"running after contention": running216,
		"still contended":          &ch.Exception{Code: 394, Message: "secret-server-text"},
	} {
		t.Run(name, func(t *testing.T) {
			db, rec := controlServer(t, func(n int, _ string) error {
				if n == 0 {
					return &ch.Exception{Code: 394}
				}
				return second
			})
			running, err := db.QueryRunning(context.Background(), "retl-x")
			require.Len(t, rec.queries(), 2, "retry contention exactly once")
			if second != nil && classify(second).ServerCode == 394 {
				requireCode(t, err, "CH_OUTCOME_UNKNOWN")
				require.EqualValues(t, 394, classify(err).ServerCode)
				require.NotContains(t, err.Error(), "secret-server-text")
			} else {
				require.NoError(t, err)
				require.Equal(t, second == running216, running)
			}
		})
	}
}

func TestSQ28_DeprecatedMethodsUseTheNewPrimitives(t *testing.T) {
	orig := sleep
	sleep = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { sleep = orig })
	db, rec := controlServer(t, func(n int, _ string) error {
		if n == 0 {
			return running216
		}
		return nil
	})
	res, err := db.KillQuery(context.Background(), "retl-x")
	require.NoError(t, err)
	require.Equal(t, []sqlconnect.KillRow{{QueryID: "retl-x", KillStatus: "finished"}}, res.Rows)
	require.Equal(t, []string{runningSQL, stopSQL, runningSQL}, rec.queries())

	db, _ = controlServer(t, func(int, string) error { return nil })
	res, err = db.KillQuery(context.Background(), "retl-x")
	require.NoError(t, err)
	require.Empty(t, res.Rows, "no query ran, so no row, as KILL QUERY answered")

	for want, answer := range map[sqlconnect.QueryOutcome]error{sqlconnect.QueryRunning: running216, sqlconnect.QueryFailed: nil} {
		db, rec := controlServer(t, func(int, string) error { return answer })
		got, err := db.QueryOutcome(context.Background(), "retl-x", "Insert")
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, []string{runningSQL}, rec.queries())
	}
	db, _ = controlServer(t, func(int, string) error { return &ch.Exception{Code: 497} })
	_, err = db.QueryOutcome(context.Background(), "retl-x", "Insert")
	requireCode(t, err, "CH_PERMISSION")
}

// settingsOf reads the settings map the fork sends for ctx, as queryIDOf does
// for the query id.
func settingsOf(ctx context.Context) map[string]any {
	got := map[string]any{}
	_ = ch.Context(ctx, func(o *ch.QueryOptions) error {
		m := reflect.ValueOf(o).Elem().FieldByName("settings")
		for _, k := range m.MapKeys() {
			v := m.MapIndex(k).Elem()
			switch v.Kind() {
			case reflect.Int:
				got[k.String()] = int(v.Int())
			default:
				got[k.String()] = v.String()
			}
		}
		return nil
	})
	return got
}

// queryIDOf reads the query id the fork sends for ctx. The fork has no public
// getter, so a probe option reads the unexported field through reflection.
func queryIDOf(ctx context.Context) string {
	var got string
	_ = ch.Context(ctx, func(o *ch.QueryOptions) error {
		got = reflect.ValueOf(o).Elem().FieldByName("queryID").String()
		return nil
	})
	return got
}

type stubCall struct {
	ctx  context.Context
	q    string
	args []any
}

type stubRecorder struct {
	mu  sync.Mutex
	log []stubCall
}

func (r *stubRecorder) add(c stubCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = append(r.log, c)
}

func (r *stubRecorder) calls() []stubCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]stubCall(nil), r.log...)
}

func (r *stubRecorder) queries() []string {
	var qs []string
	for _, c := range r.calls() {
		qs = append(qs, c.q)
	}
	return qs
}

func (r *stubRecorder) last() stubCall {
	c := r.calls()
	return c[len(c)-1]
}

// unitDBWithRows is a DB on a stub pool that answers every query with cols
// and rows and records each call.
func unitDBWithRows(t *testing.T, cols []string, rows [][]driver.Value) (*DB, *stubRecorder) {
	t.Helper()
	return unitDBScripted(t, func(string) ([]string, [][]driver.Value) { return cols, rows })
}

// unitDBScripted is unitDBWithRows with an answer chosen per query text.
func unitDBScripted(t *testing.T, answer func(q string) ([]string, [][]driver.Value)) (*DB, *stubRecorder) {
	t.Helper()
	cfg, err := parseConfig(validJSONInternal(), false)
	require.NoError(t, err)
	rec := &stubRecorder{}
	pool := sql.OpenDB(stubConnector{query: func(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
		c := stubCall{ctx: ctx, q: q}
		for _, a := range args {
			c.args = append(c.args, a.Value)
		}
		rec.add(c)
		cols, rows := answer(q)
		return &tableRows{cols: cols, rows: rows}, nil
	}})
	d := &DB{cfg: cfg}
	d.DB = base.NewDB(pool, func() error { return nil }, base.WithDialect(newDialect()))
	t.Cleanup(func() { _ = d.Close() })
	return d, rec
}

// tableRows is a stub result with named columns.
type tableRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *tableRows) Columns() []string { return r.cols }
func (*tableRows) Close() error        { return nil }

func (r *tableRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

func TestControlPool_StopAndCheckSkipABusyMainPool(t *testing.T) {
	db, _ := controlServer(t, func(n int, _ string) error {
		if n == 0 {
			return running216
		}
		return nil
	})
	db.SetMaxOpenConns(1)
	held, err := db.Conn(context.Background()) // a run holds every main pool connection
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stopped, err := db.StopQuery(ctx, "retl-x")
	require.NoError(t, err)
	require.True(t, stopped)
	running, err := db.QueryRunning(ctx, "retl-x")
	require.NoError(t, err)
	require.False(t, running)
}

func TestControlPool_OpenedAndClosedWithTheDB(t *testing.T) {
	db := mustDBInternal(t)
	require.NotNil(t, db.control)
	require.NotSame(t, db.DB.DB, db.control)
	require.Equal(t, controlConns, db.control.Stats().MaxOpenConnections)
	require.NoError(t, db.Close())
	require.ErrorContains(t, db.control.PingContext(context.Background()), "database is closed")
}

func TestTargetCtx_KeepsTheCheckRow(t *testing.T) {
	t.Parallel()
	for _, replace := range []bool{false, true} {
		got := settingsOf(targetCtx(context.Background(), "retl-x", replace))
		require.Equal(t, 0, got["limit"], "a profile limit must not hide the check row")
		require.Equal(t, 0, got["offset"], "a profile offset must not hide the check row")
		require.Equal(t, "", got["additional_result_filter"])
	}
}

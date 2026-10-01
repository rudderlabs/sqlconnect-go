package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chctx"
)

func TestSQ28_KillRowsDecodedVerbatim(t *testing.T) {
	db, rec := unitDBWithRows(t, []string{"kill_status", "query_id", "user", "query"}, [][]driver.Value{
		{"waiting", "retl-x", "rudder_retl", "INSERT ..."},
		{"cant_cancel", "retl-x", "rudder_retl", "INSERT ..."},
	})
	res, err := db.KillQuery(context.Background(), "retl-x")
	require.NoError(t, err)
	require.Equal(t, []sqlconnect.KillRow{{QueryID: "retl-x", KillStatus: "waiting"}, {QueryID: "retl-x", KillStatus: "cant_cancel"}}, res.Rows,
		"KillQuery returns every row and interprets none")
	require.Equal(t, []string{"KILL QUERY WHERE query_id = ? AND user = currentUser() SYNC"}, rec.queries())
	require.Equal(t, []any{"retl-x"}, rec.last().args, "the target id is a bound argument, never spliced into the SQL")
	require.True(t, chctx.Has(rec.last().ctx), "an unmarked context gets the control map")
}

func TestSQ28_ControlContextNeverReusesTheCallerID(t *testing.T) {
	db, rec := unitDBWithRows(t, []string{"count()"}, [][]driver.Value{{uint64(0)}})
	caller := clickhousequery.WithStatement(context.Background(), map[string]any{"send_progress_in_http_headers": 0}, "retl-target")
	_, err := db.QueryOutcome(caller, "retl-target", "Insert")
	require.NoError(t, err)
	for _, c := range rec.calls() {
		require.NotEqual(t, caller, c.ctx, "the caller context is never sent unchanged")
		require.True(t, chctx.Has(c.ctx), "the caller settings map stays marked")
		require.NotEqual(t, "retl-target", queryIDOf(c.ctx), "a control statement never runs under the caller's query id")
		require.Regexp(t, `^retl-[0-9a-f-]{36}$`, queryIDOf(c.ctx))
	}
	require.NotEqual(t, queryIDOf(rec.calls()[0].ctx), queryIDOf(rec.calls()[1].ctx), "each control statement gets its own id")
}

func TestSQ28_OutcomeMapping(t *testing.T) {
	for _, c := range []struct {
		running uint64
		logType []driver.Value
		want    sqlconnect.QueryOutcome
	}{
		{1, nil, sqlconnect.QueryRunning},
		{0, []driver.Value{"QueryFinish"}, sqlconnect.QueryFinished},
		{0, []driver.Value{"ExceptionWhileProcessing"}, sqlconnect.QueryFailed},
		{0, []driver.Value{"ExceptionBeforeStart"}, sqlconnect.QueryFailed},
		{0, nil, sqlconnect.QueryNotFound},
	} {
		db, rec := unitDBScripted(t, func(q string) ([]string, [][]driver.Value) {
			if q == processesSQL {
				return []string{"count()"}, [][]driver.Value{{c.running}}
			}
			if c.logType == nil {
				return []string{"type"}, nil
			}
			return []string{"type"}, [][]driver.Value{c.logType}
		})
		got, err := db.QueryOutcome(context.Background(), "retl-x", "Insert")
		require.NoError(t, err)
		require.Equal(t, c.want, got)
		if c.running > 0 {
			require.Equal(t, []string{processesSQL}, rec.queries(), "a running query needs no query_log read")
			continue
		}
		require.Equal(t, []string{processesSQL, queryLogSQL}, rec.queries())
		require.Equal(t, []any{"retl-x", "Insert"}, rec.last().args)
	}
	require.Contains(t, processesSQL, "user = currentUser()")
	require.Contains(t, queryLogSQL, "user = currentUser()")
	require.Contains(t, queryLogSQL, "query_kind = ?")
}

func TestSQ28_ServerErrorsAreBounded(t *testing.T) {
	secret := "other-user-secret-text"
	db, _ := unitDBScripted(t, func(string) ([]string, [][]driver.Value) { return nil, nil })
	db.DB.DB = sql.OpenDB(stubConnector{query: func(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
		return nil, &ch.Exception{Code: 497, Message: secret}
	}})
	_, err := db.KillQuery(context.Background(), "retl-x")
	requireCode(t, err, "CH_PERMISSION")
	require.NotContains(t, err.Error(), secret)
	_, err = db.QueryOutcome(context.Background(), "retl-x", "Insert")
	requireCode(t, err, "CH_PERMISSION")
	require.NotContains(t, err.Error(), secret)
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
	cfg, err := parseConfig(validJSONInternal(nil), false)
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

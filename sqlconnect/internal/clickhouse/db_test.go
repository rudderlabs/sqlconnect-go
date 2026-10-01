package clickhouse

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chctx"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
)

func validJSONInternal() json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"host": "ch.example.com", "database": "analytics", "user": "rudder_retl",
		"password": "s3cret", "secure": true, "scratchDatabase": "_rudderstack_ws",
	})
	return b
}

// mustDBInternal opens a lazy DB under the strict policy. It never connects.
func mustDBInternal(t *testing.T) *DB {
	t.Helper()
	db, err := newDB(validJSONInternal(), openEnv{
		policySet: true, resolver: net.DefaultResolver,
		dialTimeout: time.Second, readTimeout: clickhousequery.MaxRunBudget + 60*time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestNewDB_RefusesBeforePolicy(t *testing.T) {
	_, err := newDB(validJSONInternal(), openEnv{policy: chpolicy.Policy{}, policySet: false})
	requireCode(t, err, "CH_CONFIG_INVALID")
}

func TestSQ23_UnsupportedAPIs(t *testing.T) {
	db := mustDBInternal(t)
	_, e1 := db.Begin()
	_, e2 := db.BeginTx(context.Background(), nil)
	_, e3 := db.Prepare("SELECT 1")
	_, e4 := db.PrepareContext(context.Background(), "SELECT 1")
	for _, c := range []struct {
		err  error
		code string
	}{
		{e1, "CH_TRANSACTIONS_UNSUPPORTED"},
		{e2, "CH_TRANSACTIONS_UNSUPPORTED"},
		{e3, "CH_PREPARE_UNSUPPORTED"},
		{e4, "CH_PREPARE_UNSUPPORTED"},
	} {
		requireCode(t, c.err, c.code)
		require.ErrorIs(t, c.err, sqlconnect.ErrNotSupported)
	}

	g := &guardConn{inner: &recordingConn{}}
	_, err := g.Begin()
	requireCode(t, err, "CH_TRANSACTIONS_UNSUPPORTED")
	_, err = g.BeginTx(context.Background(), driver.TxOptions{})
	requireCode(t, err, "CH_TRANSACTIONS_UNSUPPORTED")
	_, err = g.Prepare("SELECT 1")
	requireCode(t, err, "CH_PREPARE_UNSUPPORTED")
	_, err = g.PrepareContext(context.Background(), "SELECT 1")
	requireCode(t, err, "CH_PREPARE_UNSUPPORTED")
}

func TestTransport_PoolAndOptions(t *testing.T) {
	db := mustDBInternal(t)
	o := db.opts
	require.Equal(t, 10, db.Stats().MaxOpenConnections)
	require.True(t, o.Protocol == ch.HTTP && len(o.Settings) == 0 && o.ReadTimeout > clickhousequery.MaxRunBudget)
	require.True(t, o.TLS != nil && !o.TLS.InsecureSkipVerify && o.TLS.RootCAs == nil, "system roots, verification on")
	require.Equal(t, ch.CompressionLZ4, o.Compression.Method)
	require.True(t, o.HTTPProxyURL == nil && len(o.HttpHeaders) == 0 && o.HttpUrlPath == "" && o.GetJWT == nil)
	require.Equal(t, []string{"ch.example.com:8443"}, o.Addr, "the URL keeps the configured hostname")
	require.Equal(t, 5, o.MaxIdleConns)
	require.Equal(t, 30*time.Minute, o.ConnMaxLifetime)
	require.NotNil(t, o.DialContext)
	require.NotNil(t, o.TransportFunc)

	p, ok := chpolicy.Current()
	require.True(t, ok, "TestMain installs the strict policy")
	d, err := NewDB(validJSONInternal())
	require.NoError(t, err)
	defer func() { _ = d.Close() }()
	require.Equal(t, p, d.env.policy)
	require.Equal(t, clickhousequery.MaxRunBudget+60*time.Second, d.opts.ReadTimeout)
	require.Equal(t, 90*time.Second, d.env.dialTimeout)
}

func TestSettingsMaps(t *testing.T) {
	s := driverScratchSettings()
	require.Equal(t, 0, s["send_progress_in_http_headers"])
	require.Equal(t, 0, s["async_insert"])
	for _, k := range overflowModes {
		require.Equal(t, "throw", s[k], k)
	}
	r := driverReadSettings()
	require.Equal(t, 2, r["readonly"])
	require.NotContains(t, r, "async_insert")
	require.Equal(t, 256<<20, r["max_result_bytes"], "a driver read cannot stream an unbounded result")
	require.NotContains(t, r, "max_memory_usage", "the customer profile bounds server memory")
	require.NotContains(t, s, "max_result_bytes", "the scratch map fills tables of any size")
	require.NotContains(t, s, "readonly", "the scratch map writes")
	require.Equal(t, map[string]any{"send_progress_in_http_headers": 0}, controlSettings())
	u := stage2UnionSettings(42)
	require.Equal(t, 42, u["max_execution_time"])
	require.Equal(t, 1, u["final"])
	require.Equal(t, 2, u["readonly"])
	s["limit"] = 99
	require.Equal(t, 0, driverScratchSettings()["limit"], "each call returns a fresh map")
}

// recordingConn is a driver connection stub that records the last context.
type recordingConn struct {
	lastCtx  context.Context
	execErr  error
	queryErr error
	rows     driver.Rows
	closed   bool
}

func (c *recordingConn) Prepare(string) (driver.Stmt, error) { return nil, fmt.Errorf("stub") }
func (c *recordingConn) Close() error                        { c.closed = true; return nil }
func (c *recordingConn) Begin() (driver.Tx, error)           { return nil, fmt.Errorf("stub") }

func (c *recordingConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	c.lastCtx = ctx
	if c.execErr != nil {
		return nil, c.execErr
	}
	return driver.RowsAffected(0), nil
}

func (c *recordingConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	c.lastCtx = ctx
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return c.rows, nil
}

type errRows struct{ err error }

func (r *errRows) Columns() []string              { return []string{"a"} }
func (r *errRows) Close() error                   { return nil }
func (r *errRows) Next(dest []driver.Value) error { return r.err }

func TestGuardConn_DefaultsAndBoundedErrors(t *testing.T) {
	stub := &recordingConn{execErr: &ch.HTTPError{StatusCode: 500, Err: &ch.Exception{Code: 516, Message: "sentinel-pw-9"}}}
	g := &guardConn{inner: stub}
	_, err := g.ExecContext(context.Background(), "SELECT 1", nil)
	require.NotContains(t, fmt.Sprintf("%+v", err), "sentinel-pw-9")
	requireCode(t, err, "CH_AUTHENTICATION")
	require.True(t, chctx.Has(stub.lastCtx), "an unmarked context gets the driver map")
	caller := clickhousequery.WithStatement(context.Background(), map[string]any{"x": 1}, "retl-caller")
	stub.execErr = driver.ErrBadConn
	_, err = g.ExecContext(caller, "SELECT 1", nil)
	require.Same(t, driver.ErrBadConn, err, "ErrBadConn stays bare for database/sql")
	require.Equal(t, caller, stub.lastCtx, "a marked context passes unchanged")

	stub.execErr = fmt.Errorf("wrapped: %w", driver.ErrBadConn)
	_, err = g.ExecContext(caller, "SELECT 1", nil)
	require.Same(t, driver.ErrBadConn, err, "a wrapped ErrBadConn is returned bare, without its text")

	stub.queryErr = &ch.Exception{Code: 60, Message: "sentinel-pw-9"}
	_, err = g.QueryContext(context.Background(), "SELECT 1", nil)
	requireCode(t, err, "CH_OBJECT_NOT_FOUND")
	requireChainClean(t, err, "sentinel-pw-9")

	stub.queryErr = nil
	stub.rows = &errRows{err: &ch.Exception{Code: 394, Message: "sentinel-pw-9"}}
	rows, err := g.QueryContext(context.Background(), "SELECT 1", nil)
	require.NoError(t, err)
	err = rows.Next(make([]driver.Value, 1))
	requireCode(t, err, "CH_CANCELLED")
	requireChainClean(t, err, "sentinel-pw-9")
	stub.rows = &errRows{err: io.EOF}
	rows, err = g.QueryContext(context.Background(), "SELECT 1", nil)
	require.NoError(t, err)
	require.Equal(t, io.EOF, rows.Next(make([]driver.Value, 1)), "io.EOF ends the rows unchanged")
	stub.rows = &errRows{err: fmt.Errorf("truncated: %w", io.ErrUnexpectedEOF)}
	rows, err = g.QueryContext(context.Background(), "SELECT 1", nil)
	require.NoError(t, err)
	requireCode(t, rows.Next(make([]driver.Value, 1)), "CH_NETWORK")
}

type hangingConnector struct{ lastCtx context.Context }

func (h *hangingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	h.lastCtx = ctx
	<-ctx.Done()
	return nil, ctx.Err()
}

func (h *hangingConnector) Driver() driver.Driver { return nil }

func TestConnectGuard_HonoursCallerDeadline(t *testing.T) {
	hang := &hangingConnector{}
	g := &connectGuard{next: hang}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := g.Connect(ctx)
	require.Error(t, err)
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.False(t, chctx.Has(hang.lastCtx), "no caller statement options reach the connection-open SELECT")

	caller, stop := context.WithCancel(clickhousequery.WithStatement(context.Background(), map[string]any{"x": 1}, "retl-caller"))
	time.AfterFunc(100*time.Millisecond, stop)
	start = time.Now()
	_, err = g.Connect(caller)
	requireCode(t, err, "CH_CANCELLED")
	require.Less(t, time.Since(start), 500*time.Millisecond, "the caller's cancellation ends the open")
	require.False(t, chctx.Has(hang.lastCtx))
}

func TestClassify_PlainAnswerToTLSClient(t *testing.T) {
	err := &url.Error{Op: "Post", URL: "https://ch.example.com:8443/", Err: http.ErrSchemeMismatch}
	require.Equal(t, "CH_TLS", classify(err).Code, "a plain HTTP answer to a TLS client is a TLS failure")
}

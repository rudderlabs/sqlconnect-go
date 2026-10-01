package clickhouse_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

// requireCode asserts that err carries an adapter error with the given code.
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var ce *cherr.Error
	require.True(t, errors.As(err, &ce), "want %s, got %v", code, err)
	require.Equal(t, code, ce.Code, "%v", err)
}

// requireChainClean walks every Unwrap and Join branch of err and asserts that
// no formatting of any link contains secret.
func requireChainClean(t *testing.T, err error, secret string) {
	t.Helper()
	var walk func(e error)
	walk = func(e error) {
		if e == nil {
			return
		}
		require.NotContains(t, fmt.Sprintf("%v %+v %#v", e, e, e), secret)
		switch u := e.(type) {
		case interface{ Unwrap() []error }:
			for _, x := range u.Unwrap() {
				walk(x)
			}
		case interface{ Unwrap() error }:
			walk(u.Unwrap())
		}
	}
	walk(err)
}

// withHostPort replaces host and port in an account config.
func withHostPort(cfg json.RawMessage, host string, port int) json.RawMessage {
	m := map[string]any{}
	if err := json.Unmarshal(cfg, &m); err != nil {
		panic(err)
	}
	m["host"], m["port"] = host, port
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

// testPolicy admits the loopback fixture and the plain HTTP arm.
var testPolicy = chpolicy.Policy{AllowLoopback: true, AllowPlainHTTP: true}

// openVia opens an admin DB that connects through the proxy p.
func openVia(t *testing.T, srv *chtest.Server, p *chtest.Proxy, secure bool) *clickhouse.DB {
	t.Helper()
	return openViaWithPassword(t, srv, p, secure, srv.AdminPassword)
}

func openViaWithPassword(t *testing.T, srv *chtest.Server, p *chtest.Proxy, secure bool, password string) *clickhouse.DB {
	t.Helper()
	cfg := withHostPort(srv.Config(srv.AdminUser, password, "default", "scratch_db", secure), "localhost", p.Port())
	db, err := clickhouse.NewDBForTest(cfg, testPolicy, srv.CA)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// isHello matches the query the fork sends when it opens a connection.
func isHello(r chtest.Request) bool {
	return strings.HasPrefix(strings.TrimSpace(r.SQL), "SELECT displayName(), version(), revision(), timezone()")
}

// countingResolver answers every lookup with one address and counts the calls.
type countingResolver struct {
	answer string
	calls  atomic.Int32
}

func (r *countingResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	r.calls.Add(1)
	return []net.IPAddr{{IP: net.ParseIP(r.answer)}}, nil
}

func (r *countingResolver) Calls() int { return int(r.calls.Load()) }

// mutableResolver answers lookups for one host with an address that Set changes.
type mutableResolver struct {
	host string
	mu   sync.Mutex
	ip   string
}

func newMutableResolver(host, ip string) *mutableResolver {
	return &mutableResolver{host: host, ip: ip}
}

func (r *mutableResolver) Set(ip string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ip = ip
}

func (r *mutableResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if host != r.host {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return []net.IPAddr{{IP: net.ParseIP(r.ip)}}, nil
}

// staticResolver maps host names to one address each.
type staticResolver map[string]string

func (r staticResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	ip, ok := r[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return []net.IPAddr{{IP: net.ParseIP(ip)}}, nil
}

// sniFront is a TCP relay in front of the server's HTTPS port. It reads the
// client's TLS ClientHello, reports the server name to hook, and replays the
// bytes upstream, so the server's own certificate answers the handshake.
type sniFront struct {
	ln net.Listener
}

var errPeeked = errors.New("client hello read")

func newSNIFront(t *testing.T, srv *chtest.Server, hook func(serverName string)) *sniFront {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &sniFront{ln: ln}
	upstream := net.JoinHostPort("127.0.0.1", strconv.Itoa(srv.HTTPSPort))
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() { relaySNI(c, upstream, hook) })
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return f
}

func (f *sniFront) Port() int { return f.ln.Addr().(*net.TCPAddr).Port }

func relaySNI(c net.Conn, upstream string, hook func(string)) {
	defer func() { _ = c.Close() }()
	rec := &recordingConn{Conn: c}
	_ = tls.Server(rec, &tls.Config{GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
		hook(h.ServerName)
		return nil, errPeeked
	}}).Handshake()
	up, err := net.Dial("tcp", upstream)
	if err != nil {
		return
	}
	defer func() { _ = up.Close() }()
	if _, err := up.Write(rec.read.Bytes()); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
}

// recordingConn keeps every byte read and drops every write, so the peek
// handshake sends nothing to the client.
type recordingConn struct {
	net.Conn
	read bytes.Buffer
}

func (r *recordingConn) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	r.read.Write(p[:n])
	return n, err
}

func (r *recordingConn) Write(p []byte) (int, error) { return len(p), nil }

// openFloorWithServer starts the pinned 26.3 container and opens an admin DB
// over HTTPS with the fixture CA.
func openFloorWithServer(t *testing.T) (*chtest.Server, *clickhouse.DB) {
	t.Helper()
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	return srv, openAdmin(t, srv)
}

// openFloor is openFloorWithServer without the server handle.
func openFloor(t *testing.T) *clickhouse.DB {
	t.Helper()
	_, db := openFloorWithServer(t)
	return db
}

// openAdmin opens a DB as the fixture admin on the database "default".
func openAdmin(t *testing.T, srv *chtest.Server) *clickhouse.DB {
	t.Helper()
	return openWith(t, srv.Config(srv.AdminUser, srv.AdminPassword, "default", "scratch_db", true), srv)
}

// openScoped opens a DB as a user that CreateScopedUser made for customer_db
// and scratch_db.
func openScoped(t *testing.T, srv *chtest.Server, user, password string) *clickhouse.DB {
	t.Helper()
	return openWith(t, srv.Config(user, password, "customer_db", "scratch_db", true), srv)
}

func openWith(t *testing.T, cfg json.RawMessage, srv *chtest.Server) *clickhouse.DB {
	t.Helper()
	db, err := clickhouse.NewDBForTest(cfg, testPolicy, srv.CA)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedSchema creates a fresh database and runs each ";"-separated statement of
// script through db, with {{.schema}} replaced by the database name.
func seedSchema(t *testing.T, db *clickhouse.DB, script string) sqlconnect.SchemaRef {
	t.Helper()
	schema := sqlconnect.SchemaRef{Name: "seed_" + strings.ToLower(rand.String(8))}
	ctx := context.Background()
	require.NoError(t, db.CreateSchema(ctx, schema))
	t.Cleanup(func() { _ = db.DropSchema(context.Background(), schema) })
	for stmt := range strings.SplitSeq(strings.ReplaceAll(script, "{{.schema}}", db.QuoteIdentifier(schema.Name)), ";") {
		if stmt = strings.TrimSpace(stmt); stmt != "" {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err, stmt)
		}
	}
	return schema
}

// orDefault returns v, or def when v is empty.
func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// openFloorTZ opens an admin DB on a pinned 26.3 server that runs in tz.
func openFloorTZ(t *testing.T, tz string) *clickhouse.DB {
	t.Helper()
	return openAdmin(t, chtest.Start(t, chtest.Options{Tag: "26.3", Timezone: tz}))
}

// queryJSONErr runs q through the JSON mapper and returns every row as JSON,
// or the first error.
func queryJSONErr(db *clickhouse.DB, q string) ([]json.RawMessage, error) {
	ch, leave := sqlconnect.QueryJSONAsync(context.Background(), db, q)
	defer leave()
	var rows []json.RawMessage
	for v := range ch {
		if v.Err != nil {
			return nil, v.Err
		}
		rows = append(rows, v.Value)
	}
	return rows, nil
}

// queryJSON is queryJSONErr that fails the test on an error.
func queryJSON(t *testing.T, db *clickhouse.DB, q string) []json.RawMessage {
	t.Helper()
	rows, err := queryJSONErr(db, q)
	require.NoError(t, err, q)
	return rows
}

// queryJSONMaps decodes every row with UseNumber, so numbers keep their text.
func queryJSONMaps(t *testing.T, db *clickhouse.DB, q string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range queryJSON(t, db, q) {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		m := map[string]any{}
		require.NoError(t, dec.Decode(&m))
		out = append(out, m)
	}
	return out
}

// queryJSONMap returns the only row of q.
func queryJSONMap(t *testing.T, db *clickhouse.DB, q string) map[string]any {
	t.Helper()
	rows := queryJSONMaps(t, db, q)
	require.Len(t, rows, 1, q)
	return rows[0]
}

// createTable creates a MergeTree table with columns in a fresh schema and
// runs each seed statement with {{t}} replaced by the quoted table name.
func createTable(t *testing.T, db *clickhouse.DB, columns, orderBy string, seeds ...string) sqlconnect.RelationRef {
	t.Helper()
	ref := createTableWith(t, db, nil, "CREATE TABLE {{t}} ("+columns+") ENGINE = MergeTree ORDER BY "+orderBy)
	for _, s := range seeds {
		_, err := db.ExecContext(context.Background(), strings.ReplaceAll(s, "{{t}}", db.QuoteTable(ref)))
		require.NoError(t, err, s)
	}
	return ref
}

// createNestedTable creates k UInt8, n Nested(id UInt256, label Nullable(String))
// with flatten_nested on the CREATE. Row 1 holds (1,'x') and (big,NULL); row 2
// is empty.
func createNestedTable(t *testing.T, db *clickhouse.DB, flatten int, big string) sqlconnect.RelationRef {
	t.Helper()
	ref := createTableWith(t, db, map[string]any{"flatten_nested": flatten},
		"CREATE TABLE {{t}} (k UInt8, n Nested(id UInt256, label Nullable(String))) ENGINE = MergeTree ORDER BY k")
	insert := "INSERT INTO {{t}} VALUES (1, [(1, 'x'), (" + big + ", NULL)]), (2, [])"
	if flatten == 1 {
		insert = "INSERT INTO {{t}} (k, `n.id`, `n.label`) VALUES (1, [1, " + big + "], ['x', NULL]), (2, [], [])"
	}
	_, err := db.ExecContext(context.Background(), strings.ReplaceAll(insert, "{{t}}", db.QuoteTable(ref)))
	require.NoError(t, err, insert)
	return ref
}

// createTableWith runs create in a fresh schema, with settings on the
// statement when settings is not nil.
func createTableWith(t *testing.T, db *clickhouse.DB, settings map[string]any, create string) sqlconnect.RelationRef {
	t.Helper()
	schema := seedSchema(t, db, "")
	ref := sqlconnect.NewRelationRef("t_"+strings.ToLower(rand.String(8)), sqlconnect.WithSchema(schema.Name))
	ctx := context.Background()
	if settings != nil {
		ctx = stmtCtx(ctx, settings, clickhousequery.NewQueryID())
	}
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.ExecContext(ctx, strings.ReplaceAll(create, "{{t}}", db.QuoteTable(ref)))
	require.NoError(t, err, create)
	return ref
}

// scopedExec is a caller executor: one pinned connection of db. Statements
// without their own map get the driver map from the connection wrapper.
func scopedExec(t *testing.T, db *clickhouse.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// recExec runs every statement on one pinned connection under the driver
// write map and a fresh query id, and records both.
type recExec struct {
	conn   *sql.Conn
	mu     sync.Mutex
	stmts  []struct{ ID, SQL string }
	closed bool
}

var _ sqlconnect.QueryExecutor = (*recExec)(nil)

func recordingExec(t *testing.T, db *clickhouse.DB) *recExec {
	t.Helper()
	return &recExec{conn: scopedExec(t, db)}
}

func (e *recExec) stmt(ctx context.Context, q string) context.Context {
	id := clickhousequery.NewQueryID()
	e.mu.Lock()
	e.stmts = append(e.stmts, struct{ ID, SQL string }{id, q})
	e.mu.Unlock()
	return clickhousequery.WithStatement(ctx, clickhouse.DriverScratchSettings(), id)
}

func (e *recExec) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return e.conn.ExecContext(e.stmt(ctx, q), q, args...)
}

func (e *recExec) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return e.conn.QueryContext(e.stmt(ctx, q), q, args...)
}

func (e *recExec) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return e.conn.QueryRowContext(e.stmt(ctx, q), q, args...)
}

// Close marks the executor closed. The driver must never call it.
func (e *recExec) Close() error {
	e.closed = true
	return nil
}

// idExec runs every statement on conn under the driver write map. A statement
// that starts with a listed prefix gets the caller-chosen query id.
type idExec struct {
	conn     *sql.Conn
	prefixes map[string]string
}

func fixedIDExec(conn *sql.Conn, prefixes map[string]string) sqlconnect.QueryExecutor {
	return idExec{conn: conn, prefixes: prefixes}
}

func (e idExec) stmt(ctx context.Context, q string) context.Context {
	id := clickhousequery.NewQueryID()
	for p, fixed := range e.prefixes {
		if strings.HasPrefix(q, p) {
			id = fixed
		}
	}
	m := clickhouse.DriverScratchSettings()
	m["max_query_size"] = 16 << 20 // room for the padded test queries
	return clickhousequery.WithStatement(ctx, m, id)
}

func (e idExec) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return e.conn.ExecContext(e.stmt(ctx, q), q, args...)
}

func (e idExec) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return e.conn.QueryContext(e.stmt(ctx, q), q, args...)
}

func (e idExec) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return e.conn.QueryRowContext(e.stmt(ctx, q), q, args...)
}

// openAs opens a DB as u on the database "default".
func openAs(t *testing.T, srv *chtest.Server, u chtest.User) *clickhouse.DB {
	t.Helper()
	return openWith(t, srv.Config(u.Name, u.Password, "default", "scratch_db", true), srv)
}

// openScopedOn opens a DB as user with database as the customer database.
func openScopedOn(t *testing.T, srv *chtest.Server, user, password, database string) *clickhouse.DB {
	t.Helper()
	return openWith(t, srv.Config(user, password, database, "scratch_db", true), srv)
}

// mkSchema creates a fresh database named prefix_<random> and drops it when
// the test ends.
func mkSchema(t *testing.T, db *clickhouse.DB, prefix string) sqlconnect.SchemaRef {
	t.Helper()
	schema := sqlconnect.SchemaRef{Name: prefix + "_" + strings.ToLower(rand.String(8))}
	require.NoError(t, db.CreateSchema(context.Background(), schema))
	t.Cleanup(func() { _ = db.DropSchema(context.Background(), schema) })
	return schema
}

// countRequests counts the requests p received that match.
func countRequests(p *chtest.Proxy, match func(chtest.Request) bool) int {
	n := 0
	for _, r := range p.Requests() {
		if match(r) {
			n++
		}
	}
	return n
}

// rawTypes returns the RawType of each column.
func rawTypes(cols []sqlconnect.ColumnRef) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.RawType
	}
	return out
}

// retlFixture starts the pinned 26.3 server with the published-script user
// rudder_retl, the table scratch_db.sink (n UInt64) and a DB scoped to that
// user.
func retlFixture(t *testing.T) (*chtest.Server, *clickhouse.DB) {
	t.Helper()
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	srv.CreateScopedUser(t, "rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", false)
	srv.AdminExec(t, "CREATE TABLE scratch_db.sink (n UInt64) ENGINE = MergeTree ORDER BY n")
	return srv, openScoped(t, srv, "rudder_retl", "pw_Retl_123")
}

// idCtx is stmtCtx with no setting other than the progress header switch.
func idCtx(ctx context.Context, id string) context.Context {
	return stmtCtx(ctx, map[string]any{}, id)
}

// startSlow runs an INSERT that takes about 60 s on a pinned connection under
// id and sends its error on the returned channel.
func startSlow(t *testing.T, db *clickhouse.DB, ctx context.Context, id string) chan error {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		defer func() { _ = conn.Close() }()
		_, err := conn.ExecContext(stmtCtx(ctx, map[string]any{"max_block_size": 1}, id),
			"INSERT INTO scratch_db.sink SELECT number FROM numbers(600) WHERE sleepEachRow(0.1) = 0")
		done <- err
	}()
	return done
}

// openScopedVia opens a DB as user on customer_db that connects through the
// front p (a chtest.Proxy or a chtest.Balancer) over TLS.
func openScopedVia(t *testing.T, srv *chtest.Server, p interface{ Port() int }, user, password string) *clickhouse.DB {
	t.Helper()
	cfg := withHostPort(srv.Config(user, password, "customer_db", "scratch_db", true), "localhost", p.Port())
	db, err := clickhouse.NewDBForTest(cfg, testPolicy, srv.CA)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// stageOf returns the stage and tag of a validation failure, or -1 and "".
func stageOf(err error) (int, string) {
	var se sqlconnect.ValidationStageError
	if !errors.As(err, &se) {
		return -1, ""
	}
	return se.Stage, se.Tag
}

// requireStage asserts that err is a validation failure of stage n and tag.
func requireStage(t *testing.T, err error, n int, tag string) {
	t.Helper()
	stage, got := stageOf(err)
	require.Equal(t, []any{n, tag}, []any{stage, got}, "%v", err)
}

// probeCount returns the number of validation probe tables in scratch_db.
func probeCount(t *testing.T, srv *chtest.Server) string {
	t.Helper()
	return strconv.Itoa(len(srv.ProbeTables(t)))
}

// openScopedOnDatabase opens a DB as user with the given customer and scratch
// databases.
func openScopedOnDatabase(t *testing.T, srv *chtest.Server, user, password, database, scratch string) *clickhouse.DB {
	t.Helper()
	return openWith(t, srv.Config(user, password, database, scratch, true), srv)
}

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

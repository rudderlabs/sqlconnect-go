package clickhouse_test

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

func stmtCtx(ctx context.Context, m map[string]any, id string) context.Context {
	m = maps.Clone(m)
	m["send_progress_in_http_headers"] = 0
	return clickhousequery.WithStatement(ctx, m, id)
}

func TestSQ13_SQ29_StatementSettings(t *testing.T) {
	srv, db := openFloorWithServer(t)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	run := func(e interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}, c context.Context, m map[string]any,
	) string {
		id := clickhousequery.NewQueryID()
		_, err := e.ExecContext(stmtCtx(c, m, id), "SELECT 1")
		require.NoError(t, err)
		return id
	}
	pinned := []string{run(conn, ctx, map[string]any{"join_use_nulls": 1, "max_execution_time": 77}), run(conn, ctx, map[string]any{"join_use_nulls": 0, "max_execution_time": 77})}
	dctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	dl := run(db, dctx, map[string]any{"max_execution_time": 77}) // pool path
	short, cancel2 := context.WithTimeout(ctx, 900*time.Millisecond)
	defer cancel2()
	sid := run(db, short, map[string]any{})
	// A sub-second deadline with a map bound of 77 s: the fork keeps the map
	// value, and the context still ends a slow statement on both paths.
	for _, e := range []interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}{db, conn} {
		sctx, scancel := context.WithTimeout(ctx, 900*time.Millisecond)
		start := time.Now()
		_, err := e.ExecContext(stmtCtx(sctx, map[string]any{"max_execution_time": 77}, clickhousequery.NewQueryID()), "SELECT sleep(3)")
		scancel()
		require.Error(t, err)
		require.Less(t, time.Since(start), 2500*time.Millisecond, "the deadline ends the request before the statement does")
		require.Contains(t, []string{"CH_TIMEOUT", "CH_CANCELLED"}, db.ClassifyError(err).Code, "%v", err)
	}
	ids := make([]string, 20) // SQ13: concurrent maps do not leak
	var wg sync.WaitGroup
	for i := range ids {
		// max_threads starts at 101: query_log omits a value equal to the auto default.
		wg.Go(func() { ids[i] = run(db, ctx, map[string]any{"max_threads": i + 101}) })
	}
	wg.Wait()
	child := run(db, stmtCtx(ctx, map[string]any{"max_threads": 3}, clickhousequery.NewQueryID()), map[string]any{})
	db.SetMaxIdleConns(0) // the next statement opens a new connection
	oid := run(db, ctx, map[string]any{"max_threads": 7})
	srv.FlushLogs(t)
	for i, id := range pinned {
		st := srv.QueryLogSettings(t, id)
		require.Equal(t, []string{"77", strconv.Itoa(1 - i)}, []string{st["max_execution_time"], orDefault(st["join_use_nulls"], "0")}, "no deadline: the map value is sent")
	}
	got, _ := strconv.Atoi(srv.QueryLogSettings(t, dl)["max_execution_time"])
	require.InDelta(t, 45, got, 2, "a deadline overwrites max_execution_time with remaining seconds plus five")
	_, has := srv.QueryLogSettings(t, sid)["max_execution_time"]
	require.False(t, has, "a deadline under 1 s adds no stale bound; the context ends the request")
	for i, id := range ids {
		require.Equal(t, strconv.Itoa(i+101), srv.QueryLogSettings(t, id)["max_threads"])
	}
	_, has = srv.QueryLogSettings(t, child)["max_threads"]
	require.False(t, has, "the child map replaces the parent map")
	require.Empty(t, clickhouse.InspectOptions(db).Settings, "no connection-level setting")
	require.Equal(t, 1, srv.QueryLogCount(t, oid), "exactly one row carries the statement id")
	hello := srv.LatestHello(t)
	require.True(t, hello.QueryID != oid && strings.HasPrefix(hello.QueryID, "retl-"), hello.QueryID)
	require.NotContains(t, hello.Settings, "max_threads", "the connection-open SELECT carries none of the statement's options")
}

func TestSQ31_SQ11_InheritedAdminCarriesScratchMap(t *testing.T) {
	srv, db := openFloorWithServer(t)
	ctx := context.Background()
	schema := sqlconnect.SchemaRef{Name: "adm_" + strings.ToLower(rand.String(6))}
	tbl := sqlconnect.NewRelationRef("t", sqlconnect.WithSchema(schema.Name))
	renamed := sqlconnect.NewRelationRef("t2", sqlconnect.WithSchema(schema.Name))
	marker := time.Now()
	for _, step := range []func() error{
		func() error { return db.CreateSchema(ctx, schema) },
		func() error {
			if ok, err := db.SchemaExists(ctx, schema); err != nil || !ok {
				return fmt.Errorf("SchemaExists = %v, %v", ok, err)
			}
			return nil
		},
		func() error {
			all, err := db.ListSchemas(ctx)
			if err == nil && !slices.Contains(all, schema) {
				err = fmt.Errorf("ListSchemas misses %s", schema.Name)
			}
			return err
		},
		func() error { return db.CreateTestTable(ctx, tbl) },
		func() error { return db.CreateTestTable(ctx, tbl) }, // SQ11: idempotent, engine checked below
		func() error {
			if show := srv.AdminQuery(t, "SHOW CREATE TABLE "+schema.Name+".t")[0][0]; !strings.Contains(show, "ENGINE = MergeTree") || !strings.Contains(show, "ORDER BY c1") {
				return fmt.Errorf("CreateTestTable engine: %s", show)
			}
			return nil
		},
		func() error { _, err := db.ListTables(ctx, schema); return err },
		func() error { _, err := db.TableExists(ctx, tbl); return err },
		func() error { _, err := db.ListColumns(ctx, tbl); return err },
		func() error { _, err := db.CountTableRows(ctx, tbl); return err },
		func() error { return db.TruncateTable(ctx, tbl) },
		func() error { return db.RenameTable(ctx, tbl, renamed) },
		func() error { return db.DropTable(ctx, renamed) },
		func() error { _, err := db.GetRowCountForQuery(ctx, "SELECT 2"); return err },
		func() error { return db.DropSchema(ctx, schema) },
	} {
		require.NoError(t, step())
	}
	srv.FlushLogs(t)
	logged := srv.QueryLogSince(t, marker)
	require.NotEmpty(t, logged, "the admin calls reach query_log")
	for _, row := range logged {
		if row.IsHello {
			continue
		}
		require.True(t, strings.HasPrefix(row.QueryID, "retl-"), row.Query)
		// query_log omits a value equal to the server default, so an absent
		// send_progress_in_http_headers means 0; join_use_nulls=1 proves the driver map.
		require.Equal(t, []string{"0", "UTC", "1"}, []string{
			orDefault(row.Settings["send_progress_in_http_headers"], "0"), row.Settings["session_timezone"], row.Settings["join_use_nulls"],
		}, row.Query)
	}
}

func TestSQ22_MidStreamErrors(t *testing.T) {
	srv, db := openFloorWithServer(t)
	ctx := context.Background()
	// The server buffers about 1 MiB of output before the first flush, so each
	// row carries 1 MiB of poorly compressible padding.
	const slow = "SELECT number, randomPrintableASCII(1048576) AS pad, sleepEachRow(0.5) FROM numbers(100)"
	for _, c := range []struct {
		m    map[string]any
		kill bool
		want sqlconnect.ErrorInfo
	}{
		{map[string]any{"max_block_size": 1}, true, sqlconnect.ErrorInfo{Category: "cancellation", Code: "CH_CANCELLED", ServerCode: 394}},
		{map[string]any{"max_block_size": 1, "max_execution_time": 2}, false, sqlconnect.ErrorInfo{Category: "timeout", Code: "CH_TIMEOUT", ServerCode: 159}},
	} {
		id := clickhousequery.NewQueryID()
		rows, err := db.QueryContext(stmtCtx(ctx, c.m, id), slow)
		require.NoError(t, err)
		require.True(t, rows.Next(), "the first block arrives before the failure")
		if c.kill {
			srv.AdminExec(t, "KILL QUERY WHERE query_id = '"+id+"' SYNC")
		}
		for rows.Next() {
		}
		require.Equal(t, c.want, db.ClassifyError(rows.Err()), "HTTP 200 with an __exception__ block, never a truncated success")
	}
}

func TestSQ22_IntegrationClassification(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	srv.CreateScopedUser(t, "rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", false)
	srv.AdminExec(t, "CREATE TABLE customer_db.secret (k UInt8) ENGINE = MergeTree ORDER BY k")
	srv.AdminExec(t, "REVOKE SELECT ON customer_db.secret FROM rudder_retl")
	scoped, admin, bad := openScoped(t, srv, "rudder_retl", "pw_Retl_123"), openAdmin(t, srv), openScoped(t, srv, "rudder_retl", "wrong")
	ctx := context.Background()
	with := func(m map[string]any) context.Context { return stmtCtx(ctx, m, clickhousequery.NewQueryID()) }
	for _, c := range []struct {
		db     *clickhouse.DB
		ctx    context.Context
		q      string
		code   string
		server int32
	}{
		{bad, ctx, "SELECT 1", "CH_AUTHENTICATION", 516},
		{scoped, ctx, "SELECT k FROM customer_db.secret", "CH_PERMISSION", 497},
		{scoped, ctx, "CREATE TABLE customer_db.x (k UInt8) ENGINE = MergeTree ORDER BY k", "CH_PERMISSION", 497},
		{admin, ctx, "SELEC 1", "CH_QUERY_INVALID", 62},
		{admin, with(map[string]any{"max_execution_time": 1, "timeout_overflow_mode": "throw"}), "SELECT sleep(3)", "CH_TIMEOUT", 159},
		{admin, with(map[string]any{"max_memory_usage": 1}), "SELECT number % 100000 AS k, count() FROM numbers(1000000) GROUP BY k", "CH_RESOURCE", 241},
		{admin, with(map[string]any{"max_rows_in_set": 1, "set_overflow_mode": "throw"}), "SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(5))", "CH_RESOURCE", 191},
	} {
		_, err := c.db.ExecContext(c.ctx, c.q)
		info := admin.ClassifyError(err)
		require.Equal(t, []any{c.code, c.server}, []any{info.Code, info.ServerCode}, c.q)
	}
	p := chtest.NewProxy(t, srv, chtest.ProxyOptions{}) // broken connection
	conn, err := openVia(t, srv, p, true).Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	p.ResetOnce(func(r chtest.Request) bool { return strings.Contains(r.SQL, "SELECT 42") })
	_, err = conn.ExecContext(ctx, "SELECT 42")
	require.Equal(t, "CH_NETWORK", admin.ClassifyError(err).Code, "%v", err)
}

func TestCP22_GoBindsTabString(t *testing.T) {
	db := openFloor(t)
	ctx := context.Background()
	var n uint64
	const q = "SELECT count() FROM (SELECT 'a\tb' AS s) WHERE s = "
	require.NoError(t, db.QueryRowContext(ctx, q+"?", "a\tb").Scan(&n)) // the fork renders ? values into the text
	require.EqualValues(t, 1, n, "the tab survives the ? path")
	err := db.QueryRowContext(ctx, q+"{p:String}", ch.Named("p", "a\tb")).Scan(&n) // param_p goes unescaped
	require.EqualValues(t, 457, db.ClassifyError(err).ServerCode, "a raw tab ends the value early")
	require.NoError(t, db.QueryRowContext(ctx, q+"{p:String}", ch.Named("p", `a\tb`)).Scan(&n))
	require.EqualValues(t, 1, n, "the escaped form matches; callers of the named path escape (seam S19)")
}

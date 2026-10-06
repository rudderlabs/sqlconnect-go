//go:build clickhouse_cloud

// The before-GA ClickHouse Cloud arm. CI never runs it; run it with
// `make test-clickhouse-cloud` and CLICKHOUSE_CLOUD_CONFIG set to an account config.
package clickhouse_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse"
)

func cloudDB(t *testing.T, port int) *clickhouse.DB {
	t.Helper()
	cfg := os.Getenv("CLICKHOUSE_CLOUD_CONFIG")
	if cfg == "" {
		t.Fatal("CLICKHOUSE_CLOUD_CONFIG is required")
	}
	raw := json.RawMessage(cfg)
	if port != 0 {
		raw = withPort(t, raw, port)
	}
	db, err := clickhouse.NewDBForTestWith(raw, clickhouse.TestEnv{}) // system roots, strict policy
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSQ24_CloudSmoke(t *testing.T) {
	db, ctx := cloudDB(t, 0), context.Background()
	res, err := db.ValidateContext(sqlconnect.WithValidationOptions(ctx, sqlconnect.ValidationOptions{WorkingDatabase: scratchOf(t)}))
	require.NoError(t, err)
	t.Logf("cloud version=%s", res.ServerVersion)
	scratch := scratchOf(t)
	a := scratchTable(t, db, scratch, "smoke_a_")
	b := scratchTable(t, db, scratch, "smoke_b_")
	require.NoError(t, db.CreateTableFromQuery(ctx, a, "SELECT toUInt64(number) AS id, toJSONString(map('k', number)) AS j FROM numbers(3)"))
	require.Regexp(t, `SharedMergeTree(.|\n)*ORDER BY \(?id\)?`, showCreate(t, db, a))
	rows := jsonRows(t, db, a)
	require.Len(t, rows, 3, "the JSON mapper reads the published rows")
	for i, r := range rows {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(r), &m), r)
		require.EqualValues(t, fmt.Sprint(i), fmt.Sprint(m["id"]), r)
		require.Contains(t, m["j"], `"k"`, r)
	}
	require.NoError(t, db.RenameTable(ctx, a, b))
	require.Equal(t, 3, countRows(t, db, b), "rename keeps the rows")
	require.NoError(t, db.CreateTableFromQuery(ctx, a, "SELECT toUInt64(9) AS id, '' AS j"))
	_, err = pinned(t, db).ExecContext(ctx, "EXCHANGE TABLES "+db.QuoteTable(a)+" AND "+db.QuoteTable(b))
	require.NoError(t, err)
	require.Equal(t, 3, countRows(t, db, a), "exchange publishes the three-row table under the first name")
	require.Equal(t, 1, countRows(t, db, b))
	_, err = pinned(t, db).ExecContext(ctx, "EXCHANGE TABLES "+db.QuoteTable(a)+" AND "+db.QuoteIdentifier(scratch)+".`missing_x`")
	require.EqualValues(t, 60, db.ClassifyError(err).ServerCode)
	require.NoError(t, db.TruncateTable(ctx, a))
	require.Zero(t, countRows(t, db, a), "truncate empties the table")
}

func TestSQ5_Native9440(t *testing.T) {
	db := cloudDB(t, 9440)
	_, err := db.ExecContext(context.Background(), "SELECT 1")
	require.Equal(t, "CH_NETWORK", db.ClassifyError(err).Code, "never CH_UNKNOWN")
	t.Logf("9440 error: %v", err)
}

func TestCP26_CloudReplicatedDefault(t *testing.T) {
	db, ctx := cloudDB(t, 0), context.Background()
	var engine string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT value FROM system.settings WHERE name = 'default_table_engine'").Scan(&engine))
	t.Logf("cloud default_table_engine=%s", engine)
	scratch := scratchOf(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	replicated := settingsExec{conn: conn, settings: map[string]any{"default_table_engine": "ReplicatedMergeTree"}}
	plain := scratchTable(t, db, scratch, "cp26_default_")
	_, err = replicated.ExecContext(ctx, "CREATE TABLE "+db.QuoteTable(plain)+" (a UInt8) ORDER BY a")
	require.NoError(t, err, "an engine-free CREATE uses the ReplicatedMergeTree default")
	require.Regexp(t, `(Shared|Replicated)MergeTree`, showCreate(t, db, plain))

	hostile := settingsExec{conn: conn, settings: map[string]any{"default_table_engine": "Log"}}
	free := scratchTable(t, db, scratch, "cp26_free_")
	_, err = hostile.ExecContext(ctx, "CREATE TABLE "+db.QuoteTable(free)+" AS SELECT 1 AS a")
	require.EqualValues(t, 36, db.ClassifyError(err).ServerCode, "negative control: the engine-free CTAS used the hostile default")

	for name, ex := range map[string]sqlconnect.QueryExecutor{"replicated default": replicated, "hostile default": hostile} {
		ours := scratchTable(t, db, scratch, "cp26_ours_")
		_, err = db.CreateTableForQueryWithOptions(ctx, ex, ours, "SELECT toUInt64(1) AS id", sqlconnect.MaterializationOptions{SortingKey: []string{"id"}})
		require.NoError(t, err, "%s: the explicit ENGINE decides the engine", name)
		require.Regexp(t, `SharedMergeTree(.|\n)*ORDER BY \(?id\)?`, showCreate(t, db, ours), name)
	}
}

// TestSQ28_CloudCancellation runs the kill and outcome lookups on Cloud, where
// the answering replica can differ between requests.
func TestSQ28_CloudCancellation(t *testing.T) {
	db, ctx := cloudDB(t, 0), context.Background()
	scratch := scratchOf(t)
	sink := scratchTable(t, db, scratch, "cancel_sink_")
	_, err := pinned(t, db).ExecContext(ctx, "CREATE TABLE "+db.QuoteTable(sink)+" (n UInt64) ENGINE = MergeTree ORDER BY n")
	require.NoError(t, err)

	t.Run("kill a running insert", func(t *testing.T) {
		id := clickhousequery.NewQueryID()
		conn := pinned(t, db)
		done := make(chan error, 1)
		go func() {
			_, err := conn.ExecContext(stmtCtx(ctx, map[string]any{"max_block_size": 1}, id),
				"INSERT INTO "+db.QuoteTable(sink)+" SELECT number FROM numbers(600) WHERE sleepEachRow(0.1) = 0")
			done <- err
		}()
		require.Eventually(t, func() bool {
			o, err := db.QueryOutcome(ctx, id, "Insert")
			return err == nil && o == sqlconnect.QueryRunning
		}, 2*time.Minute, time.Second)
		var res sqlconnect.KillResult
		require.Eventually(t, func() bool {
			res, err = db.KillQuery(ctx, id)
			return err == nil && len(res.Rows) == 1 && res.Rows[0].KillStatus == "finished"
		}, 2*time.Minute, time.Second, "the kill reaches the replica that runs the insert")
		werr := <-done
		require.Error(t, werr)
		require.EqualValues(t, 394, db.ClassifyError(werr).ServerCode, "the killed insert fails mid-stream with code 394")
		require.Eventually(t, func() bool {
			o, err := db.QueryOutcome(ctx, id, "Insert")
			return err == nil && o == sqlconnect.QueryFailed
		}, 2*time.Minute, 2*time.Second)
	})

	t.Run("finished", func(t *testing.T) {
		id := clickhousequery.NewQueryID()
		_, err := pinned(t, db).ExecContext(stmtCtx(ctx, map[string]any{}, id), "INSERT INTO "+db.QuoteTable(sink)+" SELECT number FROM numbers(10)")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			o, err := db.QueryOutcome(ctx, id, "Insert")
			return err == nil && o == sqlconnect.QueryFinished
		}, 2*time.Minute, 2*time.Second)
	})

	t.Run("mid-stream kill", func(t *testing.T) {
		id := clickhousequery.NewQueryID()
		rows, err := db.QueryContext(stmtCtx(ctx, map[string]any{"max_block_size": 1}, id),
			midStreamSQL)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		require.True(t, rows.Next(), "the first block arrives before the kill")
		require.Eventually(t, func() bool {
			res, err := db.KillQuery(ctx, id)
			return err == nil && len(res.Rows) == 1 && res.Rows[0].KillStatus == "finished"
		}, 2*time.Minute, time.Second)
		drain(rows)
		require.EqualValues(t, 394, db.ClassifyError(rows.Err()).ServerCode, "the stream ends with code 394")
		require.Equal(t, "CH_CANCELLED", db.ClassifyError(rows.Err()).Code)
	})

	t.Run("mid-stream timeout", func(t *testing.T) {
		rows, err := db.QueryContext(stmtCtx(ctx, map[string]any{"max_execution_time": 2, "max_block_size": 1}, clickhousequery.NewQueryID()),
			midStreamSQL)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		require.True(t, rows.Next(), "the first block arrives before the time limit")
		drain(rows)
		require.EqualValues(t, 159, db.ClassifyError(rows.Err()).ServerCode, "the stream ends with code 159")
		require.Equal(t, "CH_TIMEOUT", db.ClassifyError(rows.Err()).Code)
	})
}

// midStreamSQL streams 1 MiB rows slowly. The server buffers about 1 MiB of
// output before its first flush, so small rows would all arrive at the end.
const midStreamSQL = "SELECT number, randomPrintableASCII(1048576) AS pad FROM numbers(600) WHERE sleepEachRow(0.1) = 0"

func drain(rows *sql.Rows) {
	var (
		n   uint64
		pad string
	)
	for rows.Next() {
		_ = rows.Scan(&n, &pad)
	}
}

// pinned returns a connection of its own for writes, so database/sql never
// replays a write on a new connection after driver.ErrBadConn.
func pinned(t *testing.T, db *clickhouse.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// scratchTable returns a fresh table name in the _rudderstack database and drops
// that table at test end, so a failed assertion leaves nothing on the service.
func scratchTable(t *testing.T, db *clickhouse.DB, scratch, prefix string) sqlconnect.RelationRef {
	t.Helper()
	ref := sqlconnect.NewRelationRef(prefix+strings.ToLower(rand.String(8)), sqlconnect.WithSchema(scratch))
	t.Cleanup(func() {
		if err := db.DropTable(context.Background(), ref); err != nil {
			t.Errorf("drop %s: %v", ref.Name, err)
		}
	})
	return ref
}

func countRows(t *testing.T, db *clickhouse.DB, ref sqlconnect.RelationRef) int {
	t.Helper()
	n, err := db.CountTableRows(context.Background(), ref)
	require.NoError(t, err)
	return n
}

// jsonRows reads every row of ref through the driver's JSON mapper, ordered by id.
func jsonRows(t *testing.T, db *clickhouse.DB, ref sqlconnect.RelationRef) []string {
	t.Helper()
	ch, leave := sqlconnect.QueryJSONAsync(context.Background(), db, "SELECT * FROM "+db.QuoteTable(ref)+" ORDER BY id")
	defer leave()
	var out []string
	for r := range ch {
		require.NoError(t, r.Err)
		out = append(out, string(r.Value))
	}
	return out
}

// withPort returns the account config with its port replaced.
func withPort(t *testing.T, raw json.RawMessage, port int) json.RawMessage {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	m["port"] = port
	out, err := json.Marshal(m)
	require.NoError(t, err)
	return out
}

// scratchOf checks the account and returns the working database that the cloud tests use.
func scratchOf(t *testing.T) string {
	t.Helper()
	_, err := clickhouse.ParseConfig(json.RawMessage(os.Getenv("CLICKHOUSE_CLOUD_CONFIG")))
	require.NoError(t, err)
	return "_rudderstack"
}

func showCreate(t *testing.T, db *clickhouse.DB, ref sqlconnect.RelationRef) string {
	t.Helper()
	var ddl string
	require.NoError(t, db.QueryRowContext(context.Background(), "SHOW CREATE TABLE "+db.QuoteTable(ref)).Scan(&ddl))
	return ddl
}

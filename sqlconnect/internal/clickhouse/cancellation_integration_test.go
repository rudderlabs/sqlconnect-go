package clickhouse_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

func TestSQ28_KillAndOutcome(t *testing.T) {
	srv, db := retlFixture(t)
	ctx := context.Background()
	target := clickhousequery.NewQueryID()
	done := startSlow(t, db, ctx, target)
	require.Eventually(t, func() bool {
		o, err := db.QueryOutcome(ctx, target, "Insert")
		return err == nil && o == sqlconnect.QueryRunning
	}, time.Minute, 100*time.Millisecond)
	callerCtx := idCtx(ctx, target)
	res, err := db.KillQuery(callerCtx, target) // a fresh id even with the target's id on the context
	require.NoError(t, err)
	require.Equal(t, []sqlconnect.KillRow{{QueryID: target, KillStatus: "finished"}}, res.Rows)
	require.Equal(t, sqlconnect.ErrorInfo{Category: "cancellation", Code: "CH_CANCELLED", ServerCode: 394}, db.ClassifyError(<-done))
	empty, err := db.KillQuery(ctx, clickhousequery.NewQueryID())
	require.NoError(t, err)
	require.Empty(t, empty.Rows)
	require.Eventually(t, func() bool {
		o, err := db.QueryOutcome(callerCtx, target, "Insert")
		return err == nil && o == sqlconnect.QueryFailed
	}, time.Minute, time.Second, "polls past the 7 to 10 s flush lag; its own rows never count")
	kills := srv.KillStatements(t)
	require.Len(t, kills, 2, "both KillQuery calls reached the server")
	for _, k := range kills {
		require.NotContains(t, k, "ON CLUSTER")
	}
}

func TestSQ28_ContextCancelLeavesServerRunning(t *testing.T) {
	srv, db := retlFixture(t)
	id := clickhousequery.NewQueryID()
	ctx, cancel := context.WithCancel(context.Background())
	done := startSlow(t, db, ctx, id)
	require.Eventually(t, func() bool { return srv.ProcessCount(t, id) == 1 }, time.Minute, 100*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	time.Sleep(2 * time.Second)
	require.Equal(t, 1, srv.ProcessCount(t, id), "a context cancel alone leaves the INSERT running")
	srv.AdminExec(t, "KILL QUERY WHERE query_id = '"+id+"' SYNC")
}

func TestSQ28_LostResponseOutcomes(t *testing.T) {
	srv, _ := retlFixture(t)
	p := chtest.NewProxy(t, srv, chtest.ProxyOptions{})
	db := openScopedVia(t, srv, p, "rudder_retl", "pw_Retl_123")
	ctx := context.Background()
	for sql, want := range map[string]sqlconnect.QueryOutcome{
		"INSERT INTO scratch_db.sink SELECT number FROM numbers(10)":                      sqlconnect.QueryFinished,
		"INSERT INTO scratch_db.sink SELECT throwIf(number = 5, 'boom') FROM numbers(10)": sqlconnect.QueryFailed,
	} {
		id := clickhousequery.NewQueryID()
		p.DropResponseOnce(func(r chtest.Request) bool { return r.Query.Get("query_id") == id })
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(idCtx(ctx, id), sql)
		require.Error(t, err)
		_ = conn.Close()
		require.Eventually(t, func() bool {
			o, err := db.QueryOutcome(idCtx(ctx, id), id, "Insert")
			return err == nil && o == want
		}, time.Minute, time.Second, "the lookup runs under its own id")
	}
}

func TestSQ28_OwnUserOnlyAndGrants(t *testing.T) {
	srv, db := retlFixture(t)
	srv.CreateScopedUser(t, "other_user", "pw_Other_123", "customer_db", "other_scratch", false)
	srv.FlushLogs(t) // a fresh server creates system.query_log at the first flush
	ctx := context.Background()
	id := clickhousequery.NewQueryID()
	// One row per block, 3 s each: the query runs long enough to be seen.
	other := srv.StartAs(t, "other_user", "pw_Other_123", id, "SELECT 'other-user-secret-text', sleep(3) FROM numbers(3) SETTINGS max_block_size = 1")
	res, err := db.KillQuery(ctx, id)
	require.NoError(t, err)
	require.Empty(t, res.Rows, "the kill filters on user = currentUser()")
	require.Equal(t, 1, srv.ProcessCount(t, id), "the other user's query keeps running")
	o, err := db.QueryOutcome(ctx, id, "Select")
	require.NoError(t, err)
	require.Equal(t, sqlconnect.QueryNotFound, o)
	require.NotContains(t, fmt.Sprintf("%v %v", res, err), "other-user-secret-text")
	other.Wait(t)
	srv.FlushLogs(t)
	o, err = db.QueryOutcome(ctx, id, "Select")
	require.NoError(t, err)
	require.Equal(t, sqlconnect.QueryNotFound, o, "another user's finished query_log row is never read")

	srv.AdminExec(t, "REVOKE SELECT ON system.processes FROM rudder_retl")
	_, err = db.KillQuery(ctx, clickhousequery.NewQueryID())
	require.Equal(t, sqlconnect.ErrorInfo{Category: "permission", Code: "CH_PERMISSION", ServerCode: 497}, db.ClassifyError(err))
	srv.AdminExec(t, "GRANT SELECT ON system.processes TO rudder_retl")
	srv.AdminExec(t, "REVOKE SELECT ON system.query_log FROM rudder_retl")
	_, err = db.QueryOutcome(ctx, clickhousequery.NewQueryID(), "Insert")
	require.EqualValues(t, 497, db.ClassifyError(err).ServerCode)

	admin := openAdmin(t, srv) // query_kind evidence for rudder-sources (seam S17)
	kinds := []struct{ name, sql, id string }{
		{"INSERT SELECT", "INSERT INTO scratch_db.sink SELECT 1", ""},
		{"CREATE", "CREATE TABLE scratch_db.k (a UInt8) ENGINE = MergeTree ORDER BY a", ""},
		{"RENAME", "RENAME TABLE scratch_db.k TO scratch_db.k2", ""},
		{"EXCHANGE", "EXCHANGE TABLES scratch_db.k2 AND scratch_db.sink", ""},
		{"ALTER DELETE", "ALTER TABLE scratch_db.k2 DELETE WHERE n = 1 SETTINGS mutations_sync = 1", ""},
		{"DROP", "DROP TABLE scratch_db.k2 SYNC", ""},
	}
	for i := range kinds {
		kinds[i].id = clickhousequery.NewQueryID()
		_, err := admin.ExecContext(idCtx(ctx, kinds[i].id), kinds[i].sql)
		require.NoError(t, err, kinds[i].name)
	}
	srv.FlushLogs(t)
	for _, k := range kinds {
		t.Logf("query_kind %s = %s", k.name, srv.QueryLogKind(t, k.id))
	}
	got := make([]string, len(kinds))
	for i, k := range kinds {
		got[i] = srv.QueryLogKind(t, k.id)
	}
	require.Equal(t, []string{"Insert", "Create", "Rename", "Rename", "Alter", "Drop"}, got, "EXCHANGE logs as Rename")
}

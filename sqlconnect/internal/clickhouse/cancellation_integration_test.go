package clickhouse_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

// TestSQ28_StopAndRunningCheck runs the stop and the running check for a user
// without system grants and for one with them. Both must behave the same.
func TestSQ28_StopAndRunningCheck(t *testing.T) {
	srv, plain := retlFixture(t)
	srv.CreateScopedUser(t, "retl_sys", "pw_Sys_12345", "customer_db", "_rudderstack", false)
	srv.AdminExec(t, "GRANT SELECT ON system.processes TO retl_sys")
	srv.AdminExec(t, "GRANT SELECT ON system.query_log TO retl_sys")
	ctx := context.Background()
	for name, db := range map[string]*clickhouse.DB{"no system grants": plain, "system grants": openScoped(t, srv, "retl_sys", "pw_Sys_12345")} {
		target := clickhousequery.NewQueryID()
		done := startSlow(t, db, ctx, target)
		require.Eventually(t, func() bool {
			running, err := db.QueryRunning(ctx, target)
			return err == nil && running
		}, time.Minute, 100*time.Millisecond, name)
		require.Equal(t, 1, srv.ProcessCount(t, target), "%s: the running check leaves the target running", name)
		stopped, err := db.StopQuery(idCtx(ctx, target), target)
		require.NoError(t, err, name)
		require.True(t, stopped, name)
		require.Equal(t, sqlconnect.ErrorInfo{Category: "cancellation", Code: "CH_CANCELLED", ServerCode: 394}, db.ClassifyError(<-done), name)
		require.Equal(t, 0, srv.ProcessCount(t, target), name)
		running, err := db.QueryRunning(ctx, target)
		require.NoError(t, err, name)
		require.False(t, running, "%s: the running check is false after the stop", name)
		stopped, err = db.StopQuery(ctx, target)
		require.NoError(t, err, name)
		require.False(t, stopped, "%s: nothing runs, so nothing stops", name)

		// The deprecated methods sit on the same primitives.
		res, err := db.KillQuery(ctx, target)
		require.NoError(t, err, name)
		require.Empty(t, res.Rows, name)
		o, err := db.QueryOutcome(ctx, target, "Insert")
		require.NoError(t, err, name)
		require.Equal(t, sqlconnect.QueryFailed, o, "%s: an ended query is failed until the caller verifies it", name)
	}
	srv.FlushLogs(t)
	require.Empty(t, srv.KillStatements(t), "the driver never sends KILL QUERY")
	require.Empty(t, srv.QueryLogLike(t, "%system.processes%"), "the driver never reads system.processes")
	require.Empty(t, srv.QueryLogLike(t, "%system.query_log%"), "the driver never reads system.query_log")
}

// TestSQ28_StopRefusedFailsClosed: a constraint pins replace_running_query to
// 0, so the server refuses the stop. StopQuery returns a classified error at
// once, and the target keeps running.
func TestSQ28_StopRefusedFailsClosed(t *testing.T) {
	srv, _ := retlFixture(t)
	srv.CreateScopedUser(t, "retl_pinned", "pw_Pin_12345", "customer_db", "_rudderstack", false)
	srv.AdminExec(t, "ALTER USER retl_pinned SETTINGS replace_running_query = 0 CONST")
	db := openScoped(t, srv, "retl_pinned", "pw_Pin_12345")
	ctx := context.Background()
	target := clickhousequery.NewQueryID()
	done := startSlow(t, db, ctx, target)
	require.Eventually(t, func() bool { return srv.ProcessCount(t, target) == 1 }, time.Minute, 100*time.Millisecond)
	start := time.Now()
	stopped, err := db.StopQuery(ctx, target)
	require.False(t, stopped)
	require.Equal(t, sqlconnect.ErrorInfo{Category: "permission", Code: "CH_PERMISSION", ServerCode: 452}, db.ClassifyError(err))
	require.Less(t, time.Since(start), 10*time.Second, "a refused stop returns at once")
	require.Equal(t, 1, srv.ProcessCount(t, target), "the refused stop leaves the target running")
	srv.AdminExec(t, "KILL QUERY WHERE query_id = '"+target+"' SYNC")
	<-done
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

// TestSQ28_LostResponseRunningCheck: after a lost response the running check
// says the write has ended, and the caller's own table shows what it wrote.
func TestSQ28_LostResponseRunningCheck(t *testing.T) {
	srv, _ := retlFixture(t)
	p := chtest.NewProxy(t, srv, chtest.ProxyOptions{})
	db := openScopedVia(t, srv, p, "rudder_retl", "pw_Retl_123")
	ctx := context.Background()
	// The cases share the sink, so they run in order: the failed write must
	// leave the count from the finished write unchanged.
	for _, c := range []struct{ sql, rows string }{
		{"INSERT INTO _rudderstack.sink SELECT number FROM numbers(10)", "10"},
		{"INSERT INTO _rudderstack.sink SELECT throwIf(number = 5, 'boom') FROM numbers(10)", "10"},
	} {
		sql, rows := c.sql, c.rows
		id := clickhousequery.NewQueryID()
		p.DropResponseOnce(func(r chtest.Request) bool { return r.Query.Get("query_id") == id })
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(idCtx(ctx, id), sql)
		require.Error(t, err)
		_ = conn.Close()
		require.Eventually(t, func() bool {
			running, err := db.QueryRunning(ctx, id)
			return err == nil && !running
		}, time.Minute, 100*time.Millisecond)
		require.Equal(t, rows, srv.AdminQuery(t, "SELECT count() FROM _rudderstack.sink")[0][0], "only the finished write landed")
	}
}

// TestSQ28_OwnUserOnly: the server matches a query id only against queries
// of the same user, so the stop cannot reach another user's query. The
// running check sees that id as taken, and the stop then fails closed.
func TestSQ28_OwnUserOnly(t *testing.T) {
	srv, db := retlFixture(t)
	srv.CreateScopedUser(t, "other_user", "pw_Other_123", "customer_db", "other_scratch", false)
	ctx := context.Background()
	id := clickhousequery.NewQueryID()
	// One row per block, 3 s each: the query runs long enough to be seen.
	other := srv.StartAs(t, "other_user", "pw_Other_123", id, "SELECT 'other-user-secret-text', sleep(3) FROM numbers(3) SETTINGS max_block_size = 1")
	running, err := db.QueryRunning(ctx, id)
	require.NoError(t, err)
	require.True(t, running, "the id is taken on the server")
	stopped, err := db.StopQuery(ctx, id)
	require.False(t, stopped)
	require.Equal(t, "CH_OUTCOME_UNKNOWN", db.ClassifyError(err).Code)
	require.NotContains(t, fmt.Sprintf("%v", err), "other-user-secret-text")
	require.Equal(t, 1, srv.ProcessCount(t, id), "the other user's query keeps running")
	other.Wait(t)
}

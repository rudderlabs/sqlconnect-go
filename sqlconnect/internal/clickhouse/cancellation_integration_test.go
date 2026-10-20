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
			running, err := db.RunningForTest(ctx, target)
			return err == nil && running
		}, time.Minute, 100*time.Millisecond, name)
		require.Equal(t, 1, srv.ProcessCount(t, target), "%s: the running check leaves the target running", name)
		stopped, err := db.StopQuery(idCtx(ctx, target), target)
		require.NoError(t, err, name)
		require.Equal(t, sqlconnect.StopLanded, stopped, name)
		require.Equal(t, sqlconnect.ErrorInfo{Category: "cancellation", Code: "CH_CANCELLED", ServerCode: 394}, db.ClassifyError(<-done), name)
		require.Equal(t, 0, srv.ProcessCount(t, target), name)
		running, err := db.RunningForTest(ctx, target)
		require.NoError(t, err, name)
		require.False(t, running, "%s: the running check is false after the stop", name)
		stopped, err = db.StopQuery(ctx, target)
		require.NoError(t, err, name)
		require.Equal(t, sqlconnect.StopMissed, stopped, "%s: nothing runs, so the stop misses", name)
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
	require.Equal(t, sqlconnect.StopError, stopped)
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

// TestSQ28_ContextCancelStopsRead: a read under the driver read map stops on
// the server when its context ends. No stop statement is needed. The test
// connects over plain HTTP. On 26.3 a server that ends TLS itself sees the
// close only when it next writes output, so a read that sends little output
// runs on until its time limit. Cloud ends TLS in front of the server.
func TestSQ28_ContextCancelStopsRead(t *testing.T) {
	srv, _ := retlFixture(t)
	db := openWith(t, srv.Config("rudder_retl", "pw_Retl_123", "customer_db", false), srv)
	id := clickhousequery.NewQueryID()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		rows, err := db.QueryContext(clickhouse.ReadCtxForTest(ctx, id), "SELECT number FROM numbers(600) WHERE sleepEachRow(0.1) = 0 SETTINGS max_block_size = 1")
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			_ = rows.Close()
		}
		done <- err
	}()
	require.Eventually(t, func() bool { return srv.ProcessCount(t, id) == 1 }, time.Minute, 100*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Eventually(t, func() bool { return srv.ProcessCount(t, id) == 0 }, 5*time.Second, 50*time.Millisecond,
		"the server stops a read when its client goes away")
}

// TestSQ28_OwnUserOnly: the server replaces only a query of the same user, so
// the stop cannot reach another user's query. Another user's query still
// holds the id: the running check answers 216, and the stop fails closed.
func TestSQ28_OwnUserOnly(t *testing.T) {
	srv, db := retlFixture(t)
	srv.CreateScopedUser(t, "other_user", "pw_Other_123", "customer_db", "other_scratch", false)
	ctx := context.Background()
	id := clickhousequery.NewQueryID()
	// One row per block, 3 s each: the query runs long enough to be seen.
	other := srv.StartAs(t, "other_user", "pw_Other_123", id, "SELECT 'other-user-secret-text', sleep(3) FROM numbers(3) SETTINGS max_block_size = 1")
	running, err := db.RunningForTest(ctx, id)
	require.NoError(t, err)
	require.True(t, running, "the id is taken on the server")
	stopped, err := db.StopQuery(ctx, id)
	require.Equal(t, sqlconnect.StopError, stopped)
	require.Equal(t, "CH_OUTCOME_UNKNOWN", db.ClassifyError(err).Code)
	require.NotContains(t, fmt.Sprintf("%v", err), "other-user-secret-text")
	require.Equal(t, 1, srv.ProcessCount(t, id), "the other user's query keeps running")
	other.Wait(t)
}

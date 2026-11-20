package maxcompute_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/maxcomputequery"
)

func TestIntegrationExec(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	t.Run("ping", func(t *testing.T) {
		require.NoError(t, db.PingContext(ctx))
	})

	t.Run("D-C5 the driver adds the terminator and refuses several statements", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "SELECT 1")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SELECT 1; SELECT 2")
		require.ErrorIs(t, err, maxcomputequery.ErrMultipleStatements)
	})

	t.Run("D-C4 a deadline stops a long job", func(t *testing.T) {
		var id string
		dctx, cancel := context.WithTimeout(maxcomputequery.WithInstanceIDHandler(ctx, func(s string) { id = s }), 2*time.Second)
		defer cancel()
		start := time.Now()
		_, err := db.ExecContext(dctx, slowQuery)
		elapsed := time.Since(start)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, 32*time.Second, "within the deadline plus TerminateWait")
		require.NotEmpty(t, id)
		require.Contains(t, err.Error(), id)
		t.Logf("deadline returned after %s: %v", elapsed.Round(time.Millisecond), err)

		outcome, err := db.StopQuery(ctx, id)
		require.NoError(t, err)
		require.Equal(t, sqlconnect.StopMissed, outcome, "a stopped job cannot be stopped again")
	})

	t.Run("StopQuery stops a running job", func(t *testing.T) {
		ids := make(chan string, 1)
		done := make(chan error, 1)
		go func() {
			_, err := db.ExecContext(maxcomputequery.WithInstanceIDHandler(ctx, func(s string) { ids <- s }), slowQuery)
			done <- err
		}()
		id := <-ids
		time.Sleep(2 * time.Second)
		outcome, err := db.StopQuery(ctx, id)
		require.NoError(t, err)
		require.Equal(t, sqlconnect.StopLanded, outcome)
		select {
		case err := <-done:
			require.Error(t, err, "the statement fails once its job is terminated")
			t.Logf("stopped statement: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the statement did not end after its job was terminated")
		}
		outcome, err = db.StopQuery(ctx, id)
		require.NoError(t, err)
		require.Equal(t, sqlconnect.StopMissed, outcome, "a second stop is a no-op")
	})
}

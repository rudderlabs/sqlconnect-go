package maxcompute_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/maxcomputequery"
)

// D-C10: the job tag and the task name are stored with the job, and a query
// with the renamed task still reads its rows through the Instance Tunnel.
func TestIntegrationJobTag(t *testing.T) {
	db := testDB(t)
	ctx, err := maxcomputequery.WithJobTag(context.Background(), "run-"+runID)
	require.NoError(t, err)
	var id string
	ctx = maxcomputequery.WithInstanceIDHandler(ctx, func(s string) { id = s })
	var n int64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT 7 AS a").Scan(&n))
	require.EqualValues(t, 7, n)

	source, err := db.JobSourceForTest(context.Background(), id)
	require.NoError(t, err)
	require.Contains(t, source, "rudderstack.job.tag")
	require.Contains(t, source, "rudderstack-retl:run-"+runID)
	require.Contains(t, source, "<Name>rudderstack_retl</Name>")
}

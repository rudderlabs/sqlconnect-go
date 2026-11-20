package maxcompute_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/maxcomputequery"
)

func TestIntegrationLifecycle(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	schema := testSchema(t, db, "lifecycle")
	ref := func(name string) sqlconnect.RelationRef { return sqlconnect.NewSchemaTableRef(schema, name) }

	require.NoError(t, db.CreateTestTable(ctx, ref("validation")))
	days, err := db.TableLifecycleForTest(ctx, ref("validation"))
	require.NoError(t, err)
	require.Equal(t, 1, days, "the connection test table carries LIFECYCLE 1")

	lctx, err := maxcomputequery.WithLifecycle(ctx, 3650)
	require.NoError(t, err)
	require.NoError(t, db.CreateTableFromQuery(lctx, ref("ctas"), "SELECT 1 AS a"))
	days, err = db.TableLifecycleForTest(ctx, ref("ctas"))
	require.NoError(t, err)
	require.Equal(t, 3650, days)

	require.NoError(t, db.MoveTable(lctx, ref("ctas"), ref("moved")))
	days, err = db.TableLifecycleForTest(ctx, ref("moved"))
	require.NoError(t, err)
	require.Equal(t, 3650, days)
}

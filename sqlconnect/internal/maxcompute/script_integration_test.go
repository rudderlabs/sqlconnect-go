package maxcompute_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/maxcomputequery"
)

func TestIntegrationScriptsAndCancel(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	schema := testSchema(t, db, "script")
	q := func(name string) string { return db.QuoteTable(sqlconnect.NewSchemaTableRef(schema, name)) }
	exists := func(t *testing.T, name string) bool {
		t.Helper()
		ok, err := db.TableExists(ctx, sqlconnect.NewSchemaTableRef(schema, name))
		require.NoError(t, err)
		return ok
	}
	count := func(t *testing.T, name string) int {
		t.Helper()
		n, err := db.CountTableRows(ctx, sqlconnect.NewSchemaTableRef(schema, name))
		require.NoError(t, err)
		return n
	}

	t.Run("D-C3 cancelling a running CTAS terminates it and creates nothing", func(t *testing.T) {
		// Cancel 2 s after the job exists: a submit on a fresh connection can
		// itself take more than 2 s, and a cancel during the submit is the
		// abandoned-submit path, not a running CTAS.
		ids := make(chan string, 1)
		cctx, cancel := context.WithCancel(maxcomputequery.WithInstanceIDHandler(ctx, func(s string) { ids <- s }))
		var id string
		go func() {
			id = <-ids
			time.Sleep(2 * time.Second)
			cancel()
		}()
		_, err := db.ExecContext(cctx, "CREATE TABLE "+q("cancelled_ctas")+" LIFECYCLE 1 AS "+slowQuery)
		require.ErrorIs(t, err, context.Canceled)
		require.Contains(t, err.Error(), "terminated", "the driver waited for the job to stop")
		require.False(t, exists(t, "cancelled_ctas"))
		for range 2 {
			outcome, err := db.StopQuery(ctx, id)
			require.NoError(t, err)
			require.Equal(t, sqlconnect.StopMissed, outcome, "a stopped job cannot be stopped again")
		}
	})

	t.Run("D-C6 a script that reads after a write fails to compile and applies nothing", func(t *testing.T) {
		_, err := db.ExecContext(maxcomputequery.WithScript(ctx),
			"CREATE TABLE "+q("raw_a")+" (id BIGINT) LIFECYCLE 1;\n"+
				"INSERT INTO "+q("raw_a")+" SELECT 1;\n"+
				"CREATE TABLE "+q("raw_b")+" LIFECYCLE 1 AS SELECT * FROM "+q("raw_a")+";")
		require.Error(t, err)
		t.Logf("compile failure: %v", err)
		require.False(t, exists(t, "raw_a"), "nothing applied")
		require.False(t, exists(t, "raw_b"))
	})

	t.Run("D-C11 a script that fails at run time keeps its DDL and rolls back its DML", func(t *testing.T) {
		for _, stmt := range []string{
			"CREATE TABLE " + q("src") + " (id BIGINT, pat STRING) LIFECYCLE 1",
			"INSERT OVERWRITE TABLE " + q("src") + " SELECT pos + 1, '(' FROM (SELECT posexplode(split(repeat('a,', 5), ',')) AS (pos, v)) g",
			"CREATE TABLE " + q("out1") + " (sentinel STRING) LIFECYCLE 1",
			"CREATE TABLE " + q("out2") + " (sentinel STRING) LIFECYCLE 1",
			"INSERT INTO " + q("out1") + " SELECT 'sentinel'",
			"INSERT INTO " + q("out2") + " SELECT 'sentinel'",
		} {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err, stmt)
		}
		// The failing expression depends on each row, so the optimizer cannot
		// fold it away; it fails while pat is "(" (an invalid pattern).
		script := "DROP TABLE IF EXISTS " + q("out1") + ";\n" +
			"CREATE TABLE " + q("out1") + " (id BIGINT) LIFECYCLE 1;\n" +
			"DROP TABLE IF EXISTS " + q("out2") + ";\n" +
			"CREATE TABLE " + q("out2") + " (id BIGINT) LIFECYCLE 1;\n" +
			"INSERT OVERWRITE TABLE " + q("out1") + " SELECT id FROM " + q("src") + ";\n" +
			"INSERT OVERWRITE TABLE " + q("out2") + " SELECT IF(regexp_replace('a', concat(pat, CAST(id AS STRING)), '') = 'zz', -1L, id) FROM " + q("src") + ";"
		_, err := db.ExecContext(maxcomputequery.WithScript(ctx), script)
		require.Error(t, err, "the last INSERT fails at run time")
		t.Logf("runtime failure: %v", err)
		cols, err := db.ListColumns(ctx, sqlconnect.NewSchemaTableRef(schema, "out1"))
		require.NoError(t, err)
		require.Equal(t, "id", cols[0].Name, "the DROP and CREATE stayed applied: the new schema is in place")
		require.Zero(t, count(t, "out1"), "no INSERT applied, not even the one before the failure")
		require.Zero(t, count(t, "out2"))

		_, err = db.ExecContext(ctx, "INSERT OVERWRITE TABLE "+q("src")+" SELECT id, '' FROM "+q("src"))
		require.NoError(t, err)
		_, err = db.ExecContext(maxcomputequery.WithScript(ctx), script)
		require.NoError(t, err, "a whole-script rerun succeeds")
		require.Equal(t, 5, count(t, "out1"))
		require.Equal(t, 5, count(t, "out2"))
	})

	t.Run("D-C12 no job carries a time-zone hint", func(t *testing.T) {
		var zone string
		require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_TIMEZONE()").Scan(&zone))
		t.Logf("job time zone: %s (the project's, with no hint)", zone)
		require.NotEmpty(t, zone)
		require.NotContains(t, maxcomputequery.FixedHints(), "odps.sql.timezone")
		_, err := maxcomputequery.WithHints(ctx, map[string]string{"odps.sql.timezone": "UTC"})
		require.ErrorIs(t, err, maxcomputequery.ErrHintNotAllowed)
	})
}

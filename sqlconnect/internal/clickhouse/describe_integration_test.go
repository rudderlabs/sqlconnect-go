package clickhouse_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
)

func TestSQ10_ZeroRowMetadata(t *testing.T) {
	srv, db := openFloorWithServer(t)
	ctx := context.Background()
	schema := seedSchema(t, db, `CREATE TABLE {{.schema}}.src (id UInt64, ts Nullable(DateTime64(3)), a Array(UInt8), z DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id`)
	src := schema.Name + ".src"
	want := []sqlconnect.ColumnRef{
		{Name: "id", Type: "string", RawType: "UInt64"},
		{Name: "ts", Type: "datetime", RawType: "Nullable(DateTime64(3))"},
		{Name: "a", Type: "array", RawType: "Array(UInt8)"},
		{Name: "z", Type: "datetime", RawType: "DateTime64(3, 'UTC')"},
	}
	for _, q := range []string{
		"SELECT id, ts, a, z FROM " + src + " LIMIT 0",
		"SELECT id, ts, a, z FROM " + src + " WHERE id = 42",
		"WITH c AS (SELECT * FROM " + src + ") SELECT id, ts, a, z FROM c;",
		"SELECT id, ts, a, z FROM " + src + " -- trailing comment",
	} {
		cols, err := db.ListColumnsForSqlQuery(ctx, q)
		require.NoError(t, err, q)
		require.Equal(t, want, cols, q)
	}
	cols, err := db.ListColumnsForSqlQuery(ctx, "SELECT if(id > 0, id, NULL) AS n, CAST(map('k', 1) AS Map(String, UInt8)) AS m FROM "+src)
	require.NoError(t, err)
	require.Len(t, cols, 2)
	require.Equal(t, []string{"Nullable(UInt64)", "unsupported"}, []string{cols[0].RawType, cols[1].Type}, "unsupported metadata, not a decoding failure")

	// The guard refuses before any request: the refused text never reaches
	// the server.
	for _, q := range []string{
		"SELECT id FROM " + src + " FORMAT JSON",
		"SELECT 1) UNION ALL (SELECT 2",
		"SELECT 1; SELECT 2",
	} {
		_, err = db.ListColumnsForSqlQuery(ctx, q)
		requireCode(t, err, "CH_QUERY_INVALID")
	}
	require.Empty(t, srv.RawHTTPS(t, "SELECT id FROM "+src+" WHERE 0", url.Values{"default_format": {"Native"}}), "negative control: zero-row Native over HTTP is an empty body")

	// A caller statement context is kept: its query id reaches the server.
	callerID := clickhousequery.NewQueryID()
	callerCtx := clickhousequery.WithStatement(ctx, map[string]any{"send_progress_in_http_headers": 0, "readonly": 2}, callerID)
	_, err = db.ListColumnsForSqlQuery(callerCtx, "SELECT id FROM "+src)
	require.NoError(t, err)

	srv.FlushLogs(t)
	require.Zero(t, srv.QueryLogCountLike(t, "%SELECT id, ts, a, z FROM "+src+" WHERE id = 42%", "Select"), "DESCRIBE does not run the query")
	require.Empty(t, srv.QueryLogLike(t, "%FORMAT JSON%"), "a refused query sends no request")
	require.Empty(t, srv.QueryLogLike(t, "%UNION ALL (SELECT 2%"), "a refused query sends no request")
	require.Equal(t, 1, srv.QueryLogCount(t, callerID), "the caller's query id is kept")
	described := srv.QueryLogLike(t, "DESCRIBE (%")
	require.Len(t, described, 6, "one DESCRIBE per accepted query")
	for _, row := range described {
		require.Equal(t, "Describe", row.Kind, row.Query)
		require.True(t, strings.HasPrefix(row.QueryID, "retl-"), "an unmarked context gets a fresh retl- id")
		require.Equal(t, "2", row.Settings["readonly"], "and the driver read map")
	}
}

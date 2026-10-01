//go:build clickhouse_cloud

// The before-GA ClickHouse Cloud arm. CI never runs it; run it with
// `make test-clickhouse-cloud` and CLICKHOUSE_CLOUD_CONFIG set to an account config.
package clickhouse_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
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
	res, err := db.ValidateContext(ctx)
	require.NoError(t, err)
	t.Logf("cloud version=%s", res.ServerVersion)
	scratch := scratchOf(t)
	a := sqlconnect.NewRelationRef("smoke_a_"+strings.ToLower(rand.String(6)), sqlconnect.WithSchema(scratch))
	b := sqlconnect.NewRelationRef(a.Name+"_b", sqlconnect.WithSchema(scratch))
	require.NoError(t, db.CreateTableFromQuery(ctx, a, "SELECT toUInt64(number) AS id, toJSONString(map('k', number)) AS j FROM numbers(3)"))
	require.Regexp(t, `SharedMergeTree(.|\n)*ORDER BY id`, showCreate(t, db, a))
	require.NoError(t, db.RenameTable(ctx, a, b))
	require.NoError(t, db.CreateTableFromQuery(ctx, a, "SELECT toUInt64(9) AS id, '' AS j"))
	_, err = db.ExecContext(ctx, "EXCHANGE TABLES "+db.QuoteTable(a)+" AND "+db.QuoteTable(b))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "EXCHANGE TABLES "+db.QuoteTable(a)+" AND "+db.QuoteIdentifier(scratch)+".`missing_x`")
	require.EqualValues(t, 60, db.ClassifyError(err).ServerCode)
	require.NoError(t, db.TruncateTable(ctx, a))
	require.NoError(t, db.DropTable(ctx, a))
	require.NoError(t, db.DropTable(ctx, b))
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
	hostile := settingsExec{conn: conn, settings: map[string]any{"default_table_engine": "Log"}}
	free := sqlconnect.NewRelationRef("cp26_free_"+strings.ToLower(rand.String(6)), sqlconnect.WithSchema(scratch))
	_, err = hostile.ExecContext(ctx, "CREATE TABLE "+db.QuoteTable(free)+" AS SELECT 1 AS a")
	require.EqualValues(t, 36, db.ClassifyError(err).ServerCode, "negative control: the engine-free CTAS used the hostile default")
	ours := sqlconnect.NewRelationRef("cp26_"+strings.ToLower(rand.String(6)), sqlconnect.WithSchema(scratch))
	_, err = db.CreateTableForQueryWithOptions(ctx, hostile, ours, "SELECT toUInt64(1) AS id", sqlconnect.MaterializationOptions{SortingKey: []string{"id"}})
	require.NoError(t, err, "the explicit ENGINE ignores the hostile default")
	require.Regexp(t, `SharedMergeTree(.|\n)*ORDER BY id`, showCreate(t, db, ours))
	require.NoError(t, db.DropTable(ctx, ours))
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

// scratchOf reads the scratch database from the account config.
func scratchOf(t *testing.T) string {
	t.Helper()
	var c struct {
		ScratchDatabase string `json:"scratchDatabase"`
	}
	require.NoError(t, json.Unmarshal([]byte(os.Getenv("CLICKHOUSE_CLOUD_CONFIG")), &c))
	require.NotEmpty(t, c.ScratchDatabase)
	return c.ScratchDatabase
}

func showCreate(t *testing.T, db *clickhouse.DB, ref sqlconnect.RelationRef) string {
	t.Helper()
	var ddl string
	require.NoError(t, db.QueryRowContext(context.Background(), "SHOW CREATE TABLE "+db.QuoteTable(ref)).Scan(&ddl))
	return ddl
}

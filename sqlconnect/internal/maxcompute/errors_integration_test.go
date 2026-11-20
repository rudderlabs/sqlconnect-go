package maxcompute_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/maxcomputequery"
)

func TestIntegrationErrors(t *testing.T) {
	cfg := testConfig(t)
	var account struct {
		Endpoint    string `json:"endpoint"`
		AccessKeyID string `json:"accessKeyId"`
	}
	require.NoError(t, json.Unmarshal(cfg, &account))
	ctx := context.Background()

	pingCategory := func(t *testing.T, cfg []byte) (string, string) {
		t.Helper()
		db, err := maxcompute.NewDB(cfg)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		err = db.PingContext(ctx)
		require.Error(t, err)
		return db.ClassifyError(err).Category, err.Error()
	}

	t.Run("D-C8 wrong secret", func(t *testing.T) {
		bad, err := sjson.SetBytes(cfg, "accessKeySecret", "rs-retl-probe-wrong-secret")
		require.NoError(t, err)
		category, text := pingCategory(t, bad)
		require.Equal(t, maxcomputequery.CategoryAuth, category, text)
		require.NotContains(t, text, account.AccessKeyID)
	})

	t.Run("D-C8 unknown key id", func(t *testing.T) {
		fakeID := "LTAI5tRsRetlProbeFake0001"
		bad, err := sjson.SetBytes(cfg, "accessKeyId", fakeID)
		require.NoError(t, err)
		category, text := pingCategory(t, bad)
		require.Equal(t, maxcomputequery.CategoryAuth, category, text)
		require.NotContains(t, text, fakeID, "the echoed key id is redacted")
		require.NotContains(t, text, "RsRetlProbeFake")
	})

	t.Run("D-C8 wrong region", func(t *testing.T) {
		other := "https://service.eu-central-1.maxcompute.aliyun.com/api"
		if account.Endpoint == other {
			other = "https://service.us-west-1.maxcompute.aliyun.com/api"
		}
		bad, err := sjson.SetBytes(cfg, "endpoint", other)
		require.NoError(t, err)
		category, text := pingCategory(t, bad)
		require.Equal(t, maxcomputequery.CategoryProjectNotFound, category, text)
	})

	t.Run("D-C8 missing /api is refused before any call", func(t *testing.T) {
		bad, err := sjson.SetBytes(cfg, "endpoint", strings.TrimSuffix(account.Endpoint, "/api"))
		require.NoError(t, err)
		_, err = maxcompute.NewDB(bad)
		require.ErrorContains(t, err, "endpoint")
	})

	db := testDB(t)
	t.Run("job errors are classified", func(t *testing.T) {
		for sql, category := range map[string]string{
			"selec 1":                            maxcomputequery.CategorySyntax,
			"SELECT nope FROM (SELECT 1 AS a) t": maxcomputequery.CategorySemantic,
			"SELECT * FROM " + objectName("no_schema") + ".t": maxcomputequery.CategoryNotFound,
		} {
			_, err := db.ExecContext(ctx, sql)
			require.Error(t, err, sql)
			require.Equal(t, category, db.ClassifyError(err).Category, "%s: %v", sql, err)
			d, ok := maxcomputequery.Describe(err)
			require.True(t, ok)
			require.Equal(t, category, d.Category)
		}
	})

	t.Run("D-C9 a partitioned table without a predicate", func(t *testing.T) {
		schema := testSchema(t, db, "fullscan")
		table := db.QuoteTable(sqlconnect.NewSchemaTableRef(schema, "parted"))
		for _, stmt := range []string{
			"CREATE TABLE " + table + " (id BIGINT) PARTITIONED BY (ds STRING) LIFECYCLE 1",
			"INSERT INTO " + table + " PARTITION (ds='2026-11-18') VALUES (1), (2)",
		} {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err, stmt)
		}
		var n int64
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n)
		if err == nil {
			t.Skip("the dev project allows full scans (odps.sql.allow.fullscan=true); the guard cannot be exercised here")
		}
		require.Equal(t, maxcomputequery.CategoryFullScan, db.ClassifyError(err).Category, err.Error())
		hinted, err := maxcomputequery.WithHints(ctx, map[string]string{maxcomputequery.HintAllowFullScan: "true"})
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(hinted, "SELECT COUNT(*) FROM "+table).Scan(&n))
		require.EqualValues(t, 2, n)
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE ds = '2026-11-18'").Scan(&n), "a partition predicate needs no hint")
	})
}

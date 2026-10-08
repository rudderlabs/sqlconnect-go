package clickhouse

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
)

func TestListColumnsForSqlQueryDoesNotGuardAudienceSQL(t *testing.T) {
	db, recorder := unitDBWithRows(t, []string{"name", "type"}, [][]driver.Value{{"id", "UInt64"}})
	for _, query := range []string{
		"SELECT id FROM db.t FINAL; \n",
		"SELECT id FROM db.t SETTINGS max_threads = 1",
		"SELECT id FROM db.t -- trailing comment",
	} {
		columns, err := db.ListColumnsForSqlQuery(context.Background(), query)
		require.NoError(t, err)
		require.Equal(t, []sqlconnect.ColumnRef{{Name: "id", Type: "string", RawType: "UInt64"}}, columns)
	}
	require.Equal(t, []string{
		"DESCRIBE (\nSELECT id FROM db.t FINAL\n)",
		"DESCRIBE (\nSELECT id FROM db.t SETTINGS max_threads = 1\n)",
		"DESCRIBE (\nSELECT id FROM db.t -- trailing comment\n)",
	}, recorder.queries())
}

func TestListColumnsForSqlQueryNormalizationErrors(t *testing.T) {
	db, recorder := unitDBWithRows(t, []string{"name", "type"}, nil)
	for _, query := range []string{"SELECT 1; SELECT 2", "SELECT 'unterminated"} {
		_, err := db.ListColumnsForSqlQuery(context.Background(), query)
		details, ok := clickhousequery.Describe(err)
		require.True(t, ok)
		require.Equal(t, "CH_QUERY_INVALID", details.Code)
	}
	require.Empty(t, recorder.queries())
}

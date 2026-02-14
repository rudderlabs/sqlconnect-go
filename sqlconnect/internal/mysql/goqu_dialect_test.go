package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/sqlgen"
)

func TestExpressions(t *testing.T) {
	expressions := GoquExpressions()

	t.Run("TimestampAdd", func(t *testing.T) {
		t.Run("literal", func(t *testing.T) {
			require.Equal(t, "DATE_ADD(CURRENT_DATE, INTERVAL 1 HOUR)", toSQL(t, expressions.TimestampAdd(goqu.L("CURRENT_DATE"), 1, "hour")))
			require.Equal(t, "DATE_ADD('2020-01-01', INTERVAL 1 DAY)", toSQL(t, expressions.TimestampAdd(goqu.L("'2020-01-01'"), 1, "day")))
		})

		t.Run("time", func(t *testing.T) {
			now, err := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
			require.NoError(t, err)
			require.Equal(t, "DATE_ADD('2020-01-01 00:00:00', INTERVAL 1 DAY)", toSQL(t, expressions.TimestampAdd(now, 1, "day")))
		})
	})

	t.Run("DateAdd", func(t *testing.T) {
		t.Run("literal", func(t *testing.T) {
			require.Equal(t, "DATE_ADD(DATE(CURRENT_DATE), INTERVAL 1 DAY)", toSQL(t, expressions.DateAdd(goqu.L("CURRENT_DATE"), 1, "day")))
			require.Equal(t, "DATE_ADD(DATE('2020-01-01'), INTERVAL 1 DAY)", toSQL(t, expressions.DateAdd(goqu.L("'2020-01-01'"), 1, "day")))
		})

		t.Run("time", func(t *testing.T) {
			now, err := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
			require.NoError(t, err)
			require.Equal(t, "DATE_ADD(DATE('2020-01-01 00:00:00'), INTERVAL 1 DAY)", toSQL(t, expressions.DateAdd(now, 1, "day")))
		})
	})
}

func toSQL(t *testing.T, expression any) string {
	esg := sqlgen.NewExpressionSQLGenerator(DatabaseType, GoquDialectOptions())
	sql, _, err := sqlgen.GenerateExpressionSQL(esg, false, expression)
	require.NoError(t, err)
	return sql
}

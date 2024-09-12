package redshift

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
			require.Equal(t, "DATEADD(hour, 1, CAST(CURRENT_DATE AS TIMESTAMP))", toSQL(t, expressions.TimestampAdd(goqu.L("CURRENT_DATE"), 1, "hour")))
			require.Equal(t, "DATEADD(day, 1, CAST('2020-01-01' AS TIMESTAMP))", toSQL(t, expressions.TimestampAdd(goqu.L("'2020-01-01'"), 1, "day")))
		})

		t.Run("time", func(t *testing.T) {
			now, err := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
			require.NoError(t, err)
			require.Equal(t, "DATEADD(day, 1, CAST('2020-01-01T00:00:00Z' AS TIMESTAMP))", toSQL(t, expressions.TimestampAdd(now, 1, "day")))
		})
	})

	t.Run("DateAdd", func(t *testing.T) {
		t.Run("literal", func(t *testing.T) {
			require.Equal(t, "DATEADD(day, 1, DATE(CURRENT_DATE))", toSQL(t, expressions.DateAdd(goqu.L("CURRENT_DATE"), 1, "day")))
			require.Equal(t, "DATEADD(day, 1, DATE('2020-01-01'))", toSQL(t, expressions.DateAdd(goqu.L("'2020-01-01'"), 1, "day")))
		})

		t.Run("time", func(t *testing.T) {
			now, err := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
			require.NoError(t, err)
			require.Equal(t, "DATEADD(day, 1, DATE('2020-01-01T00:00:00Z'))", toSQL(t, expressions.DateAdd(now, 1, "day")))
		})
	})
}

func toSQL(t *testing.T, expression interface{}) string {
	esg := sqlgen.NewExpressionSQLGenerator(DatabaseType, GoquDialectOptions())
	sql, _, err := sqlgen.GenerateExpressionSQL(esg, false, expression)
	require.NoError(t, err)
	return sql
}

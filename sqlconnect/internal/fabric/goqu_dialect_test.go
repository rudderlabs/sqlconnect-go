package fabric

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/sqlgen"
)

func TestGoquExpressions(t *testing.T) {
	expressions := GoquExpressions()
	require.Equal(t, "DATEADD(HOUR, 1, CURRENT_TIMESTAMP)", toSQL(t, expressions.TimestampAdd(goqu.L("CURRENT_TIMESTAMP"), 1, "hour")))
	require.Equal(t, "DATEADD(DAY, 1, CAST('2020-01-01' AS DATE))", toSQL(t, expressions.DateAdd(goqu.L("'2020-01-01'"), 1, "day")))

	value, err := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
	require.NoError(t, err)
	require.Equal(t, "DATEADD(DAY, 1, '2020-01-01 00:00:00')", toSQL(t, expressions.TimestampAdd(value, 1, "day")))
}

func TestGoquDialectOptions(t *testing.T) {
	expression, err := newDialect().QueryCondition("enabled", "eq", true)
	require.NoError(t, err)
	require.Equal(t, "(enabled = 1)", expression.String())
}

func TestGoquDialectUsesCallerQuotedIdentifiersAndEscapesStrings(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		value      any
		want       string
	}{
		{name: "apostrophe literal", identifier: "[name]", value: "O'Reilly", want: "([name] = 'O''Reilly')"},
		{name: "injection-shaped literal", identifier: "[name]", value: "x'; DROP TABLE dbo.events; --", want: "([name] = 'x''; DROP TABLE dbo.events; --')"},
		{name: "space in identifier", identifier: "[first name]", value: "Ada", want: "([first name] = 'Ada')"},
		{name: "closing bracket in identifier", identifier: "[na]]me]", value: 1, want: "([na]]me] = 1)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := newDialect().QueryCondition(tt.identifier, "eq", tt.value)
			require.NoError(t, err)
			require.Equal(t, tt.want, expression.String())
		})
	}
}

func TestGoquDialectInlastUsesFabricCurrentDate(t *testing.T) {
	expression, err := newDialect().QueryCondition("CAST([created_at] AS DATE)", "inlast", 1, "day")
	require.NoError(t, err)
	require.Equal(t, "(CAST([created_at] AS DATE) >= DATEADD(DAY, -1, CAST(CAST(CURRENT_TIMESTAMP AS DATE) AS DATE)))", expression.String())
}

func toSQL(t *testing.T, expression any) string {
	esg := sqlgen.NewExpressionSQLGenerator(DatabaseType, GoquDialectOptions())
	sql, _, err := sqlgen.GenerateExpressionSQL(esg, false, expression)
	require.NoError(t, err)
	return sql
}

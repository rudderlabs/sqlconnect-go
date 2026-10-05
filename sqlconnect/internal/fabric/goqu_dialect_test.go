package fabric

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/goqu/v10"
)

func TestGoquExpressions(t *testing.T) {
	expressions := GoquExpressions()
	require.Equal(t, "DATEADD(HOUR, 1, CURRENT_TIMESTAMP)", expressionSQL(t, expressions.TimestampAdd(goqu.L("CURRENT_TIMESTAMP"), 1, "hour")))
	require.Equal(t, "DATEADD(DAY, 1, CAST('2020-01-01' AS DATE))", expressionSQL(t, expressions.DateAdd(goqu.L("'2020-01-01'"), 1, "day")))

	value, err := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
	require.NoError(t, err)
	require.Equal(t, "DATEADD(DAY, 1, '2020-01-01 00:00:00')", expressionSQL(t, expressions.TimestampAdd(value, 1, "day")))
}

func TestGoquDialectOptions(t *testing.T) {
	expression, err := newDialect().QueryCondition("enabled", "eq", true)
	require.NoError(t, err)
	require.Equal(t, "([enabled] = 1)", expression.String())
}

func TestGoquDialectQuotesIdentifiersAndEscapesStrings(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		value      any
		want       string
	}{
		{name: "apostrophe literal", identifier: "name", value: "O'Reilly", want: "([name] = 'O''Reilly')"},
		{name: "injection-shaped literal", identifier: "name", value: "x'; DROP TABLE dbo.events; --", want: "([name] = 'x''; DROP TABLE dbo.events; --')"},
		{name: "space in identifier", identifier: "first name", value: "Ada", want: "([first name] = 'Ada')"},
		{name: "closing bracket in identifier", identifier: "na]me", value: 1, want: "([na]]me] = 1)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := newDialect().QueryCondition(tt.identifier, "eq", tt.value)
			require.NoError(t, err)
			require.Equal(t, tt.want, expression.String())
		})
	}
}

func expressionSQL(t *testing.T, expression any) string {
	sql, err := fabricGoquExpressionSQL(expression.(goqu.Expression))
	require.NoError(t, err)
	return sql
}

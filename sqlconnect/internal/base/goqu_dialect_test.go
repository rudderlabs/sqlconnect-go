package base

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/dialect/sqlserver"
)

func TestQueryConditionInlastDefaultsToCurrentDate(t *testing.T) {
	opts := sqlserver.DialectOptions()
	opts.QuoteIdentifiers = false
	dialect := NewGoquDialect("test", opts, &Expressions{
		DateAdd: func(date any, _ int, _ string) goqu.Expression {
			return goqu.L("DATE_ADD(?)", date)
		},
	})

	expression, err := dialect.QueryCondition("created_at", "inlast", 1, "day")
	require.NoError(t, err)
	require.Equal(t, "(created_at >= DATE_ADD(CURRENT_DATE))", expression.String())
}

package snowflake

import (
	"fmt"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/sqlgen"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func init() {
	goqu.RegisterDialect(DatabaseType, GoquDialectOptions())
}

func GoquDialectOptions() *sqlgen.SQLDialectOptions {
	o := sqlgen.DefaultDialectOptions()
	o.QuoteIdentifiers = false
	o.QuoteRune = '"'
	o.UseEqForBooleanDataTypes = true
	return o
}

func GoquExpressions() *base.Expressions {
	return &base.Expressions{
		TimestampAdd: func(timeValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(%s, %d, ?)", unit, interval), timeValue)
		},
		DateAdd: func(timeValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(%s, %d, DATE(?))", unit, interval), timeValue)
		},
	}
}

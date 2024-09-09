package trino

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/exp"
	"github.com/rudderlabs/goqu/v10/sqlgen"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func GoquDialectOptions() *sqlgen.SQLDialectOptions {
	o := sqlgen.DefaultDialectOptions()
	o.QuoteIdentifiers = false
	o.UseEqForBooleanDataTypes = true
	o.QuoteRune = '"'
	o.TimeFunctionLiteral = "from_iso8601_timestamp(?)"
	return o
}

func GoquExpressions() *base.Expressions {
	return &base.Expressions{
		TimestampAdd: func(timeValue any, interval int, unit string) goqu.Expression {
			switch tt := timeValue.(type) {
			case exp.LiteralExpression:
				if strings.HasPrefix(tt.Literal(), "'") && strings.HasSuffix(tt.Literal(), "'") {
					timeValue = goqu.L(fmt.Sprintf("TIMESTAMP %s", tt.Literal()), tt.Args()...)
				}
			}
			return goqu.L(fmt.Sprintf("DATE_ADD('%s', %d, ?)", unit, interval), timeValue)
		},
		DateAdd: func(dateValue any, interval int, unit string) goqu.Expression {
			switch dd := dateValue.(type) {
			case exp.LiteralExpression:
				if strings.HasPrefix(dd.Literal(), "'") && strings.HasSuffix(dd.Literal(), "'") {
					dateValue = goqu.L(fmt.Sprintf("TIMESTAMP %s", dd.Literal()), dd.Args()...)
				}
			}
			return goqu.L(fmt.Sprintf("DATE_ADD('%s', %d, DATE(?))", unit, interval), dateValue)
		},
	}
}

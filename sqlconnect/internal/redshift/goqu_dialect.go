package redshift

import (
	"fmt"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/sqlgen"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func GoquDialectOptions() *sqlgen.SQLDialectOptions {
	o := sqlgen.DefaultDialectOptions()
	o.QuoteIdentifiers = false
	o.QuoteRune = '"'
	return o
}

func GoquExpressions() *base.Expressions {
	return &base.Expressions{
		TimestampAdd: func(timeValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(%s, %d, CAST(? AS TIMESTAMP))", unit, interval), timeValue)
		},
		DateAdd: func(dateValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(%s, %d, DATE(CAST(? AS TIMESTAMP)))", unit, interval), dateValue)
		},
	}
}

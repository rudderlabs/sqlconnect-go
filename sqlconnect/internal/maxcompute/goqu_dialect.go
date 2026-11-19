package maxcompute

import (
	"fmt"
	"time"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/exp"
	"github.com/rudderlabs/goqu/v10/sqlgen"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func init() {
	goqu.RegisterDialect(DatabaseType, GoquDialectOptions())
}

// GoquDialectOptions returns the goqu options for MaxCompute: backtick
// identifiers and string literals escaped with backslashes,
// because MaxCompute reads 'it”s' as two adjacent literals.
func GoquDialectOptions() *sqlgen.SQLDialectOptions {
	o := sqlgen.DefaultDialectOptions()
	o.QuoteIdentifiers = false
	o.QuoteRune = '`'
	o.UseEqForBooleanDataTypes = true // MaxCompute does not parse IS TRUE
	o.EscapedRunes = map[rune][]byte{
		'\'': []byte(`\'`),
		'\\': []byte(`\\`),
		'\n': []byte(`\n`),
		'\r': []byte(`\r`),
		'\t': []byte(`\t`),
	}
	// A string cast to TIMESTAMP is read in the job's time zone, the project's,
	// so this form is not exact; QueryCondition and the Expressions render a
	// time.Time as an epoch instead (see timeLiteral). This covers goqu
	// expressions a caller builds directly.
	o.TimeFormat = "2006-01-02 15:04:05.999999999"
	o.TimeFunctionLiteral = "CAST(? AS TIMESTAMP)"
	return o
}

// timeLiteral renders t as FROM_UTC_TIMESTAMP(<epoch ms>L, 'UTC'): an epoch is
// an instant, exact in every project time zone (findings 16), at millisecond
// precision.
func timeLiteral(t time.Time) exp.LiteralExpression {
	return goqu.L(fmt.Sprintf("FROM_UTC_TIMESTAMP(%dL, 'UTC')", t.UnixMilli()))
}

// datePartUnits maps sqlconnect's units to DATEADD date parts.
var datePartUnits = map[string]string{
	"second": "ss", "minute": "mi", "hour": "hh", "day": "dd", "month": "mm", "year": "yyyy",
}

// niladic adds the parentheses MaxCompute needs on CURRENT_TIMESTAMP and
// CURRENT_DATE, and renders a time.Time as an epoch instant.
func niladic(v any) any {
	switch x := v.(type) {
	case exp.LiteralExpression:
		switch x.Literal() {
		case "CURRENT_TIMESTAMP", "current_timestamp":
			return goqu.L("CURRENT_TIMESTAMP()")
		case "CURRENT_DATE", "current_date":
			return goqu.L("CURRENT_DATE()")
		}
	case time.Time:
		return timeLiteral(x)
	}
	return v
}

// GoquExpressions returns the MaxCompute expressions.
func GoquExpressions() *base.Expressions {
	return &base.Expressions{
		TimestampAdd: func(timeValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(?, %d, '%s')", interval, datePartUnits[unit]), niladic(timeValue))
		},
		DateAdd: func(dateValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(DATE(?), %d, '%s')", interval, datePartUnits[unit]), niladic(dateValue))
		},
		CurrentDate: "CURRENT_DATE()",
	}
}

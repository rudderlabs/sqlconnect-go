package clickhouse

import (
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/exp"
	"github.com/rudderlabs/goqu/v10/sqlgen"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func init() {
	goqu.RegisterDialect(DatabaseType, GoquDialectOptions())
}

// GoquDialectOptions returns the Goqu options of the ClickHouse dialect. Goqu
// writes identifier text between quote runes without escaping it, so the
// dialect validates every identifier node before rendering (see walkIdentifiers).
func GoquDialectOptions() *sqlgen.SQLDialectOptions {
	o := sqlgen.DefaultDialectOptions()
	o.QuoteIdentifiers = true
	o.QuoteRune = '`'
	o.UseEqForBooleanDataTypes = true
	o.StringQuote = '\''
	o.EscapedRunes = map[rune][]byte{
		'\'': []byte(`\'`),
		'\\': []byte(`\\`),
		'\n': []byte(`\n`),
		'\r': []byte(`\r`),
		0:    []byte(`\0`),
	}
	// Goqu renders time.Time in its package location, UTC by default.
	o.TimeFormat = time.RFC3339Nano
	// parseDateTime64BestEffort with scale 9 keeps nanoseconds.
	o.TimeFunctionLiteral = "parseDateTime64BestEffort(?, 9, 'UTC')"
	return o
}

// GoquExpressions renders TimestampAdd and DateAdd with dateAdd. The base
// dialect has already checked the unit and converted weeks to days.
func GoquExpressions() *base.Expressions {
	return &base.Expressions{
		TimestampAdd: func(timeValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("dateAdd(%s, %d, ?)", unit, interval), chTime(timeValue))
		},
		DateAdd: func(dateValue any, interval int, unit string) goqu.Expression {
			if isLiteral(dateValue, "CURRENT_DATE") {
				return goqu.L(fmt.Sprintf("dateAdd(%s, %d, today())", unit, interval))
			}
			return goqu.L(fmt.Sprintf("dateAdd(%s, %d, toDate(?))", unit, interval), chTime(dateValue))
		},
	}
}

// chTime maps the shared SQL time literals that callers of every dialect pass
// to their ClickHouse forms. CURRENT_TIMESTAMP is not a ClickHouse function.
func chTime(v any) any {
	l, ok := v.(exp.LiteralExpression)
	if !ok || len(l.Args()) != 0 {
		return v
	}
	s := strings.TrimSpace(l.Literal())
	switch {
	case strings.EqualFold(s, "CURRENT_TIMESTAMP"), strings.EqualFold(s, "NOW()"):
		return goqu.L("now64(9, 'UTC')")
	case strings.EqualFold(s, "CURRENT_DATE"):
		return goqu.L("today()")
	case strings.HasPrefix(s, "'"):
		return goqu.L("parseDateTime64BestEffort(" + s + ", 9, 'UTC')")
	}
	return v
}

func isLiteral(v any, want string) bool {
	l, ok := v.(exp.LiteralExpression)
	return ok && len(l.Args()) == 0 && strings.EqualFold(strings.TrimSpace(l.Literal()), want)
}

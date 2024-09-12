package base

import (
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/exp"
	"github.com/rudderlabs/goqu/v10/sqlgen"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/op"
)

func NewGoquDialect(dialect string, o *sqlgen.SQLDialectOptions, expressions *Expressions) *GoquDialect {
	return &GoquDialect{
		esg:         sqlgen.NewExpressionSQLGenerator(dialect, o),
		expressions: expressions,
	}
}

type GoquDialect struct {
	esg         sqlgen.ExpressionSQLGenerator
	expressions *Expressions
}

type Expressions struct {
	TimestampAdd func(time any, interval int, unit string) goqu.Expression
	DateAdd      func(date any, interval int, unit string) goqu.Expression
}

func (gq *GoquDialect) QueryCondition(identifier, operator string, args ...any) (sql string, err error) {
	args = lo.Map(args, func(a any, _ int) any {
		if s, ok := a.(sqlconnect.Expression); ok { // unwrap sqlconnect.Expression
			return s.GoquExpression()
		}
		return a
	})
	var expr goqu.Expression
	switch op.Operator(strings.ToLower(operator)) {
	case op.Eq:
		if len(args) != 1 {
			return "", fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).Eq(args[0])
	case op.Neq:
		if len(args) != 1 {
			return "", fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).Neq(args[0])
	case op.In:
		if len(args) == 0 {
			return "", fmt.Errorf("%s operator requires at least one argument", operator)
		}
		expr = goqu.C(identifier).In(args...)
	case op.Nin:
		if len(args) == 0 {
			return "", fmt.Errorf("%s operator requires at least one argument", operator)
		}
		expr = goqu.C(identifier).NotIn(args...)
	case op.Like:
		if len(args) != 1 {
			return "", fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).Like(args[0])
	case op.NLike:
		if len(args) != 1 {
			return "", fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).NotLike(args[0])
	case op.Nnull:
		if len(args) != 0 {
			return "", fmt.Errorf("%s operator requires no arguments, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).IsNotNull()
	case op.Null:
		if len(args) != 0 {
			return "", fmt.Errorf("%s operator requires no arguments, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).IsNull()
	case op.Gt:
		if len(args) != 1 {
			return "", fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).Gt(args[0])
	case op.Gte:
		if len(args) != 1 {
			return "", fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).Gte(args[0])
	case op.Lt:
		if len(args) != 1 {
			return "", fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).Lt(args[0])
	case op.Lte:
		if len(args) != 1 {
			return "", fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).Lte(args[0])
	case op.Btw:
		if len(args) != 2 {
			return "", fmt.Errorf("%s operator requires exactly two arguments, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).Between(exp.NewRangeVal(args[0], args[1]))
	case op.Nbtw:
		if len(args) != 2 {
			return "", fmt.Errorf("%s operator requires exactly two arguments, got %d", operator, len(args))
		}
		expr = goqu.C(identifier).NotBetween(exp.NewRangeVal(args[0], args[1]))
	case op.Inlast:
		if len(args) != 2 {
			return "", fmt.Errorf("%s operator requires exactly two arguments, got %d", operator, len(args))
		}
		var (
			interval int
			unit     string
			ok       bool
		)
		if interval, ok = args[0].(int); !ok {
			return "", fmt.Errorf("nbfinterval operator requires first argument to be an integer")
		}
		if unit, ok = args[1].(string); !ok {
			return "", fmt.Errorf("nbfinterval operator requires second argument to be a string")
		}
		dateAddExpr, err := gq.DateAdd("CURRENT_DATE", -interval, unit)
		if err != nil {
			return "", err
		}
		expr = goqu.C(identifier).Gte(dateAddExpr.GoquExpression())
	default:
		return "", fmt.Errorf("unsupported operator: %s", operator)
	}

	return gq.GoquExpressionToSQL(expr)
}

func (gq *GoquDialect) GoquExpressionToSQL(expression sqlconnect.GoquExpression) (sql string, err error) {
	sql, _, err = sqlgen.GenerateExpressionSQL(gq.esg, false, expression)
	return
}

func (gq *GoquDialect) Expressions() sqlconnect.Expressions {
	return gq
}

func (gq *GoquDialect) TimestampAdd(timeValue any, interval int, unit string) (sqlconnect.Expression, error) {
	switch unit {
	case "second", "minute", "hour", "day", "month", "year":
	case "week":
		unit = "day"
		interval *= 7
	default:
		return nil, fmt.Errorf("unsupported unit: %s", unit)
	}

	var v any
	switch timeValue := timeValue.(type) {
	case string:
		v = goqu.L(timeValue)
	default:
		v = timeValue
	}

	goquExpression := gq.expressions.TimestampAdd(v, interval, unit)
	sql, _, err := sqlgen.GenerateExpressionSQL(gq.esg, false, goquExpression)
	return &expression{Expression: goquExpression, sql: sql}, err
}

func (gq *GoquDialect) DateAdd(timeValue any, interval int, unit string) (sqlconnect.Expression, error) {
	switch unit {
	case "day", "month", "year":
	case "week":
		unit = "day"
		interval *= 7
	default:
		return nil, fmt.Errorf("unsupported unit: %s", unit)
	}

	var v any
	switch timeValue := timeValue.(type) {
	case string:
		v = goqu.L(timeValue)
	default:
		v = timeValue
	}

	goquExpression := gq.expressions.DateAdd(v, interval, unit)
	sql, _, err := sqlgen.GenerateExpressionSQL(gq.esg, false, goquExpression)
	return &expression{Expression: goquExpression, sql: sql}, err
}

type expression struct {
	goqu.Expression
	sql string
}

func (e *expression) GoquExpression() goqu.Expression {
	return e.Expression
}

func (e *expression) String() string {
	return e.sql
}

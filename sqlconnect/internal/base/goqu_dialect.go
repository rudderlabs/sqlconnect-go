package base

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/exp"
	"github.com/rudderlabs/goqu/v10/sqlgen"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
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
		if s, ok := a.(sqlconnect.Expression); ok {
			return s.GoquExpression()
		}
		return a
	})
	var expr goqu.Expression
	switch operator {
	case "eq":
		if len(args) != 1 {
			return "", fmt.Errorf("eq operator requires exactly one argument, got %d", len(args))
		}
		expr = goqu.C(identifier).Eq(args[0])
	case "neq":
		if len(args) != 1 {
			return "", fmt.Errorf("neq operator requires exactly one argument, got %d", len(args))
		}
		expr = goqu.C(identifier).Neq(args[0])
	case "in":
		if len(args) == 0 {
			return "", fmt.Errorf("in operator requires at least one argument")
		}
		expr = goqu.C(identifier).In(args...)
	case "notin":
		if len(args) == 0 {
			return "", fmt.Errorf("notin operator requires at least one argument")
		}
		expr = goqu.C(identifier).NotIn(args...)
	case "like":
		if len(args) != 1 {
			return "", fmt.Errorf("like operator requires exactly one argument, got %d", len(args))
		}
		expr = goqu.C(identifier).Like(args[0])
	case "notlike":
		if len(args) != 1 {
			return "", fmt.Errorf("notlike operator requires exactly one argument, got %d", len(args))
		}
		expr = goqu.C(identifier).NotLike(args[0])
	case "isset":
		if len(args) != 0 {
			return "", fmt.Errorf("isset operator requires no arguments, got %d", len(args))
		}
		expr = goqu.C(identifier).IsNotNull()
	case "notset":
		if len(args) != 0 {
			return "", fmt.Errorf("isnotset operator requires no arguments, got %d", len(args))
		}
		expr = goqu.C(identifier).IsNull()
	case "gt":
		if len(args) != 1 {
			return "", fmt.Errorf("gt operator requires exactly one argument, got %d", len(args))
		}
		expr = goqu.C(identifier).Gt(args[0])
	case "gte":
		if len(args) != 1 {
			return "", fmt.Errorf("gte operator requires exactly one argument, got %d", len(args))
		}
		expr = goqu.C(identifier).Gte(args[0])
	case "lt":
		if len(args) != 1 {
			return "", fmt.Errorf("lt operator requires exactly one argument, got %d", len(args))
		}
		expr = goqu.C(identifier).Lt(args[0])
	case "lte":
		if len(args) != 1 {
			return "", fmt.Errorf("lte operator requires exactly one argument, got %d", len(args))
		}
		expr = goqu.C(identifier).Lte(args[0])
	case "between":
		if len(args) != 2 {
			return "", fmt.Errorf("between operator requires exactly two arguments, got %d", len(args))
		}
		expr = goqu.C(identifier).Between(exp.NewRangeVal(args[0], args[1]))
	case "notbetween":
		if len(args) != 2 {
			return "", fmt.Errorf("notbetween operator requires exactly two arguments, got %d", len(args))
		}
		expr = goqu.C(identifier).NotBetween(exp.NewRangeVal(args[0], args[1]))
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

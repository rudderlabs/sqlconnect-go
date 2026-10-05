package fabric

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/exp"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/op"
)

func newDialect() sqlconnect.Dialect {
	return dialect{base.NewGoquDialectWithSQLPostProcessor(DatabaseType, fabricExpressionDialectOptions(), GoquExpressions(), replaceFabricGoquIdentifierQuotes)}
}

type dialect struct {
	*base.GoquDialect
}

func (d dialect) QueryCondition(identifier, operator string, args ...any) (sqlconnect.Expression, error) {
	for i, arg := range args {
		if expression, ok := arg.(sqlconnect.Expression); ok {
			args[i] = expression.GoquExpression()
		}
	}

	lhs := fabricConditionIdentifier(identifier)
	var goquExpression goqu.Expression
	switch op.Operator(strings.ToLower(operator)) {
	case op.Eq:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		goquExpression = lhs.Eq(args[0])
	case op.Neq:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		goquExpression = lhs.Neq(args[0])
	case op.In:
		if len(args) == 0 {
			return nil, fmt.Errorf("%s operator requires at least one argument", operator)
		}
		goquExpression = lhs.In(args...)
	case op.Nin:
		if len(args) == 0 {
			return nil, fmt.Errorf("%s operator requires at least one argument", operator)
		}
		goquExpression = lhs.NotIn(args...)
	case op.Like:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		goquExpression = lhs.Like(args[0])
	case op.NLike:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		goquExpression = lhs.NotLike(args[0])
	case op.Nnull:
		if len(args) != 0 {
			return nil, fmt.Errorf("%s operator requires no arguments, got %d", operator, len(args))
		}
		goquExpression = lhs.IsNotNull()
	case op.Null:
		if len(args) != 0 {
			return nil, fmt.Errorf("%s operator requires no arguments, got %d", operator, len(args))
		}
		goquExpression = lhs.IsNull()
	case op.Gt:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		goquExpression = lhs.Gt(args[0])
	case op.Gte:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		goquExpression = lhs.Gte(args[0])
	case op.Lt:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		goquExpression = lhs.Lt(args[0])
	case op.Lte:
		if len(args) != 1 {
			return nil, fmt.Errorf("%s operator requires exactly one argument, got %d", operator, len(args))
		}
		goquExpression = lhs.Lte(args[0])
	case op.Btw:
		if len(args) != 2 {
			return nil, fmt.Errorf("%s operator requires exactly two arguments, got %d", operator, len(args))
		}
		goquExpression = lhs.Between(exp.NewRangeVal(args[0], args[1]))
	case op.Nbtw:
		if len(args) != 2 {
			return nil, fmt.Errorf("%s operator requires exactly two arguments, got %d", operator, len(args))
		}
		goquExpression = lhs.NotBetween(exp.NewRangeVal(args[0], args[1]))
	case op.Inlast:
		if len(args) != 2 {
			return nil, fmt.Errorf("%s operator requires exactly two arguments, got %d", operator, len(args))
		}
		interval, ok := args[0].(int)
		if !ok {
			return nil, fmt.Errorf("nbfinterval operator requires first argument to be an integer")
		}
		unit, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("nbfinterval operator requires second argument to be a string")
		}
		dateAddExpr, err := d.DateAdd("CURRENT_DATE", -interval, unit)
		if err != nil {
			return nil, err
		}
		goquExpression = lhs.Gte(dateAddExpr.GoquExpression())
	default:
		return nil, fmt.Errorf("unsupported operator: %s", operator)
	}
	return d.ParseGoquExpression(goquExpression)
}

type fabricConditionExpression interface {
	exp.Expression
	exp.Comparable
	exp.Inable
	exp.Likeable
	exp.Isable
	exp.Rangeable
}

func fabricConditionIdentifier(identifier string) fabricConditionExpression {
	if strings.ContainsAny(identifier, "()") {
		return goqu.L(identifier)
	}
	return goqu.C(identifier)
}

func (d dialect) QuoteTable(table sqlconnect.RelationRef) string {
	if table.Catalog != "" && table.Schema != "" {
		return d.QuoteIdentifier(table.Catalog) + "." + d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	if table.Schema != "" {
		return d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	return d.QuoteIdentifier(table.Name)
}

func (dialect) QuoteIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

// Fabric identifiers retain their case because Fabric's default collation is case-sensitive.
func (dialect) FormatTableName(name string) string { return name }

func (dialect) NormaliseIdentifier(identifier string) string {
	return base.NormaliseIdentifierWithDelimiters(identifier, '[', ']', func(value string) string { return value })
}

func (dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return base.ParseRelationRefWithDelimiters(identifier, '[', ']', func(value string) string { return value })
}

func init() {
	sqlconnect.RegisterDialectFactory(DatabaseType, func(_ json.RawMessage) (sqlconnect.Dialect, error) {
		return newDialect(), nil
	})
}

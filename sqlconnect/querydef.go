package sqlconnect

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
)

// QueryDef describes a query that consists of a table and columns that should be queried.
type QueryDef struct {
	Table      RelationRef       `json:"table"`                // Reference to table that should be queried
	Columns    []string          `json:"columns,omitempty"`    // Columns that should be included. Defaults to "*" if nil or empty.
	Conditions []*QueryCondition `json:"conditions,omitempty"` // Conditions is a list of query conditions.
	OrderBy    *QueryOrder       `json:"order_by,omitempty"`   // OrderBy defines the query's order by clause.
}

// QueryCondition defines a query condition.
type QueryCondition struct {
	Column   string `json:"column,omitempty"`
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
}

// QueryOrder defines the query's order by clause.This only supports one order by column.
type QueryOrder struct {
	Column string // the order by column
	Order  string // supported values are ('ASC', 'DESC')
}

func (query *QueryDef) ToSQL(d Dialect) string {
	var cols string
	if len(query.Columns) == 0 {
		cols = "*"
	} else {
		for i, column := range query.Columns {
			cols += d.QuoteIdentifier(column)
			if i < len(query.Columns)-1 {
				cols += ","
			}
		}
	}
	// create data query
	sql := fmt.Sprintf("SELECT %s FROM %s", cols, d.QuoteTable(query.Table))
	// add condition clauses
	if len(query.Conditions) > 0 {
		sql += " WHERE " + strings.Join(lo.Map(query.Conditions, func(condition *QueryCondition, _ int) string {
			return fmt.Sprintf(`%s %s %s`, d.QuoteIdentifier(condition.Column), condition.Operator, condition.Value)
		}), " AND ")
	}
	// add order by clause
	if query.OrderBy != nil {
		sql += fmt.Sprintf(` ORDER BY %s %s`, d.QuoteIdentifier(query.OrderBy.Column), query.OrderBy.Order)
	}
	return sql
}

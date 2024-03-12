package sqlconnect_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestQueryDef(t *testing.T) {
	t.Run("with columns", func(t *testing.T) {
		table := sqlconnect.NewRelationRef("table")
		q := sqlconnect.QueryDef{
			Table:   table,
			Columns: []string{"col1", "col2"},
			Conditions: []*sqlconnect.QueryCondition{
				{Column: "col1", Operator: "=", Value: "'1'"},
				{Column: "col2", Operator: ">", Value: "2"},
			},
			OrderBy: &sqlconnect.QueryOrder{
				Column: "col1",
				Order:  "ASC",
			},
		}

		sql := q.ToSQL(testDialect{})
		expected := `SELECT "col1","col2" FROM "table" WHERE "col1" = '1' AND "col2" > 2 ORDER BY "col1" ASC`
		require.Equal(t, expected, sql, "query should be formatted correctly")
	})

	t.Run("without columns", func(t *testing.T) {
		table := sqlconnect.NewRelationRef("table")
		q := sqlconnect.QueryDef{
			Table: table,
			Conditions: []*sqlconnect.QueryCondition{
				{Column: "col1", Operator: "=", Value: "'1'"},
				{Column: "col2", Operator: ">", Value: "2"},
			},
		}

		sql := q.ToSQL(testDialect{})
		expected := `SELECT * FROM "table" WHERE "col1" = '1' AND "col2" > 2`
		require.Equal(t, expected, sql, "query should be formatted correctly")
	})
}

type testDialect struct{}

func (d testDialect) FormatTableName(name string) string {
	return name
}

func (d testDialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, name)
}

func (d testDialect) QuoteTable(relation sqlconnect.RelationRef) string {
	if relation.Schema != "" {
		return fmt.Sprintf(`"%s"."%s"`, relation.Schema, relation.Name)
	}
	return fmt.Sprintf(`"%s"`, relation.Name)
}

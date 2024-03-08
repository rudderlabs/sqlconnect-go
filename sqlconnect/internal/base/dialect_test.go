package base

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestDialect(t *testing.T) {
	var d dialect
	t.Run("format table", func(t *testing.T) {
		formatted := d.FormatTableName("TaBle")
		require.Equal(t, "table", formatted, "table name should be lowercased")
	})

	t.Run("quote identifier", func(t *testing.T) {
		quoted := d.QuoteIdentifier("column")
		require.Equal(t, `"column"`, quoted, "column name should be quoted with double quotes")
	})

	t.Run("quote table", func(t *testing.T) {
		quoted := d.QuoteTable(sqlconnect.NewRelationRef("table"))
		require.Equal(t, `"table"`, quoted, "table name should be quoted with double quotes")

		quoted = d.QuoteTable(sqlconnect.NewSchemaTableRef("schema", "table"))
		require.Equal(t, `"schema"."table"`, quoted, "schema and table name should be quoted with double quotes")
	})
}

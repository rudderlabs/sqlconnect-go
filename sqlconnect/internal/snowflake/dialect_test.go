package snowflake

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestDialect(t *testing.T) {
	var d dialect
	t.Run("format table", func(t *testing.T) {
		formatted := d.FormatTableName("TaBle")
		require.Equal(t, "TABLE", formatted, "table name should be uppercased")
	})

	t.Run("quote identifier", func(t *testing.T) {
		quoted := d.QuoteIdentifier("column")
		require.Equal(t, `"column"`, quoted, "column name should be quoted with double quotes")
	})

	t.Run("quote table", func(t *testing.T) {
		quoted := d.QuoteTable(sqlconnect.NewRelationRef("table"))
		require.Equal(t, `"table"`, quoted, "table name should be quoted with double quotes")

		quoted = d.QuoteTable(sqlconnect.NewRelationRef("table", sqlconnect.WithSchema("schema")))
		require.Equal(t, `"schema"."table"`, quoted, "schema and table name should be quoted with double quotes")
	})

	t.Run("normalise identifier", func(t *testing.T) {
		normalised := d.NormaliseIdentifier("COLUMN")
		require.Equal(t, "COLUMN", normalised, "column name should be normalised to uppercase")

		normalised = d.NormaliseIdentifier("column")
		require.Equal(t, "COLUMN", normalised, "column name should be normalised to uppercase")

		normalised = d.NormaliseIdentifier(`"ColUmn"`)
		require.Equal(t, `"ColUmn"`, normalised, "quoted column name should not be normalised")

		normalised = d.NormaliseIdentifier(`TaBle."ColUmn"`)
		require.Equal(t, `TABLE."ColUmn"`, normalised, "non quoted parts should be normalised")

		normalised = d.NormaliseIdentifier(`"Sh""EmA".TABLE."ColUmn"`)
		require.Equal(t, `"Sh""EmA".TABLE."ColUmn"`, normalised, "non quoted parts should be normalised")
	})

	t.Run("parse relation", func(t *testing.T) {
		parsed, err := d.ParseRelationRef("TABLE")
		require.NoError(t, err)
		require.Equal(t, sqlconnect.RelationRef{Name: "TABLE"}, parsed)

		parsed, err = d.ParseRelationRef("table")
		require.NoError(t, err)
		require.Equal(t, sqlconnect.RelationRef{Name: "TABLE"}, parsed)

		parsed, err = d.ParseRelationRef(`"TaBle"`)
		require.NoError(t, err)
		require.Equal(t, sqlconnect.RelationRef{Name: `TaBle`}, parsed)

		parsed, err = d.ParseRelationRef(`ScHeMA."TaBle"`)
		require.NoError(t, err)
		require.Equal(t, sqlconnect.RelationRef{Schema: "SCHEMA", Name: "TaBle"}, parsed)

		parsed, err = d.ParseRelationRef(`"CaTa""LoG".ScHeMA."TaBle"`)
		require.NoError(t, err)
		require.Equal(t, sqlconnect.RelationRef{Catalog: "CaTa\"LoG", Schema: "SCHEMA", Name: "TaBle"}, parsed)
	})
}

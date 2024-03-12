package sqlconnect_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestRelationRef(t *testing.T) {
	t.Run("name", func(t *testing.T) {
		ref := sqlconnect.NewRelationRef("table")
		require.Equal(t, sqlconnect.RelationRef{Name: "table", Type: "table"}, ref)
		require.Equal(t, "table", ref.String())

		refJSON, _ := json.Marshal(ref)
		var ref1 sqlconnect.RelationRef
		err := ref1.UnmarshalJSON(refJSON)
		require.NoError(t, err)
		require.Equal(t, ref, ref1)
	})

	t.Run("name and schema", func(t *testing.T) {
		ref := sqlconnect.NewRelationRef("table", sqlconnect.WithSchema("schema"))
		require.Equal(t, sqlconnect.RelationRef{Name: "table", Schema: "schema", Type: "table"}, ref)
		require.Equal(t, "schema.table", ref.String())

		refJSON, _ := json.Marshal(ref)
		var ref1 sqlconnect.RelationRef
		err := ref1.UnmarshalJSON(refJSON)
		require.NoError(t, err)
		require.Equal(t, ref, ref1)
	})

	t.Run("name and schema and catalog", func(t *testing.T) {
		ref := sqlconnect.NewRelationRef("table", sqlconnect.WithSchema("schema"), sqlconnect.WithCatalog("catalog"))
		require.Equal(t, sqlconnect.RelationRef{Name: "table", Schema: "schema", Catalog: "catalog", Type: "table"}, ref)
		require.Equal(t, "catalog.schema.table", ref.String())

		refJSON, _ := json.Marshal(ref)
		var ref1 sqlconnect.RelationRef
		err := ref1.UnmarshalJSON(refJSON)
		require.NoError(t, err)
		require.Equal(t, ref, ref1)
	})

	t.Run("view instead of table", func(t *testing.T) {
		ref := sqlconnect.NewRelationRef("view", sqlconnect.WithRelationType(sqlconnect.ViewRelation))
		require.Equal(t, sqlconnect.RelationRef{Name: "view", Type: "view"}, ref)
	})

	t.Run("unmarshal without a type", func(t *testing.T) {
		var ref sqlconnect.RelationRef
		err := ref.UnmarshalJSON([]byte(`{"name":"table"}`))
		require.NoError(t, err)
		require.Equal(t, sqlconnect.NewRelationRef("table"), ref)
	})
}

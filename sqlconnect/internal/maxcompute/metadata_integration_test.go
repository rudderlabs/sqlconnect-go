package maxcompute_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestIntegrationMetadata(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	schema := testSchema(t, db, "meta")
	ref := func(name string) sqlconnect.RelationRef { return sqlconnect.NewSchemaTableRef(schema, name) }

	for _, stmt := range []string{
		"CREATE TABLE " + db.QuoteTable(ref("snap_a")) + " (id BIGINT, name STRING, amount DECIMAL(22,6)) LIFECYCLE 1",
		"CREATE TABLE " + db.QuoteTable(ref("snapxb")) + " (id BIGINT) LIFECYCLE 1",
		"CREATE TABLE " + db.QuoteTable(ref("parted")) + " (id BIGINT) PARTITIONED BY (ds STRING) LIFECYCLE 1",
		"CREATE VIEW " + db.QuoteTable(ref("snap_view")) + " AS SELECT id FROM " + db.QuoteTable(ref("snap_a")),
	} {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	t.Run("schemas", func(t *testing.T) {
		exists, err := db.SchemaExists(ctx, sqlconnect.SchemaRef{Name: schema})
		require.NoError(t, err)
		require.True(t, exists)
		exists, err = db.SchemaExists(ctx, sqlconnect.SchemaRef{Name: objectName("missing")})
		require.NoError(t, err)
		require.False(t, exists)
		schemas, err := db.ListSchemas(ctx)
		require.NoError(t, err)
		require.Contains(t, schemas, sqlconnect.SchemaRef{Name: schema})
	})

	t.Run("tables, views and a literal prefix", func(t *testing.T) {
		tables, err := db.ListTables(ctx, sqlconnect.SchemaRef{Name: schema})
		require.NoError(t, err)
		require.ElementsMatch(t, []sqlconnect.RelationRef{
			ref("snap_a"), ref("snapxb"), ref("parted"),
			sqlconnect.NewRelationRef("snap_view", sqlconnect.WithSchema(schema), sqlconnect.WithRelationType(sqlconnect.ViewRelation)),
		}, tables)

		tables, err = db.ListTables(ctx, sqlconnect.SchemaRef{Name: schema}, sqlconnect.WithPrefix("snap_"))
		require.NoError(t, err)
		require.ElementsMatch(t, []sqlconnect.RelationRef{
			ref("snap_a"),
			sqlconnect.NewRelationRef("snap_view", sqlconnect.WithSchema(schema), sqlconnect.WithRelationType(sqlconnect.ViewRelation)),
		}, tables, "_ is not a wildcard")

		_, err = db.ListTables(ctx, sqlconnect.SchemaRef{Name: objectName("missing")})
		require.ErrorContains(t, err, "not found")
	})

	t.Run("exists and columns", func(t *testing.T) {
		exists, err := db.TableExists(ctx, ref("snap_a"))
		require.NoError(t, err)
		require.True(t, exists)
		exists, err = db.TableExists(ctx, ref("missing"))
		require.NoError(t, err)
		require.False(t, exists)

		cols, err := db.ListColumns(ctx, ref("snap_a"))
		require.NoError(t, err)
		require.Equal(t, []sqlconnect.ColumnRef{
			{Name: "id", Type: "int", RawType: "BIGINT"},
			{Name: "name", Type: "string", RawType: "STRING"},
			{Name: "amount", Type: "float", RawType: "DECIMAL(22,6)"},
		}, cols)

		cols, err = db.ListColumns(ctx, ref("parted"))
		require.NoError(t, err)
		require.Equal(t, []sqlconnect.ColumnRef{
			{Name: "id", Type: "int", RawType: "BIGINT"},
			{Name: "ds", Type: "string", RawType: "STRING"},
		}, cols, "partition columns are listed")

		_, err = db.ListColumns(ctx, ref("missing"))
		require.ErrorContains(t, err, "not found")

		cols, err = db.ListColumnsForSqlQuery(ctx, "SELECT id, amount * 2 AS doubled FROM "+db.QuoteTable(ref("snap_a")))
		require.NoError(t, err)
		require.Equal(t, []sqlconnect.ColumnRef{
			{Name: "id", Type: "int", RawType: "BIGINT"},
			{Name: "doubled", Type: "float", RawType: "DECIMAL(24,6)"},
		}, cols)
	})

	t.Run("rename and count", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "INSERT INTO "+db.QuoteTable(ref("snapxb"))+" VALUES (1), (2)")
		require.NoError(t, err)
		n, err := db.CountTableRows(ctx, ref("snapxb"))
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.NoError(t, db.RenameTable(ctx, ref("snapxb"), ref("renamed")))
		exists, err := db.TableExists(ctx, ref("renamed"))
		require.NoError(t, err)
		require.True(t, exists)
	})
}

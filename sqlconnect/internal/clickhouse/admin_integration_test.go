package clickhouse_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestSQ9_Metadata(t *testing.T) {
	db := openFloor(t)
	ctx := context.Background()
	schema := seedSchema(t, db, `CREATE TABLE {{.schema}}.a_1 (z String, a UInt8, m Nullable(DateTime64(3))) ENGINE = MergeTree ORDER BY a;
		CREATE TABLE {{.schema}}.ax1 (k UInt8) ENGINE = MergeTree ORDER BY k;
		CREATE TABLE {{.schema}}."a%1" (k UInt8) ENGINE = MergeTree ORDER BY k;
		CREATE TABLE {{.schema}}.ab1 (k UInt8) ENGINE = MergeTree ORDER BY k;
		CREATE VIEW {{.schema}}.v AS SELECT 1 AS k`)
	for prefix, want := range map[string]int{"a_": 1, "a%": 1, "": 5} { // '_' and '%' are literal: ax1 and ab1 never match
		tables, err := db.ListTables(ctx, schema, sqlconnect.WithPrefix(prefix))
		require.NoError(t, err)
		require.Len(t, tables, want, prefix)
		if want == 1 {
			require.Equal(t, strings.TrimSuffix(prefix, "")+"1", tables[0].Name)
		}
	}
	all, _ := db.ListTables(ctx, schema)
	require.Contains(t, all, sqlconnect.NewRelationRef("v", sqlconnect.WithSchema(schema.Name), sqlconnect.WithRelationType(sqlconnect.ViewRelation)))
	cols, _ := db.ListColumns(ctx, sqlconnect.NewRelationRef("a_1", sqlconnect.WithSchema(schema.Name)))
	require.Equal(t, []sqlconnect.ColumnRef{
		{Name: "z", Type: "string", RawType: "String"},
		{Name: "a", Type: "int", RawType: "UInt8"},
		{Name: "m", Type: "datetime", RawType: "Nullable(DateTime64(3))"},
	}, cols)
	_, err := db.ListColumns(ctx, sqlconnect.NewRelationRef("missing", sqlconnect.WithSchema(schema.Name)))
	requireCode(t, err, "CH_OBJECT_NOT_FOUND")
}

func TestSQ7_Catalogs(t *testing.T) {
	db := openFloor(t)
	ctx := context.Background()
	_, e1 := db.CurrentCatalog(ctx)
	_, e2 := db.ListCatalogs(ctx)
	require.ErrorIs(t, e1, sqlconnect.ErrNotSupported)
	require.ErrorIs(t, e2, sqlconnect.ErrNotSupported)
	s, err := db.ListSchemas(ctx, sqlconnect.WithCatalog(""))
	require.NoError(t, err)
	require.NotEmpty(t, s)
	s, errS := db.ListSchemas(ctx, sqlconnect.WithCatalog("c"))
	tables, errT := db.ListTables(ctx, sqlconnect.SchemaRef{Name: "default"}, sqlconnect.WithCatalog("c"))
	exists, errE := db.SchemaExists(ctx, sqlconnect.SchemaRef{Name: "default"}, sqlconnect.WithCatalog("c"))
	require.NoError(t, errors.Join(errS, errT, errE))
	require.Empty(t, s, "a non-empty discovery filter returns an empty result")
	require.Empty(t, tables)
	require.False(t, exists)
	withCat := sqlconnect.RelationRef{Catalog: "c", Schema: "db", Name: "t"}
	_, errExists := db.TableExists(ctx, withCat)
	_, errCols := db.ListColumns(ctx, withCat)
	_, errCount := db.CountTableRows(ctx, withCat)
	for _, err := range []error{
		db.CreateTestTable(ctx, withCat), db.DropTable(ctx, withCat), db.TruncateTable(ctx, withCat),
		db.RenameTable(ctx, withCat, withCat), errExists, errCols, errCount,
	} {
		requireCode(t, err, "CH_CATALOG_UNSUPPORTED")
		require.ErrorIs(t, err, sqlconnect.ErrNotSupported)
	}
}

func TestSQ8_RoundTrip(t *testing.T) {
	db := openFloor(t)
	ctx := context.Background()
	schema := sqlconnect.SchemaRef{Name: "rt_" + strings.ToLower(rand.String(6))}
	require.NoError(t, db.CreateSchema(ctx, schema))
	for _, name := range []string{"MixedCase", "with space", "Ünïcödé", "dot.ted", "apos'trophe", `dou"ble`, "back`tick", `back\slash`} {
		ref := sqlconnect.NewRelationRef(name, sqlconnect.WithSchema(schema.Name))
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s UInt8) ENGINE = MergeTree ORDER BY tuple()", db.QuoteTable(ref), db.QuoteIdentifier(name)))
		require.NoError(t, err, name)
		exists, _ := db.TableExists(ctx, ref)
		cols, _ := db.ListColumns(ctx, ref)
		parsed, _ := db.ParseRelationRef(db.QuoteTable(ref))
		require.Equal(t, []any{true, name, name}, []any{exists, cols[0].Name, parsed.Name}, "byte-equal round trip of %q", name)
	}
}

func TestCP39_CaseThroughAdapter(t *testing.T) {
	db := openFloor(t)
	ctx := context.Background()
	schema := "cp39_" + strings.ToLower(rand.String(6))
	require.NoError(t, db.CreateSchema(ctx, sqlconnect.SchemaRef{Name: schema}))
	ref, err := db.ParseRelationRef(db.QuoteIdentifier(schema) + "." + db.QuoteIdentifier("Case_T"))
	require.NoError(t, err)
	create := fmt.Sprintf("CREATE TABLE %s (%s String, %s String) ENGINE = MergeTree ORDER BY %s",
		db.QuoteTable(ref), db.QuoteIdentifier("ID"), db.QuoteIdentifier("id"), db.QuoteIdentifier("ID"))
	require.Contains(t, create, "`ID` String, `id` String", "generated SQL keeps both identifiers")
	for _, q := range []string{create, "INSERT INTO " + db.QuoteTable(ref) + " VALUES ('A','Alice'),('a','alice')"} {
		_, err = db.ExecContext(ctx, q)
		require.NoError(t, err)
	}
	cols, err := db.ListColumns(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, []string{"ID", "id"}, []string{cols[0].Name, cols[1].Name})
	cond, _ := db.QueryCondition("ID", "eq", "A")
	n, err := db.GetRowCountForQuery(ctx, "SELECT count() FROM "+db.QuoteTable(ref)+" WHERE "+cond.String())
	require.Equal(t, []any{1, nil}, []any{n, err}, "keys 'A' and 'a' stay distinct")
	var same uint8
	require.NoError(t, db.QueryRowContext(ctx, "SELECT sipHash128Reference(tuple('Alice')) = sipHash128Reference(tuple('alice'))").Scan(&same))
	require.Zero(t, same, "a case-only payload change flips the hash")
}

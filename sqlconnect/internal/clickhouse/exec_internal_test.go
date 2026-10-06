package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chctx"
)

func TestResolve(t *testing.T) {
	db := mustDBInternal(t)
	got, err := db.resolve(sqlconnect.RelationRef{Name: "t"})
	require.NoError(t, err)
	require.Equal(t, "analytics", got.Schema, "an omitted schema is the configured database")
	for _, ref := range []sqlconnect.RelationRef{{Name: ""}, {Schema: "db", Name: "a\x00b"}, {Schema: "d\x00b", Name: "t"}} {
		_, err := db.resolve(ref)
		requireCode(t, err, "CH_INVALID_REFERENCE")
	}
	_, err = db.resolveSchema(sqlconnect.SchemaRef{Name: "x\x00"})
	requireCode(t, err, "CH_INVALID_REFERENCE")
}

// The refusals come before any request: the host never resolves under the
// strict policy, so a sent statement would fail with another code.
func TestAdminRefusesBadReferencesBeforeSQL(t *testing.T) {
	db := mustDBInternal(t)
	ctx := context.Background()
	bad := sqlconnect.RelationRef{Schema: "db", Name: "a\x00b"}
	_, errExists := db.TableExists(ctx, bad)
	_, errCols := db.ListColumns(ctx, bad)
	_, errCount := db.CountTableRows(ctx, bad)
	for _, err := range []error{
		db.CreateTestTable(ctx, bad), db.DropTable(ctx, bad), db.TruncateTable(ctx, bad), db.RenameTable(ctx, bad, bad),
		db.CreateSchema(ctx, sqlconnect.SchemaRef{Name: "x\x00"}), db.DropSchema(ctx, sqlconnect.SchemaRef{Name: "x\x00"}),
		errExists, errCols, errCount,
	} {
		requireCode(t, err, "CH_INVALID_REFERENCE")
	}
	err := db.RenameTable(ctx, sqlconnect.RelationRef{Schema: "a", Name: "t"}, sqlconnect.RelationRef{Schema: "b", Name: "t"})
	requireCode(t, err, "CH_CROSS_DATABASE_MOVE_UNSUPPORTED")
	require.ErrorIs(t, err, sqlconnect.ErrNotSupported)
}

func TestCountToInt(t *testing.T) {
	n, err := countToInt(math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, math.MaxInt, n)
	_, err = countToInt(uint64(math.MaxInt) + 1)
	requireCode(t, err, "CH_COUNT_OVERFLOW")
}

func TestReadCtx(t *testing.T) {
	db := mustDBInternal(t)
	marked := clickhousequery.WithStatement(context.Background(), map[string]any{"max_threads": 3}, clickhousequery.NewQueryID())
	require.Equal(t, marked, db.readCtx(marked), "a caller map stays")
	require.True(t, chctx.Has(db.readCtx(context.Background())), "an unmarked context gets the read map")
}

func TestOptionErrorsHideCallerValues(t *testing.T) {
	db := mustDBInternal(t)
	ctx := context.Background()
	const secret = "customer-secret"
	_, e1 := db.ListSchemas(ctx, sqlconnect.WithSchema(secret))
	_, e2 := db.SchemaExists(ctx, sqlconnect.SchemaRef{Name: "x"}, sqlconnect.WithSchema(secret))
	_, e3 := db.ListTables(ctx, sqlconnect.SchemaRef{Name: "x"}, sqlconnect.WithSchema(secret))
	_, e4 := db.ListSchemas(ctx, sqlconnect.WithRelationType(sqlconnect.RelationType(secret)))
	for _, err := range []error{e1, e2, e3, e4} {
		requireCode(t, err, "CH_INVALID_REFERENCE")
		require.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), secret)
		require.NoError(t, errors.Unwrap(err), "no cause is kept")
	}
}

// An empty schema never defaults to the configured database in schema DDL:
// DropSchema(SchemaRef{}) must not drop the customer database.
func TestSchemaDDLRefusesEmptyName(t *testing.T) {
	db := mustDBInternal(t)
	ctx := context.Background()
	requireCode(t, db.CreateSchema(ctx, sqlconnect.SchemaRef{}), "CH_INVALID_REFERENCE")
	requireCode(t, db.DropSchema(ctx, sqlconnect.SchemaRef{}), "CH_INVALID_REFERENCE")
}

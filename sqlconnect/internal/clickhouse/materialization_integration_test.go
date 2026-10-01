package clickhouse_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

func TestSQ11_DDLLifecycleExplicitEngine(t *testing.T) {
	srv, db := openFloorWithServer(t)
	ctx := context.Background()
	schema := seedSchema(t, db, `CREATE TABLE {{.schema}}.src (email String, id UInt64) ENGINE = MergeTree ORDER BY id;
		INSERT INTO {{.schema}}.src VALUES ('a@x', 2), ('b@x', 1)`)
	dst, moved := sqlconnect.NewRelationRef("dst", sqlconnect.WithSchema(schema.Name)), sqlconnect.NewRelationRef("moved", sqlconnect.WithSchema(schema.Name))
	require.NoError(t, db.CreateTableFromQuery(ctx, dst, "SELECT email, id FROM "+schema.Name+".src;"))
	require.Regexp(t, `ENGINE = MergeTree(.|\n)*ORDER BY email`, srv.Show(t, schema.Name+".dst"), "legacy create: first eligible column")
	require.NoError(t, db.MoveTable(ctx, dst, moved))
	require.Regexp(t, `ENGINE = MergeTree(.|\n)*ORDER BY`, srv.Show(t, schema.Name+".moved"))
	n, err := db.CountTableRows(ctx, moved)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	exists, _ := db.TableExists(ctx, dst)
	require.False(t, exists)
}

func TestCP26_HostileDefaultEngine(t *testing.T) {
	srv, _ := openFloorWithServer(t) // the ReplicatedMergeTree arm needs Keeper; it runs on Cloud (Task 23)
	user := srv.CreateUserWithProfile(t, map[string]string{"default_table_engine": "Log"})
	srv.AdminExec(t, "CREATE DATABASE IF NOT EXISTS cp26")
	srv.AdminExec(t, "GRANT CREATE TABLE, INSERT, SELECT, DROP TABLE ON cp26.* TO "+user.Name)
	srv.QueryAs(t, user, "CREATE TABLE cp26.free AS SELECT 1 AS a")
	require.Contains(t, srv.Show(t, "cp26.free"), "ENGINE = Log", "negative control")
	db := openAs(t, srv, user)
	_, err := db.CreateTableForQueryWithOptions(context.Background(), scopedExec(t, db), sqlconnect.NewRelationRef("ours", sqlconnect.WithSchema("cp26")),
		"SELECT toUInt64(1) AS id", sqlconnect.MaterializationOptions{SortingKey: []string{"id"}})
	require.NoError(t, err)
	require.Regexp(t, `ENGINE = MergeTree(.|\n)*ORDER BY id`, srv.Show(t, "cp26.ours"))
}

func TestSQ_MaterializationOptions(t *testing.T) {
	srv, db := openFloorWithServer(t)
	ctx := context.Background()
	schema := mkSchema(t, db, "mo")
	ref := sqlconnect.NewRelationRef("snap", sqlconnect.WithSchema(schema.Name))
	ddl, dml := recordingExec(t, db), recordingExec(t, db)
	q := "SELECT email, id FROM (SELECT 'a' AS email, toUInt64(1) AS id)"
	o := sqlconnect.MaterializationOptions{SortingKey: []string{"id"}, Columns: []sqlconnect.ColumnRef{
		{Name: "email", RawType: "Nullable(String)"}, {Name: "id", RawType: "UInt64"},
	}}
	uuid, err := db.CreateTableForQueryWithOptions(ctx, ddl, ref, q, o)
	require.NoError(t, err)
	require.Equal(t, srv.AdminQuery(t, "SELECT toString(uuid) FROM system.tables WHERE database='"+schema.Name+"' AND name='snap'")[0][0], uuid)
	n, _ := db.CountTableRows(ctx, ref)
	require.Zero(t, n, "Create runs no INSERT")
	require.NoError(t, db.InsertFromQueryWithOptions(ctx, dml, ref, q, o))
	srv.FlushLogs(t)
	require.Len(t, dml.stmts, 2)
	require.True(t, strings.HasPrefix(dml.stmts[0].SQL, "DESCRIBE ("), "projection check first")
	require.True(t, strings.HasPrefix(dml.stmts[1].SQL, "INSERT INTO "))
	require.Equal(t, "Insert", srv.QueryLogKind(t, dml.stmts[1].ID), "the INSERT ran on the second executor under its id")
	for _, st := range ddl.stmts {
		require.False(t, strings.HasPrefix(st.SQL, "DESCRIBE"), "CREATE with explicit Columns runs no metadata read")
	}
	require.Error(t, db.InsertFromQueryWithOptions(ctx, dml, ref, "SELECT 'x' AS email, CAST(NULL AS Nullable(UInt64)) AS id", o), "a non-nullable key rejects NULL")
	require.False(t, ddl.closed || dml.closed, "the driver never closes the executor")
	lax := settingsExec{conn: scopedExec(t, db), settings: map[string]any{"insert_null_as_default": 1}}
	require.Error(t, db.InsertFromQueryWithOptions(ctx, lax, ref, "SELECT 'x' AS email, CAST(NULL AS Nullable(UInt64)) AS id", o),
		"a caller map that allows NULL as default still cannot turn the NULL into 0")
	n, _ = db.CountTableRows(ctx, ref)
	require.Equal(t, 1, n, "only the first INSERT wrote")
}

// settingsExec runs every statement on conn under the caller map settings
// and a fresh query id, as a caller with its own statement map does.
type settingsExec struct {
	conn     *sql.Conn
	settings map[string]any
}

func (e settingsExec) stmt(ctx context.Context) context.Context {
	return stmtCtx(ctx, e.settings, clickhousequery.NewQueryID())
}

func (e settingsExec) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return e.conn.ExecContext(e.stmt(ctx), q, args...)
}

func (e settingsExec) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return e.conn.QueryContext(e.stmt(ctx), q, args...)
}

func (e settingsExec) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return e.conn.QueryRowContext(e.stmt(ctx), q, args...)
}

func TestSQ12_CreateFailureAndMoveSentinel(t *testing.T) {
	srv, db := openFloorWithServer(t)
	ctx := context.Background()
	schema := seedSchema(t, db, `CREATE TABLE {{.schema}}.old (id UInt64) ENGINE = MergeTree ORDER BY id; INSERT INTO {{.schema}}.old VALUES (1)`)
	ref := func(n string) sqlconnect.RelationRef {
		return sqlconnect.NewRelationRef(n, sqlconnect.WithSchema(schema.Name))
	}
	require.Error(t, db.CreateTableFromQuery(ctx, ref("old"), "SELECT 1 AS id"), "an existing target fails")
	require.Error(t, db.CreateTableFromQuery(ctx, ref("fail_target"), "SELECT throwIf(number = 2500000, 'boom') AS x, number AS id FROM numbers(3000000)"))
	exists, _ := db.TableExists(ctx, ref("fail_target"))
	require.True(t, exists, "the target stays for the caller to reconcile")
	require.Error(t, db.MoveTable(ctx, ref("old"), ref("fail_target")))
	exists, _ = db.TableExists(ctx, ref("old"))
	require.True(t, exists, "move never drops the old table after a copy failure")
	// mv_user may create, fill and drop tables in the schema, except DROP on old.
	srv.CreateScopedUser(t, "mv_user", "pw_Mv_123", schema.Name+"_c", schema.Name, false)
	srv.AdminExec(t, "REVOKE DROP TABLE ON "+schema.Name+".old FROM mv_user")
	t.Cleanup(func() { srv.AdminExec(t, "DROP DATABASE IF EXISTS "+schema.Name+"_c SYNC") })
	err := openScopedOn(t, srv, "mv_user", "pw_Mv_123", schema.Name).MoveTable(ctx, ref("old"), ref("copy"))
	require.ErrorIs(t, err, sqlconnect.ErrDropOldTablePostCopy)
	var ex *ch.Exception
	require.ErrorAs(t, err, &ex, "the cause stays reachable")
	require.EqualValues(t, 497, ex.Code)
	n, err := db.CountTableRows(ctx, ref("copy"))
	require.NoError(t, err)
	require.Equal(t, 1, n, "the copy finished before the drop failed")
	_, err = db.MoveTableWithOptions(ctx, scopedExec(t, db), ref("old"), sqlconnect.NewRelationRef("x", sqlconnect.WithSchema("other_db")), sqlconnect.MaterializationOptions{})
	requireCode(t, err, "CH_CROSS_DATABASE_MOVE_UNSUPPORTED")
}

func TestSQ31_PoolWriteReplay(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	arms := map[string]func(p *chtest.Proxy, m func(chtest.Request) bool){
		"EOF after execution":   func(p *chtest.Proxy, m func(chtest.Request) bool) { p.DropResponseOnce(m) },
		"reset after execution": func(p *chtest.Proxy, m func(chtest.Request) bool) { p.ResetAfterForwardOnce(m) },
		"reset while writing":   func(p *chtest.Proxy, m func(chtest.Request) bool) { p.ResetBeforeBodyOnce(m) },
	}
	for _, secure := range []bool{true, false} {
		for name, arm := range arms {
			t.Run(fmt.Sprintf("%s secure=%v", name, secure), func(t *testing.T) {
				p := chtest.NewProxy(t, srv, chtest.ProxyOptions{PlainHTTP: !secure})
				db := openVia(t, srv, p, secure)
				ctx := context.Background()
				schema := seedSchema(t, db, `CREATE TABLE {{.schema}}.src (id UInt64) ENGINE = MergeTree ORDER BY id; INSERT INTO {{.schema}}.src VALUES (1),(2);
					CREATE TABLE {{.schema}}.dst (id UInt64) ENGINE = MergeTree ORDER BY id`)
				insertID := clickhousequery.NewQueryID()
				isInsert := func(r chtest.Request) bool { return r.Query.Get("query_id") == insertID }
				arm(p, isInsert)
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				q := "SELECT id FROM " + schema.Name + ".src\n-- " + strings.Repeat("x", 4<<20) // keeps the client writing
				err = db.InsertFromQueryWithOptions(ctx, fixedIDExec(conn, map[string]string{"INSERT": insertID}),
					sqlconnect.NewRelationRef("dst", sqlconnect.WithSchema(schema.Name)), q,
					sqlconnect.MaterializationOptions{Columns: []sqlconnect.ColumnRef{{Name: "id", RawType: "UInt64"}}})
				require.Equal(t, "CH_NETWORK", db.ClassifyError(err).Code, "an unknown outcome, never a silent success: %v", err)
				srv.FlushLogs(t)
				require.LessOrEqual(t, srv.QueryLogCount(t, insertID), 1, "the server ran the INSERT at most once")
				require.Contains(t, []string{"0", "2"}, srv.AdminQuery(t, "SELECT count() FROM "+schema.Name+".dst")[0][0])
				require.Equal(t, 1, countRequests(p, isInsert), "database/sql sent the INSERT once")
			})
		}
	}
}

func TestSQ10_EmptyTableBuild(t *testing.T) {
	db := openFloor(t)
	ctx := context.Background()
	schema := seedSchema(t, db, `CREATE TABLE {{.schema}}.src (id UInt64, ts Nullable(DateTime64(3)), a Array(UInt8), z DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id`)
	ref := sqlconnect.NewRelationRef("empty", sqlconnect.WithSchema(schema.Name))
	_, err := db.CreateTableForQueryWithOptions(ctx, scopedExec(t, db), ref, "SELECT id, ts, a, z FROM "+schema.Name+".src WHERE 0",
		sqlconnect.MaterializationOptions{SortingKey: []string{"id"}})
	require.NoError(t, err)
	cols, err := db.ListColumns(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, []string{"UInt64", "Nullable(DateTime64(3))", "Array(UInt8)", "DateTime64(3, 'UTC')"}, rawTypes(cols))
}

func TestSQ_PayloadDeclarations(t *testing.T) {
	db := openFloor(t)
	ctx := context.Background()
	schema := mkSchema(t, db, "pd")
	ex := scopedExec(t, db) // one pinned connection: each scopedExec holds a pool slot until the test ends
	for i, decl := range []string{
		"LowCardinality(Nullable(String))", "Nullable(Int32)", "DateTime64(3, 'Europe/Athens')", "DateTime64(6)",
		"Decimal(18, 4)", "UInt64", "Bool", "Enum8('a' = 1, 'b' = 2)", "FixedString(4)", "Array(Nullable(String))",
	} {
		q := fmt.Sprintf("SELECT toUInt64(1) AS k, defaultValueOfTypeName('%s') AS v", strings.ReplaceAll(decl, "'", "\\'"))
		described, err := db.ListColumnsForSqlQuery(ctx, q)
		require.NoError(t, err, decl)
		var printed [2]string
		for j := range 2 {
			ref := sqlconnect.NewRelationRef(fmt.Sprintf("t%d_%d", i, j), sqlconnect.WithSchema(schema.Name))
			_, err := db.CreateTableForQueryWithOptions(ctx, ex, ref, q, sqlconnect.MaterializationOptions{SortingKey: []string{"k"}, Columns: described})
			require.NoError(t, err, decl)
			cols, err := db.ListColumns(ctx, ref)
			require.NoError(t, err)
			printed[j] = cols[1].RawType
			require.Equal(t, described[1].Type, cols[1].Type, "canonical label is stable")
		}
		require.Equal(t, printed[0], printed[1], "same list, same system.columns.type")
		if printed[0] != described[1].RawType {
			t.Logf("server-print normalization: DESCRIBE %q -> system.columns %q", described[1].RawType, printed[0])
		}
	}
	snap, unload := sqlconnect.NewRelationRef("snap", sqlconnect.WithSchema(schema.Name)), sqlconnect.NewRelationRef("unload", sqlconnect.WithSchema(schema.Name))
	nullQ := "SELECT CAST(NULL AS Nullable(UInt64)) AS id"
	o := sqlconnect.MaterializationOptions{SortingKey: []string{"id"}, Columns: []sqlconnect.ColumnRef{{Name: "id", RawType: "UInt64"}}}
	_, err := db.CreateTableForQueryWithOptions(ctx, ex, snap, "SELECT CAST(1 AS Nullable(UInt64)) AS id", o)
	require.NoError(t, err)
	cols, _ := db.ListColumns(ctx, snap)
	require.Equal(t, "UInt64", cols[0].RawType, "a key supplied without its Nullable wrapper stays unwrapped")
	require.Error(t, db.InsertFromQueryWithOptions(ctx, ex, snap, nullQ, o))
	on := sqlconnect.MaterializationOptions{SortingKey: []string{"id"}, AllowNullableKey: true, Columns: []sqlconnect.ColumnRef{{Name: "id", RawType: "Nullable(UInt64)"}}}
	_, err = db.CreateTableForQueryWithOptions(ctx, ex, unload, nullQ, on)
	require.NoError(t, err)
	require.NoError(t, db.InsertFromQueryWithOptions(ctx, ex, unload, nullQ, on))
	n, _ := db.CountTableRows(ctx, unload)
	require.Equal(t, 1, n, "a nullable unload key keeps the NULL row")
}

func TestSQ_ExplicitColumnsProjectionCheck(t *testing.T) {
	db := openFloor(t)
	ctx := context.Background()
	ref := sqlconnect.NewRelationRef("t", sqlconnect.WithSchema(mkSchema(t, db, "pc").Name))
	ex := scopedExec(t, db)
	o := sqlconnect.MaterializationOptions{SortingKey: []string{"id"}, Columns: []sqlconnect.ColumnRef{{Name: "id", RawType: "UInt64"}, {Name: "v", RawType: "String"}}}
	_, err := db.CreateTableForQueryWithOptions(ctx, ex, ref, "SELECT toUInt64(1) AS id, 'x' AS v", o)
	require.NoError(t, err)
	for _, q := range []string{
		"SELECT toUInt64(1) AS id, 'x' AS v, 2 AS extra", "SELECT toUInt64(1) AS id",
		"SELECT 'x' AS v, toUInt64(1) AS id", "SELECT toUInt64(1) AS id, 'x' AS w",
	} {
		requireCode(t, db.InsertFromQueryWithOptions(ctx, ex, ref, q, o), "CH_SCHEMA_MISMATCH")
	}
	n, _ := db.CountTableRows(ctx, ref)
	require.Zero(t, n, "nothing written on a mismatch")
	require.NoError(t, db.InsertFromQueryWithOptions(ctx, ex, ref, "SELECT toUInt64(1) AS id, 'x' AS v", o))
}

func TestSQ7_CatalogOnMaterialization(t *testing.T) {
	db := openFloor(t)
	ctx := context.Background()
	withCat := sqlconnect.RelationRef{Catalog: "c", Schema: "db", Name: "t"}
	ex := scopedExec(t, db)
	var none sqlconnect.MaterializationOptions
	_, e1 := db.CreateTableForQueryWithOptions(ctx, ex, withCat, "SELECT 1 AS a", none)
	_, e2 := db.CreateTableFromQueryWithOptions(ctx, ex, withCat, "SELECT 1 AS a", none)
	_, e3 := db.MoveTableWithOptions(ctx, ex, withCat, withCat, none)
	for _, err := range []error{
		e1, e2, e3, db.InsertFromQueryWithOptions(ctx, ex, withCat, "SELECT 1 AS a", none),
		db.CreateTableFromQuery(ctx, withCat, "SELECT 1 AS a"), db.MoveTable(ctx, withCat, withCat),
	} {
		requireCode(t, err, "CH_CATALOG_UNSUPPORTED")
		require.ErrorIs(t, err, sqlconnect.ErrNotSupported)
	}
}

package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func TestSortingKey_Rules(t *testing.T) {
	cols := []sqlconnect.ColumnRef{
		{Name: "email", RawType: "String"},
		{Name: "id", RawType: "UInt64"},
		{Name: "tags", RawType: "Array(String)"},
		{Name: "n", RawType: "Nullable(Int32)"},
	}
	k, _, err := chooseSortingKey(cols, sqlconnect.MaterializationOptions{SortingKey: []string{"id", "email"}})
	require.NoError(t, err)
	require.Equal(t, []string{"id", "email"}, k, "explicit keys regardless of projection order")
	for _, bad := range [][]string{{"id", "id"}, {"missing"}, {"tags"}, {"n"}} {
		_, _, err := chooseSortingKey(cols, sqlconnect.MaterializationOptions{SortingKey: bad})
		requireCode(t, err, "CH_SORTING_KEY_UNAVAILABLE")
	}
	_, nullable, err := chooseSortingKey(cols, sqlconnect.MaterializationOptions{SortingKey: []string{"n"}, AllowNullableKey: true})
	require.NoError(t, err)
	require.True(t, nullable)
	k, _, _ = chooseSortingKey([]sqlconnect.ColumnRef{{Name: "lc", RawType: "LowCardinality(String)"}}, sqlconnect.MaterializationOptions{})
	require.Equal(t, []string{"lc"}, k, "legacy fallback: first eligible column, LowCardinality unwrapped")
	arr := []sqlconnect.ColumnRef{{Name: "t", RawType: "Array(UInt8)"}}
	k, _, err = chooseSortingKey(arr, sqlconnect.MaterializationOptions{})
	require.NoError(t, err)
	require.Nil(t, k, "tuple() when nothing qualifies and no key is required")
	_, _, err = chooseSortingKey(arr, sqlconnect.MaterializationOptions{RequireSortingKey: true})
	requireCode(t, err, "CH_SORTING_KEY_UNAVAILABLE")
}

func TestRenderCreate(t *testing.T) {
	q := newDialect().QuoteIdentifier
	cols := []sqlconnect.ColumnRef{{Name: "a`b", RawType: "Nullable(UInt64)"}, {Name: "v", RawType: "String"}}
	require.Equal(t, "CREATE TABLE `s`.`t` (`a\\`b` Nullable(UInt64), `v` String)\nENGINE = MergeTree\nORDER BY (`a\\`b`, `v`)\nSETTINGS allow_nullable_key = 1",
		renderCreate(q, "`s`.`t`", cols, []string{"a`b", "v"}, true))
	require.Equal(t, "CREATE TABLE t (`v` String)\nENGINE = MergeTree\nORDER BY tuple()", renderCreate(q, "t", cols[1:], nil, false))
}

// Caller-supplied names and declarations reach CREATE TABLE, so every one is
// checked before any SQL is sent.
func TestMaterialization_RefusesBadColumnsBeforeSQL(t *testing.T) {
	db, attempts := unitDBBadConn(t)
	ctx := context.Background()
	ex := unitScopedExec(t, db)
	ref := sqlconnect.NewRelationRef("t", sqlconnect.WithSchema("s"))
	for code, cols := range map[string][]sqlconnect.ColumnRef{
		"CH_QUERY_INVALID": {
			{Name: "a", RawType: "UInt8) ENGINE = Log AS SELECT 1 --"},
			{Name: "b", RawType: "Enum8('a' = 1); DROP TABLE x"},
			{Name: "c", RawType: "String /* x */"},
		},
		"CH_INVALID_IDENTIFIER": {{Name: "a\x00b", RawType: "UInt8"}},
		"CH_DUPLICATE_COLUMN":   {{Name: "a", RawType: "UInt8"}, {Name: "a", RawType: "String"}},
	} {
		attempts.Reset()
		_, err := db.CreateTableForQueryWithOptions(ctx, ex, ref, "SELECT 1 AS a", sqlconnect.MaterializationOptions{Columns: cols})
		requireCode(t, err, code)
		requireCode(t, db.InsertFromQueryWithOptions(ctx, ex, ref, "SELECT 1 AS a", sqlconnect.MaterializationOptions{Columns: cols}), code)
		require.Zero(t, attempts.Writes()+attempts.Reads(), code)
	}
	for _, q := range []string{"SELECT 1; SELECT 2", "SELECT 1; -- x", " ; "} {
		requireCode(t, db.InsertFromQueryWithOptions(ctx, ex, ref, q, sqlconnect.MaterializationOptions{}), "CH_QUERY_INVALID")
		_, err := db.CreateTableForQueryWithOptions(ctx, ex, ref, q, sqlconnect.MaterializationOptions{})
		requireCode(t, err, "CH_QUERY_INVALID")
	}
	// A pool executor would let database/sql replay a write after ErrBadConn.
	for _, pool := range []sqlconnect.QueryExecutor{nil, db, db.DB, db.DB.DB, poolExec{db.DB.DB}} {
		_, err := db.CreateTableForQueryWithOptions(ctx, pool, ref, "SELECT 1", sqlconnect.MaterializationOptions{})
		requireCode(t, err, "CH_CONFIG_INVALID")
		_, err = db.CreateTableFromQueryWithOptions(ctx, pool, ref, "SELECT 1", sqlconnect.MaterializationOptions{})
		requireCode(t, err, "CH_CONFIG_INVALID")
		requireCode(t, db.InsertFromQueryWithOptions(ctx, pool, ref, "SELECT 1", sqlconnect.MaterializationOptions{}), "CH_CONFIG_INVALID")
		_, err = db.MoveTableWithOptions(ctx, pool, ref, ref, sqlconnect.MaterializationOptions{})
		requireCode(t, err, "CH_CONFIG_INVALID")
	}
	_, err := db.MoveTableWithOptions(ctx, ex, ref, sqlconnect.NewRelationRef("t2", sqlconnect.WithSchema("other")), sqlconnect.MaterializationOptions{})
	requireCode(t, err, "CH_CROSS_DATABASE_MOVE_UNSUPPORTED")
	require.ErrorIs(t, err, sqlconnect.ErrNotSupported)
	require.Zero(t, attempts.Writes()+attempts.Reads())
}

// database/sql retries a pool Exec on ErrBadConn and never retries on a *sql.Conn: one attempt proves the pinned path.
func TestSQ31_EveryWriteRunsPinned(t *testing.T) {
	db, attempts := unitDBBadConn(t)
	ctx := context.Background()
	ref, ref2 := sqlconnect.NewRelationRef("t", sqlconnect.WithSchema("s")), sqlconnect.NewRelationRef("t2", sqlconnect.WithSchema("s"))
	opts := sqlconnect.MaterializationOptions{Columns: []sqlconnect.ColumnRef{{Name: "a", RawType: "UInt8"}}}
	for name, call := range map[string]func() error{
		"CreateSchema":         func() error { return db.CreateSchema(ctx, sqlconnect.SchemaRef{Name: "s"}) },
		"DropSchema":           func() error { return db.DropSchema(ctx, sqlconnect.SchemaRef{Name: "s"}) },
		"CreateTestTable":      func() error { return db.CreateTestTable(ctx, ref) },
		"DropTable":            func() error { return db.DropTable(ctx, ref) },
		"TruncateTable":        func() error { return db.TruncateTable(ctx, ref) },
		"RenameTable":          func() error { return db.RenameTable(ctx, ref, ref2) },
		"CreateTableFromQuery": func() error { return db.CreateTableFromQuery(ctx, ref, "SELECT toUInt8(1) AS a") },
		"MoveTable":            func() error { return db.MoveTable(ctx, ref, ref2) },
		"CreateTableForQueryWithOptions": func() error {
			_, err := db.CreateTableForQueryWithOptions(ctx, unitScopedExec(t, db), ref, "SELECT toUInt8(1) AS a", opts)
			return err
		},
	} {
		attempts.Reset()
		require.Error(t, call(), name)
		require.Equal(t, 1, attempts.Writes(), "%s: one write attempt, so it ran on a *sql.Conn", name)
	}
	attempts.Reset()
	_, err := db.ExecContext(ctx, "SELECT 1")
	require.Error(t, err)
	require.Greater(t, attempts.Writes(), 1, "control: the pool replays a write on ErrBadConn")
}

// badConnCounter counts the statements the badConn driver received.
type badConnCounter struct{ writes, reads atomic.Int32 }

func (c *badConnCounter) Reset()      { c.writes.Store(0); c.reads.Store(0) }
func (c *badConnCounter) Writes() int { return int(c.writes.Load()) }
func (c *badConnCounter) Reads() int  { return int(c.reads.Load()) }

// unitDBBadConn is a DB on a stub pool. Every exec fails with
// driver.ErrBadConn; every query answers as DESCRIBE with one row "a UInt8".
func unitDBBadConn(t *testing.T) (*DB, *badConnCounter) {
	t.Helper()
	cfg, err := parseConfig(validJSONInternal(nil), false)
	require.NoError(t, err)
	counter := &badConnCounter{}
	d := &DB{cfg: cfg}
	d.DB = base.NewDB(sql.OpenDB(badConnConnector{counter}), func() error { return nil }, base.WithDialect(newDialect()))
	t.Cleanup(func() { _ = d.Close() })
	return d, counter
}

// unitScopedExec is a caller executor: one pinned connection of db.
func unitScopedExec(t *testing.T, db *DB) sqlconnect.QueryExecutor {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type badConnConnector struct{ c *badConnCounter }

func (b badConnConnector) Connect(context.Context) (driver.Conn, error) { return badConn(b), nil }
func (badConnConnector) Driver() driver.Driver                          { return stubDriver{} }

type badConn struct{ c *badConnCounter }

func (badConn) Prepare(string) (driver.Stmt, error)      { return nil, driver.ErrSkip }
func (badConn) Close() error                             { return nil }
func (badConn) Begin() (driver.Tx, error)                { return nil, driver.ErrSkip }
func (badConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (b badConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	b.c.writes.Add(1)
	return nil, driver.ErrBadConn
}

func (b badConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	b.c.reads.Add(1)
	return &describeRows{}, nil
}

// describeRows is a DESCRIBE answer with one column "a" of type UInt8.
type describeRows struct{ done bool }

func (*describeRows) Columns() []string { return []string{"name", "type"} }
func (*describeRows) Close() error      { return nil }

func (r *describeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0], dest[1] = "a", "UInt8"
	return nil
}

// The INSERT never turns a NULL into the column default: the driver map
// disables insert_null_as_default, which ClickHouse enables by default.
func TestDriverMapRefusesNullAsDefault(t *testing.T) {
	require.Equal(t, 0, driverScratchSettings()["insert_null_as_default"])
}

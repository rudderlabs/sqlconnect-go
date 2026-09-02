package sqlconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// A minimal driver so the test can obtain a real *sql.Rows, which JsonQueryDB.QueryContext must
// return. The scanned column is irrelevant — the row mapper below ignores it and supplies the cell
// values under test — so the driver only has to yield the right number of rows.
type nonFiniteFakeDriver struct{ rows int }

func (d nonFiniteFakeDriver) Open(string) (driver.Conn, error) {
	return &nonFiniteFakeConn{n: d.rows}, nil
}

type nonFiniteFakeConn struct{ n int }

func (c *nonFiniteFakeConn) Prepare(string) (driver.Stmt, error) { return nil, io.EOF }
func (c *nonFiniteFakeConn) Close() error                        { return nil }
func (c *nonFiniteFakeConn) Begin() (driver.Tx, error)           { return nil, io.EOF }
func (c *nonFiniteFakeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &nonFiniteFakeRows{n: c.n}, nil
}

type nonFiniteFakeRows struct{ n, i int }

func (r *nonFiniteFakeRows) Columns() []string { return []string{"c"} }
func (r *nonFiniteFakeRows) Close() error      { return nil }
func (r *nonFiniteFakeRows) Next(dest []driver.Value) error {
	if r.i >= r.n {
		return io.EOF
	}
	r.i++
	dest[0] = int64(r.i)
	return nil
}

func init() { sql.Register("sqlconnect-nonfinite-fake", nonFiniteFakeDriver{rows: 1}) }

// fakeJSONDB is a JsonQueryDB whose mapper returns a caller-supplied row, so a cell shape that no
// real fixture contains (a NaN, or a NaN nested in a driver's own slice type) can be driven through
// the actual QueryJSONAsync code path.
type fakeJSONDB struct {
	db     *sql.DB
	newRow func() map[string]any
}

func newFakeJSONDB(t *testing.T, newRow func() map[string]any) *fakeJSONDB {
	t.Helper()
	db, err := sql.Open("sqlconnect-nonfinite-fake", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return &fakeJSONDB{db: db, newRow: newRow}
}

func (f *fakeJSONDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return f.db.QueryContext(ctx, query, args...)
}

func (f *fakeJSONDB) JSONRowMapper() RowMapper[map[string]any] {
	return func([]*sql.ColumnType, RowScan) (map[string]any, error) { return f.newRow(), nil }
}

func collect(t *testing.T, db *fakeJSONDB) (json.RawMessage, error) {
	t.Helper()
	ch, leave := QueryJSONAsync(context.Background(), db, "SELECT 1")
	defer leave()
	for v := range ch {
		if v.Err != nil {
			return nil, v.Err
		}
		return v.Value, nil
	}
	t.Fatal("no row received")
	return nil, nil
}

// The wiring itself: without nullifyNonFiniteFloats being called from QueryJSONAsync, this query
// fails outright instead of returning a row. Deleting that call makes this test fail, which the
// helper-only test could not detect.
func TestQueryJSONAsync_TopLevelNonFinite(t *testing.T) {
	db := newFakeJSONDB(t, func() map[string]any {
		return map[string]any{"nan": math.NaN(), "finite": 1.5}
	})

	raw, err := collect(t, db)
	require.NoError(t, err, "a NaN cell must not fail the query")

	var back map[string]any
	require.NoError(t, json.Unmarshal(raw, &back))
	require.Nil(t, back["nan"])
	require.Equal(t, 1.5, back["finite"])
}

// bigquery.Value is interface{}, so an ARRAY cell arrives as a []bigquery.Value: a named slice type
// that a `case []any` would not match. This stands in for it — same shape, no bigquery import.
type fakeDriverValue any

func TestQueryJSONAsync_NonFiniteInsideDriverSlice(t *testing.T) {
	db := newFakeJSONDB(t, func() map[string]any {
		return map[string]any{"arr": []fakeDriverValue{1.5, math.NaN(), "x"}}
	})

	raw, err := collect(t, db)
	require.NoError(t, err, "a NaN nested in a driver slice type must not fail the query")

	var back map[string]any
	require.NoError(t, json.Unmarshal(raw, &back))
	require.Equal(t, []any{1.5, nil, "x"}, back["arr"])
}

func TestQueryJSONAsync_NonFiniteNestedInContainers(t *testing.T) {
	db := newFakeJSONDB(t, func() map[string]any {
		return map[string]any{
			"slice":  []any{math.Inf(1), 2.0},
			"nested": map[string]any{"inner": map[string]any{"bad": math.Inf(-1), "ok": "keep"}},
			"deep":   []any{[]any{map[string]any{"f32": float32(math.NaN())}}},
		}
	})

	raw, err := collect(t, db)
	require.NoError(t, err, "a NaN at depth must not fail the query")

	var back map[string]any
	require.NoError(t, json.Unmarshal(raw, &back))
	require.Equal(t, []any{nil, 2.0}, back["slice"])
	require.Equal(t, map[string]any{"inner": map[string]any{"bad": nil, "ok": "keep"}}, back["nested"])
	require.Equal(t, []any{[]any{map[string]any{"f32": nil}}}, back["deep"])
}

// QueryJSONMapAsync is deliberately not normalised: it hands back the native map, where NaN is
// representable. Pinned so the recursion above is never extended onto that path by accident.
func TestQueryJSONMapAsync_LeavesNonFiniteIntact(t *testing.T) {
	db := newFakeJSONDB(t, func() map[string]any {
		return map[string]any{"nan": math.NaN()}
	})

	ch, leave := QueryJSONMapAsync(context.Background(), db, "SELECT 1")
	defer leave()
	for v := range ch {
		require.NoError(t, v.Err)
		f, ok := v.Value["nan"].(float64)
		require.True(t, ok, "value should still be a float64, not nil")
		require.True(t, math.IsNaN(f), "QueryJSONMapAsync must not normalise NaN")
		return
	}
	t.Fatal("no row received")
}

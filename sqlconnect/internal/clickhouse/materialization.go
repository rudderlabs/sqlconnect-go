package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chsql"
)

var _ sqlconnect.MaterializationAdmin = (*DB)(nil)

// eligibleKeyTypes are the scalar types a sorting key may use, after the
// LowCardinality and Nullable wrappers are removed.
var eligibleKeyTypes = setOf(strings.Fields(`Int8 Int16 Int32 Int64 Int128 Int256 UInt8 UInt16 UInt32 UInt64 UInt128 UInt256
	Decimal Decimal32 Decimal64 Decimal128 Decimal256 Float32 Float64 String FixedString Enum8 Enum16
	Date Date32 DateTime DateTime64 Time Time64 UUID IPv4 IPv6 Bool`))

func setOf(names []string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// keyEligible reports whether a column of type raw can be a sorting key, and
// whether it is nullable. A Nullable column qualifies only with allowNullable.
func keyEligible(raw string, allowNullable bool) (ok, nullable bool) {
	t, err := parseType(raw)
	if err != nil {
		return false, false
	}
	if t.Name == "LowCardinality" {
		t = t.Children[0]
	}
	if t.Name == "Nullable" {
		if !allowNullable {
			return false, true
		}
		nullable, t = true, t.Children[0]
	}
	return eligibleKeyTypes[t.Name], nullable
}

// chooseSortingKey returns the ORDER BY columns for cols. An explicit key is
// used as given or refused; it never falls back to another column. Without
// one, the first eligible column is the key. A nil key means tuple().
func chooseSortingKey(cols []sqlconnect.ColumnRef, o sqlconnect.MaterializationOptions) (key []string, nullable bool, err error) {
	unavailable := func() error {
		return cherr.New(cherr.CodeSortingKeyUnavailable, "sortingKey", "the sorting key column is missing, duplicated or not orderable")
	}
	byName := make(map[string]sqlconnect.ColumnRef, len(cols))
	for _, c := range cols {
		byName[c.Name] = c
	}
	if len(o.SortingKey) > 0 {
		seen, anyNullable := map[string]bool{}, false
		for _, k := range o.SortingKey {
			c, ok := byName[k]
			if !ok || seen[k] {
				return nil, false, unavailable()
			}
			seen[k] = true
			eligible, nullable := keyEligible(c.RawType, o.AllowNullableKey)
			if !eligible {
				return nil, false, unavailable()
			}
			anyNullable = anyNullable || nullable
		}
		return slices.Clone(o.SortingKey), anyNullable, nil
	}
	for _, c := range cols {
		if eligible, nullable := keyEligible(c.RawType, o.AllowNullableKey); eligible {
			return []string{c.Name}, nullable, nil
		}
	}
	if o.RequireSortingKey {
		return nil, false, unavailable()
	}
	return nil, false, nil
}

// renderCreate builds the CREATE TABLE statement. Each RawType goes in
// verbatim, so the caller must have checked it with parseType.
func renderCreate(q func(string) string, table string, cols []sqlconnect.ColumnRef, key []string, nullableKey bool) string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = q(c.Name) + " " + c.RawType
	}
	order := "tuple()"
	if len(key) > 0 {
		quoted := make([]string, len(key))
		for i, k := range key {
			quoted[i] = q(k)
		}
		order = "(" + strings.Join(quoted, ", ") + ")"
	}
	s := "CREATE TABLE " + table + " (" + strings.Join(defs, ", ") + ")\nENGINE = MergeTree\nORDER BY " + order
	if nullableKey {
		s += "\nSETTINGS allow_nullable_key = 1"
	}
	return s
}

// checkExecutor refuses a missing executor and a pool. database/sql replays a
// pool statement after driver.ErrBadConn, so a pool executor could apply a
// CREATE, INSERT or DROP twice. Callers pass a *sql.Conn.
func checkExecutor(ex sqlconnect.QueryExecutor) error {
	switch ex.(type) {
	case nil:
		return cherr.New(cherr.CodeConfigInvalid, "executor", "an executor is required")
	case *sql.DB, *base.DB, *DB, poolExec:
		return cherr.New(cherr.CodeConfigInvalid, "executor", "the executor must be one pinned connection, not the pool")
	}
	return nil
}

// normalizeQuery strips one terminal semicolon. A second statement, text after
// the semicolon or an empty query is CH_QUERY_INVALID.
func normalizeQuery(query string) (string, error) {
	norm, err := chsql.TrimTerminalSemicolon(query)
	if err != nil || strings.TrimSpace(norm) == "" {
		return "", cherr.New(cherr.CodeQueryInvalid, "query", "the query is not a single statement")
	}
	return norm, nil
}

// columnsFor returns the destination columns: o.Columns when set, else the
// DESCRIBE answer for query on ex. Either list is checked before any DDL is
// built from it.
func (db *DB) columnsFor(ctx context.Context, ex sqlconnect.QueryExecutor, query string, o sqlconnect.MaterializationOptions) ([]sqlconnect.ColumnRef, error) {
	cols := o.Columns
	if len(cols) == 0 {
		norm, err := normalizeQuery(query)
		if err != nil {
			return nil, err
		}
		if cols, err = db.describe(ctx, ex, norm); err != nil {
			return nil, err
		}
	}
	if err := checkColumns(cols); err != nil {
		return nil, err
	}
	return cols, nil
}

// checkColumns refuses an empty list, an empty or NUL-carrying name, a
// duplicate name and a declaration that parseType refuses.
func checkColumns(cols []sqlconnect.ColumnRef) error {
	if len(cols) == 0 {
		return cherr.New(cherr.CodeQueryInvalid, "columns", "the query has no columns")
	}
	seen := make(map[string]bool, len(cols))
	for _, c := range cols {
		if c.Name == "" || strings.ContainsRune(c.Name, 0) {
			return cherr.New(cherr.CodeInvalidIdentifier, "columns", fixedMessages[cherr.CodeInvalidIdentifier])
		}
		if seen[c.Name] {
			return cherr.New(cherr.CodeDuplicateColumn, "columns", fixedMessages[cherr.CodeDuplicateColumn])
		}
		seen[c.Name] = true
		if _, err := parseType(c.RawType); err != nil {
			return cherr.New(cherr.CodeQueryInvalid, "columns", "a column type declaration is malformed")
		}
	}
	return nil
}

// sameNames reports whether a and b hold the same names in the same order.
func sameNames(a, b []sqlconnect.ColumnRef) bool {
	return slices.EqualFunc(a, b, func(x, y sqlconnect.ColumnRef) bool { return x.Name == y.Name })
}

// CreateTableForQueryWithOptions creates an empty MergeTree table for query on
// ex, waits until it is visible and returns its UUID. It runs no INSERT.
func (db *DB) CreateTableForQueryWithOptions(ctx context.Context, ex sqlconnect.QueryExecutor, ref sqlconnect.RelationRef, query string, o sqlconnect.MaterializationOptions) (string, error) {
	if err := checkExecutor(ex); err != nil {
		return "", err
	}
	ref, err := db.resolve(ref)
	if err != nil {
		return "", err
	}
	cols, err := db.columnsFor(ctx, ex, query, o)
	if err != nil {
		return "", err
	}
	key, nullable, err := chooseSortingKey(cols, o)
	if err != nil {
		return "", err
	}
	if _, err := ex.ExecContext(ctx, renderCreate(db.QuoteIdentifier, db.QuoteTable(ref), cols, key, nullable)); err != nil {
		return "", bound("create table", "", err)
	}
	return db.AwaitTable(ctx, ex, ref, "", o.Visibility)
}

// InsertFromQueryWithOptions fills a table that CreateTableForQueryWithOptions
// created with the same options. With explicit Columns it first checks the
// query projection through DESCRIBE, so a mismatch writes nothing.
func (db *DB) InsertFromQueryWithOptions(ctx context.Context, ex sqlconnect.QueryExecutor, ref sqlconnect.RelationRef, query string, o sqlconnect.MaterializationOptions) error {
	if err := checkExecutor(ex); err != nil {
		return err
	}
	ref, err := db.resolve(ref)
	if err != nil {
		return err
	}
	norm, err := normalizeQuery(query)
	if err != nil {
		return err
	}
	cols, err := db.columnsFor(ctx, ex, norm, o)
	if err != nil {
		return err
	}
	if len(o.Columns) > 0 {
		projected, err := db.describe(ctx, ex, norm)
		if err != nil {
			return err
		}
		if !sameNames(projected, o.Columns) {
			return cherr.New(cherr.CodeSchemaMismatch, "columns", "the query projection does not match the destination columns")
		}
	}
	list := make([]string, len(cols))
	for i, c := range cols {
		list[i] = db.QuoteIdentifier(c.Name)
	}
	names := strings.Join(list, ", ")
	// The statement-level setting overrides any caller map: ClickHouse
	// otherwise writes the column default for a NULL into a non-nullable
	// column.
	_, err = ex.ExecContext(ctx, "INSERT INTO "+db.QuoteTable(ref)+" ("+names+")\nSETTINGS insert_null_as_default = 0\nSELECT "+names+" FROM (\n"+norm+"\n) AS _sqlconnect_source")
	return bound("insert from query", "", err)
}

// CreateTableFromQueryWithOptions creates the table, waits until it is
// visible, then fills it. A failed INSERT leaves the table for the caller.
func (db *DB) CreateTableFromQueryWithOptions(ctx context.Context, ex sqlconnect.QueryExecutor, ref sqlconnect.RelationRef, query string, o sqlconnect.MaterializationOptions) (string, error) {
	uuid, err := db.CreateTableForQueryWithOptions(ctx, ex, ref, query, o)
	if err != nil {
		return "", err
	}
	if err := db.InsertFromQueryWithOptions(ctx, ex, ref, query, o); err != nil {
		return "", err
	}
	return uuid, nil
}

// MoveTableWithOptions copies oldRef into a new table newRef in the same
// database, verifies equal row counts, then drops oldRef. A failed copy or
// count check drops nothing. A failed drop
// returns an error that joins sqlconnect.ErrDropOldTablePostCopy.
func (db *DB) MoveTableWithOptions(ctx context.Context, ex sqlconnect.QueryExecutor, oldRef, newRef sqlconnect.RelationRef, o sqlconnect.MaterializationOptions) (string, error) {
	if err := checkExecutor(ex); err != nil {
		return "", err
	}
	oldRef, err := db.resolve(oldRef)
	if err != nil {
		return "", err
	}
	newRef, err = db.resolve(newRef)
	if err != nil {
		return "", err
	}
	if oldRef.Schema != newRef.Schema {
		return "", wrap(cherr.CodeCrossDatabaseMove, "", fixedMessages[cherr.CodeCrossDatabaseMove], sqlconnect.ErrNotSupported)
	}
	uuid, err := db.CreateTableFromQueryWithOptions(ctx, ex, newRef, "SELECT * FROM "+db.QuoteTable(oldRef), o)
	if err != nil {
		return "", err
	}
	var sourceCount, copyCount uint64
	for _, count := range []struct {
		ref   sqlconnect.RelationRef
		value *uint64
	}{{oldRef, &sourceCount}, {newRef, &copyCount}} {
		if err := ex.QueryRowContext(clickhousequery.WithStatement(ctx, driverReadSettings(), clickhousequery.NewQueryID()), "SELECT count() FROM "+db.QuoteTable(count.ref)).Scan(count.value); err != nil {
			return "", bound("count moved table rows", "", err)
		}
	}
	if sourceCount != copyCount {
		return "", cherr.New(cherr.CodeRowCountMismatch, "", fixedMessages[cherr.CodeRowCountMismatch])
	}
	if _, err := ex.ExecContext(ctx, "DROP TABLE IF EXISTS "+db.QuoteTable(oldRef)+" SYNC"); err != nil {
		return "", errors.Join(sqlconnect.ErrDropOldTablePostCopy, bound("drop old table", "", err))
	}
	return uuid, nil
}

// CreateTableFromQuery overrides the base CTAS, which names no engine. It runs
// on one pinned connection with zero options.
func (db *DB) CreateTableFromQuery(ctx context.Context, table sqlconnect.RelationRef, query string) error {
	if _, err := db.resolve(table); err != nil {
		return err
	}
	return db.withConn(ctx, func(ex driverExec) error {
		_, err := db.CreateTableFromQueryWithOptions(ctx, ex, table, query, sqlconnect.MaterializationOptions{})
		return err
	})
}

// MoveTable overrides the base move. It runs on one pinned connection with
// zero options.
func (db *DB) MoveTable(ctx context.Context, oldRef, newRef sqlconnect.RelationRef) error {
	if _, err := db.resolve(oldRef); err != nil {
		return err
	}
	if _, err := db.resolve(newRef); err != nil {
		return err
	}
	return db.withConn(ctx, func(ex driverExec) error {
		_, err := db.MoveTableWithOptions(ctx, ex, oldRef, newRef, sqlconnect.MaterializationOptions{})
		return err
	})
}

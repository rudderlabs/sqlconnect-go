package clickhouse

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// viewEngines are the engines that ListTables reports as views.
var viewEngines = map[string]bool{"View": true, "MaterializedView": true, "WindowView": true}

// ddl resolves ref and runs one statement built from the quoted table on a
// pinned connection.
func (db *DB) ddl(ctx context.Context, op string, ref sqlconnect.RelationRef, build func(table string) string) error {
	ref, err := db.resolve(ref)
	if err != nil {
		return err
	}
	return db.withConn(ctx, func(ex driverExec) error {
		_, err := ex.ExecContext(ctx, build(db.QuoteTable(ref)))
		return bound(op, "", err)
	})
}

// CreateTestTable creates the shared suite's test table if it is absent.
func (db *DB) CreateTestTable(ctx context.Context, table sqlconnect.RelationRef) error {
	return db.ddl(ctx, "create test table", table, func(t string) string {
		return "CREATE TABLE IF NOT EXISTS " + t + " (c1 Int64, c2 String) ENGINE = MergeTree ORDER BY (c1)"
	})
}

// ListTables lists the tables and views of one database whose names start
// with the prefix. '%' and '_' in the prefix are literal. A non-empty catalog
// filter returns an empty result and runs no SQL.
func (db *DB) ListTables(ctx context.Context, schema sqlconnect.SchemaRef, opts ...sqlconnect.Option) ([]sqlconnect.RelationRef, error) {
	o, err := sqlconnect.NewTableListOptions(opts...)
	if err != nil {
		return nil, optionUnsupported()
	}
	if o.Catalog != "" {
		return nil, nil
	}
	if schema.Name == "" {
		schema.Name = db.cfg.Database
	}
	schema, err = db.resolveSchema(schema)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(db.readCtx(ctx), `SELECT name, engine
FROM system.tables
WHERE database = ?
  AND is_temporary = 0
  AND startsWith(name, ?)
ORDER BY name`, schema.Name, o.Prefix)
	if err != nil {
		return nil, bound("list tables", "", err)
	}
	defer func() { _ = rows.Close() }()
	var res []sqlconnect.RelationRef
	for rows.Next() {
		var name, engine string
		if err := rows.Scan(&name, &engine); err != nil {
			return nil, bound("list tables", "", err)
		}
		kind := sqlconnect.TableRelation
		if viewEngines[engine] {
			kind = sqlconnect.ViewRelation
		}
		res = append(res, sqlconnect.NewRelationRef(name, sqlconnect.WithSchema(schema.Name), sqlconnect.WithRelationType(kind)))
	}
	if err := rows.Err(); err != nil {
		return nil, bound("list tables", "", err)
	}
	return res, nil
}

// TableExists reports whether the table or view exists and is visible.
func (db *DB) TableExists(ctx context.Context, relation sqlconnect.RelationRef) (bool, error) {
	ref, err := db.resolve(relation)
	if err != nil {
		return false, err
	}
	n, err := db.queryCount(ctx, "table exists", `SELECT count()
FROM system.tables
WHERE database = ?
  AND name = ?
  AND is_temporary = 0`, ref.Schema, ref.Name)
	return n > 0, err
}

// ListColumns lists the columns in declaration order. RawType keeps the
// server's type text byte for byte. Zero rows mean the table is absent or
// invisible: CH_OBJECT_NOT_FOUND.
func (db *DB) ListColumns(ctx context.Context, relation sqlconnect.RelationRef) ([]sqlconnect.ColumnRef, error) {
	ref, err := db.resolve(relation)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(db.readCtx(ctx), `SELECT name, type
FROM system.columns
WHERE database = ?
  AND table = ?
ORDER BY position`, ref.Schema, ref.Name)
	if err != nil {
		return nil, bound("list columns", "", err)
	}
	defer func() { _ = rows.Close() }()
	var res []sqlconnect.ColumnRef
	for rows.Next() {
		var c sqlconnect.ColumnRef
		if err := rows.Scan(&c.Name, &c.RawType); err != nil {
			return nil, bound("list columns", "", err)
		}
		c.Type = canonicalType(c.RawType)
		res = append(res, c)
	}
	if err := rows.Err(); err != nil {
		return nil, bound("list columns", "", err)
	}
	if len(res) == 0 {
		return nil, cherr.New(cherr.CodeObjectNotFound, "table", fixedMessages[cherr.CodeObjectNotFound])
	}
	return res, nil
}

// CountTableRows counts the rows of a table. A count above the platform int
// returns CH_COUNT_OVERFLOW.
func (db *DB) CountTableRows(ctx context.Context, table sqlconnect.RelationRef) (int, error) {
	ref, err := db.resolve(table)
	if err != nil {
		return 0, err
	}
	return db.queryCount(ctx, "count table rows", "SELECT count() FROM "+db.QuoteTable(ref))
}

// DropTable drops the table now if it exists.
func (db *DB) DropTable(ctx context.Context, ref sqlconnect.RelationRef) error {
	return db.ddl(ctx, "drop table", ref, func(t string) string { return "DROP TABLE IF EXISTS " + t + " SYNC" })
}

// TruncateTable removes every row. Only the shared test suite calls it.
func (db *DB) TruncateTable(ctx context.Context, ref sqlconnect.RelationRef) error {
	return db.ddl(ctx, "truncate table", ref, func(t string) string { return "TRUNCATE TABLE " + t + " SYNC" })
}

// RenameTable renames a table inside one database. Another database returns
// CH_CROSS_DATABASE_MOVE_UNSUPPORTED.
func (db *DB) RenameTable(ctx context.Context, oldRef, newRef sqlconnect.RelationRef) error {
	oldRef, err := db.resolve(oldRef)
	if err != nil {
		return err
	}
	newRef, err = db.resolve(newRef)
	if err != nil {
		return err
	}
	if oldRef.Schema != newRef.Schema {
		return wrap(cherr.CodeCrossDatabaseMove, "", fixedMessages[cherr.CodeCrossDatabaseMove], sqlconnect.ErrNotSupported)
	}
	return db.withConn(ctx, func(ex driverExec) error {
		_, err := ex.ExecContext(ctx, "RENAME TABLE "+db.QuoteTable(oldRef)+" TO "+db.QuoteTable(newRef))
		return bound("rename table", "", err)
	})
}

// GetRowCountForQuery runs the caller's count query on the pool and returns
// its single value. A count above the platform int returns CH_COUNT_OVERFLOW.
func (db *DB) GetRowCountForQuery(ctx context.Context, query string, params ...any) (int, error) {
	return db.queryCount(ctx, "row count for query", query, params...)
}

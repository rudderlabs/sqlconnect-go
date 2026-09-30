package clickhouse

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// optionUnsupported replaces an option validation error, which quotes the
// caller's value, with a fixed message.
func optionUnsupported() error {
	return cherr.New(cherr.CodeInvalidReference, "option", "the listing option is not supported")
}

// CreateSchema creates a database. The connector never calls it; it serves
// the shared test suite.
func (db *DB) CreateSchema(ctx context.Context, schema sqlconnect.SchemaRef) error {
	schema, err := db.resolveSchema(schema)
	if err != nil {
		return err
	}
	return db.withConn(ctx, func(ex driverExec) error {
		_, err := ex.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+db.QuoteIdentifier(schema.Name))
		return bound("create schema", "", err)
	})
}

// ListSchemas lists the databases other than the system ones. A non-empty
// catalog filter returns an empty result and runs no SQL.
func (db *DB) ListSchemas(ctx context.Context, opts ...sqlconnect.Option) ([]sqlconnect.SchemaRef, error) {
	o, err := sqlconnect.NewFilterOptions(opts...)
	if err != nil {
		return nil, optionUnsupported()
	}
	if o.Catalog != "" {
		return nil, nil
	}
	rows, err := db.QueryContext(db.readCtx(ctx), `SELECT name
FROM system.databases
WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
ORDER BY name`)
	if err != nil {
		return nil, bound("list schemas", "", err)
	}
	defer func() { _ = rows.Close() }()
	var res []sqlconnect.SchemaRef
	for rows.Next() {
		var s sqlconnect.SchemaRef
		if err := rows.Scan(&s.Name); err != nil {
			return nil, bound("list schemas", "", err)
		}
		res = append(res, s)
	}
	if err := rows.Err(); err != nil {
		return nil, bound("list schemas", "", err)
	}
	return res, nil
}

// SchemaExists reports whether the database exists. A non-empty catalog
// filter returns false and runs no SQL.
func (db *DB) SchemaExists(ctx context.Context, schema sqlconnect.SchemaRef, opts ...sqlconnect.Option) (bool, error) {
	o, err := sqlconnect.NewFilterOptions(opts...)
	if err != nil {
		return false, optionUnsupported()
	}
	if o.Catalog != "" {
		return false, nil
	}
	n, err := db.queryCount(ctx, "schema exists", "SELECT count()\nFROM system.databases\nWHERE name = ?", schema.Name)
	return n > 0, err
}

// DropSchema drops a database now. A missing database returns
// CH_OBJECT_NOT_FOUND (server code 81).
func (db *DB) DropSchema(ctx context.Context, schema sqlconnect.SchemaRef) error {
	schema, err := db.resolveSchema(schema)
	if err != nil {
		return err
	}
	return db.withConn(ctx, func(ex driverExec) error {
		_, err := ex.ExecContext(ctx, "DROP DATABASE "+db.QuoteIdentifier(schema.Name)+" SYNC")
		return bound("drop schema", "", err)
	})
}

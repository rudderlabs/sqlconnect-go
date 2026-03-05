package base

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// CreateSchema creates a schema
func (db *DB) CreateSchema(ctx context.Context, schema sqlconnect.SchemaRef) error {
	if _, err := db.ExecContext(ctx, db.sqlCommands.CreateSchema(QuotedIdentifier(db.QuoteIdentifier(schema.Name)))); err != nil {
		return fmt.Errorf("creating schema %s: %w", schema, err)
	}
	return nil
}

// ListSchemas returns a list of schemas, optionally filtered by a single catalog
func (db *DB) ListSchemas(ctx context.Context, opts ...sqlconnect.FilterOptions) ([]sqlconnect.SchemaRef, error) {
	if len(opts) > 1 {
		return nil, fmt.Errorf("listing schemas: at most one filter option can be provided, got %d", len(opts))
	}
	var catalogName string
	if len(opts) > 0 {
		catalogName = opts[0].Catalog
	}
	if err := db.ValidateCatalog(ctx, catalogName); err != nil {
		return nil, err
	}
	stmt, colName := db.sqlCommands.ListSchemas(UnquotedIdentifier(catalogName))
	return db.listSchemasFromQuery(ctx, stmt, colName)
}

// listSchemasFromQuery executes the given SQL statement and scans the results into a list of SchemaRefs
func (db *DB) listSchemasFromQuery(ctx context.Context, stmt, colName string) ([]sqlconnect.SchemaRef, error) {
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("querying list schemas: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns in list schemas: %w", err)
	}
	cols = lo.Map(cols, func(col string, _ int) string { return strings.ToLower(col) })
	var schema sqlconnect.SchemaRef
	scanValues := make([]any, len(cols))
	if len(cols) == 1 {
		scanValues[0] = &schema.Name
	} else {
		tableNameColIdx := lo.IndexOf(cols, strings.ToLower(colName))
		if tableNameColIdx == -1 {
			return nil, fmt.Errorf("column %s not found in result set: %+v", colName, cols)
		}
		var otherCol sqlconnect.NilAny
		for i := 0; i < len(cols); i++ {
			if i == tableNameColIdx {
				scanValues[i] = &schema.Name
			} else {
				scanValues[i] = &otherCol
			}
		}
	}
	var res []sqlconnect.SchemaRef
	for rows.Next() {
		err = rows.Scan(scanValues...)
		if err != nil {
			return nil, fmt.Errorf("scanning list schemas: %w", err)
		}
		res = append(res, schema)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating list schemas: %w", err)
	}
	return res, nil
}

// SchemaExists returns true if the schema exists
func (db *DB) SchemaExists(ctx context.Context, schemaRef sqlconnect.SchemaRef, opts ...sqlconnect.FilterOptions) (bool, error) {
	var catalogName string
	if len(opts) > 0 {
		catalogName = opts[0].Catalog
	}
	if err := db.ValidateCatalog(ctx, catalogName); err != nil {
		return false, err
	}
	rows, err := db.QueryContext(ctx, db.sqlCommands.SchemaExists(UnquotedIdentifier(schemaRef.Name), UnquotedIdentifier(catalogName)))
	if err != nil {
		return false, fmt.Errorf("querying schema exists: %w", err)
	}
	defer func() { _ = rows.Close() }()
	exists := rows.Next()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterating schema exists: %w", err)
	}
	return exists, nil
}

// DropSchema drops a schema
func (db *DB) DropSchema(ctx context.Context, schemaRef sqlconnect.SchemaRef, opts ...sqlconnect.FilterOptions) error {
	var catalogName string
	if len(opts) > 0 {
		catalogName = opts[0].Catalog
	}
	if err := db.ValidateCatalog(ctx, catalogName); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, db.sqlCommands.DropSchema(QuotedIdentifier(db.QuoteIdentifier(schemaRef.Name)), UnquotedIdentifier(catalogName))); err != nil {
		return fmt.Errorf("dropping schema: %w", err)
	}
	return nil
}

package mysql

import (
	"context"
	"fmt"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// CurrentCatalog returns an error because it is not supported by MySQL
func (db *DB) CurrentCatalog(ctx context.Context) (sqlconnect.CatalogRef, error) {
	return sqlconnect.CatalogRef{}, sqlconnect.ErrNotSupported
}

// ListCatalogs returns an error because it is not supported by MySQL
func (db *DB) ListCatalogs(ctx context.Context) ([]sqlconnect.CatalogRef, error) {
	return nil, sqlconnect.ErrNotSupported
}

// ListSchemas returns an error when a catalog is provided because MySQL does not support catalog-scoped operations
func (db *DB) ListSchemas(ctx context.Context, opts ...sqlconnect.FilterOptions) ([]sqlconnect.SchemaRef, error) {
	if len(opts) > 1 {
		return nil, fmt.Errorf("listing schemas: at most one filter option can be provided, got %d", len(opts))
	}
	if len(opts) > 0 && opts[0].Catalog != "" {
		return nil, sqlconnect.ErrNotSupported
	}
	return db.DB.ListSchemas(ctx)
}

// ListTables returns an error when a catalog is provided because MySQL does not support catalog-scoped operations
func (db *DB) ListTables(ctx context.Context, schema sqlconnect.SchemaRef, opts ...sqlconnect.TableListOptions) ([]sqlconnect.RelationRef, error) {
	if len(opts) > 0 && opts[0].Catalog != "" {
		return nil, sqlconnect.ErrNotSupported
	}
	return db.DB.ListTables(ctx, schema, opts...)
}

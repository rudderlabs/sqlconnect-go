package mysql

import (
	"context"

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
func (db *DB) ListSchemas(ctx context.Context, opts ...sqlconnect.Option) ([]sqlconnect.SchemaRef, error) {
	filterCatalogOpts, err := sqlconnect.NewFilterOptions(opts...)
	if err != nil {
		return nil, err
	}
	if filterCatalogOpts.Catalog != "" {
		return nil, sqlconnect.ErrNotSupported
	}
	return db.DB.ListSchemas(ctx, opts...)
}

// ListTables returns an error when a catalog is provided because MySQL does not support catalog-scoped operations
func (db *DB) ListTables(ctx context.Context, schema sqlconnect.SchemaRef, opts ...sqlconnect.Option) ([]sqlconnect.RelationRef, error) {
	listOpts, err := sqlconnect.NewTableListOptions(opts...)
	if err != nil {
		return nil, err
	}
	if listOpts.Catalog != "" {
		return nil, sqlconnect.ErrNotSupported
	}
	return db.DB.ListTables(ctx, schema, opts...)
}

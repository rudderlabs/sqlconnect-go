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

// ListSchemasInCatalog returns an error because it is not supported by MySQL
func (db *DB) ListSchemasInCatalog(ctx context.Context, catalog sqlconnect.CatalogRef) ([]sqlconnect.SchemaRef, error) {
	return nil, sqlconnect.ErrNotSupported
}

// ListTablesInCatalog returns an error because it is not supported by MySQL
func (db *DB) ListTablesInCatalog(ctx context.Context, catalog sqlconnect.CatalogRef, schema sqlconnect.SchemaRef) ([]sqlconnect.RelationRef, error) {
	return nil, sqlconnect.ErrNotSupported
}

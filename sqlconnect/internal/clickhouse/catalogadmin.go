package clickhouse

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// CurrentCatalog is not supported: ClickHouse has no catalogs. It runs no SQL.
func (db *DB) CurrentCatalog(context.Context) (sqlconnect.CatalogRef, error) {
	return sqlconnect.CatalogRef{}, catalogUnsupported()
}

// ListCatalogs is not supported: ClickHouse has no catalogs. It runs no SQL.
func (db *DB) ListCatalogs(context.Context) ([]sqlconnect.CatalogRef, error) {
	return nil, catalogUnsupported()
}

package bigquery

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// ListTablesInCatalog returns tables for the given catalog (GCP project) and schema (dataset).
// Since the BigQuery client is scoped to a single project, only the current project is supported.
// Requesting a different catalog returns [sqlconnect.ErrNotSupported].
func (db *DB) ListTablesInCatalog(ctx context.Context, catalog sqlconnect.CatalogRef, schema sqlconnect.SchemaRef) ([]sqlconnect.RelationRef, error) {
	currentCatalog, err := db.CurrentCatalog(ctx)
	if err != nil {
		return nil, err
	}
	if catalog.Name != currentCatalog.Name {
		return nil, sqlconnect.ErrNotSupported
	}
	return db.ListTables(ctx, schema)
}

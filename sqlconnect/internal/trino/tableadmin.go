package trino

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// ListTables overrides the base implementation to handle nonexistent catalog gracefully
func (db *DB) ListTables(ctx context.Context, schema sqlconnect.SchemaRef, opts ...sqlconnect.Option) ([]sqlconnect.RelationRef, error) {
	tables, err := db.DB.ListTables(ctx, schema, opts...)
	if err != nil {
		if isCatalogNotFoundError(err) {
			return []sqlconnect.RelationRef{}, nil
		}
		return nil, err
	}
	return tables, nil
}

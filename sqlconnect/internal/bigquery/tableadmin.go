package bigquery

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// ListTables overrides the base implementation to filter by catalog
func (db *DB) ListTables(ctx context.Context, schema sqlconnect.SchemaRef, opts ...sqlconnect.Option) ([]sqlconnect.RelationRef, error) {
	listOpts, err := sqlconnect.NewTableListOptions(opts...)
	if err != nil {
		return nil, err
	}
	if listOpts.Catalog != "" {
		currentCatalog, err := db.CurrentCatalog(ctx)
		if err != nil {
			return nil, err
		}
		if currentCatalog.Name != listOpts.Catalog {
			return []sqlconnect.RelationRef{}, nil
		}
	}
	return db.DB.ListTables(ctx, schema, opts...)
}

// TableExists overrides the base implementation to filter by catalog
func (db *DB) TableExists(ctx context.Context, relation sqlconnect.RelationRef) (bool, error) {
	if relation.Catalog != "" {
		currentCatalog, err := db.CurrentCatalog(ctx)
		if err != nil {
			return false, err
		}
		if currentCatalog.Name != relation.Catalog {
			return false, nil
		}
	}
	return db.DB.TableExists(ctx, relation)
}

package databricks

import (
	"context"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// catalogExists checks if the given catalog exists by listing all catalogs
func (db *DB) catalogExists(ctx context.Context, catalog string) (bool, error) {
	catalogs, err := db.ListCatalogs(ctx)
	if err != nil {
		return false, err
	}
	return lo.ContainsBy(catalogs, func(c sqlconnect.CatalogRef) bool {
		return strings.EqualFold(c.Name, catalog)
	}), nil
}

// SchemaExists overrides the base implementation to handle nonexistent catalog gracefully
func (db *DB) SchemaExists(ctx context.Context, schemaRef sqlconnect.SchemaRef, opts ...sqlconnect.Option) (bool, error) {
	filterOpts, err := sqlconnect.NewFilterOptions(opts...)
	if err != nil {
		return false, err
	}
	if filterOpts.Catalog != "" {
		exists, err := db.catalogExists(ctx, filterOpts.Catalog)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
	}
	return db.DB.SchemaExists(ctx, schemaRef, opts...)
}

// ListSchemas overrides the base implementation to handle nonexistent catalog gracefully
func (db *DB) ListSchemas(ctx context.Context, opts ...sqlconnect.Option) ([]sqlconnect.SchemaRef, error) {
	filterOpts, err := sqlconnect.NewFilterOptions(opts...)
	if err != nil {
		return nil, err
	}
	if filterOpts.Catalog != "" {
		exists, err := db.catalogExists(ctx, filterOpts.Catalog)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}
	}
	return db.DB.ListSchemas(ctx, opts...)
}

// ListTables overrides the base implementation to handle nonexistent catalog gracefully
func (db *DB) ListTables(ctx context.Context, schema sqlconnect.SchemaRef, opts ...sqlconnect.Option) ([]sqlconnect.RelationRef, error) {
	listOpts, err := sqlconnect.NewTableListOptions(opts...)
	if err != nil {
		return nil, err
	}
	if listOpts.Catalog != "" {
		exists, err := db.catalogExists(ctx, listOpts.Catalog)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}
	}
	return db.DB.ListTables(ctx, schema, opts...)
}

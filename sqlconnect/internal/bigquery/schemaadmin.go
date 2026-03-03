package bigquery

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// SchemaExists uses the bigquery client instead of [INFORMATION_SCHEMA.SCHEMATA] due to absence of a region qualifier
// https://cloud.google.com/bigquery/docs/information-schema-datasets-schemata#scope_and_syntax
func (db *DB) SchemaExists(ctx context.Context, schemaRef sqlconnect.SchemaRef) (bool, error) {
	if err := db.DB.ValidateCatalog(ctx, schemaRef.Catalog); err != nil {
		return false, err
	}
	var exists bool
	if err := db.WithBigqueryClient(ctx, func(c *bigquery.Client) error {
		if _, err := c.Dataset(schemaRef.Name).Metadata(ctx); err != nil {
			var e *googleapi.Error
			if ok := errors.As(err, &e); ok {
				if e.Code == 404 { // not found
					return nil
				}
			}
			return err
		}
		exists = true
		return nil
	}); err != nil {
		return false, err
	}
	return exists, nil
}

// ListSchemas uses the bigquery client instead of [INFORMATION_SCHEMA.SCHEMATA] due to absence of a region qualifier
// https://cloud.google.com/bigquery/docs/information-schema-datasets-schemata#scope_and_syntax
func (db *DB) ListSchemas(ctx context.Context, catalog ...sqlconnect.CatalogRef) ([]sqlconnect.SchemaRef, error) {
	if len(catalog) > 1 {
		return nil, fmt.Errorf("listing schemas: at most one catalog can be provided, got %d", len(catalog))
	}
	var catalogName string
	if len(catalog) > 0 {
		catalogName = catalog[0].Name
	}
	if err := db.DB.ValidateCatalog(ctx, catalogName); err != nil {
		return nil, err
	}
	var schemas []sqlconnect.SchemaRef
	if err := db.WithBigqueryClient(ctx, func(c *bigquery.Client) error {
		datasets := c.Datasets(ctx)
		for {
			var dataset *bigquery.Dataset
			dataset, err := datasets.Next()
			if err != nil {
				if err == iterator.Done {
					return nil
				}
				return err
			}
			schemas = append(schemas, sqlconnect.SchemaRef{Name: dataset.DatasetID})
		}
	}); err != nil {
		return nil, err
	}
	return schemas, nil
}

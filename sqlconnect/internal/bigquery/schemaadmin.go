package bigquery

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// SchemaExists uses the bigquery client instead of [INFORMATION_SCHEMA.SCHEMATA] due to absence of a region qualifier
// https://cloud.google.com/bigquery/docs/information-schema-datasets-schemata#scope_and_syntax
func (db *DB) SchemaExists(ctx context.Context, schemaRef sqlconnect.SchemaRef) (bool, error) {
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
func (db *DB) ListSchemas(ctx context.Context) ([]sqlconnect.SchemaRef, error) {
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

// ListSchemasInCatalog returns schemas for the given catalog (GCP project).
// Since the BigQuery client is scoped to a single project, only the current project is supported.
// Requesting a different catalog returns [sqlconnect.ErrNotSupported].
func (db *DB) ListSchemasInCatalog(ctx context.Context, catalog sqlconnect.CatalogRef) ([]sqlconnect.SchemaRef, error) {
	currentCatalog, err := db.CurrentCatalog(ctx)
	if err != nil {
		return nil, err
	}
	if catalog.Name != currentCatalog.Name {
		return nil, sqlconnect.ErrNotSupported
	}
	return db.ListSchemas(ctx)
}

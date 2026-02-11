package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func (db *DB) CurrentCatalog(ctx context.Context) (sqlconnect.CatalogRef, error) {
	var catalogName string
	if err := db.WithBigqueryClient(ctx, func(c *bigquery.Client) error {
		catalogName = c.Project()
		return nil
	}); err != nil {
		return sqlconnect.CatalogRef{}, err
	}
	return sqlconnect.CatalogRef{Name: catalogName}, nil
}

// ListCatalogs returns the current GCP project
// Note: The BigQuery Go client (cloud.google.com/go/bigquery) is scoped to a single project
// and does not provide a method to list all accessible projects. Listing all projects would
// require the Cloud Resource Manager API with additional permissions, which is out of scope
// for a BigQuery-only connection. See: https://github.com/googleapis/google-cloud-go/issues/10044
func (db *DB) ListCatalogs(ctx context.Context) ([]sqlconnect.CatalogRef, error) {
	var res []sqlconnect.CatalogRef
	if err := db.WithBigqueryClient(ctx, func(c *bigquery.Client) error {
		// Return the current project that this client is connected to
		res = append(res, sqlconnect.CatalogRef{Name: c.Project()})
		return nil
	}); err != nil {
		return nil, err
	}
	return res, nil
}

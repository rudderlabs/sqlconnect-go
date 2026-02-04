package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func (db *DB) CurrentCatalog(ctx context.Context) (string, error) {
	var catalog string
	if err := db.WithBigqueryClient(ctx, func(c *bigquery.Client) error {
		catalog = c.Project()
		return nil
	}); err != nil {
		return "", err
	}
	return catalog, nil
}

// ListCatalogs returns all GCP projects accessible by the credentials
func (db *DB) ListCatalogs(ctx context.Context) ([]sqlconnect.CatalogRef, error) {
	var res []sqlconnect.CatalogRef
	if err := db.WithBigqueryClient(ctx, func(c *bigquery.Client) error {
		// BigQuery doesn't have a native way to list all projects
		// We can only return the current project
		res = append(res, sqlconnect.CatalogRef{Name: c.Project()})
		return nil
	}); err != nil {
		return nil, err
	}
	return res, nil
}

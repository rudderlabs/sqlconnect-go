package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
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

package driver

import (
	"errors"

	"cloud.google.com/go/bigquery"
)

type bigQueryResult struct {
	rowIterator *bigquery.RowIterator
}

func (result *bigQueryResult) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId is not supported")
}

func (result *bigQueryResult) RowsAffected() (int64, error) {
	return int64(result.rowIterator.TotalRows), nil
}

package driver

import (
	"cloud.google.com/go/bigquery"
)

type bigQuerySource interface {
	GetSchema() bigQuerySchema
	Next() ([]bigquery.Value, error)
}

type bigQueryRowIteratorSource struct {
	iterator   *bigquery.RowIterator
	prevValues map[string]bigquery.Value
	prevError  error
}

func (source *bigQueryRowIteratorSource) GetSchema() bigQuerySchema {
	return createBigQuerySchema(source.iterator.Schema)
}

func (source *bigQueryRowIteratorSource) Next() ([]bigquery.Value, error) {
	// Using a map[string]bigquery.Value instead of a []bigquery.Value for properly mapping structs.
	// If we were to use a slice, structs would be mapped as an array of values, e.g. [value1, value2, ...]
	// instead of {field1: value1, field2: value2, ...}
	var values map[string]bigquery.Value
	var err error
	if source.prevValues != nil || source.prevError != nil {
		values = source.prevValues
		err = source.prevError
		source.prevValues = nil
		source.prevError = nil
	} else {
		err = source.iterator.Next(&values)
	}
	var res []bigquery.Value
	if err != nil {
		return res, err
	}
	res = make([]bigquery.Value, len(source.iterator.Schema))
	for i, s := range source.iterator.Schema {
		res[i] = values[s.Name]
	}
	return res, err
}

func createSourceFromRowIterator(rowIterator *bigquery.RowIterator) bigQuerySource {
	source := &bigQueryRowIteratorSource{
		iterator: rowIterator,
	}
	// Call RowIterator.Next once so that calls to source.iterator.Schema will return values
	if source.iterator != nil {
		source.prevError = source.iterator.Next(&source.prevValues)
	}
	return source
}

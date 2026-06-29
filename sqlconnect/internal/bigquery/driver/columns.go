package driver

import (
	"database/sql/driver"
	"fmt"

	"cloud.google.com/go/bigquery"
)

type bigQuerySchema interface {
	ColumnNames() []string
	ConvertColumnValue(index int, value bigquery.Value) (driver.Value, error)
	ColumnTypeDatabaseTypeName(index int) string
}

type bigQueryColumns struct {
	names   []string
	columns []bigQueryColumn
}

func (columns bigQueryColumns) ConvertColumnValue(index int, value bigquery.Value) (driver.Value, error) {
	if index > -1 && len(columns.columns) > index {
		column := columns.columns[index]
		return column.ConvertValue(value)
	}

	return value, nil
}

func (columns bigQueryColumns) ColumnNames() []string {
	return columns.names
}

func (columns bigQueryColumns) ColumnTypeDatabaseTypeName(index int) string {
	if index > -1 && len(columns.columns) > index {
		column := columns.columns[index]
		if column.FieldSchema.Repeated {
			// Carry the element type (e.g. ARRAY<STRING>) so the result-set/live path matches what
			// INFORMATION_SCHEMA reports on the catalog path; element-aware type mapping (string
			// arrays → the array rudder-type) depends on it. A bare "ARRAY" collapses to json.
			return fmt.Sprintf("ARRAY<%s>", column.FieldSchema.Type)
		}
		return string(column.FieldSchema.Type)
	}

	return ""
}

type bigQueryColumn struct {
	Name        string
	FieldSchema *bigquery.FieldSchema
}

func (column bigQueryColumn) ConvertValue(value bigquery.Value) (driver.Value, error) {
	return value, nil
}

func createBigQuerySchema(schema bigquery.Schema) bigQuerySchema {
	var names []string
	var columns []bigQueryColumn
	for _, column := range schema {

		name := column.Name

		names = append(names, name)
		columns = append(columns, bigQueryColumn{
			Name:        name,
			FieldSchema: column,
		})
	}
	return &bigQueryColumns{
		names,
		columns,
	}
}

package driver

import (
	"database/sql/driver"
	"io"

	"google.golang.org/api/iterator"
)

type bigQueryRows struct {
	source bigQuerySource
	schema bigQuerySchema
}

func (rows *bigQueryRows) ensureSchema() {
	if rows.schema == nil {
		rows.schema = rows.source.GetSchema()
	}
}

func (rows *bigQueryRows) Columns() []string {
	rows.ensureSchema()
	return rows.schema.ColumnNames()
}

func (rows *bigQueryRows) Close() error {
	return nil
}

func (rows *bigQueryRows) Next(dest []driver.Value) error {
	rows.ensureSchema()

	values, err := rows.source.Next()
	if err == iterator.Done {
		return io.EOF
	}

	if err != nil {
		return err
	}

	length := len(values)
	for i := range dest {
		if i < length {
			dest[i], err = rows.schema.ConvertColumnValue(i, values[i])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (rows *bigQueryRows) ColumnTypeDatabaseTypeName(index int) string {
	rows.ensureSchema()
	return rows.schema.ColumnTypeDatabaseTypeName(index)
}

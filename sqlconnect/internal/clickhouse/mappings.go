package clickhouse

import (
	"database/sql"
	"unicode/utf8"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func columnTypeMapper(column base.ColumnType) string { return canonicalType(column.DatabaseTypeName()) }

func (db *DB) JSONRowMapper() sqlconnect.RowMapper[map[string]any] {
	return func(columns []*sql.ColumnType, row sqlconnect.RowScan) (map[string]any, error) {
		values := make([]any, len(columns))
		names := map[string]bool{}
		for i, column := range columns {
			if names[column.Name()] {
				return nil, adapterError("CH_DUPLICATE_COLUMN", "duplicate output column", nil)
			}
			if !utf8.ValidString(column.Name()) {
				return nil, adapterError("CH_VALUE_ENCODING", "invalid column name encoding", nil)
			}
			if canonicalType(column.DatabaseTypeName()) == "unsupported" {
				return nil, adapterError("CH_TYPE_UNSUPPORTED", "project unsupported columns before export", nil)
			}
			names[column.Name()] = true
			values[i] = new(sqlconnect.NilAny)
		}
		if err := row.Scan(values...); err != nil {
			return nil, err
		}
		result := make(map[string]any, len(columns))
		for i, column := range columns {
			value, err := mapValue(column.DatabaseTypeName(), values[i].(*sqlconnect.NilAny).Value)
			if err != nil {
				return nil, err
			}
			result[column.Name()] = value
		}
		return result, nil
	}
}

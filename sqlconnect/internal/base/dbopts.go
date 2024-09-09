package base

import (
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

type Option func(*DB)

// WithColumnTypeMappings sets the column type mappings for the client
func WithColumnTypeMappings(columnTypeMappings map[string]string) Option {
	return func(db *DB) {
		db.columnTypeMapper = func(c ColumnType) string {
			if mappedType, ok := columnTypeMappings[strings.ToLower(c.DatabaseTypeName())]; ok {
				return mappedType
			}
			if mappedType, ok := columnTypeMappings[strings.ToUpper(c.DatabaseTypeName())]; ok {
				return mappedType
			}
			return c.DatabaseTypeName()
		}
	}
}

// WithColumnTypeMapper sets the column type mapper for the client
func WithColumnTypeMapper(columnTypeMapper func(ColumnType) string) Option {
	return func(db *DB) {
		db.columnTypeMapper = columnTypeMapper
	}
}

// WithJsonRowMapper sets the json row mapper for the client
func WithJsonRowMapper(jsonRowMapper func(string, any) any) Option {
	return func(db *DB) {
		db.jsonRowMapper = jsonRowMapper
	}
}

// WithDialect sets the dialect for the client
func WithDialect(dialect sqlconnect.Dialect) Option {
	return func(db *DB) {
		db.Dialect = dialect
	}
}

// WithGoquDialect sets the goqu dialect for the client
func WithGoquDialect(gqd *GoquDialect) Option {
	return func(db *DB) {
		db.Dialect = &dialect{gqd}
	}
}

// WithSQLCommandsOverride allows for overriding some of the sql commands that the client uses
func WithSQLCommandsOverride(override func(defaultCommands SQLCommands) SQLCommands) Option {
	return func(db *DB) {
		db.sqlCommands = override(db.sqlCommands)
	}
}

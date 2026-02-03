package postgres

import (
	"encoding/json"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)


func init() {
	// Register dialect factory - uses base dialect implementation
	sqlconnect.RegisterDialectFactory(DatabaseType, func(optionsJSON json.RawMessage) (sqlconnect.Dialect, error) {
		return NewDialect(base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())), nil
	})
}

// NewDialect creates a new dialect using the provided GoquDialect.
// This is the standard dialect implementation suitable for PostgreSQL and similar databases.
func NewDialect(goquDialect *base.GoquDialect) sqlconnect.Dialect {
	return &base.Dialect{GoquDialect: goquDialect}
}


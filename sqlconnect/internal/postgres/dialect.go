package postgres

import (
	"encoding/json"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func init() {
	sqlconnect.RegisterDialectFactory(DatabaseType, func(optionsJSON json.RawMessage) (sqlconnect.Dialect, error) {
		return NewDialect(), nil
	})
}

// NewDialect creates a new base.Dialect using the provided GoquDialect.
func NewDialect() sqlconnect.Dialect {
	return base.Dialect{GoquDialect: base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())}
}

package postgres

import (
	"encoding/json"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func init() {
	sqlconnect.RegisterDialectFactory(DatabaseType, func(_ json.RawMessage) (sqlconnect.Dialect, error) {
		return newDialect(), nil
	})
}

// newDialect creates a new postgres dialect
func newDialect() sqlconnect.Dialect {
	return base.Dialect{GoquDialect: base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())}
}

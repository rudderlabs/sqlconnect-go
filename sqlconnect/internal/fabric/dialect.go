package fabric

import (
	"encoding/json"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func newDialect() sqlconnect.Dialect {
	return dialect{base.Dialect{GoquDialect: base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())}}
}

type dialect struct{ base.Dialect }

func identity(value string) string { return value }

// Fabric identifiers retain their case because Fabric's default collation is case-sensitive.
func (dialect) FormatTableName(name string) string { return name }

func (dialect) NormaliseIdentifier(identifier string) string {
	return base.NormaliseIdentifier(identifier, '"', identity)
}

func (dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return base.ParseRelationRef(identifier, '"', identity)
}

func init() {
	sqlconnect.RegisterDialectFactory(DatabaseType, func(_ json.RawMessage) (sqlconnect.Dialect, error) {
		return newDialect(), nil
	})
}

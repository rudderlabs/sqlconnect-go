package fabric

import (
	"encoding/json"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func newDialect() sqlconnect.Dialect {
	return dialect{base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())}
}

type dialect struct {
	*base.GoquDialect
}

func (d dialect) QuoteTable(table sqlconnect.RelationRef) string {
	if table.Catalog != "" && table.Schema != "" {
		return d.QuoteIdentifier(table.Catalog) + "." + d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	if table.Schema != "" {
		return d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	return d.QuoteIdentifier(table.Name)
}

func (dialect) QuoteIdentifier(name string) string {
	return quoteFabricIdentifier(name)
}

// Fabric identifiers retain their case because Fabric's default collation is case-sensitive.
func (dialect) FormatTableName(name string) string { return name }

func (dialect) NormaliseIdentifier(identifier string) string {
	return base.NormaliseIdentifierWithDelimiters(identifier, '[', ']', func(value string) string { return value })
}

func (dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return base.ParseRelationRefWithDelimiters(identifier, '[', ']', func(value string) string { return value })
}

func init() {
	sqlconnect.RegisterDialectFactory(DatabaseType, func(_ json.RawMessage) (sqlconnect.Dialect, error) {
		return newDialect(), nil
	})
}

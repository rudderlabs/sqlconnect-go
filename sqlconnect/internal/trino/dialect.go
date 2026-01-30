package trino

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// NewDialect returns a Trino dialect for identifier handling without requiring a DB connection.
// This is useful for SQL generation where you need proper identifier quoting and normalization.
func NewDialect() sqlconnect.Dialect {
	return dialect{base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())}
}

type dialect struct {
	*base.GoquDialect
}

// QuoteTable quotes a table name
func (d dialect) QuoteTable(table sqlconnect.RelationRef) string {
	if table.Schema != "" {
		return d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	return d.QuoteIdentifier(table.Name)
}

// QuoteIdentifier quotes an identifier, e.g. a column name
func (d dialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

// FormatTableName formats a table name, typically by lower or upper casing it, depending on the database
func (d dialect) FormatTableName(name string) string {
	return strings.ToLower(name)
}

// NormaliseIdentifier normalises all identifier parts by lower casing them.
func (d dialect) NormaliseIdentifier(identifier string) string {
	// Identifiers are not treated as case sensitive.
	// https://trino.io/docs/current/language/reserved.html#:~:text=Identifiers%20are%20not%20treated%20as%20case%20sensitive.
	return strings.ToLower(identifier)
}

// ParseRelationRef parses a string into a RelationRef after normalising the identifier and stripping out surrounding quotes.
// The result is a RelationRef with case-sensitive fields, i.e. it can be safely quoted (see [QuoteTable] and, for instance, used for matching against the database's information schema.
func (d dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return base.ParseRelationRef(strings.ToLower(identifier), '"', strings.ToLower)
}

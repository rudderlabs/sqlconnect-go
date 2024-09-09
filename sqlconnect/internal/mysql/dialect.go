package mysql

import (
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

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
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// FormatTableName formats a table name, typically by lower or upper casing it, depending on the database
func (d dialect) FormatTableName(name string) string {
	return strings.ToLower(name)
}

var identityFn = func(s string) string { return s }

// NormaliseIdentifier normalises identifier parts that are unquoted, typically by lower or upper casing them, depending on the database
func (d dialect) NormaliseIdentifier(identifier string) string {
	return base.NormaliseIdentifier(identifier, '`', identityFn)
}

// ParseRelationRef parses a string into a RelationRef after normalising the identifier and stripping out surrounding quotes.
// The result is a RelationRef with case-sensitive fields, i.e. it can be safely quoted (see [QuoteTable] and, for instance, used for matching against the database's information schema.
func (d dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return base.ParseRelationRef(identifier, '`', identityFn)
}

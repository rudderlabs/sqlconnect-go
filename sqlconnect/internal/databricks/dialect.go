package databricks

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

// NormaliseIdentifier normalises all identifier parts by lower casing them.
func (d dialect) NormaliseIdentifier(identifier string) string {
	// Identifiers are case-insensitive
	// https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html#:~:text=Identifiers%20are%20case%2Dinsensitive
	// Unity Catalog stores all object names as lowercase
	// https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html#:~:text=Unity%20Catalog%20stores%20all%20object%20names%20as%20lowercase
	return strings.ToLower(identifier)
}

// ParseRelationRef parses a string into a RelationRef after normalising the identifier and stripping out surrounding quotes.
// The result is a RelationRef with case-sensitive fields, i.e. it can be safely quoted (see [QuoteTable] and, for instance, used for matching against the database's information schema.
func (d dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return base.ParseRelationRef(strings.ToLower(identifier), '`', strings.ToLower)
}

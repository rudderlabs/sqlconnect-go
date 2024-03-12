package base

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

type dialect struct{}

// QuoteTable quotes a table name
func (d dialect) QuoteTable(table sqlconnect.RelationRef) string {
	if table.Schema != "" {
		return d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	return d.QuoteIdentifier(table.Name)
}

// QuoteIdentifier quotes an identifier, e.g. a column name
func (d dialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, name)
}

// FormatTableName formats a table name, typically by lower or upper casing it, depending on the database
func (d dialect) FormatTableName(name string) string {
	return strings.ToLower(name)
}

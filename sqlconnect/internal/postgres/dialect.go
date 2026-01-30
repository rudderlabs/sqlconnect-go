package postgres

import (
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// NewDialect returns a PostgreSQL dialect for identifier handling without requiring a DB connection.
// This is useful for SQL generation where you need proper identifier quoting and normalization.
func NewDialect() sqlconnect.Dialect {
	return &dialect{base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())}
}

// dialect wraps base.GoquDialect for PostgreSQL.
// PostgreSQL uses the default base dialect behavior (lowercase normalization, double-quote quoting).
type dialect struct {
	*base.GoquDialect
}

// QuoteTable quotes a table name
func (d *dialect) QuoteTable(table sqlconnect.RelationRef) string {
	if table.Schema != "" {
		return d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	return d.QuoteIdentifier(table.Name)
}

// QuoteIdentifier quotes an identifier, e.g. a column name
func (d *dialect) QuoteIdentifier(name string) string {
	return `"` + name + `"`
}

// FormatTableName formats a table name by lowercasing it (PostgreSQL default)
func (d *dialect) FormatTableName(name string) string {
	return base.NormaliseIdentifier(name, '"', toLower)
}

// NormaliseIdentifier normalises identifier parts that are unquoted by lowercasing them
func (d *dialect) NormaliseIdentifier(identifier string) string {
	return base.NormaliseIdentifier(identifier, '"', toLower)
}

// ParseRelationRef parses a string into a RelationRef after normalising the identifier
func (d *dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return base.ParseRelationRef(identifier, '"', toLower)
}

func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		result[i] = c
	}
	return string(result)
}

package redshift

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// newDialect returns a Redshift dialect
func newDialect(config DialectConfig) sqlconnect.Dialect {
	return dialect{
		GoquDialect:   base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions()),
		caseSensitive: config.EnableCaseSensitiveIdentifier,
	}
}

type dialect struct {
	*base.GoquDialect
	caseSensitive bool
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
	if d.caseSensitive {
		return base.NormaliseIdentifier(identifier, '"', strings.ToLower)
	}
	// ASCII letters in standard and delimited identifiers are case-insensitive and are folded to lowercase in the database
	// https://docs.aws.amazon.com/redshift/latest/dg/r_names.html#:~:text=ASCII%20letters%20in%20standard%20and%20delimited%20identifiers%20are%20case%2Dinsensitive%20and%20are%20folded%20to%20lowercase%20in%20the%20database
	return strings.ToLower(identifier)
}

// ParseRelationRef parses a string into a RelationRef after normalising the identifier and stripping out surrounding quotes.
// The result is a RelationRef with case-sensitive fields, i.e. it can be safely quoted (see [QuoteTable] and, for instance, used for matching against the database's information schema.
func (d dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	identifier = d.NormaliseIdentifier(identifier)
	return base.ParseRelationRef(identifier, '"', strings.ToLower)
}

func init() {
	sqlconnect.RegisterDialectFactory(DatabaseType, func(optionsJSON json.RawMessage) (sqlconnect.Dialect, error) {
		var config DialectConfig
		if optionsJSON != nil {
			if err := config.Parse(optionsJSON); err != nil {
				return nil, err
			}
		}
		return newDialect(config), nil
	})
}

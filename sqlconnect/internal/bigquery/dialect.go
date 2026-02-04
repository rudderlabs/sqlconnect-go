package bigquery

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// newDialect returns a BigQuery dialect
func newDialect() sqlconnect.Dialect {
	return dialect{base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())}
}

type dialect struct {
	*base.GoquDialect
}

var (
	escape   = regexp.MustCompile("('|\"|`)")
	unescape = regexp.MustCompile("\\\\('|\")")
)

// QuoteTable quotes a table name
func (d dialect) QuoteTable(table sqlconnect.RelationRef) string {
	if table.Schema != "" {
		return d.QuoteIdentifier(table.Schema + "." + table.Name)
	}
	return d.QuoteIdentifier(table.Name)
}

// QuoteIdentifier quotes an identifier, e.g. a column name
func (d dialect) QuoteIdentifier(name string) string {
	return "`" + escape.ReplaceAllString(name, "\\$1") + "`"
}

// FormatTableName formats a table name, typically by lower or upper casing it, depending on the database
func (d dialect) FormatTableName(name string) string {
	return strings.ToLower(name)
}

var identityFn = func(s string) string { return s }

// NormaliseIdentifier normalises identifier parts that are unquoted, typically by lower or upper casing them, depending on the database
func (d dialect) NormaliseIdentifier(identifier string) string {
	return escapeSpecial(base.NormaliseIdentifier(unescapeSpecial(identifier), '`', identityFn))
}

// ParseRelationRef parses a string into a RelationRef after normalising the identifier and stripping out surrounding quotes.
// The result is a RelationRef with case-sensitive fields, i.e. it can be safely quoted (see [QuoteTable] and, for instance, used for matching against the database's information schema.
func (d dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return base.ParseRelationRef(unescapeSpecial(identifier), '`', identityFn)
}

// unescapeSpecial unescapes special characters in an identifier and replaces escaped backticks with a double backtick
func unescapeSpecial(identifier string) string {
	identifier = strings.ReplaceAll(identifier, "\\`", "``")
	return unescape.ReplaceAllString(identifier, "$1")
}

// escapeSpecial escapes special characters in an identifier and replaces double backticks with an escaped backtick
func escapeSpecial(identifier string) string {
	identifier = strings.ReplaceAll(identifier, "``", "\\`")
	identifier = strings.ReplaceAll(identifier, "'", "\\'")
	identifier = strings.ReplaceAll(identifier, "\"", "\\\"")
	return identifier
}

func init() {
	sqlconnect.RegisterDialectFactory(DatabaseType, func(_ json.RawMessage) (sqlconnect.Dialect, error) {
		return newDialect(), nil
	})
}

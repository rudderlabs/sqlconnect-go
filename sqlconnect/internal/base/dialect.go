package base

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// Dialect is the standard dialect implementation suitable for PostgreSQL and similar databases.
type Dialect struct {
	*GoquDialect
}

// QuoteTable quotes a table name
func (d Dialect) QuoteTable(table sqlconnect.RelationRef) string {
	if table.Catalog != "" && table.Schema != "" {
		return d.QuoteIdentifier(table.Catalog) + "." + d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	if table.Schema != "" {
		return d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	return d.QuoteIdentifier(table.Name)
}

// QuoteIdentifier quotes an identifier, e.g. a column name
func (d Dialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

// FormatTableName formats a table name, typically by lower or upper casing it, depending on the database
func (d Dialect) FormatTableName(name string) string {
	return strings.ToLower(name)
}

// NormaliseIdentifier normalises identifier parts that are unquoted, typically by lower or upper casing them, depending on the database
func (d Dialect) NormaliseIdentifier(identifier string) string {
	return NormaliseIdentifier(identifier, '"', strings.ToLower)
}

// ParseRelationRef parses a string into a RelationRef after normalising the identifier and stripping out surrounding quotes.
// The result is a RelationRef with case-sensitive fields, i.e. it can be safely quoted (see [QuoteTable] and, for instance, used for matching against the database's information schema.
func (d Dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return ParseRelationRef(identifier, '"', strings.ToLower)
}

func ParseRelationRef(identifier string, quote rune, normF func(string) string) (sqlconnect.RelationRef, error) {
	return ParseRelationRefWithDelimiters(identifier, quote, quote, normF)
}

// ParseRelationRefWithDelimiters parses a relation reference whose quoted identifiers
// use distinct opening and closing delimiters, such as SQL Server's [identifier].
func ParseRelationRefWithDelimiters(identifier string, openQuote, closeQuote rune, normF func(string) string) (sqlconnect.RelationRef, error) {
	parts := normaliseIdentifierParts(identifier, openQuote, closeQuote, normF, true)
	switch len(parts) {
	case 1:
		return sqlconnect.RelationRef{Name: parts[0]}, nil
	case 2:
		return sqlconnect.RelationRef{Schema: parts[0], Name: parts[1]}, nil
	case 3:
		return sqlconnect.RelationRef{Catalog: parts[0], Schema: parts[1], Name: parts[2]}, nil
	default:
		return sqlconnect.RelationRef{}, fmt.Errorf("invalid relation reference: %s", identifier)
	}
}

func NormaliseIdentifier(identifier string, quote rune, normF func(string) string) string {
	return NormaliseIdentifierWithDelimiters(identifier, quote, quote, normF)
}

// NormaliseIdentifierWithDelimiters normalises only unquoted identifier parts.
func NormaliseIdentifierWithDelimiters(identifier string, openQuote, closeQuote rune, normF func(string) string) string {
	return strings.Join(normaliseIdentifierParts(identifier, openQuote, closeQuote, normF, false), ".")
}

func normaliseIdentifierParts(identifier string, openQuote, closeQuote rune, normF func(string) string, stripQuotes bool) []string {
	var (
		parts              []string
		part               strings.Builder
		inQuotedIdentifier bool
	)
	runes := []rune(identifier)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		if !inQuotedIdentifier && c == openQuote {
			inQuotedIdentifier = true
			if !stripQuotes {
				part.WriteRune(c)
			}
			continue
		}
		if inQuotedIdentifier && c == closeQuote {
			if i+1 < len(runes) && runes[i+1] == closeQuote {
				if stripQuotes {
					part.WriteRune(closeQuote)
				} else {
					part.WriteRune(closeQuote)
					part.WriteRune(closeQuote)
				}
				i++
				continue
			}
			inQuotedIdentifier = false
			if !stripQuotes {
				part.WriteRune(c)
			}
			continue
		}
		if !inQuotedIdentifier && c == '.' {
			parts = append(parts, part.String())
			part.Reset()
			continue
		}
		if inQuotedIdentifier {
			part.WriteRune(c)
		} else {
			part.WriteString(normF(string(c)))
		}
	}
	return append(parts, part.String())
}

// EscapeSqlString escapes a string for use in SQL, e.g. by doubling single quotes
func EscapeSqlString(value UnquotedIdentifier) string {
	return strings.ReplaceAll(string(value), "'", "''")
}

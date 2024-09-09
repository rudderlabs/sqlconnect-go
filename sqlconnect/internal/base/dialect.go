package base

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

type dialect struct {
	*GoquDialect
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

// NormaliseIdentifier normalises identifier parts that are unquoted, typically by lower or upper casing them, depending on the database
func (d dialect) NormaliseIdentifier(identifier string) string {
	return NormaliseIdentifier(identifier, '"', strings.ToLower)
}

// ParseRelationRef parses a string into a RelationRef after normalising the identifier and stripping out surrounding quotes.
// The result is a RelationRef with case-sensitive fields, i.e. it can be safely quoted (see [QuoteTable] and, for instance, used for matching against the database's information schema.
func (d dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	return ParseRelationRef(identifier, '"', strings.ToLower)
}

func ParseRelationRef(identifier string, quote rune, normF func(string) string) (sqlconnect.RelationRef, error) {
	normalised := doNormaliseIdentifier(identifier, quote, normF, true)
	parts := strings.Split(normalised, ".")
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
	return doNormaliseIdentifier(identifier, quote, normF, false)
}

func doNormaliseIdentifier(identifier string, quote rune, normF func(string) string, stripQuotes bool) string {
	var result strings.Builder
	var inQuotedIdentifier bool
	var inEscapedQuote bool
	next := func(input string, i int) (rune, bool) {
		runes := []rune(input)
		if len(input) > i+1 {
			return runes[i+1], true
		}
		return 0, false
	}
	for i, c := range identifier {
		if c == quote {
			if !stripQuotes {
				result.WriteRune(c)
			}
			if inQuotedIdentifier {
				if inEscapedQuote {
					inEscapedQuote = false
					if stripQuotes {
						result.WriteRune(c)
					}
				} else {
					if next, ok := next(identifier, i); ok {
						if next == quote {
							inEscapedQuote = true
						} else {
							inQuotedIdentifier = false
						}
					}
				}
			} else {
				inQuotedIdentifier = true
			}
		} else if !inQuotedIdentifier {
			result.WriteString(normF(string(c)))
		} else {
			result.WriteRune(c)
		}
	}
	return result.String()
}

// EscapeSqlString escapes a string for use in SQL, e.g. by doubling single quotes
func EscapeSqlString(value UnquotedIdentifier) string {
	return strings.ReplaceAll(string(value), "'", "''")
}

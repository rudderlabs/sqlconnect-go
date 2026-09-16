package clickhouse

import (
	"encoding/json"
	"strings"
	"unicode"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/sqlgen"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

type dialect struct{ *base.GoquDialect }

func GoquDialectOptions() *sqlgen.SQLDialectOptions {
	options := sqlgen.DefaultDialectOptions()
	options.QuoteIdentifiers = true
	options.QuoteRune = '`'
	options.UseEqForBooleanDataTypes = true
	options.TimeFunctionLiteral = "parseDateTime64BestEffort(?, 9, 'UTC')"
	options.EscapedRunes = map[rune][]byte{'\'': []byte(`\'`), '\\': []byte(`\\`), '\n': []byte(`\n`), '\r': []byte(`\r`), 0: []byte(`\0`)}
	return options
}

func newDialect() sqlconnect.Dialect {
	return dialect{base.NewGoquDialect(DatabaseType, GoquDialectOptions(), &base.Expressions{
		TimestampAdd: func(value any, interval int, unit string) goqu.Expression {
			return goqu.L("dateAdd(?, ?, ?)", unit, interval, value)
		},
		DateAdd: func(value any, interval int, unit string) goqu.Expression {
			return goqu.L("dateAdd(?, ?, toDate(?))", unit, interval, value)
		},
	})}
}

func (d dialect) QuoteIdentifier(name string) string {
	return "`" + strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(name) + "`"
}
func (d dialect) QuoteTable(ref sqlconnect.RelationRef) string {
	if ref.Schema != "" {
		return d.QuoteIdentifier(ref.Schema) + "." + d.QuoteIdentifier(ref.Name)
	}
	return d.QuoteIdentifier(ref.Name)
}
func (d dialect) FormatTableName(name string) string     { return name }
func (d dialect) NormaliseIdentifier(name string) string { return name }
func (d dialect) QueryCondition(identifier, operator string, args ...any) (sqlconnect.Expression, error) {
	if strings.ContainsRune(identifier, 0) {
		return nil, adapterError("CH_INVALID_IDENTIFIER", "NUL in identifier", nil)
	}
	// Goqu surrounds identifiers with quotes but does not escape their contents.
	quoted := d.QuoteIdentifier(identifier)
	return d.GoquDialect.QueryCondition(quoted[1:len(quoted)-1], operator, args...)
}

func (d dialect) ParseRelationRef(input string) (sqlconnect.RelationRef, error) {
	invalid := func() (sqlconnect.RelationRef, error) {
		return sqlconnect.RelationRef{}, adapterError("CH_INVALID_REFERENCE", "expected table or database.table", nil)
	}
	input = strings.TrimSpace(input)
	if input == "" || strings.ContainsRune(input, 0) {
		return invalid()
	}
	var parts []string
	for len(input) > 0 {
		var part strings.Builder
		if input[0] == '`' || input[0] == '"' {
			quote := input[0]
			input = input[1:]
			closed := false
			for len(input) > 0 {
				c := input[0]
				input = input[1:]
				if c == '\\' {
					if len(input) == 0 {
						return invalid()
					}
					part.WriteByte(input[0])
					input = input[1:]
				} else if c == quote {
					if len(input) > 0 && input[0] == quote {
						part.WriteByte(quote)
						input = input[1:]
						continue
					}
					closed = true
					break
				} else {
					part.WriteByte(c)
				}
			}
			if !closed {
				return invalid()
			}
		} else {
			i := strings.IndexByte(input, '.')
			if i < 0 {
				i = len(input)
			}
			name := strings.TrimSpace(input[:i])
			for j, r := range name {
				if !(unicode.IsLetter(r) || r == '_' || (j > 0 && unicode.IsDigit(r))) {
					return invalid()
				}
			}
			part.WriteString(name)
			input = input[i:]
		}
		if part.Len() == 0 {
			return invalid()
		}
		parts = append(parts, part.String())
		input = strings.TrimSpace(input)
		if input == "" {
			break
		}
		if len(parts) >= 2 || input[0] != '.' {
			return invalid()
		}
		input = strings.TrimSpace(input[1:])
		if input == "" {
			return invalid()
		}
	}
	if len(parts) == 1 {
		return sqlconnect.RelationRef{Name: parts[0]}, nil
	}
	return sqlconnect.RelationRef{Schema: parts[0], Name: parts[1]}, nil
}

func init() {
	goqu.RegisterDialect(DatabaseType, GoquDialectOptions())
	sqlconnect.RegisterDialectFactory(DatabaseType, func(json.RawMessage) (sqlconnect.Dialect, error) { return newDialect(), nil })
}

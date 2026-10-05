package fabric

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/dialect/sqlserver"
	"github.com/rudderlabs/goqu/v10/sqlgen"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func init() {
	goqu.RegisterDialect(DatabaseType, GoquDialectOptions())
}

func GoquDialectOptions() *sqlgen.SQLDialectOptions {
	opts := sqlserver.DialectOptions()
	// Brackets are applied by Fabric's public dialect. Keeping generated column
	// references unquoted avoids goqu's single-rune quote model mishandling ']'.
	opts.QuoteIdentifiers = false
	// SQL Server/Fabric string literals escape apostrophes by doubling them;
	// backslash escapes inherited from goqu's SQL Server dialect are not valid T-SQL.
	opts.EscapedRunes = map[rune][]byte{'\'': []byte("''")}
	opts.BooleanDataTypeSupported = true
	opts.UseEqForBooleanDataTypes = true
	return opts
}

const fabricGoquQuoteRune = '\ue000'

func fabricExpressionDialectOptions() *sqlgen.SQLDialectOptions {
	opts := GoquDialectOptions()
	opts.QuoteIdentifiers = true
	opts.QuoteRune = fabricGoquQuoteRune
	return opts
}

func fabricGoquExpressionSQL(goquExpression goqu.Expression) (string, error) {
	esg := sqlgen.NewExpressionSQLGenerator(DatabaseType, fabricExpressionDialectOptions())
	sql, _, err := sqlgen.GenerateExpressionSQL(esg, false, goquExpression)
	if err != nil {
		return "", err
	}
	return replaceFabricGoquIdentifierQuotes(sql), nil
}

func replaceFabricGoquIdentifierQuotes(sql string) string {
	var out strings.Builder
	out.Grow(len(sql))
	inString := false
	runes := []rune(sql)
	for i := 0; i < len(runes); i++ {
		current := runes[i]
		if current == '\'' {
			out.WriteRune(current)
			if inString && i+1 < len(runes) && runes[i+1] == '\'' {
				i++
				out.WriteRune(runes[i])
				continue
			}
			inString = !inString
			continue
		}
		if !inString && current == fabricGoquQuoteRune {
			var identifier strings.Builder
			for i++; i < len(runes) && runes[i] != fabricGoquQuoteRune; i++ {
				identifier.WriteRune(runes[i])
			}
			if i < len(runes) {
				out.WriteString(quoteFabricIdentifier(identifier.String()))
			} else {
				out.WriteRune(current)
				out.WriteString(identifier.String())
			}
			continue
		}
		out.WriteRune(current)
	}
	return out.String()
}

func GoquExpressions() *base.Expressions {
	return &base.Expressions{
		TimestampAdd: func(timeValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(%s, %d, ?)", strings.ToUpper(unit), interval), timeValue)
		},
		DateAdd: func(dateValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(%s, %d, CAST(? AS DATE))", strings.ToUpper(unit), interval), dateValue)
		},
	}
}

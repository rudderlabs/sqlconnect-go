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

func GoquExpressions() *base.Expressions {
	return &base.Expressions{
		CurrentDate: "CAST(CURRENT_TIMESTAMP AS DATE)",
		TimestampAdd: func(timeValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(%s, %d, ?)", strings.ToUpper(unit), interval), timeValue)
		},
		DateAdd: func(dateValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATEADD(%s, %d, CAST(? AS DATE))", strings.ToUpper(unit), interval), dateValue)
		},
	}
}

package mysql

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/exp"
	"github.com/rudderlabs/goqu/v10/sqlgen"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func GoquDialectOptions() *sqlgen.SQLDialectOptions {
	o := sqlgen.DefaultDialectOptions()
	o.QuoteIdentifiers = false
	o.SupportsReturn = false
	o.SupportsOrderByOnUpdate = true
	o.SupportsLimitOnUpdate = true
	o.SupportsLimitOnDelete = true
	o.SupportsOrderByOnDelete = true
	o.SupportsConflictUpdateWhere = false
	o.SupportsInsertIgnoreSyntax = true
	o.SupportsConflictTarget = false
	o.SupportsWithCTE = false
	o.SupportsWithCTERecursive = false
	o.SupportsDistinctOn = false
	o.SupportsWindowFunction = false
	o.SupportsDeleteTableHint = true

	o.UseFromClauseForMultipleUpdateTables = false

	o.PlaceHolderFragment = []byte("?")
	o.IncludePlaceholderNum = false
	o.QuoteRune = '`'
	o.DefaultValuesFragment = []byte("")
	o.True = []byte("1")
	o.False = []byte("0")
	o.TimeFormat = "2006-01-02 15:04:05"
	o.BooleanOperatorLookup = map[exp.BooleanOperation][]byte{
		exp.EqOp:             []byte("="),
		exp.NeqOp:            []byte("!="),
		exp.GtOp:             []byte(">"),
		exp.GteOp:            []byte(">="),
		exp.LtOp:             []byte("<"),
		exp.LteOp:            []byte("<="),
		exp.InOp:             []byte("IN"),
		exp.NotInOp:          []byte("NOT IN"),
		exp.IsOp:             []byte("IS"),
		exp.IsNotOp:          []byte("IS NOT"),
		exp.LikeOp:           []byte("LIKE BINARY"),
		exp.NotLikeOp:        []byte("NOT LIKE BINARY"),
		exp.ILikeOp:          []byte("LIKE"),
		exp.NotILikeOp:       []byte("NOT LIKE"),
		exp.RegexpLikeOp:     []byte("REGEXP BINARY"),
		exp.RegexpNotLikeOp:  []byte("NOT REGEXP BINARY"),
		exp.RegexpILikeOp:    []byte("REGEXP"),
		exp.RegexpNotILikeOp: []byte("NOT REGEXP"),
	}
	o.BitwiseOperatorLookup = map[exp.BitwiseOperation][]byte{
		exp.BitwiseInversionOp:  []byte("~"),
		exp.BitwiseOrOp:         []byte("|"),
		exp.BitwiseAndOp:        []byte("&"),
		exp.BitwiseXorOp:        []byte("^"),
		exp.BitwiseLeftShiftOp:  []byte("<<"),
		exp.BitwiseRightShiftOp: []byte(">>"),
	}
	o.EscapedRunes = map[rune][]byte{
		'\'': []byte("\\'"),
		'"':  []byte("\\\""),
		'\\': []byte("\\\\"),
		'\n': []byte("\\n"),
		'\r': []byte("\\r"),
		0:    []byte("\\x00"),
		0x1a: []byte("\\x1a"),
	}
	o.InsertIgnoreClause = []byte("INSERT IGNORE INTO")
	o.ConflictFragment = []byte("")
	o.ConflictDoUpdateFragment = []byte(" ON DUPLICATE KEY UPDATE ")
	o.ConflictDoNothingFragment = []byte("")
	return o
}

func GoquExpressions() *base.Expressions {
	return &base.Expressions{
		TimestampAdd: func(timeValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATE_ADD(?, INTERVAL %d %s)", interval, strings.ToUpper(unit)), timeValue)
		},
		DateAdd: func(dateValue any, interval int, unit string) goqu.Expression {
			return goqu.L(fmt.Sprintf("DATE_ADD(DATE(?), INTERVAL %d %s)", interval, strings.ToUpper(unit)), dateValue)
		},
	}
}

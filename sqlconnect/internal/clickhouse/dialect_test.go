package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/exp"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestSQ8_QuoteAndParse(t *testing.T) {
	d, err := sqlconnect.NewDialect("clickhouse", nil) // no credentials needed
	require.NoError(t, err)
	require.Equal(t, []string{"`a\\`b`", "`a\\\\b`", "`Ünï.cödé x`", "MixedCase", "`Db`.`T`"}, []string{
		d.QuoteIdentifier("a`b"), d.QuoteIdentifier(`a\b`),
		d.QuoteIdentifier("Ünï.cödé x"),
		newDialect().FormatTableName("MixedCase"), // the interface method is deprecated
		d.QuoteTable(sqlconnect.RelationRef{Catalog: "c", Schema: "Db", Name: "T"}),
	})
	require.Equal(t, "MixedCase", d.NormaliseIdentifier("MixedCase"))
	for in, want := range map[string]sqlconnect.RelationRef{
		"events": {Name: "events"}, "Analytics.Events": {Schema: "Analytics", Name: "Events"},
		"`Analytics`.`Events`": {Schema: "Analytics", Name: "Events"}, `"Analytics"."Events"`: {Schema: "Analytics", Name: "Events"},
		"`db.with.dot`.`t.dot`": {Schema: "db.with.dot", Name: "t.dot"}, "`a\\`b`.`c\\\\d`": {Schema: "a`b", Name: `c\d`},
		"`a``b`": {Name: "a`b"}, `"x""y".z`: {Schema: `x"y`, Name: "z"}, "`Ünï cödé`": {Name: "Ünï cödé"},
	} {
		got, err := d.ParseRelationRef(in)
		require.NoError(t, err, in)
		require.Equal(t, []string{want.Schema, want.Name, ""}, []string{got.Schema, got.Name, got.Catalog}, in)
		// A parsed reference quotes back to text that parses to the same reference.
		again, err := d.ParseRelationRef(d.QuoteTable(got))
		require.NoError(t, err, in)
		require.Equal(t, got, again, in)
	}
	for _, bad := range []string{
		"catalog.database.table", "a..b", "`open", "a.b c", ".a", "a.", "", "``", "`a`b", "a`b", `a"b`,
		"`a\\", "a b", " a", "a\x00b", "`a\x00b`", "`a`.`b`.`c`",
	} {
		_, err := d.ParseRelationRef(bad)
		requireCode(t, err, "CH_INVALID_REFERENCE")
	}
}

func TestExpressions_Operators(t *testing.T) {
	d, _ := sqlconnect.NewDialect("clickhouse", nil)
	for _, c := range []struct {
		op   string
		args []any
		want string
	}{
		{"eq", []any{"x"}, "(`c` = 'x')"},
		{"neq", []any{1}, "(`c` != 1)"},
		{"in", []any{1, 2}, "(`c` IN (1, 2))"},
		{"nin", []any{1}, "(`c` NOT IN (1))"},
		{"gt", []any{1}, "(`c` > 1)"},
		{"gte", []any{1}, "(`c` >= 1)"},
		{"lt", []any{1}, "(`c` < 1)"},
		{"lte", []any{1}, "(`c` <= 1)"},
		{"like", []any{"a%"}, "(`c` LIKE 'a%')"},
		{"nlike", []any{"a%"}, "(`c` NOT LIKE 'a%')"},
		{"btw", []any{1, 2}, "(`c` BETWEEN 1 AND 2)"},
		{"nbtw", []any{1, 2}, "(`c` NOT BETWEEN 1 AND 2)"},
		{"null", nil, "(`c` IS NULL)"},
		{"nnull", nil, "(`c` IS NOT NULL)"},
		{"inlast", []any{3, "day"}, "(`c` >= dateAdd(day, -3, today()))"},
		{"inlast", []any{2, "week"}, "(`c` >= dateAdd(day, -14, today()))"},
		{"eq", []any{`it's \ x`}, `(` + "`c`" + ` = 'it\'s \\ x')`},
	} {
		e, err := d.QueryCondition("c", c.op, c.args...)
		require.NoError(t, err, c.op)
		require.Equal(t, c.want, e.String(), c.op)
	}
	for _, bad := range []string{"a`b", `a\b`, "a\x00b", ""} {
		_, err := d.QueryCondition(bad, "eq", 1)
		requireCode(t, err, "CH_INVALID_IDENTIFIER")
	}
}

func TestInlast_CheckedArithmeticAndUnsafeNodes(t *testing.T) {
	d, _ := sqlconnect.NewDialect("clickhouse", nil)
	for _, in := range []struct {
		n    int
		unit string
	}{{math.MinInt, "day"}, {math.MaxInt/7 + 1, "week"}, {-(math.MaxInt/7 + 1), "week"}, {3, "hour"}} {
		_, err := d.QueryCondition("c", "inlast", in.n, in.unit)
		require.Error(t, err, "%d %s", in.n, in.unit)
	}
	e, err := d.QueryCondition("c", "inlast", math.MaxInt, "day")
	require.NoError(t, err, "MaxInt negates safely")
	require.Contains(t, e.String(), fmt.Sprintf("-%d", math.MaxInt))
	x, _ := newDialect().GoquDialect.ParseGoquExpression(goqu.I("x`y")) // the base parser does not validate
	_, err = d.QueryCondition("c", "eq", x)
	requireCode(t, err, "CH_INVALID_IDENTIFIER")
	_, err = d.ParseGoquExpression(goqu.I("db.t\\x"))
	requireCode(t, err, "CH_INVALID_IDENTIFIER")
}

// TestWalkIdentifiers_NestedNodes checks that identifiers nested in every
// supported goqu node are validated, and that nodes the walker cannot inspect
// are refused rather than rendered unchecked.
func TestWalkIdentifiers_NestedNodes(t *testing.T) {
	d, _ := sqlconnect.NewDialect("clickhouse", nil)
	bad := goqu.C("a`b")
	for name, e := range map[string]goqu.Expression{
		"and":       goqu.And(goqu.C("ok").Eq(1), bad.Eq(1)),
		"alias":     goqu.C("ok").As("x`y"),
		"func":      goqu.Func("lower", bad),
		"cast":      goqu.Cast(bad, "String"),
		"literal":   goqu.L("? + 1", bad),
		"range":     goqu.C("ok").Between(goqu.Range(1, bad)),
		"in":        goqu.C("ok").In(1, bad),
		"ex":        goqu.Ex{"a`b": 1},
		"exValue":   goqu.Ex{"ok": bad},
		"exOr":      goqu.ExOr{"a\\b": 1},
		"case":      goqu.Case().When(bad.Eq(1), 1),
		"caseElse":  goqu.Case().When(goqu.C("ok").Eq(1), 1).Else(bad),
		"order":     goqu.Func("f", bad.Asc()),
		"table":     goqu.T("t`x").Col("c"),
		"schema":    goqu.S("s\x00").Table("t"),
		"bitwise":   bad.BitwiseAnd(1),
		"window":    goqu.Func("row_number").Over(goqu.W().PartitionBy(bad)),
		"windowOrd": goqu.Func("row_number").Over(goqu.W().OrderBy(bad.Asc())),
	} {
		_, err := d.ParseGoquExpression(e)
		require.Error(t, err, name)
		requireCode(t, err, "CH_INVALID_IDENTIFIER")
		_, err = d.QueryCondition("c", "eq", e)
		require.Error(t, err, name)
	}
	_, err := d.ParseGoquExpression(goqu.From("t").Select("a`b"))
	requireCode(t, err, "CH_QUERY_INVALID")

	ok, err := d.ParseGoquExpression(goqu.And(goqu.I("db.t.c").Eq(1), goqu.Func("lower", goqu.C("x")).Eq("y")))
	require.NoError(t, err)
	require.Equal(t, "((`db`.`t`.`c` = 1) AND (lower(`x`) = 'y'))", ok.String())

	x := d.Expressions()
	_, err = x.TimestampAdd(bad, 1, "day")
	requireCode(t, err, "CH_INVALID_IDENTIFIER")
	_, err = x.DateAdd(bad, 1, "day")
	requireCode(t, err, "CH_INVALID_IDENTIFIER")
	_, err = x.Literal("?", bad)
	requireCode(t, err, "CH_INVALID_IDENTIFIER")
}

func TestExpressions_SharedLiteralsAndWeekOverflow(t *testing.T) {
	x := sqlconnect.Expressions(newDialect())
	ts := time.Date(2021, 1, 1, 0, 0, 0, 123456789, time.UTC)
	for want, mk := range map[string]func() (sqlconnect.Expression, error){
		"dateAdd(day, -1, now64(9, 'UTC'))": func() (sqlconnect.Expression, error) { return x.TimestampAdd("CURRENT_TIMESTAMP", -1, "day") },
		"dateAdd(day, 14, today())":         func() (sqlconnect.Expression, error) { return x.DateAdd("CURRENT_DATE", 2, "week") },
		"dateAdd(hour, 1, parseDateTime64BestEffort('2021-01-01T00:00:00.123456789Z', 9, 'UTC'))": func() (sqlconnect.Expression, error) { return x.TimestampAdd(ts, 1, "hour") },
		"dateAdd(minute, 2, now64(9, 'UTC'))":                                                     func() (sqlconnect.Expression, error) { return x.TimestampAdd("now()", 2, "minute") },
		"dateAdd(day, 1, toDate(now64(9, 'UTC')))":                                                func() (sqlconnect.Expression, error) { return x.DateAdd("CURRENT_TIMESTAMP", 1, "day") },
		"dateAdd(month, 1, toDate(parseDateTime64BestEffort('2021-01-01', 9, 'UTC')))":            func() (sqlconnect.Expression, error) { return x.DateAdd("'2021-01-01'", 1, "month") },
		"dateAdd(year, 1, toDate(parseDateTime64BestEffort('2021-01-01T00:00:00.123456789Z', 9, 'UTC')))": func() (sqlconnect.Expression, error) {
			return x.DateAdd(ts, 1, "year")
		},
		"dateAdd(second, 5, `col`)": func() (sqlconnect.Expression, error) { return x.TimestampAdd("`col`", 5, "second") },
	} {
		e, err := mk()
		require.NoError(t, err)
		require.Equal(t, want, e.String())
	}
	for _, bad := range []func() (sqlconnect.Expression, error){
		func() (sqlconnect.Expression, error) { return x.TimestampAdd(ts, math.MaxInt/7+1, "week") },
		func() (sqlconnect.Expression, error) { return x.DateAdd(ts, math.MinInt/7-1, "week") },
		func() (sqlconnect.Expression, error) { return x.DateAdd(ts, 1, "hour") }, // dates accept day, month, year
		func() (sqlconnect.Expression, error) { return x.TimestampAdd(ts, 1, "fortnight") },
	} {
		_, err := bad()
		require.Error(t, err)
	}
}

type failingValuer struct{}

func (failingValuer) Value() (driver.Value, error) { return nil, errors.New("sentinel-secret") }

// TestWalkIdentifiers_PointersAndBoundedErrors checks that pointers never hide
// a node from the walker and that no error echoes a caller value.
func TestWalkIdentifiers_PointersAndBoundedErrors(t *testing.T) {
	d, _ := sqlconnect.NewDialect("clickhouse", nil)
	x := d.Expressions()
	bad := goqu.C("a`b")
	ptrSlice := &[]any{bad}
	ptrPtr := &ptrSlice
	for name, call := range map[string]func() error{
		"condition":      func() error { _, err := d.QueryCondition("c", "in", ptrSlice); return err },
		"conditionPtr2":  func() error { _, err := d.QueryCondition("c", "eq", ptrPtr); return err },
		"conditionArray": func() error { _, err := d.QueryCondition("c", "in", [1]any{bad}); return err },
		"parse":          func() error { _, err := d.ParseGoquExpression(goqu.C("ok").In(ptrSlice)); return err },
		"literal":        func() error { _, err := x.Literal("?", ptrSlice); return err },
		"timestampAdd":   func() error { _, err := x.TimestampAdd(ptrSlice, 1, "day"); return err },
		"dateAdd":        func() error { _, err := x.DateAdd(ptrSlice, 1, "day"); return err },
		"opKey":          func() error { _, err := d.ParseGoquExpression(goqu.Ex{"ok": exp.Op{"a`b": 1}}); return err },
		"funcName":       func() error { _, err := d.ParseGoquExpression(goqu.Func("f(1) FROM t --", 1)); return err },
	} {
		err := call()
		require.Error(t, err, name)
		requireCode(t, err, "CH_INVALID_IDENTIFIER")
	}
	for name, call := range map[string]func() error{
		"struct": func() error {
			_, err := d.QueryCondition("c", "eq", struct{ Password string }{"sentinel-secret"})
			return err
		},
		"map": func() error {
			_, err := d.QueryCondition("c", "eq", map[string]string{"k": "sentinel-secret"})
			return err
		},
		"valuer":      func() error { _, err := d.QueryCondition("c", "eq", failingValuer{}); return err },
		"valuerParse": func() error { _, err := d.ParseGoquExpression(goqu.C("c").Eq(failingValuer{})); return err },
		"operator":    func() error { _, err := d.QueryCondition("c", "sentinel-secret", 1); return err },
		"unit":        func() error { _, err := d.QueryCondition("c", "inlast", 1, "sentinel-secret"); return err },
		"tsUnit":      func() error { _, err := x.TimestampAdd("now()", 1, "sentinel-secret"); return err },
	} {
		err := call()
		require.Error(t, err, name)
		requireCode(t, err, "CH_QUERY_INVALID")
		require.NotContains(t, fmt.Sprintf("%+v", err), "sentinel", name)
	}
	var nilTime *time.Time
	e, err := d.QueryCondition("c", "in", &[]any{1, "x"}, nilTime)
	require.NoError(t, err)
	require.Equal(t, "(`c` IN ((1, 'x'), NULL))", e.String())
}

type exprValuer struct{ v any }

func (e exprValuer) Value() (driver.Value, error) { return e.v, nil }

// TestWalkIdentifiers_CastsValuersUnsigned covers text that Goqu writes
// verbatim or converts on its own: cast types, Valuer results and unsigned values.
func TestWalkIdentifiers_CastsValuersUnsigned(t *testing.T) {
	d, _ := sqlconnect.NewDialect("clickhouse", nil)
	x := d.Expressions()
	for name, call := range map[string]func() error{
		"castInjection": func() error {
			_, err := d.QueryCondition("c", "eq", goqu.Cast(goqu.C("other"), "String) OR 1=1 OR (1"))
			return err
		},
		"castQuote":       func() error { _, err := d.ParseGoquExpression(goqu.Cast(goqu.C("o"), "Enum8('a' = 1)")); return err },
		"castComment":     func() error { _, err := d.ParseGoquExpression(goqu.Cast(goqu.C("o"), "String --")); return err },
		"valuerExpr":      func() error { _, err := d.QueryCondition("c", "eq", exprValuer{goqu.C("a`b")}); return err },
		"valuerStruct":    func() error { _, err := d.QueryCondition("c", "eq", exprValuer{struct{}{}}); return err },
		"valuerNested":    func() error { _, err := d.ParseGoquExpression(goqu.C("c").Eq(exprValuer{int64(1)})); return err },
		"valuerLiteral":   func() error { _, err := x.Literal("?", exprValuer{goqu.C("a`b")}); return err },
		"valuerTimestamp": func() error { _, err := x.TimestampAdd(exprValuer{goqu.C("a`b")}, 1, "day"); return err },
		"uintAboveInt64":  func() error { _, err := d.QueryCondition("id", "eq", uint64(math.MaxInt64)+1); return err },
		"uintMax":         func() error { _, err := d.QueryCondition("id", "eq", uint64(math.MaxUint64)); return err },
		"uintInSlice":     func() error { _, err := d.QueryCondition("id", "in", 1, uint(math.MaxUint)); return err },
	} {
		err := call()
		require.Error(t, err, name)
		requireCode(t, err, "CH_QUERY_INVALID")
	}
	for want, call := range map[string]func() (sqlconnect.Expression, error){
		"(`id` = 9223372036854775807)": func() (sqlconnect.Expression, error) {
			return d.QueryCondition("id", "eq", uint64(math.MaxInt64))
		},
		"(`c` = 'x')": func() (sqlconnect.Expression, error) {
			return d.QueryCondition("c", "eq", sql.NullString{String: "x", Valid: true})
		},
		"CAST(`o` AS Nullable(Decimal(18, 4)))": func() (sqlconnect.Expression, error) {
			return d.ParseGoquExpression(goqu.Cast(goqu.C("o"), "Nullable(Decimal(18, 4))"))
		},
	} {
		e, err := call()
		require.NoError(t, err, want)
		require.Equal(t, want, e.String())
	}
	e, err := d.QueryCondition("c", "eq", sql.NullString{})
	require.NoError(t, err, "an invalid NullString resolves to NULL")
	require.Equal(t, "(`c` IS NULL)", e.String())
}

type byteValuer uint8

func (b byteValuer) Value() (driver.Value, error) { return goqu.C("a` OR 1=1 --"), nil }

type countingValuer struct {
	s     string
	calls *int
}

func (c countingValuer) Value() (driver.Value, error) {
	*c.calls++
	return c.s, nil
}

// TestWalkIdentifiers_NamedByteSlices covers slices whose element kind is
// uint8 but whose element type carries its own Value method.
func TestWalkIdentifiers_NamedByteSlices(t *testing.T) {
	d, _ := sqlconnect.NewDialect("clickhouse", nil)
	_, err := d.QueryCondition("c", "in", []byteValuer{1})
	require.Error(t, err)
	requireCode(t, err, "CH_QUERY_INVALID")
	_, err = d.QueryCondition("c", "in", [1]byteValuer{1})
	require.Error(t, err)
	requireCode(t, err, "CH_QUERY_INVALID")

	e, err := d.QueryCondition("c", "eq", []byte("x"))
	require.NoError(t, err, "a plain []byte stays a value")
	require.Equal(t, "(`c` = 'x')", e.String())
}

// TestExpressions_ValuerStringsStayLiterals proves that a string produced by
// a driver.Valuer renders as an escaped literal, not as raw SQL.
func TestExpressions_ValuerStringsStayLiterals(t *testing.T) {
	d, _ := sqlconnect.NewDialect("clickhouse", nil)
	x := d.Expressions()
	payload := "now()) + (SELECT 1"
	for want, call := range map[string]func() (sqlconnect.Expression, error){
		"dateAdd(day, 1, parseDateTime64BestEffort('now()) + (SELECT 1', 9, 'UTC'))": func() (sqlconnect.Expression, error) {
			return x.TimestampAdd(sql.NullString{String: payload, Valid: true}, 1, "day")
		},
		"dateAdd(day, 1, toDate(parseDateTime64BestEffort('now()) + (SELECT 1', 9, 'UTC')))": func() (sqlconnect.Expression, error) {
			return x.DateAdd(sql.NullString{String: payload, Valid: true}, 1, "day")
		},
		"dateAdd(day, 1, parseDateTime64BestEffort('it\\'s', 9, 'UTC'))": func() (sqlconnect.Expression, error) {
			return x.TimestampAdd(sql.NullString{String: "it's", Valid: true}, 1, "day")
		},
		"dateAdd(minute, 2, now64(9, 'UTC'))": func() (sqlconnect.Expression, error) {
			return x.TimestampAdd("now()", 2, "minute") // a direct string stays raw SQL by contract
		},
	} {
		e, err := call()
		require.NoError(t, err, want)
		require.Equal(t, want, e.String())
	}

	for name, call := range map[string]func(v driver.Valuer) (sqlconnect.Expression, error){
		"timestampAdd": func(v driver.Valuer) (sqlconnect.Expression, error) { return x.TimestampAdd(v, 1, "day") },
		"dateAdd":      func(v driver.Valuer) (sqlconnect.Expression, error) { return x.DateAdd(v, 1, "day") },
	} {
		calls := 0
		_, err := call(countingValuer{s: "2021-01-01", calls: &calls})
		require.NoError(t, err, name)
		require.Equal(t, 1, calls, name+": Value runs once")
	}
}

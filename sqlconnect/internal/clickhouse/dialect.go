package clickhouse

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/rudderlabs/goqu/v10"
	"github.com/rudderlabs/goqu/v10/exp"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// DatabaseType is the registry key of the ClickHouse driver.
const DatabaseType = "clickhouse"

func init() {
	sqlconnect.RegisterDialectFactory(DatabaseType, func(json.RawMessage) (sqlconnect.Dialect, error) {
		return newDialect(), nil
	})
}

// dialect preserves identifier case: ClickHouse identifiers are case sensitive.
type dialect struct{ *base.GoquDialect }

func newDialect() dialect {
	return dialect{base.NewGoquDialect(DatabaseType, GoquDialectOptions(), GoquExpressions())}
}

var functionName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var identEscaper = strings.NewReplacer(`\`, `\\`, "`", "\\`")

// QuoteIdentifier quotes name with backticks and escapes a backslash and a backtick.
func (d dialect) QuoteIdentifier(name string) string {
	return "`" + identEscaper.Replace(name) + "`"
}

// QuoteTable quotes one or two components. ClickHouse has no catalog level,
// so the catalog is never emitted.
func (d dialect) QuoteTable(table sqlconnect.RelationRef) string {
	if table.Schema != "" {
		return d.QuoteIdentifier(table.Schema) + "." + d.QuoteIdentifier(table.Name)
	}
	return d.QuoteIdentifier(table.Name)
}

// FormatTableName returns name unchanged.
func (d dialect) FormatTableName(name string) string { return name }

// NormaliseIdentifier returns identifier unchanged.
func (d dialect) NormaliseIdentifier(identifier string) string { return identifier }

// ParseRelationRef accepts `table` or `database.table`. Each component is bare
// or one backtick or double-quoted identifier.
func (d dialect) ParseRelationRef(identifier string) (sqlconnect.RelationRef, error) {
	parts, err := splitReference(identifier)
	if err != nil {
		return sqlconnect.RelationRef{}, err
	}
	switch len(parts) {
	case 1:
		return sqlconnect.RelationRef{Name: parts[0]}, nil
	case 2:
		return sqlconnect.RelationRef{Schema: parts[0], Name: parts[1]}, nil
	}
	return sqlconnect.RelationRef{}, errInvalidReference()
}

func errInvalidReference() error {
	return cherr.New(cherr.CodeInvalidReference, "reference", "use table or database.table")
}

// splitReference scans s once. A backtick or double quote at the start of a
// component opens a quoted part; inside it a backslash escape or a doubled
// quote decodes once. A dot outside quotes ends a component.
func splitReference(s string) ([]string, error) {
	var (
		parts []string
		cur   strings.Builder
		rs    = []rune(s)
	)
	for i := 0; ; {
		cur.Reset()
		if i < len(rs) && (rs[i] == '`' || rs[i] == '"') {
			q := rs[i]
			i++
			closed := false
			for i < len(rs) {
				r := rs[i]
				switch {
				case r == 0:
					return nil, errInvalidReference()
				case r == '\\':
					if i+1 >= len(rs) || rs[i+1] == 0 {
						return nil, errInvalidReference()
					}
					cur.WriteRune(rs[i+1])
					i += 2
					continue
				case r == q && i+1 < len(rs) && rs[i+1] == q:
					cur.WriteRune(q)
					i += 2
					continue
				case r == q:
					closed = true
				default:
					cur.WriteRune(r)
				}
				i++
				if closed {
					break
				}
			}
			if !closed {
				return nil, errInvalidReference()
			}
		} else {
			for i < len(rs) && rs[i] != '.' {
				r := rs[i]
				if r == '`' || r == '"' || r == 0 || unicode.IsSpace(r) || unicode.IsControl(r) {
					return nil, errInvalidReference()
				}
				cur.WriteRune(r)
				i++
			}
		}
		if cur.Len() == 0 {
			return nil, errInvalidReference()
		}
		parts = append(parts, cur.String())
		if i == len(rs) {
			return parts, nil
		}
		if rs[i] != '.' || len(parts) == 2 {
			return nil, errInvalidReference()
		}
		i++
	}
}

// validateIdentifier refuses the characters that Goqu would write unescaped
// between backticks.
func validateIdentifier(s string) error {
	if s == "" || strings.ContainsAny(s, "`\\\x00") {
		return cherr.New(cherr.CodeInvalidIdentifier, "identifier", "the identifier is empty or contains a backtick, a backslash or NUL")
	}
	return nil
}

// goquWrapper is a sqlconnect.Expression built by any dialect.
type goquWrapper interface {
	GoquExpression() goqu.Expression
}

// walkIdentifiers calls check on every identifier text inside e and returns
// the first error. A Goqu node the walker cannot inspect, such as a subquery
// dataset rendered by its own dialect, is refused.
func walkIdentifiers(e any, check func(string) error) error {
	part := func(s string) error {
		if s == "" {
			return nil
		}
		return check(s)
	}
	switch v := e.(type) {
	case nil:
		return nil
	case goquWrapper:
		return walkIdentifiers(v.GoquExpression(), check)
	case exp.IdentifierExpression:
		if err := part(v.GetSchema()); err != nil {
			return err
		}
		if err := part(v.GetTable()); err != nil {
			return err
		}
		switch c := v.GetCol().(type) {
		case string:
			return part(c)
		default:
			return walkIdentifiers(c, check)
		}
	case exp.AliasedExpression:
		if err := walkIdentifiers(v.Aliased(), check); err != nil {
			return err
		}
		return walkIdentifiers(v.GetAs(), check)
	case exp.BooleanExpression:
		if err := walkIdentifiers(v.LHS(), check); err != nil {
			return err
		}
		return walkIdentifiers(v.RHS(), check)
	case exp.BitwiseExpression:
		if err := walkIdentifiers(v.LHS(), check); err != nil {
			return err
		}
		return walkIdentifiers(v.RHS(), check)
	case exp.RangeExpression:
		if err := walkIdentifiers(v.LHS(), check); err != nil {
			return err
		}
		return walkIdentifiers(v.RHS(), check)
	case exp.RangeVal:
		if err := walkIdentifiers(v.Start(), check); err != nil {
			return err
		}
		return walkIdentifiers(v.End(), check)
	case exp.CastExpression:
		// Goqu writes the cast type text verbatim.
		if !isCastType(v.Type().Literal()) {
			return cherr.New(cherr.CodeQueryInvalid, "cast", "a cast type must use letters, digits, underscores, commas, spaces and balanced parentheses")
		}
		if err := walkIdentifiers(v.Type().Args(), check); err != nil {
			return err
		}
		return walkIdentifiers(v.Casted(), check)
	case exp.LiteralExpression:
		return walkIdentifiers(v.Args(), check)
	case exp.SQLFunctionExpression:
		// Goqu writes the function name without quotes or escaping.
		if !functionName.MatchString(v.Name()) {
			return cherr.New(cherr.CodeInvalidIdentifier, "function", "a function name must use letters, digits and underscores")
		}
		return walkIdentifiers(v.Args(), check)
	case exp.SQLWindowFunctionExpression:
		if err := walkIdentifiers(v.Func(), check); err != nil {
			return err
		}
		if v.HasWindowName() {
			if err := walkIdentifiers(v.WindowName(), check); err != nil {
				return err
			}
		}
		if v.HasWindow() {
			return walkIdentifiers(v.Window(), check)
		}
		return nil
	case exp.WindowExpression:
		if v.HasName() {
			if err := walkIdentifiers(v.Name(), check); err != nil {
				return err
			}
		}
		if v.HasParent() {
			if err := walkIdentifiers(v.Parent(), check); err != nil {
				return err
			}
		}
		if v.HasPartitionBy() {
			if err := walkIdentifiers(v.PartitionCols(), check); err != nil {
				return err
			}
		}
		if v.HasOrder() {
			return walkIdentifiers(v.OrderCols(), check)
		}
		return nil
	case exp.OrderedExpression:
		return walkIdentifiers(v.SortExpression(), check)
	case exp.ColumnListExpression:
		return walkIdentifiers(v.Columns(), check)
	case exp.ExpressionList:
		return walkIdentifiers(v.Expressions(), check)
	case exp.CaseExpression:
		if err := walkIdentifiers(v.GetValue(), check); err != nil {
			return err
		}
		for _, w := range v.GetWhens() {
			if err := walkIdentifiers(w.Condition(), check); err != nil {
				return err
			}
			if err := walkIdentifiers(w.Result(), check); err != nil {
				return err
			}
		}
		if el := v.GetElse(); el != nil {
			return walkIdentifiers(el.Result(), check)
		}
		return nil
	case exp.Ex:
		return walkExMap(v, check)
	case exp.ExOr:
		return walkExMap(v, check)
	case exp.Op:
		for k, val := range v {
			if err := check(k); err != nil {
				return err
			}
			if err := walkIdentifiers(val, check); err != nil {
				return err
			}
		}
		return nil
	case exp.Record:
		for k, val := range v {
			if err := check(k); err != nil {
				return err
			}
			if err := walkIdentifiers(val, check); err != nil {
				return err
			}
		}
		return nil
	case exp.Expression:
		return cherr.New(cherr.CodeQueryInvalid, "expression", fmt.Sprintf("the driver cannot check a %T node", v))
	}
	return walkValue(e, check)
}

// walkValue admits the plain values that Goqu renders as escaped literals and
// walks through pointers, slices and arrays, which can carry nested nodes.
// Any other kind, such as a struct or a map, is refused: Goqu would format it
// with its field values, and the walker cannot see inside it.
func walkValue(e any, check func(string) error) error {
	switch e.(type) {
	case time.Time, *time.Time:
		return nil
	case driver.Valuer:
		// Goqu calls Value again at render time, so a checked result could
		// differ from the rendered one. The entry points resolve top-level
		// arguments once (see resolveValuers); a nested Valuer is refused.
		return cherr.New(cherr.CodeQueryInvalid, "expression", "resolve a driver.Valuer to a plain value before building the expression")
	}
	rv := reflect.ValueOf(e)
	switch rv.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64:
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		// Goqu converts unsigned values through int64.
		if rv.Uint() > math.MaxInt64 {
			return cherr.New(cherr.CodeQueryInvalid, "expression", "an unsigned value exceeds the Int64 range: pass it as a string")
		}
		return nil
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		return walkIdentifiers(rv.Elem().Interface(), check)
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return nil // []byte and byte arrays
		}
		for i := range rv.Len() {
			if err := walkIdentifiers(rv.Index(i).Interface(), check); err != nil {
				return err
			}
		}
		return nil
	}
	return cherr.New(cherr.CodeQueryInvalid, "expression", "the driver cannot render a value of kind "+rv.Kind().String())
}

// isCastType admits a type name with optional balanced parentheses that hold
// only letters, digits, underscores, commas and spaces.
func isCastType(s string) bool {
	if s == "" {
		return false
	}
	depth := 0
	for _, r := range s {
		switch {
		case r == '(':
			depth++
		case r == ')':
			depth--
			if depth < 0 {
				return false
			}
		case r == '_' || r == ',' || r == ' ' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
		default:
			return false
		}
	}
	return depth == 0
}

// resolveValuers calls Value once on each top-level driver.Valuer argument and
// substitutes the result, so the value that is checked is the value that
// renders. The result must be a plain driver value.
func resolveValuers(args []any) ([]any, error) {
	out := slices.Clone(args)
	for i, a := range out {
		v, ok := a.(driver.Valuer)
		if !ok {
			continue
		}
		if rv := reflect.ValueOf(a); rv.Kind() == reflect.Pointer && rv.IsNil() {
			out[i] = nil
			continue
		}
		val, err := v.Value()
		if err != nil {
			return nil, cherr.New(cherr.CodeQueryInvalid, "expression", "a driver.Valuer argument returned an error")
		}
		switch val.(type) {
		case nil, int64, float64, bool, []byte, string, time.Time:
		default:
			return nil, cherr.New(cherr.CodeQueryInvalid, "expression", "a driver.Valuer argument returned a value that is not a plain driver value")
		}
		out[i] = val
	}
	return out, nil
}

// walkExMap checks the column keys and values of a goqu.Ex or goqu.ExOr map.
func walkExMap[M ~map[string]any](m M, check func(string) error) error {
	for k, val := range m {
		if err := check(k); err != nil {
			return err
		}
		if err := walkIdentifiers(val, check); err != nil {
			return err
		}
	}
	return nil
}

// QueryCondition checks the identifier and every Goqu node in the arguments
// before it renders. The base engine checks neither.
func (d dialect) QueryCondition(identifier, operator string, args ...any) (sqlconnect.Expression, error) {
	if err := validateIdentifier(identifier); err != nil {
		return nil, err
	}
	args, err := resolveValuers(args)
	if err != nil {
		return nil, err
	}
	if err := walkIdentifiers(args, validateIdentifier); err != nil {
		return nil, err
	}
	if strings.EqualFold(operator, "inlast") {
		return d.inlast(identifier, args...)
	}
	return bounded(d.GoquDialect.QueryCondition(identifier, operator, args...))
}

// inlast checks the negation and the week multiplication that the base engine
// performs unchecked.
func (d dialect) inlast(identifier string, args ...any) (sqlconnect.Expression, error) {
	if len(args) != 2 {
		return nil, cherr.New(cherr.CodeQueryInvalid, "inlast", "inlast takes exactly two arguments")
	}
	n, ok1 := args[0].(int)
	unit, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return nil, cherr.New(cherr.CodeQueryInvalid, "inlast", "inlast takes an integer interval and a string unit")
	}
	if n == math.MinInt {
		return nil, errIntervalOverflow()
	}
	switch unit {
	case "day", "month", "year":
	case "week":
		if err := checkedWeeks(n, unit); err != nil {
			return nil, err
		}
		n, unit = n*7, "day"
	default:
		return nil, cherr.New(cherr.CodeQueryInvalid, "inlast", "inlast accepts the units day, week, month and year")
	}
	return bounded(d.GoquDialect.ParseGoquExpression(goqu.C(identifier).Gte(goqu.L(fmt.Sprintf("dateAdd(%s, %d, today())", unit, -n)))))
}

func errIntervalOverflow() error {
	return cherr.New(cherr.CodeQueryInvalid, "interval", "the interval overflows")
}

// checkedWeeks refuses a week interval whose conversion to days overflows int.
func checkedWeeks(n int, unit string) error {
	if unit == "week" && (n > math.MaxInt/7 || n < math.MinInt/7) {
		return errIntervalOverflow()
	}
	return nil
}

// ParseGoquExpression checks every identifier node before it renders.
func (d dialect) ParseGoquExpression(e sqlconnect.GoquExpression) (sqlconnect.Expression, error) {
	if err := walkIdentifiers(e, validateIdentifier); err != nil {
		return nil, err
	}
	return bounded(d.GoquDialect.ParseGoquExpression(e))
}

// bounded replaces a base or Goqu error with a fixed adapter error. Their text
// can hold the caller's values, for example a struct formatted with %+v.
func bounded(e sqlconnect.Expression, err error) (sqlconnect.Expression, error) {
	if err == nil {
		return e, nil
	}
	var ce *cherr.Error
	if errors.As(err, &ce) {
		return nil, err
	}
	return nil, cherr.New(cherr.CodeQueryInvalid, "expression", "the expression cannot be rendered: check the operator, the argument count, the unit and the value types")
}

// Expressions returns the dialect itself so that the checked methods apply.
func (d dialect) Expressions() sqlconnect.Expressions { return d }

// TimestampAdd checks the week conversion and the Goqu nodes in the value.
func (d dialect) TimestampAdd(timeValue any, interval int, unit string) (sqlconnect.Expression, error) {
	if err := checkedWeeks(interval, unit); err != nil {
		return nil, err
	}
	resolved, err := resolveValuers([]any{timeValue})
	if err != nil {
		return nil, err
	}
	timeValue = resolved[0]
	if err := walkIdentifiers(timeValue, validateIdentifier); err != nil {
		return nil, err
	}
	return bounded(d.GoquDialect.TimestampAdd(timeValue, interval, unit))
}

// DateAdd checks the week conversion and the Goqu nodes in the value.
func (d dialect) DateAdd(dateValue any, interval int, unit string) (sqlconnect.Expression, error) {
	if err := checkedWeeks(interval, unit); err != nil {
		return nil, err
	}
	resolved, err := resolveValuers([]any{dateValue})
	if err != nil {
		return nil, err
	}
	dateValue = resolved[0]
	if err := walkIdentifiers(dateValue, validateIdentifier); err != nil {
		return nil, err
	}
	return bounded(d.GoquDialect.DateAdd(dateValue, interval, unit))
}

// Literal checks the Goqu nodes in the arguments. The SQL text itself is raw by contract.
func (d dialect) Literal(sql string, args ...any) (sqlconnect.Expression, error) {
	args, err := resolveValuers(args)
	if err != nil {
		return nil, err
	}
	if err := walkIdentifiers(args, validateIdentifier); err != nil {
		return nil, err
	}
	return bounded(d.GoquDialect.Literal(sql, args...))
}

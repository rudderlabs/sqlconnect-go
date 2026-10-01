package clickhouse

import (
	"bytes"
	"database/sql"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

// JSONRowMapper replaces the base mapper, whose value callback cannot report
// an error. It returns an error instead of a partial row.
func (db *DB) JSONRowMapper() sqlconnect.RowMapper[map[string]any] {
	return func(cols []*sql.ColumnType, row sqlconnect.RowScan) (map[string]any, error) {
		types := make([]chType, len(cols))
		seen := make(map[string]int, len(cols))
		for i, c := range cols {
			if first, ok := seen[c.Name()]; ok {
				return nil, cherr.New(cherr.CodeDuplicateColumn, "",
					"columns "+position(first)+" and "+position(i)+" of the result have the same name; give each column a distinct alias")
			}
			seen[c.Name()] = i
			t, err := parseType(c.DatabaseTypeName())
			if err != nil || label(t) == "unsupported" {
				return nil, cherr.New(cherr.CodeTypeUnsupported, "", "column "+position(i)+": the column type is not supported; project this column with an explicit cast")
			}
			types[i] = t
		}
		values := make([]any, len(cols))
		for i := range values {
			values[i] = new(sqlconnect.NilAny)
		}
		if err := row.Scan(values...); err != nil {
			return nil, bound("scan", "", err)
		}
		out := make(map[string]any, len(cols))
		for i, c := range cols {
			v, err := jsonValue(types[i], values[i].(*sqlconnect.NilAny).Value)
			if err != nil {
				return nil, columnError(i, err)
			}
			out[c.Name()] = v
		}
		return out, nil
	}
}

// position names a result column by its 1-based position. Errors never carry
// the column name: an unaliased expression can hold a customer literal.
func position(i int) string { return strconv.Itoa(i + 1) }

func columnError(i int, err error) error {
	code, detail := cherr.CodeValueEncoding, "the value cannot be exported"
	var ce *cherr.Error
	if errors.As(err, &ce) {
		code, detail = ce.Code, ce.Detail
	}
	return wrap(code, "", "column "+position(i)+": "+detail, err)
}

func errEncoding(detail string) error { return cherr.New(cherr.CodeValueEncoding, "", detail) }

// jsonValue converts one scanned value of type t without loss. A value that
// cannot be exported exactly is an error, never an approximation.
func jsonValue(t chType, v any) (any, error) {
	// big.Int first: the fork returns it by value (lib/column/bigint.go:46),
	// and Text is on *big.Int only.
	switch b := v.(type) {
	case big.Int:
		return b.Text(10), nil
	case *big.Int:
		if b == nil {
			return nil, nil //nolint:nilnil // a SQL NULL exports as JSON null
		}
		return b.Text(10), nil
	case **big.Int:
		if b == nil || *b == nil {
			return nil, nil //nolint:nilnil // a SQL NULL exports as JSON null
		}
		return (*b).Text(10), nil
	}
	if u := t.unwrap(); u.Name == "JSON" || u.Name == "Object" {
		// Before deref: the fork's *chcol.JSON has MarshalJSON on the pointer
		// only, and its dereferenced struct marshals to {}.
		return jsonNumbers(v)
	}
	v = deref(v)
	if v == nil {
		return nil, nil //nolint:nilnil // a SQL NULL exports as JSON null
	}
	t = t.unwrap()
	switch t.Name {
	case "Bool":
		if _, ok := v.(bool); !ok {
			return nil, errEncoding("unexpected Bool value type")
		}
		return v, nil
	case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32":
		switch reflect.ValueOf(v).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			return json.Number(fmt.Sprint(v)), nil
		}
		return nil, errEncoding("unexpected integer value type")
	case "UInt64":
		u, ok := v.(uint64)
		if !ok {
			return nil, errEncoding("unexpected UInt64 value type")
		}
		return strconv.FormatUint(u, 10), nil
	case "Int128", "Int256", "UInt128", "UInt256":
		return nil, errEncoding("unexpected wide integer value type") // big.Int is handled above
	case "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		scale, err := decimalScale(t)
		if err != nil {
			return nil, err
		}
		return decimalText(v, scale)
	case "Float32", "Float64":
		rv := reflect.ValueOf(v)
		bits := 64
		switch rv.Kind() {
		case reflect.Float32:
			bits = 32
		case reflect.Float64:
		default:
			return nil, errEncoding("unexpected float value type")
		}
		f := rv.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, nil //nolint:nilnil // JSON has no NaN or Inf; they export as JSON null
		}
		return json.Number(strconv.FormatFloat(f, 'g', -1, bits)), nil
	case "String", "FixedString":
		s, ok := asString(v)
		if !ok {
			return nil, errEncoding("unexpected string value type")
		}
		if !utf8.ValidString(s) {
			return nil, errEncoding("the value is not valid UTF-8; project hex(column)")
		}
		return s, nil
	case "Date", "Date32":
		// Rebuild midnight UTC from the calendar fields. Calling .UTC() on
		// the value shifts the day on a server east of UTC.
		tm, ok := v.(time.Time)
		if !ok {
			return nil, errEncoding("unexpected date value type")
		}
		return time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, time.UTC).Format(time.RFC3339), nil
	case "DateTime", "DateTime64":
		tm, ok := v.(time.Time)
		if !ok {
			return nil, errEncoding("unexpected date-time value type")
		}
		return tm.UTC().Format(time.RFC3339Nano), nil
	case "Time", "Time64":
		d, ok := v.(time.Duration)
		if !ok {
			return nil, errEncoding("unexpected time value type")
		}
		return formatTimeOfDay(d, t), nil
	case "Enum8", "Enum16", "UUID", "IPv4", "IPv6":
		s := fmt.Sprint(v)
		if !utf8.ValidString(s) {
			return nil, errEncoding("the value is not valid UTF-8")
		}
		return s, nil
	case "SimpleAggregateFunction":
		return jsonValue(t.Children[0], v)
	case "Array":
		return mapSlice(t.Children[0], v)
	case "Nested":
		return mapNested(t, v)
	case "Tuple":
		return mapTuple(t, v)
	}
	return nil, cherr.New(cherr.CodeTypeUnsupported, "", "the column type is not supported")
}

// deref follows pointers and interfaces. A nil pointer becomes nil.
func deref(v any) any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	return rv.Interface()
}

// asString returns a string, or a copy of a byte slice.
func asString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(bytes.Clone(s)), true
	}
	return "", false
}

// decimalScale reads S from Decimal(P, S) or DecimalN(S).
func decimalScale(t chType) (int, error) {
	i := 0
	if t.Name == "Decimal" {
		i = 1
	}
	if len(t.Args) <= i {
		return 0, errEncoding("the decimal type has no scale")
	}
	s, err := strconv.Atoi(t.Args[i])
	if err != nil || s < 0 || s > 76 {
		return 0, errEncoding("the decimal type has an invalid scale")
	}
	return s, nil
}

// decimalText returns fixed-point text with exactly scale fraction digits.
// A top-level column arrives as the Valuer string of decimal.Decimal, which
// drops trailing zeros; an element of a container arrives as decimal.Decimal.
func decimalText(v any, scale int) (any, error) {
	switch d := v.(type) {
	case interface{ StringFixed(int32) string }:
		return d.StringFixed(int32(scale)), nil
	case string:
		return padDecimal(d, scale)
	}
	return nil, errEncoding("unexpected decimal value type")
}

// padDecimal pads plain decimal text to scale fraction digits. It refuses text
// that would need rounding or that is not plain digits.
func padDecimal(s string, scale int) (string, error) {
	sign, body := "", s
	if strings.HasPrefix(body, "-") {
		sign, body = "-", body[1:]
	}
	intPart, frac, _ := strings.Cut(body, ".")
	if intPart == "" || !allDigits(intPart) || !allDigits(frac) || len(frac) > scale {
		return "", errEncoding("the decimal value is not exact at the declared scale")
	}
	if scale == 0 {
		return sign + intPart, nil
	}
	return sign + intPart + "." + frac + strings.Repeat("0", scale-len(frac)), nil
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// formatTimeOfDay prints HH:MM:SS, with the Time64 precision as fraction.
func formatTimeOfDay(d time.Duration, t chType) string {
	neg := d < 0
	n := uint64(d)
	if neg {
		n = -n // unsigned negation also covers math.MinInt64
	}
	sec := n / uint64(time.Second)
	s := fmt.Sprintf("%02d:%02d:%02d", sec/3600, (sec/60)%60, sec%60)
	if t.Name == "Time64" && len(t.Args) == 1 {
		if p, err := strconv.Atoi(t.Args[0]); err == nil && p > 0 {
			p = min(p, 9)
			s += "." + fmt.Sprintf("%09d", n%uint64(time.Second))[:p]
		}
	}
	if neg {
		s = "-" + s
	}
	return s
}

// jsonNumbers decodes a JSON column value with UseNumber, so numbers keep
// their exact text. The fork sends the JSON text, or a *chcol.JSON object
// that normalizeJSONValue turns into plain JSON values first.
func jsonNumbers(v any) (any, error) {
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, nil //nolint:nilnil // a SQL NULL exports as JSON null
		}
		if k := rv.Elem().Kind(); k == reflect.String || (k == reflect.Slice && rv.Elem().Type().Elem().Kind() == reflect.Uint8) {
			v = rv.Elem().Interface()
		}
	}
	var raw []byte
	switch s := v.(type) {
	case nil:
		return nil, nil //nolint:nilnil // a SQL NULL exports as JSON null
	case string:
		raw = []byte(s)
	case []byte:
		raw = bytes.Clone(s)
	default:
		n, err := normalizeJSONValue(v)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(n)
		if err != nil {
			return nil, errEncoding("the JSON value cannot be encoded")
		}
		raw = b
	}
	if !utf8.Valid(raw) {
		return nil, errEncoding("the JSON value is not valid UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, errEncoding("the JSON value cannot be decoded")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, errEncoding("the JSON value has trailing data")
	}
	return out, nil
}

// normalizeJSONValue turns one value of the fork's JSON object into plain
// JSON values: nil, bool, string, json.Number, []any and map[string]any.
// json.Marshal must never see a fork type: it prints a big.Int or a
// chcol.JSON value as {}, a []uint8 as base64, and it replaces invalid UTF-8
// with U+FFFD. Any other struct is refused, never printed as {}.
func normalizeJSONValue(v any) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil // a JSON null stays null
	}
	switch x := v.(type) {
	case json.Number:
		return x, nil
	case string:
		if !utf8.ValidString(x) {
			return nil, errEncoding("a JSON string is not valid UTF-8")
		}
		return x, nil
	case bool:
		return x, nil
	case big.Int:
		return x.Text(10), nil
	case *big.Int:
		if x == nil {
			return nil, nil //nolint:nilnil // a JSON null stays null
		}
		return x.Text(10), nil
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), nil
	case time.Duration:
		return nil, cherr.New(cherr.CodeTypeUnsupported, "", "a JSON path of a time-of-day type is not supported; project it with an explicit cast")
	case interface{ NestedMap() map[string]any }: // *chcol.JSON
		if isNilPointer(v) {
			return nil, nil //nolint:nilnil // a JSON null stays null
		}
		return normalizeJSONValue(x.NestedMap())
	case interface {
		Nil() bool
		Any() any
	}: // chcol.Dynamic and chcol.Variant
		if isNilPointer(v) || x.Nil() {
			return nil, nil //nolint:nilnil // a JSON null stays null
		}
		return normalizeJSONValue(x.Any())
	case interface {
		StringFixed(int32) string
		String() string
	}: // decimal.Decimal: the object carries no scale, so the text is exact but unpadded
		return x.String(), nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer && rv.Kind() != reflect.Interface {
		// A method set on the pointer only, as on chcol.JSON, needs an
		// addressable copy.
		p := reflect.New(rv.Type())
		p.Elem().Set(rv)
		if _, ok := p.Interface().(interface{ NestedMap() map[string]any }); ok {
			return normalizeJSONValue(p.Interface())
		}
	}
	if tm, ok := v.(encoding.TextMarshaler); ok { // uuid.UUID, net.IP, netip.Addr
		if isNilPointer(v) {
			return nil, nil //nolint:nilnil // a JSON null stays null
		}
		b, err := tm.MarshalText()
		if err != nil {
			return nil, errEncoding("a JSON value cannot be printed as text")
		}
		return normalizeJSONValue(string(b))
	}
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil, nil //nolint:nilnil // a JSON null stays null
		}
		return normalizeJSONValue(rv.Elem().Interface())
	case reflect.String:
		return normalizeJSONValue(rv.String())
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return json.Number(strconv.FormatInt(rv.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return json.Number(strconv.FormatUint(rv.Uint(), 10)), nil
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, nil //nolint:nilnil // JSON has no NaN or Inf; they export as JSON null
		}
		bits := 64
		if rv.Kind() == reflect.Float32 {
			bits = 32
		}
		return json.Number(strconv.FormatFloat(f, 'g', -1, bits)), nil
	case reflect.Slice, reflect.Array: // also []uint8: numbers, never base64
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			return []any{}, nil
		}
		out := make([]any, rv.Len())
		for i := range out {
			e, err := normalizeJSONValue(rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			out[i] = e
		}
		return out, nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, cherr.New(cherr.CodeTypeUnsupported, "", "a JSON map with non-string keys is not supported")
		}
		out := make(map[string]any, rv.Len())
		for it := rv.MapRange(); it.Next(); {
			k := it.Key().String()
			if !utf8.ValidString(k) {
				return nil, errEncoding("a JSON key is not valid UTF-8")
			}
			e, err := normalizeJSONValue(it.Value().Interface())
			if err != nil {
				return nil, err
			}
			out[k] = e
		}
		return out, nil
	}
	return nil, cherr.New(cherr.CodeTypeUnsupported, "", "a JSON value of this type is not supported; project it with an explicit cast")
}

func isNilPointer(v any) bool {
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

// mapSlice maps each element with jsonValue. It keeps order and empty arrays
// and returns the first error with no partial value.
func mapSlice(elem chType, v any) (any, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, errEncoding("unexpected array value type")
	}
	out := make([]any, rv.Len())
	for i := range out {
		m, err := jsonValue(elem, rv.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		out[i] = m
	}
	return out, nil
}

// mapTuple returns an object keyed by field name for a named tuple and an
// array for an unnamed one.
func mapTuple(t chType, v any) (any, error) {
	rv := reflect.ValueOf(v)
	if len(t.Fields) > 0 {
		if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String || rv.Len() != len(t.Fields) {
			return nil, errEncoding("unexpected named tuple value type")
		}
		out := make(map[string]any, len(t.Fields))
		for _, f := range t.Fields {
			item := rv.MapIndex(reflect.ValueOf(f.Name).Convert(rv.Type().Key()))
			if !item.IsValid() {
				return nil, errEncoding("the named tuple value misses a field")
			}
			m, err := jsonValue(f.Type, item.Interface())
			if err != nil {
				return nil, err
			}
			out[f.Name] = m
		}
		return out, nil
	}
	if (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) || rv.Len() != len(t.Children) {
		return nil, errEncoding("unexpected tuple value type")
	}
	out := make([]any, len(t.Children))
	for i, c := range t.Children {
		m, err := jsonValue(c, rv.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		out[i] = m
	}
	return out, nil
}

// mapNested maps a flatten_nested=0 column: the fork reads Nested(...) as
// Array(Tuple(...)), so the value is an array of objects. With
// flatten_nested=1 the server sends dotted Array columns instead, which the
// Array branch maps.
func mapNested(t chType, v any) (any, error) {
	return mapSlice(chType{Name: "Tuple", Fields: t.Fields}, v)
}

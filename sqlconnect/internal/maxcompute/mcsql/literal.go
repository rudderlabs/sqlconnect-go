package mcsql

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	// ErrUnsupportedValue is returned for a value the renderer cannot render
	// safely. The error never prints the value.
	ErrUnsupportedValue = errors.New("unsupported bind value")
	// ErrTimeValue is returned for a time.Time: without a time-zone hint a
	// string cast to TIMESTAMP is read in the project's zone, so no literal form
	// is exact yet (LLD section 5, open for v1.1).
	ErrTimeValue = errors.New("time.Time bind values are not supported: format the value as text")
	// ErrInvalidUTF8 is returned for a string that is not valid UTF-8.
	ErrInvalidUTF8 = errors.New("string bind value is not valid UTF-8")
	// ErrNonFinite is returned for NaN and the infinities.
	ErrNonFinite = errors.New("non-finite float bind value")
)

// RenderLiteral renders v as a MaxCompute SQL literal. MaxCompute has no
// server-side binds, so every bind argument and every literal a caller builds
// into SQL text goes through here.
//
//   - nil: NULL
//   - string: '…' with \ → \\ and ' → \'. MaxCompute reads 'it”s' as two
//     adjacent literals, so ” doubling is never used. Control characters,
//     for which \u is not an escape, are spliced in with CHR(n).
//   - integers: decimal digits, with an L suffix outside the INT range; a
//     negative number is parenthesised
//   - floats: the shortest exact form, with .0 or an exponent so it stays DOUBLE
//   - bool: TRUE or FALSE
//   - []byte: UNHEX('…')
//
// Anything else, time.Time included, is refused; nothing is rendered with %v.
func RenderLiteral(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return renderString(x)
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case int:
		return renderInt(int64(x)), nil
	case int8:
		return renderInt(int64(x)), nil
	case int16:
		return renderInt(int64(x)), nil
	case int32:
		return renderInt(int64(x)), nil
	case int64:
		return renderInt(x), nil
	case uint:
		return renderUint(uint64(x))
	case uint8:
		return renderUint(uint64(x))
	case uint16:
		return renderUint(uint64(x))
	case uint32:
		return renderUint(uint64(x))
	case uint64:
		return renderUint(x)
	case float32:
		return renderFloat(float64(x), 32)
	case float64:
		return renderFloat(x, 64)
	case []byte:
		if x == nil {
			return "NULL", nil
		}
		return "UNHEX('" + strings.ToUpper(hex.EncodeToString(x)) + "')", nil
	case time.Time, *time.Time:
		return "", ErrTimeValue
	default:
		return "", fmt.Errorf("%w: %T", ErrUnsupportedValue, v)
	}
}

func renderInt(v int64) string {
	s := strconv.FormatInt(v, 10)
	if v > math.MaxInt32 || v < math.MinInt32 {
		s += "L"
	}
	return parenthesiseNegative(s)
}

// parenthesiseNegative wraps a negative number, so that "a -?" cannot become
// "a --1", a line comment that swallows the rest of the statement.
func parenthesiseNegative(s string) string {
	if strings.HasPrefix(s, "-") {
		return "(" + s + ")"
	}
	return s
}

func renderUint(v uint64) (string, error) {
	if v > math.MaxInt64 {
		return "", fmt.Errorf("%w: uint64 above the BIGINT range", ErrUnsupportedValue)
	}
	return renderInt(int64(v)), nil
}

func renderFloat(v float64, bits int) (string, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "", ErrNonFinite
	}
	s := strconv.FormatFloat(v, 'g', -1, bits)
	if !strings.ContainsAny(s, ".e") {
		s += ".0"
	}
	return parenthesiseNegative(s), nil
}

func isControl(r rune) bool { return r < 0x20 || r == 0x7f }

func renderString(s string) (string, error) {
	if !utf8.ValidString(s) {
		return "", ErrInvalidUTF8
	}
	var parts []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			parts = append(parts, "'"+cur.String()+"'")
			cur.Reset()
		}
	}
	for _, r := range s {
		switch {
		case r == '\\':
			cur.WriteString(`\\`)
		case r == '\'':
			cur.WriteString(`\'`)
		case isControl(r):
			flush()
			parts = append(parts, "CHR("+strconv.Itoa(int(r))+")")
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	switch len(parts) {
	case 0:
		return "''", nil
	case 1:
		return parts[0], nil
	default:
		return "CONCAT(" + strings.Join(parts, ", ") + ")", nil
	}
}

// ErrPlaceholderCount is returned when the number of ? placeholders and of
// bind arguments differ.
var ErrPlaceholderCount = errors.New("the number of ? placeholders and bind arguments differ")

// Bind replaces each ? placeholder outside strings, backtick identifiers and
// comments with the rendered literal of the matching argument.
func Bind(sql string, args []any) (string, error) {
	tokens, err := Scan(sql)
	if err != nil {
		return "", err
	}
	var placeholders []Token
	for _, t := range tokens {
		if t.Kind == Placeholder {
			placeholders = append(placeholders, t)
		}
	}
	if len(placeholders) != len(args) {
		return "", ErrPlaceholderCount
	}
	if len(args) == 0 {
		return sql, nil
	}
	var b strings.Builder
	last := 0
	for i, p := range placeholders {
		lit, err := RenderLiteral(args[i])
		if err != nil {
			return "", fmt.Errorf("bind argument %d: %w", i+1, err)
		}
		b.WriteString(sql[last:p.Start])
		b.WriteString(lit)
		last = p.End
	}
	b.WriteString(sql[last:])
	return b.String(), nil
}

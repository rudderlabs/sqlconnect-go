package clickhouse

import (
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/clickhouse-go/v2/lib/chcol"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

func mustType(t *testing.T, raw string) chType {
	t.Helper()
	ty, err := parseType(raw)
	require.NoError(t, err)
	return ty
}

func TestJSONValue_Scalars(t *testing.T) {
	var nilBig *big.Int
	for _, c := range []struct {
		raw  string
		in   any
		want any
	}{
		{"Float32", float32(0.1), json.Number("0.1")}, // no float64 widening noise
		{"Float64", math.Inf(-1), nil},
		{"Nullable(UInt256)", nilBig, nil},
		{"Decimal(9, 3)", "-7", "-7.000"},
		{"Decimal32(0)", "42", "42"},
		{"Time", -time.Duration(math.MaxInt64) - 1, "-2562047:47:16"},
		{"Time64(3)", 90*time.Minute + 1500*time.Microsecond, "01:30:00.001"},
		{"JSON", `{"a":12345678901234567890}`, map[string]any{"a": json.Number("12345678901234567890")}},
		{"Nullable(String)", (*string)(nil), nil},
		{"String", []byte("ab"), "ab"},
	} {
		got, err := jsonValue(mustType(t, c.raw), c.in)
		require.NoError(t, err, c.raw)
		require.Equal(t, c.want, got, c.raw)
	}
}

func TestJSONValue_Refusals(t *testing.T) {
	for _, c := range []struct {
		raw string
		in  any
	}{
		{"Decimal(9, 1)", "1.25"},    // needs rounding
		{"Decimal(9, 2)", "1e3"},     // not plain digits
		{"UInt64", int64(1)},         // never through a signed type
		{"Int32", 1.5},               // a float is not an integer
		{"String", "\xff"},           // invalid UTF-8
		{"JSON", `{"a":1} {}`},       // trailing data
		{"Array(UInt8)", "x"},        // not a slice
		{"Tuple(a UInt8)", []any{1}}, // named tuple needs an object
	} {
		_, err := jsonValue(mustType(t, c.raw), c.in)
		var ce *cherr.Error
		require.True(t, errors.As(err, &ce), c.raw)
		require.Equal(t, cherr.CodeValueEncoding, ce.Code, c.raw)
	}
}

func TestColumnError_KeepsCode(t *testing.T) {
	err := columnError(1, cherr.New(cherr.CodeTypeUnsupported, "", "x"))
	var ce *cherr.Error
	require.True(t, errors.As(err, &ce))
	require.Equal(t, cherr.CodeTypeUnsupported, ce.Code)
	require.Equal(t, "CH_TYPE_UNSUPPORTED: column 2: x", err.Error())
}

func TestJSONValue_ForkJSONObject(t *testing.T) {
	o := chcol.NewJSON()
	o.SetValueAtPath("a.b", int64(1))
	o.SetValueAtPath("w", *big.NewInt(5))
	o.SetValueAtPath("dyn", chcol.NewDynamic(*big.NewInt(6))) // a dynamic UInt256 path
	want := map[string]any{"a": map[string]any{"b": json.Number("1")}, "w": "5", "dyn": "6"}
	got, err := jsonValue(mustType(t, "JSON"), o) // a top-level column arrives as *chcol.JSON
	require.NoError(t, err)
	require.Equal(t, want, got)
	got, err = jsonValue(mustType(t, "Array(JSON)"), []chcol.JSON{*o}) // elements arrive by value
	require.NoError(t, err)
	require.Equal(t, []any{want}, got)
	got, err = jsonValue(mustType(t, "Nullable(JSON)"), (*chcol.JSON)(nil))
	require.NoError(t, err)
	require.Nil(t, got)
}

type opaque struct{ x int }

func TestNormalizeJSONValue(t *testing.T) {
	inner := chcol.NewJSON()
	inner.SetValueAtPath("x", int64(1))
	nested := chcol.NewJSON()
	nested.SetValueAtPath("o", []chcol.JSON{*inner, *inner}) // an array of objects inside a path
	id := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	for _, c := range []struct {
		name string
		in   any
		want any
	}{
		{"uint8 array", []uint8{1, 2}, []any{json.Number("1"), json.Number("2")}},
		{"uint8 fixed array", [2]uint8{3, 4}, []any{json.Number("3"), json.Number("4")}},
		{"nested object array", nested, map[string]any{"o": []any{map[string]any{"x": json.Number("1")}, map[string]any{"x": json.Number("1")}}}},
		{"object by value", *inner, map[string]any{"x": json.Number("1")}},
		{"dynamic wide integer", chcol.NewDynamic(*big.NewInt(7)), "7"},
		{"dynamic null", chcol.NewDynamic(nil), nil},
		{"big pointer", big.NewInt(-8), "-8"},
		{"uint64", uint64(math.MaxUint64), json.Number("18446744073709551615")},
		{"float32", float32(0.1), json.Number("0.1")},
		{"nan", math.NaN(), nil},
		{"uuid", id, "00000000-0000-0000-0000-000000000001"},
		{"time", time.Date(2024, 1, 1, 3, 0, 0, 5, time.FixedZone("x", 3600)), "2024-01-01T02:00:00.000000005Z"},
		{"nil slice", []string(nil), []any{}},
		{"string pointer", new("s"), "s"},
	} {
		got, err := normalizeJSONValue(c.in)
		require.NoError(t, err, c.name)
		require.Equal(t, c.want, got, c.name)
	}
	bad := chcol.NewJSON()
	bad.SetValueAtPath("s", "\xff")
	badKey := map[string]any{"\xff": 1}
	for _, c := range []struct {
		name string
		in   any
		code string
	}{
		{"invalid UTF-8 string in an object", bad, cherr.CodeValueEncoding},
		{"invalid UTF-8 key", badKey, cherr.CodeValueEncoding},
		{"invalid UTF-8 deep in an array", []any{[]any{"\xff"}}, cherr.CodeValueEncoding},
		{"other struct", opaque{x: 1}, cherr.CodeTypeUnsupported},
		{"time of day", time.Second, cherr.CodeTypeUnsupported},
	} {
		got, err := normalizeJSONValue(c.in)
		require.Nil(t, got, c.name)
		var ce *cherr.Error
		require.True(t, errors.As(err, &ce), c.name)
		require.Equal(t, c.code, ce.Code, c.name)
	}
	// jsonNumbers must not let json.Marshal replace the bad bytes with U+FFFD.
	_, err := jsonValue(mustType(t, "JSON"), bad)
	require.Error(t, err)
}

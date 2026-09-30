package clickhouse

import (
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	err := columnError("c", cherr.New(cherr.CodeTypeUnsupported, "", "x"))
	var ce *cherr.Error
	require.True(t, errors.As(err, &ce))
	require.Equal(t, cherr.CodeTypeUnsupported, ce.Code)
	require.Equal(t, `CH_TYPE_UNSUPPORTED: column "c": x`, err.Error())
}

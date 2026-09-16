package clickhouse

import (
	"encoding/json"
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"
)

func TestValueMapping(t *testing.T) {
	t.Parallel()
	huge, _ := new(big.Int).SetString("340282366920938463463374607431768211455", 10)
	var nullInt *int32
	cases := []struct {
		raw         string
		value, want any
	}{
		{"UInt64", uint64(math.MaxUint64), "18446744073709551615"},
		{"UInt128", huge, "340282366920938463463374607431768211455"},
		{"LowCardinality(Nullable(Int32))", nullInt, nil},
		{"FixedString(4)", []byte{'a', 0, 0, 0}, "a\x00\x00\x00"},
		{"Float64", math.Inf(1), nil},
		{"Array(Nullable(UInt64))", []any{uint64(math.MaxUint64), nil}, []any{"18446744073709551615", nil}},
		{"Tuple(id UInt64, label String)", map[string]any{"id": uint64(5), "label": "x"}, map[string]any{"id": "5", "label": "x"}},
		{"Nested(id UInt64, label String)", []any{map[string]any{"id": uint64(5), "label": "x"}}, []any{map[string]any{"id": "5", "label": "x"}}},
		{"Array(UInt64)", []uint64{}, []any{}},
		{"JSON", `{"n":18446744073709551615}`, map[string]any{"n": json.Number("18446744073709551615")}},
		{"Date", time.Date(2026, 9, 16, 0, 0, 0, 0, time.FixedZone("east", 3*3600)), "2026-09-16T00:00:00+03:00"},
		{"DateTime64(9)", time.Date(2026, 9, 16, 3, 0, 0, 123456789, time.FixedZone("east", 3*3600)), "2026-09-16T00:00:00.123456789Z"},
		{"Time64(3)", 25*time.Hour + 2*time.Minute + 3*time.Second + 123*time.Millisecond, "25:02:03.123"},
	}
	for _, tt := range cases {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := mapValue(tt.raw, tt.value)
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v, %v; want %#v", got, err, tt.want)
			}
		})
	}
	for _, tt := range []struct {
		raw   string
		value any
	}{{"String", []byte{0xff}}, {"Map(String, String)", map[string]string{}}, {"Array(String)", []any{"ok", []byte{0xff}}}, {"UInt64", float64(1)}} {
		if got, err := mapValue(tt.raw, tt.value); err == nil || got != nil {
			t.Fatalf("expected complete rejection for %s", tt.raw)
		}
	}
}

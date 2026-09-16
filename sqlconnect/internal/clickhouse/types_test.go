package clickhouse

import "testing"

func TestTypeMapping(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"Bool": "boolean", "Int64": "int", "UInt32": "int", "UInt64": "string", "Int128": "string", "UInt256": "string",
		"Float32": "float", "Decimal(38, 9)": "string", "Decimal256(18)": "string", "String": "string", "FixedString(16)": "string",
		"Date": "datetime", "Date32": "datetime", "DateTime('Europe/Athens')": "datetime", "DateTime64(9, 'UTC')": "datetime",
		"Time64(6)": "string", "UUID": "string", "IPv6": "string", "Enum8('a,b' = 1, 'c' = 2)": "string",
		"Nullable(Int32)": "int", "LowCardinality(Nullable(String))": "string", "Array(Nullable(UInt64))": "array",
		"Tuple(UInt64, String)": "json", "Tuple(id UInt64, label String)": "json", "Nested(id UInt64, label String)": "array",
		"SimpleAggregateFunction(sum, UInt64)": "string", "JSON": "json", "JSON(max_dynamic_paths=100)": "json",
		"Map(String, Int64)": "unsupported", "Array(Map(String, String))": "unsupported", "Dynamic": "unsupported",
		"Variant(Int64, String)": "unsupported", "AggregateFunction(sum, Int64)": "unsupported", "Mystery": "unsupported",
		"Tuple(`a b` UInt8)": "unsupported", "Tuple(Enum8('a,b' = 1), String)": "unsupported", "Tuple(x UInt8, x UInt8)": "unsupported",
		"Nullable(": "unsupported", "String); DROP TABLE x": "unsupported", "Array()": "unsupported",
	}
	for raw, want := range cases {
		t.Run(raw, func(t *testing.T) {
			if got := canonicalType(raw); got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}

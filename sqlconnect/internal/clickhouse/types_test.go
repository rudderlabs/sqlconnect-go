package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQ20_CanonicalLabels(t *testing.T) {
	for raw, want := range map[string]string{
		"Bool": "boolean", "Int8": "int", "Int64": "int", "UInt32": "int", "UInt64": "string",
		"Int128": "string", "UInt256": "string", "Float32": "float", "Float64": "float",
		"Decimal(18, 4)": "string", "Decimal32(2)": "string", "String": "string", "FixedString(4)": "string",
		"Date": "datetime", "Date32": "datetime", "DateTime": "datetime", "DateTime('Asia/Tokyo')": "datetime",
		"DateTime64(3, 'Europe/Athens')": "datetime", "Time": "string", "Time64(3)": "string",
		"Nullable(Int32)": "int", "LowCardinality(Nullable(String))": "string", "Array(Nullable(String))": "array",
		"Array(Map(String, UInt8))": "unsupported", "Map(String, UInt8)": "unsupported",
		"Tuple(UInt8, String)": "json", "Tuple(a UInt8, b String)": "json", "Tuple(`a b` UInt8)": "unsupported",
		"Tuple(a Enum8('x,y' = 1))": "unsupported", "Nested(id UInt8, label String)": "array",
		"Enum8('a' = 1, 'b' = 2)": "string", "JSON": "json", "JSON(max_dynamic_paths = 10)": "json",
		"UUID": "string", "IPv4": "string", "IPv6": "string",
		"SimpleAggregateFunction(sum, UInt64)": "string", "SimpleAggregateFunction(max, Int32)": "int",
		"AggregateFunction(uniq, String)": "unsupported", "Variant(String, UInt8)": "unsupported",
		"Dynamic": "unsupported", "Mystery(1)": "unsupported",
		"SimpleAggregateFunction": "unsupported", "Nullable": "unsupported", "Array": "unsupported",
		// Additional cases beyond the plan list.
		"Array(Tuple(a Map(String, UInt8)))": "unsupported", "Array(Tuple(`a b` UInt8))": "unsupported",
		"Nullable(Tuple(a UInt8))": "json", "Enum16('a' = -1)": "string", "Decimal(38,10)": "string",
		"  Int32  ": "int", "Nullable (String)": "string", "UInt8) --": "unsupported",
	} {
		require.Equal(t, want, canonicalType(raw), raw)
	}
	for _, base := range []string{"String", "Int32", "DateTime64(3)"} {
		labels := map[string]bool{}
		for _, w := range []string{base, "Nullable(" + base + ")", "LowCardinality(" + base + ")", "LowCardinality(Nullable(" + base + "))"} {
			labels[canonicalType(w)] = true
		}
		require.Len(t, labels, 1, "wrappers never change the label of %s", base)
	}
}

func TestSQ20_TupleDeclarations(t *testing.T) {
	ty, err := parseType("Tuple(`a,b` UInt8, \"c(d)\" String, e Tuple(f Int8))")
	require.NoError(t, err)
	require.True(t, ty.Unsupported, "quoted field identifiers are outside the decoder-safe subset")
	require.Equal(t, []string{"a,b", "c(d)", "e"}, []string{ty.Fields[0].Name, ty.Fields[1].Name, ty.Fields[2].Name})
	require.Equal(t, "Int8", ty.Fields[2].Type.Fields[0].Type.Name)
	_, err = parseType("Tuple(a UInt8, a String)")
	requireCode(t, err, "CH_DUPLICATE_COLUMN")
	_, err = parseType("Array(Nested(`x` UInt8, x String))")
	requireCode(t, err, "CH_DUPLICATE_COLUMN")
	for _, bad := range []string{
		"Tuple(UInt8", "Array()", "Enum8('a = 1)",
		// A RawType reaches CREATE verbatim, so text after the closing parenthesis or outside quotes is refused.
		"UInt8) ENGINE = Log --", "UInt8 SETTINGS final = 1", "String; DROP TABLE x", "UInt8 -- c", "UInt8 /* c */",
		"Nullable(UInt8)) ORDER BY tuple() --",
		// Characters outside the safe unquoted set, including heredoc and comment starters.
		"Enum8($$'$$ = 1) ENGINE = Log; --')", "Decimal(18 # c, 4)", "Decimal(18 -- c\n, 4)", "Decimal(18 /* c */, 4)",
		"Nullable(UInt8 DEFAULT 1)", "Array(UInt8, UInt8)", "Map(String)", "Nullable(Int8, Int8)",
		"SimpleAggregateFunction(sum)", "", "   ", "8Int", "Tuple()", "Tuple(a)x", "Tuple(`a` )",
		"Enum8('a\\", "Int8\x00", "Enum8('a\\\x00' = 1)",
		// A container type without its argument list.
		"SimpleAggregateFunction", "Nullable", "LowCardinality", "Array", "Map", "Tuple", "Nested", "Array(Nullable)", "Array(Int8)Int8", "Decimal(18,)", "Decimal(,4)", "Tuple(a UInt8 b)",
	} {
		_, err := parseType(bad)
		require.Error(t, err, bad)
	}
	_, err = parseType("Enum8('a--b' = 1, 'c;d' = 2)")
	require.NoError(t, err, "comment and semicolon characters inside a quoted label are data")
	ty, err = parseType("Enum8('it\\'s' = 1, 'a''b' = 2)")
	require.NoError(t, err, "escaped and doubled quotes stay inside the literal")
	require.Equal(t, []string{"'it\\'s' = 1", "'a''b' = 2"}, ty.Args)
	ty, err = parseType("Tuple(`a``b` UInt8, \"c\\\"d\" String)")
	require.NoError(t, err)
	require.Equal(t, []string{"a`b", `c"d`}, []string{ty.Fields[0].Name, ty.Fields[1].Name})
}

func TestSQ20_TypeStructure(t *testing.T) {
	ty, err := parseType("DateTime64(3, 'Europe/Athens')")
	require.NoError(t, err)
	require.Equal(t, "DateTime64", ty.Name)
	require.Equal(t, []string{"3", "'Europe/Athens'"}, ty.Args)
	ty, err = parseType("LowCardinality(Nullable(String))")
	require.NoError(t, err)
	require.Equal(t, "String", ty.unwrap().Name)
	ty, err = parseType("Map(String, Array(UInt8))")
	require.NoError(t, err)
	require.Equal(t, []string{"String", "Array"}, []string{ty.Children[0].Name, ty.Children[1].Name})
	require.Equal(t, "UInt8", ty.Children[1].Children[0].Name)
	ty, err = parseType("SimpleAggregateFunction(sum, UInt64)")
	require.NoError(t, err)
	require.Equal(t, "UInt64", ty.Children[0].Name)
	ty, err = parseType("Array(Tuple(`a b` UInt8))")
	require.NoError(t, err)
	require.True(t, ty.Unsupported, "the flag propagates through Array")
}

func TestCP24_NullabilityFromTypeText(t *testing.T) {
	for raw, want := range map[string]bool{
		"String": false, "Nullable(String)": true,
		"LowCardinality(String)": false, "LowCardinality(Nullable(String))": true,
		"Array(Nullable(String))": false,
	} {
		ty, err := parseType(raw)
		require.NoError(t, err)
		require.Equal(t, want, ty.nullable(), raw)
	}
}

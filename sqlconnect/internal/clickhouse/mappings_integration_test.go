package clickhouse_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestSQ15_SQ16_Integers(t *testing.T) {
	m := queryJSONMap(t, openFloor(t), `SELECT toInt8(-128) i8n, toInt8(127) i8x, toInt16(-32768) i16n, toInt16(32767) i16x,
		toInt32(-2147483648) i32n, toInt32(2147483647) i32x, toInt64(-9223372036854775808) i64n, toInt64(9223372036854775807) i64x,
		toUInt8(0) u8n, toUInt8(255) u8, toUInt16(0) u16n, toUInt16(65535) u16, toUInt32(0) u32n, toUInt32(4294967295) u32,
		toInt64(9007199254740991) p53m1, toInt64(9007199254740992) p53, toInt64(9007199254740993) p53p1,
		toUInt64(18446744073709551615) u64, toUInt128('340282366920938463463374607431768211455') u128, toInt128('-170141183460469231731687303715884105728') i128,
		toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') i256,
		toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') u256,
		CAST(NULL AS Nullable(Int128)) n128, toNullable(toInt128(5)) v128`)
	for k, want := range map[string]any{
		"i8n": "-128", "i8x": "127", "i16n": "-32768", "i16x": "32767", // exact tokens
		"i32n": "-2147483648", "i32x": "2147483647", "i64n": "-9223372036854775808", "i64x": "9223372036854775807",
		"u8n": "0", "u8": "255", "u16n": "0", "u16": "65535", "u32n": "0", "u32": "4294967295",
		"p53m1": "9007199254740991", "p53": "9007199254740992", "p53p1": "9007199254740993",
	} {
		require.Equal(t, json.Number(want.(string)), m[k], k)
	}
	require.Equal(t, []any{
		"18446744073709551615", "340282366920938463463374607431768211455", "-170141183460469231731687303715884105728", // digit strings
		"-57896044618658097711785492504343953926634992332820282019728792003956564819968",
		"115792089237316195423570985008687907853269984665640564039457584007913129639935", nil, "5",
	},
		[]any{m["u64"], m["u128"], m["i128"], m["i256"], m["u256"], m["n128"], m["v128"]})
}

func TestSQ17_18_DecimalsAndStrings(t *testing.T) {
	db := openFloor(t)
	m := queryJSONMap(t, db, `SELECT toDecimal64(12, 0) a, toDecimal64('1.2300', 4) b, toDecimal32('-0.5', 2) c,
		toDecimal128('-12345678901234567890.123456789', 9) d,
		toDecimal256('9999999999999999999999999999999999999999999999999999999999999999999999999999', 0) e,
		CAST(nan AS Float64) f, CAST(inf AS Float32) g, 1.5 h, toFixedString('ab', 4) s, hex(unhex('00ff')) x`)
	require.Equal(t, map[string]any{
		"a": "12", "b": "1.2300", "c": "-0.50", "d": "-12345678901234567890.123456789",
		"e": "9999999999999999999999999999999999999999999999999999999999999999999999999999",
		"f": nil, "g": nil, "h": json.Number("1.5"), "s": "ab\x00\x00", "x": "00FF",
	}, m)
	_, err := queryJSONErr(db, `SELECT unhex('ff') a`)
	requireCode(t, err, "CH_VALUE_ENCODING")
}

func TestSQ19_Temporals(t *testing.T) {
	db := openFloorTZ(t, "Asia/Tokyo")
	m := queryJSONMap(t, db, `SELECT toDate('2024-03-10') a, toDate32('1900-01-02') b,
		toDateTime('2024-03-31 02:59:59', 'Europe/Athens') c1, toDateTime('2024-03-31 04:00:00', 'Europe/Athens') c2,
		toDateTime64('2024-01-01 00:00:00.123456789', 9, 'UTC') d9, toDateTime64('2024-01-01 00:00:00.123456', 6, 'UTC') d6,
		toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC') d3, toDateTime64('1960-01-01 00:00:00', 0, 'UTC') d0`)
	require.Equal(t, map[string]any{
		"a": "2024-03-10T00:00:00Z", "b": "1900-01-02T00:00:00Z", // calendar date kept on a non-UTC server
		"c1": "2024-03-31T00:59:59Z", "c2": "2024-03-31T01:00:00Z", // before (UTC+2) and after (UTC+3) the DST change
		"d9": "2024-01-01T00:00:00.123456789Z", "d6": "2024-01-01T00:00:00.123456Z",
		"d3": "2024-01-01T00:00:00.123Z", "d0": "1960-01-01T00:00:00Z",
	}, m)
	cols, _ := db.ListColumns(context.Background(), createTable(t, db, "z DateTime64(3, 'Europe/Athens'), k UInt8", "k"))
	require.Equal(t, "DateTime64(3, 'Europe/Athens')", cols[0].RawType, "the zone name survives only in RawType")
}

func TestSQ20_Containers(t *testing.T) {
	db := openFloor(t)
	m := queryJSONMap(t, db, `SELECT [1, NULL, 3]::Array(Nullable(UInt8)) a, [[1],[]]::Array(Array(UInt8)) b,
		tuple(1, 'x') c, CAST(tuple(1, 'x') AS Tuple(id UInt8, label String)) d,
		CAST('a' AS Enum8('a' = 1, 'b' = 2)) e8, CAST('z' AS Enum16('y' = 1000, 'z' = 2000)) e16,
		arrayZip(mapKeys(map('k', 1, 'k', 2)), mapValues(map('k', 1, 'k', 2))) f,
		toUUID('00000000-0000-0000-0000-000000000001') g, toIPv4('10.0.0.1') h`)
	one := json.Number("1")
	require.Equal(t, map[string]any{
		"a": []any{one, nil, json.Number("3")}, "b": []any{[]any{one}, []any{}}, "c": []any{one, "x"},
		"d": map[string]any{"id": one, "label": "x"}, "e8": "a", "e16": "z",
		"f": []any{[]any{"k", one}, []any{"k", json.Number("2")}}, // arrayZip keeps duplicate keys and order
		"g": "00000000-0000-0000-0000-000000000001", "h": "10.0.0.1",
	}, m)
	_, err := queryJSONErr(db, `SELECT map('k', 1) m`)
	requireCode(t, err, "CH_TYPE_UNSUPPORTED")
}

func TestSQ20_NestedLayouts(t *testing.T) {
	db := openFloor(t)
	big := "340282366920938463463374607431768211457" // above 64 bits
	for flatten, want := range map[int][]map[string]any{
		0: {
			{"k": json.Number("1"), "n": []any{map[string]any{"id": "1", "label": "x"}, map[string]any{"id": big, "label": nil}}},
			{"k": json.Number("2"), "n": []any{}},
		},
		1: {
			{"k": json.Number("1"), "n.id": []any{"1", big}, "n.label": []any{"x", nil}},
			{"k": json.Number("2"), "n.id": []any{}, "n.label": []any{}},
		},
	} {
		// k UInt8, n Nested(id UInt256, label Nullable(String)); row 1 holds (1,'x') and (big,NULL), row 2 is empty.
		tbl := createNestedTable(t, db, flatten, big)
		require.Equal(t, want, queryJSONMaps(t, db, "SELECT * FROM "+db.QuoteTable(tbl)+" ORDER BY k"),
			"flatten_nested=%d: UInt256 keeps the string policy inside Nested", flatten)
	}
}

func TestSQ20_TupleProjections(t *testing.T) {
	db := openFloor(t)
	tbl := db.QuoteTable(createTable(t, db, "c Tuple(`a b` UInt8), k UInt8", "k", "INSERT INTO {{t}} VALUES ((7), 1)"))
	_, err := queryJSONErr(db, "SELECT * FROM "+tbl)
	require.Error(t, err, "the decoder refuses the quoted-space field; no partial row")
	require.Equal(t, []map[string]any{{"a b": json.Number("7")}, {"c": []any{json.Number("7")}}},
		[]map[string]any{queryJSONMap(t, db, "SELECT tupleElement(c, 1) AS `a b` FROM "+tbl), queryJSONMap(t, db, "SELECT CAST(c AS Tuple(UInt8)) AS c FROM "+tbl)})
}

func TestSQ21_Refusals(t *testing.T) {
	db := openFloor(t)
	tbl := createTable(t, db, "a AggregateFunction(uniq, String), k UInt8", "k",
		"INSERT INTO {{t}} SELECT uniqState('x'), 1") // a row, so the result has a block to decode
	cols, _ := db.ListColumns(context.Background(), tbl)
	require.Equal(t, "unsupported", cols[0].Type, "the column stays visible")
	_, err := queryJSONErr(db, "SELECT * FROM "+db.QuoteTable(tbl))
	require.Contains(t, []string{"CH_TYPE_UNSUPPORTED", "CH_UNKNOWN"}, db.ClassifyError(err).Code, "a named error, never a panic")
	for _, decl := range []string{"Dynamic", "Variant(String, UInt8)"} { // a newer type maps to unsupported, never to an error
		cols, err := db.ListColumnsForSqlQuery(context.Background(), "SELECT CAST(NULL AS "+decl+") AS x")
		require.Equal(t, []any{nil, "unsupported"}, []any{err, cols[0].Type}, decl)
	}
}

func TestRowErrors(t *testing.T) {
	db := openFloor(t)
	_, err := queryJSONErr(db, "SELECT a, a FROM (SELECT 1 AS a)") // the server refuses "SELECT 1 AS a, 2 AS a" itself
	requireCode(t, err, "CH_DUPLICATE_COLUMN")
	rows := queryJSON(t, db, "SELECT toString(number) s FROM numbers(3)") // copied byte slices stay stable
	require.Equal(t, []string{`{"s":"0"}`, `{"s":"2"}`}, []string{string(rows[0]), string(rows[2])})
}

func TestJSONColumn(t *testing.T) {
	db := openFloor(t)
	m := queryJSONMap(t, db, `SELECT '{"a":1,"b":{"c":"x"},"d":[1,2],"f":1.5,"n":18446744073709551615}'::JSON AS j,
		['{"a":2}'::JSON] AS arr, CAST(NULL AS Nullable(JSON)) AS nj, '{"w":5,"d":"1.50"}'::JSON(w UInt256, d Decimal(9, 2)) AS typed,
		'{"a":[1,2]}'::JSON(a Array(UInt8)) AS u8, '{"o":[{"x":1},{"x":2}]}'::JSON AS objs`)
	require.Equal(t, map[string]any{
		"j": map[string]any{
			"a": json.Number("1"), "b": map[string]any{"c": "x"}, "d": []any{json.Number("1"), json.Number("2")},
			"f": json.Number("1.5"), "n": json.Number("18446744073709551615"),
		},
		"arr": []any{map[string]any{"a": json.Number("2")}}, "nj": nil,
		// Typed paths follow the wide-integer string policy. A typed decimal
		// path keeps its exact value, but the object carries no scale.
		"typed": map[string]any{"w": "5", "d": "1.5"},
		"u8":    map[string]any{"a": []any{json.Number("1"), json.Number("2")}}, // numbers, never base64
		"objs":  map[string]any{"o": []any{map[string]any{"x": json.Number("1")}, map[string]any{"x": json.Number("2")}}},
	}, m)
}

type failingScan struct{}

func (failingScan) Scan(...any) error { return errors.New("scan failed") }

func TestMapperReturnsNoPartialRow(t *testing.T) {
	db := openFloor(t)
	for _, c := range []struct {
		q    string
		scan func(*sql.Rows) sqlconnect.RowScan
		code string
	}{
		{"SELECT 1 AS a, unhex('ff') AS b", func(r *sql.Rows) sqlconnect.RowScan { return r }, "CH_VALUE_ENCODING"},
		{"SELECT 1 AS a", func(*sql.Rows) sqlconnect.RowScan { return failingScan{} }, "CH_UNKNOWN"},
	} {
		rows, err := db.QueryContext(context.Background(), c.q)
		require.NoError(t, err)
		cols, err := rows.ColumnTypes()
		require.NoError(t, err)
		require.True(t, rows.Next())
		m, err := db.JSONRowMapper()(cols, c.scan(rows))
		require.Nil(t, m, c.q)
		requireCode(t, err, c.code)
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	}
}

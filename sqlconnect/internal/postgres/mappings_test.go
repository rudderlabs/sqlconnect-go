package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Postgres resolves rudder-types via base.dbopts using
// columnTypeMappings[strings.ToLower(DatabaseTypeName())]. Array columns report
// their element type prefixed with "_" (e.g. text[] → "_text").
func TestColumnTypeMappings_Array(t *testing.T) {
	t.Run("string-element array types map to array", func(t *testing.T) {
		for _, name := range []string{"_text", "_varchar", "_bpchar", "_char", "_name"} {
			require.Equal(t, "array", columnTypeMappings[name], name)
		}
	})

	t.Run("non-string array types are not mapped (unsupported in v1)", func(t *testing.T) {
		for _, name := range []string{"_int4", "_int8", "_float8", "_bool", "_timestamp"} {
			_, ok := columnTypeMappings[name]
			require.False(t, ok, "%s must not map to a supported rudder-type", name)
		}
	})

	t.Run("scalar text stays string", func(t *testing.T) {
		require.Equal(t, "string", columnTypeMappings["text"])
	})

	// Schema introspection (ListColumns) reads information_schema.data_type = "ARRAY" (lowercased
	// to "array" by the mapper) for every array column, with no element type available — so the
	// generic key maps to array in both the default and legacy maps.
	t.Run("generic 'array' (information_schema.data_type) maps to array", func(t *testing.T) {
		require.Equal(t, "array", columnTypeMappings["array"])
		require.Equal(t, "array", legacyColumnTypeMappings["array"])
		require.Equal(t, "array", legacyColumnTypeMappings["_text"])
	})
}

// jsonRowMapper must serialize string arrays as a JSON array (parse the "{a,b}" literal),
// not as the text-literal string, so the emitted audience trait is a real JSON array.
func TestJsonRowMapper_StringArray(t *testing.T) {
	for _, m := range []struct {
		name string
		fn   func(string, any) any
	}{{"default", jsonRowMapper}, {"legacy", legacyJsonRowMapper}} {
		t.Run(m.name, func(t *testing.T) {
			require.Equal(t, []string{"electronics", "books"}, m.fn("_TEXT", []byte("{electronics,books}")))
			require.Equal(t, []string{"electronics", "books"}, m.fn("_text", []byte("{electronics,books}")), "case-insensitive")
			require.Equal(t, []string{}, m.fn("_VARCHAR", []byte("{}")), "empty array")
			require.Equal(t, []string{"a,b", "c"}, m.fn("_TEXT", []byte(`{"a,b","c"}`)), "quoted element with comma")
			require.Nil(t, m.fn("_TEXT", nil), "NULL array → nil → JSON null")
			require.Equal(t, "hello", m.fn("TEXT", []byte("hello")), "scalar text unchanged")
		})
	}
}

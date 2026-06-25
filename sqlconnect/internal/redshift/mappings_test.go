package redshift

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedshift_SuperArrayMappingAndSerialization(t *testing.T) {
	t.Run("SUPER maps to array rudder-type (default + legacy)", func(t *testing.T) {
		require.Equal(t, "array", columnTypeMappings["super"])
		require.Equal(t, "array", legacyColumnTypeMappings["super"])
	})

	// lib/pq reports an empty DatabaseTypeName for SUPER; its JSON value must be emitted as raw
	// JSON (an array trait → JSON array), while non-JSON values and named types stay strings.
	for _, m := range []struct {
		name string
		fn   func(string, any) any
	}{{"default", jsonRowMapper}, {"legacy", legacyJsonRowMapper}} {
		t.Run(m.name, func(t *testing.T) {
			require.Equal(t, json.RawMessage(`["electronics","books"]`), m.fn("", []byte(`["electronics","books"]`)))
			require.Equal(t, json.RawMessage(`[]`), m.fn("", []byte(`[]`)))
			require.Equal(t, json.RawMessage(`{"a":1}`), m.fn("", []byte(`{"a":1}`)), "JSON object → raw JSON")
			require.Equal(t, "not json {", m.fn("", []byte("not json {")), "non-JSON SUPER value → string")
			require.Equal(t, "hello", m.fn("VARCHAR", []byte("hello")), "named scalar type unchanged")
			// A scalar value (even though valid JSON) from another unnamed-OID type must NOT be
			// emitted as raw JSON — only container shapes are.
			require.Equal(t, "123", m.fn("", []byte("123")), "bare number stays a string")
			require.Equal(t, "true", m.fn("", []byte("true")), "bare bool stays a string")
		})
	}
}

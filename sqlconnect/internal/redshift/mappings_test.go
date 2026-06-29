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

	// SUPER's JSON value must be emitted as raw JSON (an array trait → JSON array), while non-JSON
	// values and named types stay strings — on both backends: lib/pq reports an empty
	// DatabaseTypeName for SUPER, the Redshift Data API backend reports the real name "SUPER".
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

			// Redshift Data API backend reports the real type name "SUPER" and hands the value over
			// as a string (FieldMemberStringValue); the same JSON-array emission must apply there.
			require.Equal(t, json.RawMessage(`["electronics","books"]`), m.fn("SUPER", `["electronics","books"]`), "Data API: SUPER string → raw JSON array")
			require.Equal(t, json.RawMessage(`[]`), m.fn("SUPER", `[]`))
			require.Equal(t, json.RawMessage(`{"a":1}`), m.fn("SUPER", `{"a":1}`), "Data API: SUPER object → raw JSON")
			require.Equal(t, "123", m.fn("SUPER", `123`), "Data API: bare scalar SUPER stays a string")
			require.Equal(t, json.RawMessage(`["x"]`), m.fn("SUPER", []byte(`["x"]`)), "SUPER as []byte also handled")
		})
	}
}

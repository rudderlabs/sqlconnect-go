package redshift

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// superJSONValue emits a SUPER value as raw JSON (so an array trait is a JSON array, not a
// string). lib/pq reports an EMPTY DatabaseTypeName for SUPER (its OID is unknown to the driver),
// so the mapper keys on the empty type name + this check; known scalar types report their type
// name and are unaffected. To avoid mis-emitting a plain scalar held by some other unnamed-OID type
// (e.g. VARBYTE/GEOMETRY reporting "" and holding the valid-JSON literal `123`/`true`), only
// container-shaped values (a JSON array or object) are emitted as raw JSON; everything else stays a
// string. The audience use case is array<string>, which is always a JSON array.
func superJSONValue(value any) any {
	switch v := value.(type) {
	case []byte:
		if isJSONContainer(v) {
			return json.RawMessage(v)
		}
		return string(v)
	case string:
		if isJSONContainer([]byte(v)) {
			return json.RawMessage(v)
		}
	}
	return value
}

// isJSONContainer reports whether b is valid JSON whose top level is an array or object.
func isJSONContainer(b []byte) bool {
	t := bytes.TrimSpace(b)
	if len(t) == 0 || (t[0] != '[' && t[0] != '{') {
		return false
	}
	return json.Valid(t)
}

var columnTypeMappings = map[string]string{
	"int":                         "int",
	"int2":                        "int",
	"int4":                        "int",
	"int8":                        "int",
	"integer":                     "int",
	"smallint":                    "int",
	"bigint":                      "int",
	"real":                        "float",
	"float":                       "float",
	"float4":                      "float",
	"float8":                      "float",
	"numeric":                     "float",
	"double precision":            "float",
	"boolean":                     "boolean",
	"bool":                        "boolean",
	"text":                        "string",
	"varchar":                     "string",
	"character varying":           "string",
	"nchar":                       "string",
	"bpchar":                      "string",
	"character":                   "string",
	"nvarchar":                    "string",
	"string":                      "string",
	"date":                        "datetime",
	"time":                        "datetime",
	"time without time zone":      "datetime",
	"time with time zone":         "datetime",
	"timetz":                      "datetime",
	"timestamptz":                 "datetime",
	"timestamp without time zone": "datetime",
	"timestamp with time zone":    "datetime",
	"timestamp":                   "datetime",
	// SUPER holds semi-structured data; for the audience array<string> use case it carries a
	// JSON array. v1 maps it to the array rudder-type (SUPER can't distinguish array vs object
	// at the type level, so non-array SUPER columns surface as array too).
	"super": "array",
}

// jsonRowMapper maps a row's scanned column to a json object's field
func jsonRowMapper(databaseTypeName string, value any) any {
	switch databaseTypeName {
	case "NUMERIC":
		switch v := value.(type) {
		case []byte:
			if n, err := strconv.ParseFloat(string(v), 64); err == nil {
				return n
			}
		case string:
			if n, err := strconv.ParseFloat(v, 64); err == nil {
				return n
			}
		}
	case "":
		// SUPER (and other Redshift-specific types lib/pq can't name) — emit JSON as raw JSON.
		return superJSONValue(value)
	default:
		switch v := value.(type) {
		case []byte:
			return string(v)
		}
	}

	return value
}

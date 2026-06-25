package postgres

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

// pgStringArrayTypes are the Postgres string-element array type names (element type prefixed
// with "_"). Their values are parsed from the array literal into a JSON array of strings so
// the emitted trait is a real JSON array, not the text literal "{a,b}".
var pgStringArrayTypes = map[string]bool{
	"_TEXT": true, "_VARCHAR": true, "_BPCHAR": true, "_CHAR": true, "_NAME": true, "_CHARACTER VARYING": true,
}

// parsePgStringArray parses a Postgres array literal ("{a,b}") into a []string. Returns nil
// (caller falls through) if the value isn't a parseable array literal. A NULL array column
// scans to nil and is left as nil (→ JSON null).
func parsePgStringArray(value any) any {
	var arr pq.StringArray
	switch v := value.(type) {
	case []byte:
		if arr.Scan(v) == nil {
			return []string(arr)
		}
	case string:
		if arr.Scan([]byte(v)) == nil {
			return []string(arr)
		}
	}
	return nil
}

// mapping of database column types to rudder types
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
	"boolean":                     "boolean",
	"bool":                        "boolean",
	"json":                        "json",
	"jsonb":                       "json",
	// Array columns. On the data path (row scan) Postgres reports the element type prefixed
	// with "_" (e.g. text[] → "_text"), so only string-element arrays map to array — non-string
	// array types (e.g. _int4) stay unmapped → unsupported. The generic "array" key covers
	// schema introspection via information_schema.data_type, which reports "ARRAY" for every
	// array column (no element type available there).
	"array":              "array",
	"_text":              "array",
	"_varchar":           "array",
	"_bpchar":            "array",
	"_char":              "array",
	"_character varying": "array",
	"_name":              "array",
}

// jsonRowMapper maps a row's scanned column to a json object's field
func jsonRowMapper(databaseTypeName string, value any) any {
	switch databaseTypeName {
	case "JSON", "JSONB":
		switch v := value.(type) {
		case []byte:
			return json.RawMessage(v)
		case string:
			return json.RawMessage(v)
		}
	case "NUMERIC":
		switch v := value.(type) {
		case []byte:
			if n, err := strconv.ParseFloat(string(v), 64); err == nil {
				return n
			}
		}
	default:
		if pgStringArrayTypes[strings.ToUpper(databaseTypeName)] {
			if arr := parsePgStringArray(value); arr != nil {
				return arr
			}
		}
		switch v := value.(type) {
		case []byte:
			return string(v)
		}
	}

	return value
}

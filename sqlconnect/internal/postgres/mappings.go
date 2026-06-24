package postgres

import (
	"encoding/json"
	"strconv"
)

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
	// String-element array types (Postgres reports array columns as the element type
	// prefixed with "_"). Only string arrays map to the array rudder-type in v1;
	// non-string array types (e.g. _int4) stay unmapped → surfaced as unsupported.
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
		switch v := value.(type) {
		case []byte:
			return string(v)
		}
	}

	return value
}

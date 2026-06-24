package postgres

import "encoding/json"

var legacyColumnTypeMappings = map[string]string{
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
	"timestamptz":                 "datetime",
	"timestamp without time zone": "datetime",
	"timestamp with time zone":    "datetime",
	"timestamp":                   "datetime",
	"boolean":                     "boolean",
	"bool":                        "boolean",
	"jsonb":                       "json",
	// Array columns. The standard schema introspection (ListColumns) reads
	// information_schema.columns.data_type, which reports the generic "ARRAY" for every array
	// column regardless of element type (element type lives in udt_name, not selected here).
	// So this maps ALL Postgres arrays to the array rudder-type; element-type precision
	// (string-only) is enforced on the data path via columnTypeMappings (_text etc). The
	// "_text" udt family is also mapped for any caller that introspects via udt_name.
	"array":    "array",
	"_text":    "array",
	"_varchar": "array",
	"_bpchar":  "array",
	"_char":    "array",
	"_name":    "array",
}

// legacyJsonRowMapper maps a row's scanned column to a json object's field
func legacyJsonRowMapper(databaseTypeName string, value any) any {
	switch databaseTypeName {
	case "JSON":
		fallthrough
	case "JSONB":
		switch v := value.(type) {
		case []byte:
			return json.RawMessage(v)

		case string:
			return json.RawMessage(v)
		}
	default:
		switch v := value.(type) {
		case []byte:
			return string(v)
		}
	}
	return value
}

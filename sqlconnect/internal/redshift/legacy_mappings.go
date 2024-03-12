package redshift

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
	"timestamp without time zone": "datetime",
	"timestamp with time zone":    "datetime",
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

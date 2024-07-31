package postgres

import (
	"encoding/json"
	"time"
)

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
	"1266":                        "TIMETZ",
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
	case "1266":
		if value != nil {
			if t, err := time.Parse("15:04:05-07", value.(string)); err == nil {
				value = t.UTC().Format(time.RFC3339)
			}
		}
	case "TIME", "TIME WITHOUT TIME ZONE":
		switch v := value.(type) {
		case string:
			if t, err := time.Parse("15:04:05", v); err == nil {
				value = t.UTC().Format(time.RFC3339)
			}
		}
	default:
		switch v := value.(type) {
		case []byte:
			return string(v)
		case time.Time:
			return v.UTC()
		}
	}
	return value
}

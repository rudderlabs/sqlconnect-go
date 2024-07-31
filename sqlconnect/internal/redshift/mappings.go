package redshift

import (
	"strconv"
	"time"
)

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
	"1266":                        "datetime",
	"timestamptz":                 "datetime",
	"timestamp without time zone": "datetime",
	"timestamp with time zone":    "datetime",
	"timestamp":                   "datetime",
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
	case "1266":
		if value != nil {
			if t, err := time.Parse("15:04:05-07", value.(string)); err == nil {
				value = t.UTC().Format(time.RFC3339Nano)
			}
		}
	case "TIME", "TIME WITHOUT TIME ZONE":
		switch v := value.(type) {
		case string:
			if t, err := time.Parse("15:04:05", v); err == nil {
				value = t.UTC().Format(time.RFC3339Nano)
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

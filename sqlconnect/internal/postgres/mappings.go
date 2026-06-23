package postgres

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/lib/pq"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
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
	"array":                       "json", // information_schema bare ARRAY (no element type)
}

var re = regexp.MustCompile(`(\(.+\)|<.+>)`) // remove type parameters [<>] and size constraints [()]

func columnTypeMapper(columnType base.ColumnType) string {
	raw := strings.TrimSpace(columnType.DatabaseTypeName())
	lower := strings.ToLower(raw)

	// Postgres array types: text[], integer[], or lib/pq internal names like _text.
	// String-element arrays → array (filterable); numeric/boolean/complex-element
	// arrays → json — membership compares string values, so int/bool arrays must not
	// surface as array (same gate as BQ ARRAY<STRING> vs ARRAY<INT>).
	if strings.HasSuffix(lower, "[]") {
		if isStringElementArray(raw) {
			return "array"
		}
		return "json"
	}
	if strings.HasPrefix(lower, "_") {
		if elementMapsToString(lower[1:]) {
			return "array"
		}
		return "json"
	}

	databaseTypeName := strings.TrimSpace(strings.ToLower(re.ReplaceAllString(raw, "")))
	if mappedType, ok := columnTypeMappings[databaseTypeName]; ok {
		return mappedType
	}
	return databaseTypeName
}

func legacyColumnTypeMapper(columnType base.ColumnType) string {
	raw := strings.TrimSpace(columnType.DatabaseTypeName())
	if mappedType, ok := legacyColumnTypeMappings[strings.ToLower(raw)]; ok {
		return mappedType
	}
	if mappedType, ok := legacyColumnTypeMappings[strings.ToUpper(raw)]; ok {
		return mappedType
	}
	stripped := strings.TrimSpace(strings.ToLower(re.ReplaceAllString(raw, "")))
	if mappedType, ok := legacyColumnTypeMappings[stripped]; ok {
		return mappedType
	}
	return raw
}

// isStringElementArray reports whether a native array column has a string element
// type (text[], varchar(n)[], …), read from the raw type name. It reuses
// columnTypeMappings so "what is a string type" has a single source of truth.
func isStringElementArray(rawTypeName string) bool {
	trimmed := strings.TrimSpace(rawTypeName)
	if !strings.HasSuffix(strings.ToLower(trimmed), "[]") {
		return false
	}
	elem := trimmed[:len(trimmed)-2]
	return elementMapsToString(elem)
}

func elementMapsToString(elem string) bool {
	normalized := strings.TrimSpace(strings.ToLower(re.ReplaceAllString(elem, "")))
	return columnTypeMappings[normalized] == "string"
}

// jsonRowMapper maps a row's scanned column to a json object's field
func jsonRowMapper(databaseTypeName string, value any) any {
	if isPostgresStringArrayDBType(databaseTypeName) {
		if parsed, ok := parsePostgresStringArray(value); ok {
			return parsed
		}
	}

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

func isPostgresStringArrayDBType(databaseTypeName string) bool {
	lower := strings.ToLower(strings.TrimSpace(databaseTypeName))
	if strings.HasSuffix(lower, "[]") {
		return isStringElementArray(databaseTypeName)
	}
	if strings.HasPrefix(lower, "_") {
		return elementMapsToString(databaseTypeName[1:])
	}
	return false
}

func parsePostgresStringArray(value any) ([]string, bool) {
	var raw string
	switch v := value.(type) {
	case string:
		raw = v
	case []byte:
		raw = string(v)
	default:
		return nil, false
	}
	var arr pq.StringArray
	if err := arr.Scan(raw); err != nil {
		return nil, false
	}
	return []string(arr), true
}

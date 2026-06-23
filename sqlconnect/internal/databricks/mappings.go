package databricks

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// mapping of database column types to rudder types
var columnTypeMappings = map[string]string{
	"DECIMAL": "float", // DECIMAL and aliases
	"NUMERIC": "float",
	"DEC":     "float",

	"INT":     "int", // INT and aliases
	"INTEGER": "int",

	"BIGINT": "int", // BIGINT and aliases
	"LONG":   "int",

	"SMALLINT": "int", // SMALLINT and aliases
	"SHORT":    "int",

	"TINYINT": "int", // TINYINT and aliases
	"BYTE":    "int",

	"FLOAT": "float", // FLOAT and aliases
	"REAL":  "float",

	"DOUBLE":        "float",
	"BOOLEAN":       "boolean",
	"STRING":        "string",
	"CHAR":          "string",
	"VARCHAR":       "string",
	"BINARY":        "string",
	"DATE":          "datetime",
	"INTERVAL":      "datetime",
	"VOID":          "string",
	"TIMESTAMP":     "datetime",
	"TIMESTAMP_NTZ": "datetime",

	"ARRAY":  "json",
	"MAP":    "json",
	"STRUCT": "json",
}

var re = regexp.MustCompile(`(\(.+\)|<.+>)`) // remove type parameters [<>] and size constraints [()]

func columnTypeMapper(columnType base.ColumnType) string {
	if rudderType, ok := arrayRudderType(columnType.DatabaseTypeName()); ok {
		return rudderType
	}
	databaseTypeName := strings.ToUpper(re.ReplaceAllString(columnType.DatabaseTypeName(), ""))
	if mappedType, ok := columnTypeMappings[strings.ToUpper(databaseTypeName)]; ok {
		return mappedType
	}
	return databaseTypeName
}

// arrayRudderType decides the rudder type for a native array column from its raw
// type name (read BEFORE type parameters are stripped). v1 supports
// string-element arrays only: membership filters compare string values, so a
// numeric/boolean array filtered as text would never match. So ARRAY<STRING> →
// "array" (filterable); non-string scalar arrays (ARRAY<INT>, ARRAY<BOOLEAN>, …)
// and complex element arrays (struct/map/nested array) → "json". Whether the
// element is a string reuses the driver's own columnTypeMappings, so there is one
// source of truth for "what is a string type". Returns ok=false when the column
// is not a parameterised array (a bare "ARRAY" with no element type falls through
// to the standard map, preserving prior behaviour).
func arrayRudderType(rawTypeName string) (string, bool) {
	upper := strings.ReplaceAll(strings.ToUpper(rawTypeName), " ", "")
	if !strings.HasPrefix(upper, "ARRAY<") || !strings.HasSuffix(upper, ">") {
		return "", false
	}
	elem := re.ReplaceAllString(upper[len("ARRAY<"):len(upper)-1], "")
	if columnTypeMappings[elem] == "string" {
		return "array", true
	}
	return "json", true
}

// jsonRowMapper maps a row's scanned column to a json object's field
func jsonRowMapper(databaseTypeName string, value any) any {
	switch v := value.(type) {
	case []uint8:
		return string(v)
	case string:
		switch databaseTypeName {
		case "DECIMAL":
			// convert to float
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return v
			}
			return f
		case "ARRAY", "STRUCT", "MAP": // convert string to json
			var j any
			err := json.Unmarshal([]byte(v), &j)
			if err != nil {
				return v
			}
			return j
		}
	}
	return value
}

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
// type name (read BEFORE type parameters are stripped). Scalar-element arrays
// (ARRAY<STRING>, ARRAY<INT>, …) map to "array" so they can be targeted by array
// column filters; arrays of structs/maps/arrays stay "json" — nested element
// paths are out of scope (Feature 4). Returns ok=false when the column is not a
// parameterised array (a bare "ARRAY" with no element type falls through to the
// standard map, preserving prior behaviour).
func arrayRudderType(rawTypeName string) (string, bool) {
	upper := strings.ToUpper(strings.TrimSpace(rawTypeName))
	if !strings.HasPrefix(upper, "ARRAY<") || !strings.HasSuffix(upper, ">") {
		return "", false
	}
	elem := strings.TrimSpace(upper[len("ARRAY<") : len(upper)-1])
	switch {
	case strings.HasPrefix(elem, "STRUCT"),
		strings.HasPrefix(elem, "RECORD"),
		strings.HasPrefix(elem, "ARRAY"),
		strings.HasPrefix(elem, "MAP"):
		return "json", true
	}
	return "array", true
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

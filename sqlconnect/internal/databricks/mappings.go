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

	"VARIANT": "json",
	// Non-string arrays fall through here after stringElementArray (ARRAY<STRING|…> → array).
	"ARRAY":   "unsupported",
	"MAP":     "unsupported",
	"STRUCT":  "unsupported",
}

var re = regexp.MustCompile(`(\(.+\)|<.+>)`) // remove type parameters [<>] and size constraints [()]

// stringElementArray matches ARRAY<STRING|VARCHAR|CHAR ...> — string-element arrays
// that map to the array rudder-type in v1. Other element types (e.g. ARRAY<BIGINT>)
// fall through to the generic ARRAY → unsupported mapping.
var stringElementArray = regexp.MustCompile(`^ARRAY<\s*(STRING|VARCHAR|CHAR)`)

func columnTypeMapper(columnType base.ColumnType) string {
	raw := strings.ToUpper(columnType.DatabaseTypeName())
	if stringElementArray.MatchString(raw) {
		return "array"
	}
	databaseTypeName := strings.ToUpper(re.ReplaceAllString(raw, ""))
	if mappedType, ok := columnTypeMappings[strings.ToUpper(databaseTypeName)]; ok {
		return mappedType
	}
	return databaseTypeName
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

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
	raw := columnType.DatabaseTypeName()
	databaseTypeName := strings.TrimSpace(strings.ToUpper(re.ReplaceAllString(raw, "")))
	if mappedType, ok := columnTypeMappings[databaseTypeName]; ok {
		// A native array is filterable (rudder type "array") only when its element
		// is a string; numeric/boolean/complex-element arrays keep the mapped
		// "json" — string membership filters can't match non-string elements. The
		// map keys on the param-stripped name, so every array collapses to "ARRAY"
		// and the element has to be recovered from the raw name here.
		if databaseTypeName == "ARRAY" && isStringElementArray(raw) {
			return "array"
		}
		return mappedType
	}
	return databaseTypeName
}

// isStringElementArray reports whether a native array column has a string element
// type (ARRAY<STRING>, ARRAY<VARCHAR>, …), read from the raw type name before its
// parameters are stripped. It reuses columnTypeMappings so "what is a string
// type" has a single source of truth. A bare "ARRAY" (no element type) and
// non-string or complex elements (int, bool, struct, map, nested array) return
// false.
func isStringElementArray(rawTypeName string) bool {
	upper := strings.ReplaceAll(strings.ToUpper(rawTypeName), " ", "")
	if !strings.HasPrefix(upper, "ARRAY<") || !strings.HasSuffix(upper, ">") {
		return false
	}
	elem := re.ReplaceAllString(upper[len("ARRAY<"):len(upper)-1], "")
	return columnTypeMappings[elem] == "string"
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

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
	databaseTypeName := strings.ToUpper(re.ReplaceAllString(columnType.DatabaseTypeName(), ""))
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

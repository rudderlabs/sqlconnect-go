package trino

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// mapping of database column types to rudder types
var columnTypeMappings = map[string]string{
	"BOOLEAN":  "boolean",
	"TINYINT":  "int",
	"SMALLINT": "int",
	"INTEGER":  "int",
	"INT":      "int",
	"BIGINT":   "int",

	"REAL":    "float",
	"DOUBLE":  "float",
	"DECIMAL": "float",

	"VARCHAR":   "string",
	"CHAR":      "string",
	"VARBINARY": "string",

	"DATE":                     "datetime",
	"TIME":                     "datetime",
	"TIMESTAMP":                "datetime",
	"TIME WITH TIME ZONE":      "datetime",
	"TIMESTAMP WITH TIME ZONE": "datetime",

	"JSON":  "json",
	"ARRAY": "json",
	"MAP":   "json",
}

var re = regexp.MustCompile(`(\(.+\)|<.+>)`) // remove type parameters [<>] and size constraints [()]

func columnTypeMapper(columnType base.ColumnType) string {
	databaseTypeName := strings.ToUpper(re.ReplaceAllString(columnType.DatabaseTypeName(), ""))
	if mappedType, ok := columnTypeMappings[strings.ToUpper(databaseTypeName)]; ok {
		return mappedType
	}

	// TODO: is this still needed?
	if strings.Contains(databaseTypeName, "CHAR") || strings.Contains(databaseTypeName, "VARCHAR") {
		return "string"
	} else if strings.Contains(databaseTypeName, "TIMESTAMP") {
		return "datetime"
	} else if strings.Contains(databaseTypeName, "DECIMAL") {
		return "float"
	}
	return databaseTypeName
}

// jsonRowMapper maps a row's scanned column to a json object's field
func jsonRowMapper(databaseTypeName string, value any) any {
	switch databaseTypeName {
	case "DECIMAL":
		switch v := value.(type) {
		case string:
			if value, err := strconv.ParseFloat(v, 64); err == nil {
				return value
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

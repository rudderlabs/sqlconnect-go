package databricks

import (
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func legacyColumnTypeMapper(columnType base.ColumnType) string {
	columnTypeMappings := map[string]string{
		"DECIMAL":       "int",
		"NUMERIC":       "int",
		"DEC":           "int",
		"INT":           "int",
		"BIGINT":        "int",
		"SMALLINT":      "int",
		"TINYINT":       "int",
		"FLOAT":         "float",
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
		"ARRAY":         "json",
		"MAP":           "json",
		"STRUCT":        "json",
	}
	databaseTypeName := strings.ToUpper(re.ReplaceAllString(columnType.DatabaseTypeName(), ""))
	if mappedType, ok := columnTypeMappings[strings.ToUpper(databaseTypeName)]; ok {
		return mappedType
	}
	return databaseTypeName
}

// legacyJsonRowMapper maps a row's scanned column to a json object's field
func legacyJsonRowMapper(_ string, value any) any {
	switch v := value.(type) {
	case []uint8:
		return string(v)
	}
	return value
}

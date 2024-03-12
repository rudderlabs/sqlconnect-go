package bigquery

import (
	"math/big"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func legacyColumnTypeMapper(columnType base.ColumnType) string {
	columnType.DatabaseTypeName()
	columnTypeMappings := map[string]string{
		"BOOLEAN":    "boolean",
		"BOOL":       "boolean",
		"INTEGER":    "int",
		"INT64":      "int",
		"INT":        "int",
		"SMALLINT":   "int",
		"TINYINT":    "int",
		"BIGINT":     "int",
		"NUMERIC":    "float",
		"BIGNUMERIC": "float",
		"FLOAT":      "float",
		"FLOAT64":    "float",
		"DECIMAL":    "float",
		"BIGDECIMAL": "float",
		"STRING":     "string",
		"BYTES":      "string",
		"DATE":       "datetime",
		"DATETIME":   "datetime",
		"TIME":       "datetime",
		"TIMESTAMP":  "datetime",
	}
	databaseTypeName := strings.ToUpper(re.ReplaceAllString(columnType.DatabaseTypeName(), ""))
	if mappedType, ok := columnTypeMappings[strings.ToUpper(databaseTypeName)]; ok {
		return mappedType
	}
	if databaseTypeName == "ARRAY" {
		return "array"
	}
	if databaseTypeName == "STRUCT" {
		return "RECORD"
	}
	return databaseTypeName
}

// legacyJsonRowMapper maps a row's scanned column to a json object's field
func legacyJsonRowMapper(_ string, value any) any {
	switch v := (value).(type) {
	case *big.Rat:
		// Handle big.Rat values
		if !v.IsInt() {
			floatVal, _ := v.Float64()
			return floatVal
		} else {
			return v.Num().Int64()
		}
	default:
		// Handle other data types as is
		return v
	}
}

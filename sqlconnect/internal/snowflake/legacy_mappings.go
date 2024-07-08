package snowflake

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

func legacyColumnTypeMapper(columnType base.ColumnType) string {
	columnType.DatabaseTypeName()
	columnTypeMappings := map[string]string{
		"NUMBER":           "int",
		"DECIMAL":          "int",
		"NUMERIC":          "int",
		"INT":              "int",
		"INTEGER":          "int",
		"BIGINT":           "int",
		"SMALLINT":         "int",
		"TINYINT":          "int",
		"FIXED":            "float",
		"FLOAT":            "float",
		"FLOAT4":           "float",
		"FLOAT8":           "float",
		"DOUBLE":           "float",
		"REAL":             "float",
		"DOUBLE PRECISION": "float",
		"BOOLEAN":          "boolean",
		"TEXT":             "string",
		"VARCHAR":          "string",
		"CHAR":             "string",
		"CHARACTER":        "string",
		"STRING":           "string",
		"BINARY":           "string",
		"VARBINARY":        "string",
		"TIMESTAMP_NTZ":    "datetime",
		"DATE":             "datetime",
		"DATETIME":         "datetime",
		"TIME":             "datetime",
		"TIMESTAMP":        "datetime",
		"TIMESTAMP_LTZ":    "datetime",
		"TIMESTAMP_TZ":     "datetime",
		"VARIANT":          "json",
		"ARRAY":            "array",
	}
	databaseTypeName := strings.ToUpper(re.ReplaceAllString(columnType.DatabaseTypeName(), ""))
	if mappedType, ok := columnTypeMappings[strings.ToUpper(databaseTypeName)]; ok {
		return mappedType
	}
	return databaseTypeName
}

// legacyJsonRowMapper maps a row's scanned column to a json object's field
func legacyJsonRowMapper(databaseTypeName string, value any) any {
	if value == nil {
		return nil
	}

	switch databaseTypeName {
	// in case of NOT string, the function returns the value itself
	case "BOOLEAN":
		if s, ok := value.(string); ok {
			return s == "1"
		}

	case "FIXED":
		switch v := value.(type) {
		case float64:
			return v
		case string:
			n, err := strconv.ParseInt(value.(string), 10, 64)
			if err != nil {
				n, err := strconv.ParseFloat(value.(string), 64)
				if err != nil {
					panic(err)
				}
				return n
			}
			return n
		default:
			panic(fmt.Errorf("unsupported type for FIXED:%t", v))
		}

	case "OBJECT":
		return json.RawMessage(value.(string))

	case "ARRAY":
		rawValue := value.(string)
		if strings.HasPrefix(rawValue, "[") { // An ARRAY can contain undefined values in place of nulls which would cause json.Unmarshal to fail
			var jsonValue any
			if err := json.Unmarshal([]byte(rawValue), &jsonValue); err != nil {
				sanitizedJSON := strings.ReplaceAll(rawValue, "undefined", "null")
				return json.RawMessage(sanitizedJSON)
			}
		}
		return json.RawMessage(rawValue)

	case "VARIANT":
		return value.(string)

	case "DATE":
		return value.(time.Time)

	case "TIME":
		return value.(time.Time)

	case "TIMESTAMP_LTZ":
		return value.(time.Time)

	case "TIMESTAMP_NTZ":
		return value.(time.Time)

	case "TIMESTAMP_TZ":
		return value.(time.Time)
	}

	return value
}

package mysql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// mapping of database column types to rudder types
var columnTypeMappings = map[string]string{
	"INTEGER":         "int",
	"INT":             "int",
	"TINYINT":         "int",
	"SMALLINT":        "int",
	"MEDIUMINT":       "int",
	"BIGINT":          "int",
	"UNSIGNED BIGINT": "int",
	"DECIMAL":         "float",
	"NUMERIC":         "float",
	"FLOAT":           "float",
	"DOUBLE":          "float",
	"BIT":             "int",
	"CHAR":            "string",
	"VARCHAR":         "string",
	"BINARY":          "string",
	"VARBINARY":       "string",
	"BLOB":            "string",
	"TINYBLOB":        "string",
	"MEDIUMBLOB":      "string",
	"LONGBLOB":        "string",
	"TEXT":            "string",
	"TINYTEXT":        "string",
	"MEDIUMTEXT":      "string",
	"LONGTEXT":        "string",
	"ENUM":            "string",
	"SET":             "string",
	"DATE":            "datetime",
	"DATETIME":        "datetime",
	"TIMESTAMP":       "datetime",
	"TIME":            "datetime",
	"YEAR":            "datetime",
	"JSON":            "json",
}

func columnTypeMapper(mappings map[string]string) func(base.ColumnType) string {
	return func(c base.ColumnType) string {
		databaseTypeName := strings.Replace(strings.ToUpper(c.DatabaseTypeName()), "UNSIGNED ", "", 1)
		if mappedType, ok := mappings[databaseTypeName]; ok {
			return mappedType
		}
		return c.DatabaseTypeName()
	}
}

func jsonRowMapper(databaseTypeName string, value interface{}) interface{} {
	if value == nil {
		return nil
	}

	databaseTypeName = strings.Replace(databaseTypeName, "UNSIGNED ", "", 1)
	var stringValue string
	switch v := value.(type) {
	case []uint8:
		stringValue = string(v)
	case time.Time:
		stringValue = v.String()
	case string:
		stringValue = v
	case int, int32, int64, uint32, uint64:
		stringValue = fmt.Sprintf("%d", v)
	case float32, float64:
		stringValue = fmt.Sprintf("%f", v)
	default:
		return value
	}

	switch databaseTypeName {
	case "CHAR", "VARCHAR", "BLOB", "TEXT", "TINYBLOB", "TINYTEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT",
		"ENUM", "BINARY", "VARBINARY",
		"SET":
		return stringValue
	case "TIME":
		if p, err := time.Parse("15:04:05", stringValue); err == nil {
			return p
		}
		return stringValue
	case "DATE":
		if p, err := time.Parse("2006-01-02", stringValue); err == nil {
			return p
		}
		return stringValue
	case "DATETIME", "TIMESTAMP":
		if p, err := time.Parse("2006-01-02 15:04:05", stringValue); err == nil {
			return p
		}
		return stringValue
	case "YEAR":
		if p, err := time.Parse("2006", stringValue); err == nil {
			return p
		}
		return stringValue
	case "JSON":
		return json.RawMessage(stringValue)
	case "FLOAT", "DOUBLE", "DECIMAL":
		if stringValue == "" {
			return nil
		}
		n, err := strconv.ParseFloat(stringValue, 64)
		if err != nil {
			panic(err)
		}
		return n
	case "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		if stringValue == "" {
			return nil
		}
		n, err := strconv.ParseInt(stringValue, 10, 64)
		if err != nil {
			panic(err)
		}
		return n
	case "BIT":
		n := binary.BigEndian.Uint64([]byte(stringValue))
		return n
	}
	return value
}

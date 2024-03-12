package snowflake

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dlclark/regexp2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

var undefinedInArray = regexp2.MustCompile(`([\[,]\n[ ]*)undefined(?=,\n[ ]*|\n\])`, regexp2.None)

// mapping of database column types to rudder types
var columnTypeMappings = map[string]string{
	"INT":              "int",
	"DECIMAL":          "float",
	"NUMERIC":          "float",
	"INTEGER":          "int",
	"BIGINT":           "int",
	"SMALLINT":         "int",
	"TINYINT":          "int",
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
	"OBJECT":           "json",
	"ARRAY":            "json",
}

var (
	re              = regexp.MustCompile(`(\(.+\)|<.+>)`) // remove type parameters [<>] and size constraints [()]
	numberPrecision = regexp.MustCompile(`NUMBER\((?P<precision>\d+),(?P<scale>\d+)\)`)
)

func columnTypeMapper(columnType base.ColumnType) string {
	databaseTypeName := strings.ToUpper(re.ReplaceAllString(columnType.DatabaseTypeName(), ""))
	if mappedType, ok := columnTypeMappings[strings.ToUpper(databaseTypeName)]; ok {
		return mappedType
	}

	if databaseTypeName == "NUMBER" { // [DESCRIBE TABLE] returns [NUMBER(precision,scale)] for various numeric types, including [INT] types
		if matches := numberPrecision.FindStringSubmatch(columnType.DatabaseTypeName()); len(matches) > 0 {
			precisionIndex := numberPrecision.SubexpIndex("precision")
			if precision, err := strconv.ParseInt(matches[precisionIndex+1], 10, 64); err == nil && precision == 0 {
				return "int"
			}
		}
		return "float"
	}
	if databaseTypeName == "FIXED" { // When finding column types of a query, for most numeric types the driver returns [FIXED]
		if precision, decimals, ok := columnType.DecimalSize(); ok && precision > 0 && decimals > 0 {
			return "float"
		}
		return "int"
	}
	return databaseTypeName
}

// check https://godoc.org/github.com/snowflakedb/gosnowflake#hdr-Supported_Data_Types for handling snowflake data types
func jsonRowMapper(databaseTypeName string, value interface{}) interface{} {
	if value == nil {
		return nil
	}
	switch databaseTypeName {
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
	case "VARIANT", "ARRAY":
		rawValue := value.(string)
		// An ARRAY can contain undefined values in place of nulls which would cause json.Unmarshal to fail
		if strings.HasPrefix(rawValue, "[") && !json.Valid([]byte(rawValue)) {
			if r, err := undefinedInArray.Replace(rawValue, "${1}null", 0, -1); err == nil {
				return json.RawMessage(r)
			}
		}
		return json.RawMessage(rawValue)
	case "DATE", "TIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ":
		return value.(time.Time)
	case "BINARY", "VARBINARY":
		return string(value.([]byte))
	}

	return value
}

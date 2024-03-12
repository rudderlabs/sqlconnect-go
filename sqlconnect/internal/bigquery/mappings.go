package bigquery

import (
	"encoding/json"
	"math/big"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// mapping of database column types to rudder types
var columnTypeMappings = map[string]string{
	"BOOLEAN": "boolean",
	"BOOL":    "boolean",

	"INT64":    "int", // INT64 and aliases
	"INT":      "int",
	"SMALLINT": "int",
	"INTEGER":  "int",
	"BIGINT":   "int",
	"TINYINT":  "int",
	"BYTEINT":  "int",

	"INTERVAL": "int",

	"NUMERIC": "float", // NUMERIC and aliases
	"DECIMAL": "float",

	"BIGNUMERIC": "float", // BIGNUMERIC and aliases
	"BIGDECIMAL": "float",

	"FLOAT":   "float",
	"FLOAT64": "float",

	"STRING":    "string",
	"BYTES":     "string",
	"GEOGRAPHY": "string",
	"TIME":      "datetime",

	"DATE":      "datetime",
	"DATETIME":  "datetime",
	"TIMESTAMP": "datetime",

	"JSON":   "json",
	"ARRAY":  "json",
	"STRUCT": "json", // STRUCT and RECORD are represented as an array of json objects
	"RECORD": "json",
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
	switch v := (value).(type) {
	case *big.Rat:
		if !v.IsInt() {
			floatVal, _ := v.Float64()
			return floatVal
		} else {
			return v.Num().Int64()
		}
	case civil.Date:
		return time.Date(v.Year, v.Month, v.Day, 0, 0, 0, 0, time.UTC)
	case civil.Time:
		return time.Date(0, 1, 1, v.Hour, v.Minute, v.Second, v.Nanosecond, time.UTC)
	case civil.DateTime:
		return time.Date(v.Date.Year, v.Date.Month, v.Date.Day, v.Time.Hour, v.Time.Minute, v.Time.Second, v.Time.Nanosecond, time.UTC)
	case *bigquery.IntervalValue:
		return v.ToDuration()
	case []uint8:
		return string(v)
	case string:
		switch databaseTypeName {
		case "JSON":
			return json.RawMessage(v)
		}
		return v
	default:
		// Handle other data types as is
		return v
	}
}

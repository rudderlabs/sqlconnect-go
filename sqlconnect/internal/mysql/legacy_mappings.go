package mysql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// legacyJsonRowMapper maps a row's scanned column to a json object's field
func legacyJsonRowMapper(databaseTypeName string, value any) any {
	if value == nil {
		return nil
	}
	databaseTypeName = strings.Replace(databaseTypeName, "UNSIGNED ", "", 1)

	switch databaseTypeName {
	case "CHAR", "VARCHAR", "BLOB", "TEXT", "TINYBLOB", "TINYTEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT", "ENUM", "SET":
		switch v := value.(type) {
		case []uint8:
			return string(v)
		default:
			return json.RawMessage(value.(string))
		}
	case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
		switch v := value.(type) {
		case int, int32, int64, uint32, uint64:
			return fmt.Sprintf("%d", v)
		case []uint8:
			return string(v)
		default:
			return value.(time.Time)
		}

	case "FLOAT", "DOUBLE", "DECIMAL":
		switch v := value.(type) {
		case int, int32, int64, uint32, uint64:
			n, err := strconv.ParseFloat(fmt.Sprintf("%d", v), 64)
			if err != nil {
				panic(err)
			}
			return n
		case float32, float64:
			n, err := strconv.ParseFloat(fmt.Sprintf("%f", v), 64)
			if err != nil {
				panic(err)
			}
			return n
		case []uint8:
			n, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				panic(err)
			}
			return n
		default:
			n, err := strconv.ParseFloat(value.(string), 64)
			if err != nil {
				panic(err)
			}
			return n
		}
	case "INT", "TINYINT", "SMALLINt", "MEDIUMINT", "BIGINT", "UNSIGNED BIGINT":
		switch v := value.(type) {
		case int, int32, int64, uint32, uint64:
			n, err := strconv.ParseInt(fmt.Sprintf("%d", v), 10, 64)
			if err != nil {
				panic(err)
			}
			return n
		case []uint8:
			n, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				panic(err)
			}
			return n
		default:
			n, err := strconv.ParseInt(value.(string), 10, 64)
			if err != nil {
				panic(err)
			}
			return n
		}
	case "SMALLINT":
		switch v := value.(type) {
		case int, int32, int64, uint32, uint64:
			// converting to []byte to be backwards compatible
			return []byte(fmt.Sprintf("%d", v))
		case float32, float64:
			// converting to []byte to be backwards compatible
			return []byte(fmt.Sprintf("%f", v))
		}

	}

	return value
}

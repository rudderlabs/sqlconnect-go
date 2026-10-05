package fabric

import (
	"regexp"
	"strconv"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

var columnTypeMappings = map[string]string{
	"BIT":              "boolean",
	"TINYINT":          "int",
	"SMALLINT":         "int",
	"INT":              "int",
	"INTEGER":          "int",
	"BIGINT":           "int",
	"DECIMAL":          "float",
	"NUMERIC":          "float",
	"FLOAT":            "float",
	"REAL":             "float",
	"MONEY":            "float",
	"DATE":             "datetime",
	"TIME":             "datetime",
	"DATETIME":         "datetime",
	"DATETIME2":        "datetime",
	"DATETIMEOFFSET":   "datetime",
	"CHAR":             "string",
	"NCHAR":            "string",
	"VARCHAR":          "string",
	"NVARCHAR":         "string",
	"TEXT":             "string",
	"NTEXT":            "string",
	"VARBINARY":        "string",
	"IMAGE":            "string",
	"UNIQUEIDENTIFIER": "string",
}

var typeParameters = regexp.MustCompile(`\(.+\)`)

func columnTypeMapper(columnType base.ColumnType) string {
	databaseTypeName := strings.ToUpper(strings.TrimSpace(typeParameters.ReplaceAllString(columnType.DatabaseTypeName(), "")))
	if mappedType, ok := columnTypeMappings[databaseTypeName]; ok {
		return mappedType
	}
	return databaseTypeName
}

func jsonRowMapper(databaseTypeName string, value any) any {
	databaseTypeName = strings.ToUpper(typeParameters.ReplaceAllString(databaseTypeName, ""))
	var stringValue string
	switch typedValue := value.(type) {
	case []byte:
		if databaseTypeName == "UNIQUEIDENTIFIER" {
			var identifier mssql.UniqueIdentifier
			if err := identifier.Scan(typedValue); err == nil {
				return identifier.String()
			}
		}
		stringValue = string(typedValue)
	case string:
		stringValue = typedValue
	default:
		return value
	}
	if columnTypeMappings[databaseTypeName] == "float" {
		if number, err := strconv.ParseFloat(stringValue, 64); err == nil {
			return number
		}
	}
	return stringValue
}

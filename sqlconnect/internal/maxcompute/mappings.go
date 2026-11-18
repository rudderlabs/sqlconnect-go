package maxcompute

import (
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
)

// canonicalTypes maps MaxCompute base type names to the canonical types the
// web app shows. Complex types are mapped, unlike the destination's reverse
// map: a source must read them.
var canonicalTypes = map[string]string{
	"TINYINT": "int", "SMALLINT": "int", "INT": "int", "BIGINT": "int",
	"FLOAT": "float", "DOUBLE": "float", "DECIMAL": "float",
	"STRING": "string", "VARCHAR": "string", "CHAR": "string", "BINARY": "string",
	"BOOLEAN": "boolean",
	"DATE":    "datetime", "DATETIME": "datetime", "TIMESTAMP": "datetime", "TIMESTAMP_NTZ": "datetime",
	"ARRAY": "array",
	"MAP":   "json", "STRUCT": "json", "JSON": "json",
}

// baseTypeName strips parameters: DECIMAL(22,6) → DECIMAL, ARRAY<INT> → ARRAY.
func baseTypeName(databaseTypeName string) string {
	name := strings.ToUpper(strings.TrimSpace(databaseTypeName))
	if i := strings.IndexAny(name, "(<"); i >= 0 {
		name = strings.TrimSpace(name[:i])
	}
	return name
}

// canonicalType maps a MaxCompute type to its canonical type; anything else,
// such as INTERVAL, is "unsupported".
func canonicalType(databaseTypeName string) string {
	if t, ok := canonicalTypes[baseTypeName(databaseTypeName)]; ok {
		return t
	}
	return "unsupported"
}

func columnTypeMapper(c base.ColumnType) string { return canonicalType(c.DatabaseTypeName()) }

// jsonRowMapper shapes a scanned value for JSON: DECIMAL digits as a JSON
// number, JSON and complex columns as raw JSON, BINARY as base64.
func jsonRowMapper(databaseTypeName string, value any) any {
	if value == nil {
		return nil
	}
	switch baseTypeName(databaseTypeName) {
	case "DECIMAL":
		if s, ok := value.(string); ok {
			return json.Number(s)
		}
	case "JSON", "ARRAY", "MAP", "STRUCT":
		if b, ok := value.([]byte); ok {
			return json.RawMessage(b)
		}
	case "BINARY", "GEOGRAPHY":
		if b, ok := value.([]byte); ok {
			return base64.StdEncoding.EncodeToString(b)
		}
	}
	return value
}

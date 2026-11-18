package maxcomputequery

import "github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"

var (
	// ErrModelSQL is the error class of CheckModelSQL.
	ErrModelSQL = mcsql.ErrModelSQL
	// ErrTimeValue is returned for a time.Time literal or bind value: no
	// literal form is exact in every project time zone yet. Format the value
	// as text (time.DateTime, UTC) and write it into a TIMESTAMP_NTZ column.
	ErrTimeValue = mcsql.ErrTimeValue
)

// RenderLiteral renders v as a MaxCompute SQL literal, with the same rules
// the driver applies to bind arguments: backslash escaping, CHR() for control
// characters, and a refusal for anything it cannot render exactly. Use it for
// every literal built into SQL text, such as the retry filter and cursor values.
func RenderLiteral(v any) (string, error) { return mcsql.RenderLiteral(v) }

// CheckModelSQL refuses model SQL that is not one read-only SELECT or
// WITH … SELECT statement: a SET, several statements, a write, a script
// variable or a hint comment. Call it at preview, validation and run start.
func CheckModelSQL(sql string) error { return mcsql.CheckModelSQL(sql) }

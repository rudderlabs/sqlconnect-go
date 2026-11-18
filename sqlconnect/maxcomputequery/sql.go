package maxcomputequery

import (
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

var (
	// ErrModelSQL is the error class of CheckModelSQL.
	ErrModelSQL = mcsql.ErrModelSQL
	// ErrTimeValue is returned for a bare time.Time literal or bind value:
	// wrap it in TimestampNTZ or TimestampInstant.
	ErrTimeValue = mcsql.ErrTimeValue
)

// ErrTimePrecision is returned for a TimestampInstant below milliseconds.
var ErrTimePrecision = mcsql.ErrTimePrecision

// TimestampNTZ wraps a time for a TIMESTAMP_NTZ column, as a bind argument or
// for RenderLiteral: its UTC wall time, exact to the nanosecond in every
// project time zone. rETL's own timestamp columns are TIMESTAMP_NTZ.
func TimestampNTZ(t time.Time) any { return mcsql.TimestampNTZ(t) }

// TimestampInstant wraps an instant for a TIMESTAMP or DATETIME column:
// FROM_UTC_TIMESTAMP(<epoch ms>L, 'UTC'), exact in every project time zone at
// millisecond precision. A value with sub-millisecond digits is refused.
func TimestampInstant(t time.Time) any { return mcsql.TimestampInstant(t) }

// RenderLiteral renders v as a MaxCompute SQL literal, with the same rules
// the driver applies to bind arguments: backslash escaping, CHR() for control
// characters, and a refusal for anything it cannot render exactly. Use it for
// every literal built into SQL text, such as the retry filter and cursor values.
func RenderLiteral(v any) (string, error) { return mcsql.RenderLiteral(v) }

// CheckModelSQL refuses model SQL that is not one read-only SELECT or
// WITH … SELECT statement: a SET, several statements, a write, a script
// variable or a hint comment. Call it at preview, validation and run start.
func CheckModelSQL(sql string) error { return mcsql.CheckModelSQL(sql) }

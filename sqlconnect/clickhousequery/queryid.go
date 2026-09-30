// Package clickhousequery is the public surface that callers of the ClickHouse
// driver use for per-statement settings, query ids, the dial policy, the query
// guard and adapter error details.
package clickhousequery

import (
	"time"

	"github.com/google/uuid"
)

// MaxRunBudget is the longest statement budget the driver supports. The
// transport's response-header timeout sits above it, so a caller's run budget
// must stay at or below it.
const MaxRunBudget = 2 * time.Hour

// NewQueryID returns a fresh "retl-<UUID>" query id.
func NewQueryID() string { return "retl-" + uuid.NewString() }

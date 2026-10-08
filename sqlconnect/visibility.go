package sqlconnect

import (
	"context"
	"time"
)

type VisibilityPolicy struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Deadline       time.Duration
}

// VisibilityChecker is an optional interface. It confirms that a DDL step is
// visible before the next statement relies on it.
type VisibilityChecker interface {
	// AwaitTable returns the table's UUID once it is visible.
	// A non-empty wantUUID also requires that UUID.
	AwaitTable(ctx context.Context, exec QueryExecutor, ref RelationRef,
		wantUUID string, p VisibilityPolicy) (uuid string, err error)
	// AwaitTableAbsent returns once the name resolves to no table.
	AwaitTableAbsent(ctx context.Context, exec QueryExecutor, ref RelationRef,
		p VisibilityPolicy) error
}

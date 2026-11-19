package maxcompute

import (
	"context"
	"errors"
)

// The lifecycle range MaxCompute accepts, in days. Verified on the dev
// project (maxcompute-retl findings 18, out/43): 0 and 37232 fail with "table
// LIFECYCLE should be in range 1 and 37231 inclusive".
const (
	MinLifecycleDays = 1
	MaxLifecycleDays = 37231
)

// ErrLifecycle is returned for a lifecycle outside 1 to 37231 days.
var ErrLifecycle = errors.New("maxcompute: a table lifecycle must be 1 to 37231 days")

type lifecycleKey struct{}

// WithLifecycle returns a context whose tables, created by the driver's
// CreateTableFromQuery and MoveTable, carry LIFECYCLE days. The connection
// test table always carries LIFECYCLE 1. It is opt-in: without it those two
// methods send no clause, so a caller's tables never get a hidden expiry.
func WithLifecycle(ctx context.Context, days int) (context.Context, error) {
	if days < MinLifecycleDays || days > MaxLifecycleDays {
		return ctx, ErrLifecycle
	}
	return context.WithValue(ctx, lifecycleKey{}, days), nil
}

func lifecycleFrom(ctx context.Context) int {
	days, _ := ctx.Value(lifecycleKey{}).(int)
	return days
}

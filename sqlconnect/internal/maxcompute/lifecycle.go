package maxcompute

import (
	"context"
	"errors"
)

// The lifecycle range MaxCompute accepts, in days.
const (
	MinLifecycleDays = 1
	MaxLifecycleDays = 37231
)

// ErrLifecycle is returned for a lifecycle outside 1 to 37231 days.
var ErrLifecycle = errors.New("maxcompute: a table lifecycle must be 1 to 37231 days")

type lifecycleKey struct{}

// WithLifecycle returns a context whose tables, created by the driver's
// CreateTableFromQuery and MoveTable, carry LIFECYCLE days. The connection
// test table always carries LIFECYCLE 1.
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

// Package maxcomputequery is what a caller of the MaxCompute driver, such as
// rudder-sources, imports to control a statement: script mode, per-call hints,
// the time zone and the job's instance id. It sits beside the driver because
// callers cannot import sqlconnect/internal.
package maxcomputequery

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/driver"
)

var (
	// ErrHintNotAllowed is returned by WithHints for a key it does not accept.
	ErrHintNotAllowed = driver.ErrHintNotAllowed
	// ErrHintValue is returned by WithHints for an invalid value.
	ErrHintValue = driver.ErrHintValue
	// ErrMultipleStatements is returned for several statements without WithScript.
	ErrMultipleStatements = driver.ErrMultipleStatements
)

// HintAllowFullScan is the one per-call hint key [WithHints] accepts.
const HintAllowFullScan = driver.HintAllowFullScan

// WithScript marks ctx so that the next statement runs as one script job
// (odps.sql.submit.mode=script). The caller passes the whole script as one
// string with semicolons between statements, and follows MaxCompute's script
// rules; the driver does not rewrite it. A script applies all or nothing.
func WithScript(ctx context.Context) context.Context { return driver.WithScript(ctx) }

// WithHints adds per-call hints. Only odps.sql.allow.fullscan ("true" or
// "false") is accepted. The fixed hints the driver sends on every job cannot be
// overridden, and odps.sql.timezone is refused: every job runs in the project's
// time zone.
func WithHints(ctx context.Context, hints map[string]string) (context.Context, error) {
	return driver.WithHints(ctx, hints)
}

// WithInstanceIDHandler calls fn with each job's instance id as soon as the job
// is created, before the driver waits for it. The id can be passed to the DB's
// StopQuery (sqlconnect.QueryController).
func WithInstanceIDHandler(ctx context.Context, fn func(instanceID string)) context.Context {
	return driver.WithInstanceIDHandler(ctx, fn)
}

// FixedHints returns a copy of the hints the driver sends on every job.
func FixedHints() map[string]string { return driver.FixedHints() }

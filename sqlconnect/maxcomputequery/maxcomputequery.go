// Package maxcomputequery is what a caller of the MaxCompute driver, such as
// rudder-sources, imports to control a statement: script mode, per-call hints,
// the time zone and the job's instance id. It sits beside the driver because
// callers cannot import sqlconnect/internal.
package maxcomputequery

import (
	"context"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute"
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

// Logger is the part of a structured logger the driver uses (Infow, Warnw).
type Logger = driver.Logger

// HintAllowFullScan is the one per-call hint key [WithHints] accepts.
const HintAllowFullScan = driver.HintAllowFullScan

// WithScript marks ctx so that the next statement runs as one script job
// (odps.sql.submit.mode=script). The caller passes the whole script as one
// string with semicolons between statements, and follows MaxCompute's script
// rules; the driver does not rewrite it. A script that fails to compile
// applies nothing, but one that fails at run time keeps its DROP and CREATE
// statements and rolls back only its DML (findings 16): drop and create only
// tables the call fully rebuilds.
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

// WithRowCountHandler calls fn with the number of rows the server reports for
// a query's result when the result is opened, so the caller can compare it
// with the rows it consumed.
func WithRowCountHandler(ctx context.Context, fn func(rows int64)) context.Context {
	return driver.WithRowCountHandler(ctx, fn)
}

// WithLogger makes the driver log retl_maxcompute_job_submitted and
// retl_maxcompute_job_finished for each job under ctx: instance id, kind
// (exec, query, script), statement count for scripts, submit_ms, wall_ms,
// status (SUCCESS, FAILED, CANCELLED, SUBMIT_FAILED) and, on failure, the error
// category, code, retryable flag and stop outcome. No SQL text, values, names
// or credentials. A rudder-go-kit logger.Logger satisfies Logger.
func WithLogger(ctx context.Context, l Logger) context.Context {
	return driver.WithLogger(ctx, l)
}

// ErrLifecycle is returned by WithLifecycle for a value outside 1 to 37231.
var ErrLifecycle = maxcompute.ErrLifecycle

// WithLifecycle gives the tables the driver creates with
// CreateTableFromQuery and MoveTable a LIFECYCLE of days (1 to 37231; 0 and
// 37232 fail on the server with "table LIFECYCLE should be in range 1 and
// 37231 inclusive", maxcompute-retl findings 18). It is opt-in: without it no
// clause is sent. A
// project with odps.table.lifecycle=mandatory refuses a table without one.
// SQL a caller sends itself must carry its own clause: LIFECYCLE n goes last
// in a CREATE TABLE, after TBLPROPERTIES, and before AS in a CTAS.
func WithLifecycle(ctx context.Context, days int) (context.Context, error) {
	return maxcompute.WithLifecycle(ctx, days)
}

// ErrJobTag is returned by WithJobTag for a tag it does not accept.
var ErrJobTag = driver.ErrJobTag

// WithJobTag tags every job under ctx with "rudderstack-retl:<tag>" (the
// settings key rudderstack.job.tag; the SQL task is named rudderstack_retl), so
// a customer can find RudderStack's jobs and their cost in the job history.
// Pass the job run id. A tag holds 1 to 64 letters, digits, '_', '-', '.' or
// ':', never customer data, names or SQL. Without it jobs carry
// "rudderstack-retl".
func WithJobTag(ctx context.Context, tag string) (context.Context, error) {
	return driver.WithJobTag(ctx, tag)
}

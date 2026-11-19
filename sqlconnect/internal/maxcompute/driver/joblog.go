package driver

import (
	"context"
	"errors"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcsql"
)

// Log event names, from the observability plan (maxcompute-retl-source-observability.md).
const (
	EventJobSubmitted = "retl_maxcompute_job_submitted"
	EventJobFinished  = "retl_maxcompute_job_finished"
)

// Logger is the part of a structured logger the driver uses. A rudder-go-kit
// logger.Logger satisfies it; the driver takes no logging dependency.
type Logger interface {
	Infow(msg string, keysAndValues ...any)
	Warnw(msg string, keysAndValues ...any)
}

// WithLogger returns a context whose jobs log EventJobSubmitted and
// EventJobFinished to l. The events carry the instance id, the kind, timings,
// the status and, on failure, the error category and code. They never carry
// SQL text, values, names or credentials. sqlconnect has no logger of its own,
// so a caller that wants the events passes its logger here.
func WithLogger(ctx context.Context, l Logger) context.Context {
	return context.WithValue(ctx, keyLogger, l)
}

func loggerFrom(ctx context.Context) Logger {
	l, _ := ctx.Value(keyLogger).(Logger)
	return l
}

// jobLog emits the events of one job; a nil logger makes it a no-op.
type jobLog struct {
	l          Logger
	kind       string
	statements int
}

func newJobLog(ctx context.Context, kind, text string) jobLog {
	l := loggerFrom(ctx)
	if l == nil {
		return jobLog{}
	}
	j := jobLog{l: l, kind: kind, statements: 1}
	if isScript(ctx) {
		j.kind = "script"
		if tokens, err := mcsql.Scan(text); err == nil {
			j.statements = len(mcsql.Statements(tokens))
		}
	}
	return j
}

func (j jobLog) submitted(instanceID string, submit time.Duration) {
	if j.l == nil {
		return
	}
	fields := []any{"mc_instance_id", instanceID, "kind", j.kind, "submit_ms", submit.Milliseconds()}
	if j.kind == "script" {
		fields = append(fields, kv("statement_count", int64(j.statements))...)
	}
	j.l.Infow(EventJobSubmitted, fields...)
}

// Job statuses of EventJobFinished.
const (
	StatusSuccess      = "SUCCESS"
	StatusFailed       = "FAILED"
	StatusCancelled    = "CANCELLED"
	StatusSubmitFailed = "SUBMIT_FAILED"
)

func (j jobLog) finished(instanceID string, start time.Time, err error) {
	if j.l == nil {
		return
	}
	fields := []any{"mc_instance_id", instanceID, "kind", j.kind, "wall_ms", time.Since(start).Milliseconds()}
	if err == nil {
		j.l.Infow(EventJobFinished, append(fields, kv("status", StatusSuccess)...)...)
		return
	}
	status := StatusFailed
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		status = StatusCancelled
	case instanceID == "":
		status = StatusSubmitFailed
	}
	fields = append(fields, kv("status", status)...)
	if e, ok := errors.AsType[*mcerr.Error](err); ok {
		fields = append(fields, "category", e.Category, "code", e.Code, "retryable", e.Retryable, "outcome_unknown", e.OutcomeUnknown)
		if e.Stop != "" {
			fields = append(fields, "stop_outcome", e.Stop)
		}
	}
	j.l.Warnw(EventJobFinished, fields...)
}

func kv(key string, value any) []any { return []any{key, value} }

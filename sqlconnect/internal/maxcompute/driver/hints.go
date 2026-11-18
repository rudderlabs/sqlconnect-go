package driver

import (
	"context"
	"errors"
	"maps"
	"regexp"
)

// The hint keys the driver owns. This file is the one owner of the hint list.
const (
	HintTypeSystemODPS2 = "odps.sql.type.system.odps2"
	HintDecimalODPS2    = "odps.sql.decimal.odps2"
	HintNamespaceSchema = "odps.namespace.schema"
	HintValidateOrderBy = "odps.sql.validate.orderby.limit"
	HintDefaultSchema   = "odps.default.schema"
	HintAllowFullScan   = "odps.sql.allow.fullscan"
	HintSubmitMode      = "odps.sql.submit.mode"
	submitModeScript    = "script"
)

// fixedHints go on every job. MaxCompute keeps no session, so a setting holds
// for one job only, and project defaults differ between customers. Each entry
// is backed by a probe (HLD "Hints on every job"). odps.sql.timezone is
// deliberately absent and refused per call: every job, the customer's SQL
// included, runs in the project's time zone (HLD OQ10, findings 16).
var fixedHints = map[string]string{
	HintTypeSystemODPS2: "true",
	HintDecimalODPS2:    "true",
	HintNamespaceSchema: "true",
	HintValidateOrderBy: "false",
}

// FixedHints returns a copy of the hints every job carries, before the
// default schema.
func FixedHints() map[string]string { return maps.Clone(fixedHints) }

var (
	// allowedHints are the keys a caller may set per call, with their value rules.
	allowedHints = map[string]*regexp.Regexp{
		HintAllowFullScan: regexp.MustCompile(`^(true|false)$`),
	}

	// ErrHintNotAllowed is returned for a per-call hint the driver does not accept.
	ErrHintNotAllowed = errors.New("maxcompute: hint not allowed per call")
	// ErrHintValue is returned for a per-call hint with an invalid value.
	ErrHintValue = errors.New("maxcompute: invalid hint value")
)

type ctxKey int

const (
	keyScript ctxKey = iota + 1
	keyHints
	keyInstanceID
	keyRowCount
)

// WithScript marks ctx so that the next statement runs as one script job
// (odps.sql.submit.mode=script): several statements separated by semicolons,
// all applied or none. The driver does not rewrite the script.
func WithScript(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyScript, true)
}

// WithHints returns a context whose statements carry the given per-call hints
// in addition to the fixed ones. Only odps.sql.allow.fullscan is accepted
// (HLD OQ5: off unless the caller turns it on); a fixed key,
// odps.sql.timezone, any other key or an invalid value is refused. Hints from an outer WithHints are kept unless replaced.
func WithHints(ctx context.Context, hints map[string]string) (context.Context, error) {
	merged := maps.Clone(hintsFrom(ctx))
	if merged == nil {
		merged = map[string]string{}
	}
	for k, v := range hints {
		rule, ok := allowedHints[k]
		if !ok {
			return ctx, ErrHintNotAllowed
		}
		if !rule.MatchString(v) {
			return ctx, ErrHintValue
		}
		merged[k] = v
	}
	return context.WithValue(ctx, keyHints, merged), nil
}

// WithInstanceIDHandler returns a context that calls fn with the instance id of
// each job as soon as the job is created, before the driver waits for it. A
// caller can persist the id, read the job's status after a lost response, or
// stop it with StopQuery.
func WithInstanceIDHandler(ctx context.Context, fn func(instanceID string)) context.Context {
	return context.WithValue(ctx, keyInstanceID, fn)
}

// WithRowCountHandler returns a context that calls fn with the number of rows
// the server reports for a query's result, when the result is opened. A caller
// compares it with the rows it consumed: a silent cut must fail the run.
func WithRowCountHandler(ctx context.Context, fn func(rows int64)) context.Context {
	return context.WithValue(ctx, keyRowCount, fn)
}

func rowCountHandler(ctx context.Context) func(int64) {
	v, _ := ctx.Value(keyRowCount).(func(int64))
	return v
}

func isScript(ctx context.Context) bool {
	v, _ := ctx.Value(keyScript).(bool)
	return v
}

func hintsFrom(ctx context.Context) map[string]string {
	v, _ := ctx.Value(keyHints).(map[string]string)
	return v
}

func instanceIDHandler(ctx context.Context) func(string) {
	v, _ := ctx.Value(keyInstanceID).(func(string))
	return v
}

// jobHints builds the hint map of one job.
func jobHints(ctx context.Context, defaultSchema string) map[string]string {
	h := maps.Clone(fixedHints)
	if defaultSchema != "" {
		h[HintDefaultSchema] = defaultSchema
	}
	for k, v := range hintsFrom(ctx) {
		if _, fixed := h[k]; !fixed {
			h[k] = v
		}
	}
	if isScript(ctx) {
		h[HintSubmitMode] = submitModeScript
	}
	return h
}

package sqlconnect

import (
	"context"
	"fmt"
)

type ValidationWarning struct {
	Info      ErrorInfo
	Operation string
}

type ValidationResult struct {
	ServerVersion string
	GrantsChecked bool
	Warnings      []ValidationWarning
}

// ValidationReporter is an optional interface that a [DB] implements when it
// can report a structured validation result.
type ValidationReporter interface {
	ValidateContext(context.Context) (ValidationResult, error)
}

// ValidationOptions carries run inputs that are not account fields.
type ValidationOptions struct {
	SyncLogPruning bool
}

type validationOptionsKey struct{}

// WithValidationOptions returns a context that carries the options.
func WithValidationOptions(ctx context.Context, o ValidationOptions) context.Context {
	return context.WithValue(ctx, validationOptionsKey{}, o)
}

// ValidationOptionsFrom returns the options that [WithValidationOptions] put on
// the context. It returns false when the context carries none, as in
// account-only validation.
func ValidationOptionsFrom(ctx context.Context) (ValidationOptions, bool) {
	o, ok := ctx.Value(validationOptionsKey{}).(ValidationOptions)
	return o, ok
}

// ValidationStageError reports the validation stage that failed and wraps its
// cause. A consumer recovers the stage with errors.As.
type ValidationStageError struct {
	Stage int
	Tag   string
	Err   error
}

func (e ValidationStageError) Error() string {
	return fmt.Sprintf("validation stage %d (%s): %v", e.Stage, e.Tag, e.Err)
}

func (e ValidationStageError) Unwrap() error { return e.Err }

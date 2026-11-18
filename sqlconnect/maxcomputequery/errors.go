package maxcomputequery

import (
	"errors"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

// The error categories. The values are stable and can be used as metric labels.
const (
	CategoryNetwork         = mcerr.CategoryNetwork
	CategoryAuth            = mcerr.CategoryAuth
	CategoryPermission      = mcerr.CategoryPermission
	CategoryProjectNotFound = mcerr.CategoryProjectNotFound
	CategoryBadEndpoint     = mcerr.CategoryBadEndpoint
	CategorySyntax          = mcerr.CategorySyntax
	CategoryNotFound        = mcerr.CategoryNotFound
	CategoryFullScan        = mcerr.CategoryFullScan
	CategorySemantic        = mcerr.CategorySemantic
	CategoryAlreadyExists   = mcerr.CategoryAlreadyExists
	CategoryData            = mcerr.CategoryData
	CategoryCancelled       = mcerr.CategoryCancelled
	CategoryInternal        = mcerr.CategoryInternal
	CategoryNotAQuery       = mcerr.CategoryNotAQuery
	CategoryRefused         = mcerr.CategoryRefused
	CategoryUnknown         = mcerr.CategoryUnknown
)

// Details are the fields of a MaxCompute driver error. Message is redacted:
// it holds no key id, STS id, principal, ARN, URL, Context ID or debug tail,
// and is at most 512 runes.
type Details struct {
	Category, Code, Message, InstanceID string
	// Retryable reports whether running the statement again may succeed.
	Retryable bool
	// OutcomeUnknown reports that the job may have been created although the
	// submit failed; the caller must not assume the statement did not run.
	OutcomeUnknown bool
}

// StopInfo reports the stop of a statement whose context ended: the job's
// instance id (empty when no job was created) and how the stop ended. ok is
// false for any other error.
func StopInfo(err error) (instanceID string, outcome sqlconnect.StopOutcome, ok bool) {
	e, isErr := errors.AsType[*mcerr.Error](err)
	if !isErr || e.Stop == "" {
		return "", "", false
	}
	return e.InstanceID, sqlconnect.StopOutcome(e.Stop), true
}

// Describe returns the details of the first MaxCompute driver error in err's chain.
func Describe(err error) (Details, bool) {
	e, ok := errors.AsType[*mcerr.Error](err)
	if !ok {
		return Details{}, false
	}
	return Details{
		Category: e.Category, Code: e.Code, Message: mcerr.Redact(e.Message), InstanceID: e.InstanceID,
		Retryable: e.Retryable, OutcomeUnknown: e.OutcomeUnknown,
	}, true
}

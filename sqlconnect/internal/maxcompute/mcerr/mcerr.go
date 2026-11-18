// Package mcerr is the error type of the MaxCompute driver. Every error that
// leaves the driver is an *Error: its text is bounded and redacted, and it never
// wraps a raw SDK error, whose text can carry request bodies, URLs and key ids.
package mcerr

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"syscall"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/odpsclient"
)

// Categories. The values are stable: callers use them as metric labels.
const (
	CategoryNetwork         = "network"
	CategoryAuth            = "auth"
	CategoryPermission      = "permission"
	CategoryProjectNotFound = "project_not_found"
	CategoryBadEndpoint     = "bad_endpoint"
	CategorySyntax          = "syntax"
	CategoryNotFound        = "not_found"
	CategoryFullScan        = "full_scan"
	CategorySemantic        = "semantic"
	CategoryAlreadyExists   = "already_exists"
	CategoryData            = "data"
	CategoryCancelled       = "cancelled"
	CategoryInternal        = "internal"
	CategoryNotAQuery       = "not_a_query"
	CategoryRefused         = "refused"
	CategoryUnknown         = "unknown"
)

// Error is a classified, redacted MaxCompute error.
type Error struct {
	// Category is one of the Category constants.
	Category string
	// Code is the ODPS-NNNNNNN code or the HTTP error code, when the server sent one.
	Code string
	// Message is the redacted diagnostic, safe to show to a customer.
	Message string
	// InstanceID is the job's instance id, when a job was created.
	InstanceID string
	// Retryable reports whether running the same statement again may succeed.
	Retryable bool
	// OutcomeUnknown reports that the submit failed after the server may have
	// created the job: the caller must not assume the statement did not run.
	OutcomeUnknown bool

	cause error // a context error or a local sentinel; never an SDK error
}

func (e *Error) Error() string {
	msg := "maxcompute: " + e.Category
	if e.Message != "" {
		msg += ": " + e.Message
	}
	if e.InstanceID != "" {
		msg += " (instance " + e.InstanceID + ")"
	}
	return msg
}

// Unwrap returns the context error of a cancelled or timed-out statement, so
// errors.Is(err, context.Canceled) works.
func (e *Error) Unwrap() error { return e.cause }

// WithInstance sets the instance id and returns e.
func (e *Error) WithInstance(id string) *Error {
	if e != nil && e.InstanceID == "" {
		e.InstanceID = id
	}
	return e
}

// Local returns an error the driver raises itself, with a fixed message.
func Local(category, message string) *Error {
	return &Error{Category: category, Message: message}
}

// LocalWrap is Local with a cause for errors.Is, such as a sentinel.
func LocalWrap(category, message string, cause error) *Error {
	return &Error{Category: category, Message: message, cause: cause}
}

// Context returns the error of a statement stopped by its context. status
// describes how the job ended, such as "terminated".
func Context(ctxErr error, instanceID, status string) *Error {
	msg := "the statement was stopped by its context: " + ctxErr.Error()
	if status != "" {
		msg += "; job " + status
	}
	return &Error{Category: CategoryCancelled, Message: msg, InstanceID: instanceID, cause: ctxErr}
}

// FromSDK classifies an error returned by an SDK REST call.
func FromSDK(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := errors.AsType[*Error](err); ok {
		return e
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Context(err, "", "")
	}
	if IsTransport(err) {
		return &Error{Category: CategoryNetwork, Message: odpsclient.SafeError(err).Error(), Retryable: true}
	}
	return &Error{Category: CategoryUnknown, Message: odpsclient.SafeError(err).Error()}
}

// FromSubmit classifies an error of the call that creates a job. A transport
// failure after the request may have reached the server leaves the outcome unknown.
func FromSubmit(err error) *Error {
	e := FromSDK(err)
	if e.Category == CategoryNetwork && !notSent(err) {
		e.OutcomeUnknown = true
		e.Retryable = false
	}
	return e
}

// FromJob classifies the text of a failed job.
func FromJob(text string) *Error {
	return &Error{Category: CategoryUnknown, Message: odpsclient.SafeError(errors.New(text)).Error()}
}

// IsTransport reports whether err is a network failure.
func IsTransport(err error) bool {
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	if _, ok := errors.AsType[*url.Error](err); ok {
		return true
	}
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE)
}

// notSent reports a transport failure that happened before the request was sent.
func notSent(err error) bool {
	if opErr, ok := errors.AsType[*net.OpError](err); ok && opErr.Op == "dial" {
		return true
	}
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

// Errorf is a convenience for local errors with formatted, caller-controlled text.
func Errorf(category, format string, args ...any) *Error {
	return Local(category, fmt.Sprintf(format, args...))
}

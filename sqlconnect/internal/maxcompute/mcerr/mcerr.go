// Package mcerr is the error type of the MaxCompute driver. Every error that
// leaves the driver is an *Error: its text is bounded and redacted, and it never
// wraps a raw SDK error, whose text can carry request bodies, URLs and key ids.
package mcerr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"syscall"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"

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
	// Stop is how a stop of the job ended when its context ended or its status
	// poll gave up: landed, missed or error (sqlconnect.StopOutcome values);
	// empty when no stop ran.
	Stop string

	cause error // a context error or a local sentinel; never an SDK error
}

func (e *Error) Error() string {
	msg := "maxcompute: " + e.Category
	if e.Message != "" {
		msg += ": " + Redact(e.Message)
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

// CodeShortRead marks a read that ended before the server's row count.
const CodeShortRead = "MC_SHORT_READ"

// ShortRead is a read that ended before the server's row count, after the
// driver's resumes ran out. The stream broke (a cut connection ends it with a
// clean EOF as often as with an error), so it is a retryable network error,
// not a data error: rerunning the read can succeed.
func ShortRead(message string) *Error {
	return &Error{Category: CategoryNetwork, Code: CodeShortRead, Message: Redact(message), Retryable: true}
}

// Local returns an error the driver raises itself, with a fixed message.
func Local(category, message string) *Error {
	return &Error{Category: category, Message: Redact(message)}
}

// LocalWrap is Local with a cause for errors.Is, such as a sentinel.
func LocalWrap(category, message string, cause error) *Error {
	return &Error{Category: category, Message: Redact(message), cause: cause}
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
	if callerContext(err) {
		return Context(err, "", "")
	}
	if IsStreamCorrupt(err) {
		return StreamCorrupt("the result stream failed an integrity check: " + err.Error())
	}
	if IsTransport(err) {
		return &Error{Category: CategoryNetwork, Message: safe(err), Retryable: true}
	}
	if httpErr, ok := asHTTPError(err); ok {
		c := classifyHTTP(httpErr)
		return &Error{Category: c.category, Code: c.code, Message: safe(err), Retryable: c.retryable}
	}
	return FromJob(err.Error())
}

// safe is the destination's SafeError followed by the source's redaction.
// The DebugingInfo tail is cut first, so SafeError never parses it, and the
// source's patterns run on the raw text as well, before SafeError rewrites
// the anchors they key on.
func safe(err error) string {
	if httpErr, ok := asHTTPError(err); ok {
		msg := httpErr.ErrorMessage
		if msg == nil {
			msg = restclient.NewErrorMessage(httpErr.Body)
		}
		if msg != nil {
			cut := *msg
			cut.Message = redactSource(cutDebugInfo(cut.Message))
			httpErr.ErrorMessage = &cut
			err = httpErr
		}
	} else if netErr, isNet := errors.AsType[net.Error](err); !isNet || !netErr.Timeout() {
		// A timeout keeps its type, so SafeError can report it; SafeError
		// reads any other error only through its text.
		err = errors.New(redactSource(cutDebugInfo(err.Error())))
	}
	return Redact(odpsclient.SafeError(err).Error())
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
	c := classifyText(text)
	return &Error{Category: c.category, Code: c.code, Message: safe(errors.New(text)), Retryable: c.retryable}
}

// callerContext reports a context error that is not inside an HTTP error. An
// http.Client timeout also satisfies errors.Is(err, context.DeadlineExceeded)
// ("Client.Timeout exceeded while awaiting headers"), but it comes wrapped in
// a *url.Error and is a network failure, not the caller's deadline: on a
// submit the job may exist (RC3 R1).
func callerContext(err error) bool {
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if _, ok := errors.AsType[*url.Error](err); ok {
		return false
	}
	if _, ok := errors.AsType[*net.OpError](err); ok {
		return false
	}
	return true
}

// IsTransport reports whether err is a network failure.
func IsTransport(err error) bool {
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	if _, ok := errors.AsType[*url.Error](err); ok {
		return true
	}
	// A connection the load balancer closed can surface as a bare EOF, for
	// example when the SDK decodes a truncated response body.
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
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

// Info returns the sqlconnect classification of err.
func Info(err error) (category, code string, ok bool) {
	e, isErr := errors.AsType[*Error](err)
	if !isErr {
		return "", "", false
	}
	return e.Category, e.Code, true
}

// Errorf is a convenience for local errors with formatted, caller-controlled text.
func Errorf(category, format string, args ...any) *Error {
	return Local(category, fmt.Sprintf(format, args...))
}

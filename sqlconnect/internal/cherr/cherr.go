// Package cherr holds the ClickHouse adapter error type and the one error-code
// registry that the driver, the client, Lookout and the grants reference share.
package cherr

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"
)

const (
	CodeConfigInvalid         = "CH_CONFIG_INVALID"
	CodeCAInvalid             = "CH_CA_INVALID"
	CodeWorkspaceNotEnabled   = "CH_WORKSPACE_NOT_ENABLED"
	CodeHostNotAllowed        = "CH_HOST_NOT_ALLOWED"
	CodeDNSFailed             = "CH_DNS_FAILED"
	CodeRedirectRefused       = "CH_REDIRECT_REFUSED"
	CodeSortingKeyUnavailable = "CH_SORTING_KEY_UNAVAILABLE"

	CodeTLS         = "CH_TLS"
	CodeRateLimited = "CH_RATE_LIMITED"

	CodeClusterUnsupported      = "CH_CLUSTER_UNSUPPORTED"
	CodeVersionBelowFloor       = "CH_VERSION_BELOW_FLOOR"
	CodeCatalogUnsupported      = "CH_CATALOG_UNSUPPORTED"
	CodeCrossDatabaseMove       = "CH_CROSS_DATABASE_MOVE_UNSUPPORTED"
	CodeTransactionsUnsupported = "CH_TRANSACTIONS_UNSUPPORTED"
	CodePrepareUnsupported      = "CH_PREPARE_UNSUPPORTED"
	CodeTypeUnsupported         = "CH_TYPE_UNSUPPORTED"

	CodeInvalidReference  = "CH_INVALID_REFERENCE"
	CodeInvalidIdentifier = "CH_INVALID_IDENTIFIER"
	CodeQueryInvalid      = "CH_QUERY_INVALID"

	CodeObjectNotFound = "CH_OBJECT_NOT_FOUND"

	CodeValueEncoding   = "CH_VALUE_ENCODING"
	CodeDuplicateColumn = "CH_DUPLICATE_COLUMN"
	CodeCountOverflow   = "CH_COUNT_OVERFLOW"

	CodeDDLNotVisible    = "CH_DDL_NOT_VISIBLE"
	CodeRowCountMismatch = "CH_ROW_COUNT_MISMATCH"
	CodeBaselineMissing  = "CH_BASELINE_MISSING"
	CodeSchemaMismatch   = "CH_SCHEMA_MISMATCH"
	CodeLostResponse     = "CH_LOST_RESPONSE"
	CodeOutcomeUnknown   = "CH_OUTCOME_UNKNOWN"

	CodeScratchCleanupFailed = "CH_SCRATCH_CLEANUP_FAILED"

	CodeAuthentication = "CH_AUTHENTICATION"
	CodePermission     = "CH_PERMISSION"
	CodeTimeout        = "CH_TIMEOUT"
	CodeResource       = "CH_RESOURCE"
	CodeNetwork        = "CH_NETWORK"
	CodeCancelled      = "CH_CANCELLED"
	CodeUnavailable    = "CH_UNAVAILABLE"
	CodeUnknown        = "CH_UNKNOWN"
)

// byCategory is the error code registry, grouped by category. Callers in
// other services match on these codes, so each one is a contract. Each code
// appears once.
var byCategory = map[string][]string{
	"configuration": {
		CodeConfigInvalid, CodeCAInvalid, CodeWorkspaceNotEnabled, CodeHostNotAllowed, CodeDNSFailed,
		CodeRedirectRefused, CodeSortingKeyUnavailable,
	},
	"connection configuration": {CodeTLS},
	"transient":                {CodeRateLimited},
	"unsupported": {
		CodeClusterUnsupported, CodeVersionBelowFloor, CodeCatalogUnsupported, CodeCrossDatabaseMove,
		CodeTransactionsUnsupported, CodePrepareUnsupported, CodeTypeUnsupported,
	},
	"syntax":    {CodeInvalidReference, CodeInvalidIdentifier, CodeQueryInvalid},
	"not_found": {CodeObjectNotFound},
	"data":      {CodeValueEncoding, CodeDuplicateColumn, CodeCountOverflow},
	"integrity": {
		CodeDDLNotVisible, CodeRowCountMismatch, CodeBaselineMissing, CodeSchemaMismatch,
		CodeLostResponse, CodeOutcomeUnknown,
	},
	"cleanup":        {CodeScratchCleanupFailed},
	"authentication": {CodeAuthentication},
	"permission":     {CodePermission},
	"timeout":        {CodeTimeout},
	"resource":       {CodeResource},
	"network":        {CodeNetwork},
	"cancellation":   {CodeCancelled},
	"availability":   {CodeUnavailable},
	"unknown":        {CodeUnknown},
}

// Codes returns every registry code, sorted.
func Codes() []string {
	var out []string
	for _, codes := range byCategory {
		out = append(out, codes...)
	}
	slices.Sort(out)
	return out
}

// Category returns the registry category of code, or "unknown" for a code
// outside the registry.
func Category(code string) string {
	for cat, codes := range byCategory {
		if slices.Contains(codes, code) {
			return cat
		}
	}
	return "unknown"
}

// Error is an adapter error. Its text is built from bounded fields only; the
// cause stays reachable through errors.Is and errors.As but is never printed,
// because a server message can echo SQL text or credentials.
type Error struct {
	Code, Field, Detail string
	ServerCode          int32
	RetryAfter          time.Duration
	cause               error
}

func New(code, field, detail string) *Error { return &Error{Code: code, Field: field, Detail: detail} }

func Wrap(code, field, detail string, cause error) *Error {
	return &Error{Code: code, Field: field, Detail: detail, cause: cause}
}

func (e *Error) Unwrap() error { return e.cause }

// Format prints only Error() for every verb, so %+v and %#v never expose the
// cause or the struct fields.
func (e *Error) Format(f fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = io.WriteString(f, strconv.Quote(e.Error()))
		return
	}
	_, _ = io.WriteString(f, e.Error())
}

func (e *Error) Error() string {
	s := e.Code
	if e.Field != "" {
		s += " (" + e.Field + ")"
	}
	s += ": " + e.Detail
	if e.ServerCode != 0 {
		s += " [server code " + strconv.Itoa(int(e.ServerCode)) + "]"
	}
	return s
}

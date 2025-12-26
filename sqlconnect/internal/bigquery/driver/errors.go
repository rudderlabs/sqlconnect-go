package driver

import (
	"errors"
	"strings"

	"google.golang.org/api/googleapi"
)

// BigQuery error reasons (from google-cloud-go/bigquery/bigquery.go)
// These are the machine-readable reason codes returned in googleapi.ErrorItem.Reason
const (
	// ReasonRateLimitExceeded is returned for standard rate limit errors (HTTP 403/429)
	ReasonRateLimitExceeded = "rateLimitExceeded"
	// ReasonQuotaExceeded is returned when quota is exceeded
	ReasonQuotaExceeded = "quotaExceeded"
	// ReasonInvalidQuery is returned for query errors AND some rate limit errors (HTTP 400)
	// BigQuery API quirk: table metadata rate limits return invalidQuery, not rateLimitExceeded
	ReasonInvalidQuery = "invalidQuery"
	// ReasonBackendError is returned for transient backend errors
	ReasonBackendError = "backendError"
	// ReasonJobRateLimitExceeded is returned when job rate limits are exceeded
	ReasonJobRateLimitExceeded = "jobRateLimitExceeded"
	// ReasonInternalError is returned for internal BigQuery errors
	ReasonInternalError = "internalError"
)

// rateLimitPatterns contains message patterns that indicate rate limit errors
// even when the reason code is "invalidQuery" (BigQuery API quirk).
// These patterns are from BigQuery's documented error messages:
// https://cloud.google.com/bigquery/troubleshooting-errors
var rateLimitPatterns = []string{
	"too many table update operations",
	"too many dml statements",
	"exceeded rate limits",
	"quota exceeded",
	"rate limit exceeded",
}

// IsBigQueryRateLimitError determines if an error is a BigQuery rate limit error
// that should be retried. This uses a type-safe hybrid approach:
//
//  1. Type-safe: Uses errors.As() to extract googleapi.Error
//  2. Type-safe: Checks HTTP Code and Reason fields (structured data)
//  3. Pattern match: Only checks Message when Code+Reason indicate potential rate limit
//
// BigQuery API quirk: Table metadata rate limits (e.g., "too many table update operations")
// return HTTP 400 with reason "invalidQuery" instead of HTTP 429 with "rateLimitExceeded".
// The google-cloud-go library does NOT automatically retry these errors.
func IsBigQueryRateLimitError(err error) bool {
	if err == nil {
		return false
	}

	// Try type-safe extraction of googleapi.Error
	var gErr *googleapi.Error
	if !errors.As(err, &gErr) {
		// Not a googleapi.Error - check string as last resort for wrapped errors
		return containsRateLimitPattern(err.Error())
	}

	// TIER 1: Standard rate limit responses (fully type-safe)
	// HTTP 403 Forbidden with rateLimitExceeded/quotaExceeded
	// HTTP 429 Too Many Requests
	if gErr.Code == 403 || gErr.Code == 429 {
		for _, item := range gErr.Errors {
			if item.Reason == ReasonRateLimitExceeded ||
				item.Reason == ReasonQuotaExceeded ||
				item.Reason == ReasonJobRateLimitExceeded {
				return true
			}
		}
		// Even without specific reason, 429 is always rate limit
		if gErr.Code == 429 {
			return true
		}
	}

	// TIER 2: BigQuery's HTTP 400 rate limit quirk
	// Table metadata rate limits come as HTTP 400 + invalidQuery
	// We must inspect the message to distinguish from actual query errors
	if gErr.Code == 400 {
		for _, item := range gErr.Errors {
			if item.Reason == ReasonInvalidQuery {
				// Only retry if message indicates rate limiting, not syntax error
				if containsRateLimitPattern(item.Message) {
					return true
				}
			}
		}
		// Also check the main error message
		if containsRateLimitPattern(gErr.Message) {
			return true
		}
	}

	return false
}

// IsBigQueryRetryableError determines if an error is retryable (broader than just rate limits).
// This includes:
// - Rate limit errors (via IsBigQueryRateLimitError)
// - Backend errors (HTTP 5xx)
// - Transient errors with specific reasons
func IsBigQueryRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Rate limit errors are retryable
	if IsBigQueryRateLimitError(err) {
		return true
	}

	var gErr *googleapi.Error
	if !errors.As(err, &gErr) {
		return false
	}

	// HTTP 5xx errors are retryable
	if gErr.Code >= 500 && gErr.Code < 600 {
		return true
	}

	// Specific retryable reasons
	for _, item := range gErr.Errors {
		switch item.Reason {
		case ReasonBackendError, ReasonInternalError:
			return true
		}
	}

	return false
}

// containsRateLimitPattern checks if a message contains any rate limit indicator.
// Uses case-insensitive matching for resilience to message format changes.
func containsRateLimitPattern(msg string) bool {
	msgLower := strings.ToLower(msg)
	for _, pattern := range rateLimitPatterns {
		if strings.Contains(msgLower, pattern) {
			return true
		}
	}
	return false
}

// GetErrorCode extracts the HTTP status code from a googleapi.Error.
// Returns 0 if the error is not a googleapi.Error.
func GetErrorCode(err error) int {
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		return gErr.Code
	}
	return 0
}

// GetErrorReason extracts the primary reason from a googleapi.Error.
// Returns empty string if the error is not a googleapi.Error or has no reasons.
func GetErrorReason(err error) string {
	var gErr *googleapi.Error
	if errors.As(err, &gErr) && len(gErr.Errors) > 0 {
		return gErr.Errors[0].Reason
	}
	return ""
}


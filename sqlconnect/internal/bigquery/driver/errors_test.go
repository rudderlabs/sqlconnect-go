package driver

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

func TestIsBigQueryRateLimitError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "generic error",
			err:      errors.New("some random error"),
			expected: false,
		},
		{
			name: "HTTP 403 with rateLimitExceeded reason",
			err: &googleapi.Error{
				Code:    403,
				Message: "Rate Limit Exceeded",
				Errors: []googleapi.ErrorItem{
					{Reason: "rateLimitExceeded", Message: "Rate limit exceeded"},
				},
			},
			expected: true,
		},
		{
			name: "HTTP 403 with quotaExceeded reason",
			err: &googleapi.Error{
				Code:    403,
				Message: "Quota Exceeded",
				Errors: []googleapi.ErrorItem{
					{Reason: "quotaExceeded", Message: "Quota exceeded"},
				},
			},
			expected: true,
		},
		{
			name: "HTTP 429 Too Many Requests",
			err: &googleapi.Error{
				Code:    429,
				Message: "Too Many Requests",
			},
			expected: true,
		},
		{
			name: "HTTP 400 with invalidQuery - table update operations (the key case!)",
			err: &googleapi.Error{
				Code:    400,
				Message: "Exceeded rate limits: too many table update operations for this table",
				Errors: []googleapi.ErrorItem{
					{
						Reason:  "invalidQuery",
						Message: "Exceeded rate limits: too many table update operations for this table. For more information, see https://cloud.google.com/bigquery/troubleshooting-errors",
					},
				},
			},
			expected: true,
		},
		{
			name: "HTTP 400 with invalidQuery - too many DML statements",
			err: &googleapi.Error{
				Code:    400,
				Message: "too many DML statements outstanding against table",
				Errors: []googleapi.ErrorItem{
					{
						Reason:  "invalidQuery",
						Message: "too many DML statements outstanding against table",
					},
				},
			},
			expected: true,
		},
		{
			name: "HTTP 400 with invalidQuery - actual syntax error (should NOT retry)",
			err: &googleapi.Error{
				Code:    400,
				Message: "Syntax error: Expected end of input but got keyword SELECT",
				Errors: []googleapi.ErrorItem{
					{
						Reason:  "invalidQuery",
						Message: "Syntax error: Expected end of input but got keyword SELECT at [1:50]",
					},
				},
			},
			expected: false,
		},
		{
			name: "HTTP 400 with invalidQuery - column not found (should NOT retry)",
			err: &googleapi.Error{
				Code:    400,
				Message: "Unrecognized name: nonexistent_column",
				Errors: []googleapi.ErrorItem{
					{
						Reason:  "invalidQuery",
						Message: "Unrecognized name: nonexistent_column at [2:10]",
					},
				},
			},
			expected: false,
		},
		{
			name: "HTTP 500 Internal Server Error (not a rate limit)",
			err: &googleapi.Error{
				Code:    500,
				Message: "Internal Server Error",
			},
			expected: false,
		},
		{
			name: "wrapped googleapi.Error with rate limit",
			err: fmt.Errorf("query failed: %w", &googleapi.Error{
				Code:    400,
				Message: "too many table update operations",
				Errors: []googleapi.ErrorItem{
					{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
				},
			}),
			expected: true,
		},
		{
			name: "HTTP 400 with exceeded rate limits in main message",
			err: &googleapi.Error{
				Code:    400,
				Message: "Exceeded rate limits: quota exceeded for this project",
			},
			expected: true,
		},
		{
			name:     "error string containing rate limit pattern (fallback)",
			err:      errors.New("bigquery error: Exceeded rate limits: too many table update operations"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsBigQueryRateLimitError(tt.err)
			assert.Equal(t, tt.expected, result, "IsBigQueryRateLimitError(%v) = %v, want %v", tt.err, result, tt.expected)
		})
	}
}

func TestIsBigQueryRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name: "rate limit error (via IsBigQueryRateLimitError)",
			err: &googleapi.Error{
				Code:    400,
				Message: "too many table update operations",
				Errors: []googleapi.ErrorItem{
					{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
				},
			},
			expected: true,
		},
		{
			name: "HTTP 500 Internal Server Error",
			err: &googleapi.Error{
				Code:    500,
				Message: "Internal Server Error",
			},
			expected: true,
		},
		{
			name: "HTTP 502 Bad Gateway",
			err: &googleapi.Error{
				Code:    502,
				Message: "Bad Gateway",
			},
			expected: true,
		},
		{
			name: "HTTP 503 Service Unavailable",
			err: &googleapi.Error{
				Code:    503,
				Message: "Service Unavailable",
			},
			expected: true,
		},
		{
			name: "HTTP 504 Gateway Timeout",
			err: &googleapi.Error{
				Code:    504,
				Message: "Gateway Timeout",
			},
			expected: true,
		},
		{
			name: "backendError reason",
			err: &googleapi.Error{
				Code:    500,
				Message: "Backend Error",
				Errors: []googleapi.ErrorItem{
					{Reason: "backendError", Message: "Backend error occurred"},
				},
			},
			expected: true,
		},
		{
			name: "internalError reason",
			err: &googleapi.Error{
				Code:    500,
				Message: "Internal Error",
				Errors: []googleapi.ErrorItem{
					{Reason: "internalError", Message: "Internal error occurred"},
				},
			},
			expected: true,
		},
		{
			name: "HTTP 400 syntax error (should NOT retry)",
			err: &googleapi.Error{
				Code:    400,
				Message: "Syntax error",
				Errors: []googleapi.ErrorItem{
					{Reason: "invalidQuery", Message: "Syntax error at position 10"},
				},
			},
			expected: false,
		},
		{
			name: "HTTP 404 Not Found (should NOT retry)",
			err: &googleapi.Error{
				Code:    404,
				Message: "Not Found",
			},
			expected: false,
		},
		{
			name: "HTTP 401 Unauthorized (should NOT retry)",
			err: &googleapi.Error{
				Code:    401,
				Message: "Unauthorized",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsBigQueryRetryableError(tt.err)
			assert.Equal(t, tt.expected, result, "IsBigQueryRetryableError(%v) = %v, want %v", tt.err, result, tt.expected)
		})
	}
}

func TestGetErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected int
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: 0,
		},
		{
			name:     "non-googleapi error",
			err:      errors.New("some error"),
			expected: 0,
		},
		{
			name: "googleapi error with code 400",
			err: &googleapi.Error{
				Code:    400,
				Message: "Bad Request",
			},
			expected: 400,
		},
		{
			name: "wrapped googleapi error",
			err: fmt.Errorf("wrapped: %w", &googleapi.Error{
				Code:    503,
				Message: "Service Unavailable",
			}),
			expected: 503,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetErrorCode(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetErrorReason(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "",
		},
		{
			name:     "non-googleapi error",
			err:      errors.New("some error"),
			expected: "",
		},
		{
			name: "googleapi error with no error items",
			err: &googleapi.Error{
				Code:    400,
				Message: "Bad Request",
			},
			expected: "",
		},
		{
			name: "googleapi error with reason",
			err: &googleapi.Error{
				Code:    400,
				Message: "Bad Request",
				Errors: []googleapi.ErrorItem{
					{Reason: "invalidQuery", Message: "Invalid query"},
				},
			},
			expected: "invalidQuery",
		},
		{
			name: "wrapped googleapi error with reason",
			err: fmt.Errorf("wrapped: %w", &googleapi.Error{
				Code:    403,
				Message: "Forbidden",
				Errors: []googleapi.ErrorItem{
					{Reason: "rateLimitExceeded", Message: "Rate limit exceeded"},
				},
			}),
			expected: "rateLimitExceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetErrorReason(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestContainsRateLimitPattern(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		expected bool
	}{
		{
			name:     "empty string",
			msg:      "",
			expected: false,
		},
		{
			name:     "too many table update operations",
			msg:      "Exceeded rate limits: too many table update operations for this table",
			expected: true,
		},
		{
			name:     "too many DML statements",
			msg:      "too many DML statements outstanding against table foo",
			expected: true,
		},
		{
			name:     "exceeded rate limits",
			msg:      "Exceeded rate limits for project",
			expected: true,
		},
		{
			name:     "quota exceeded",
			msg:      "Quota exceeded for this API",
			expected: true,
		},
		{
			name:     "rate limit exceeded (variation)",
			msg:      "rate limit exceeded, please try again",
			expected: true,
		},
		{
			name:     "case insensitive - uppercase",
			msg:      "TOO MANY TABLE UPDATE OPERATIONS",
			expected: true,
		},
		{
			name:     "case insensitive - mixed case",
			msg:      "Too Many DML Statements",
			expected: true,
		},
		{
			name:     "syntax error (should not match)",
			msg:      "Syntax error: Expected end of input",
			expected: false,
		},
		{
			name:     "column not found (should not match)",
			msg:      "Unrecognized name: foo_column",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containsRateLimitPattern(tt.msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

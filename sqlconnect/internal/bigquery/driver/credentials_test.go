package driver_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery/driver"
)

func TestCredentialsTypeFromJSON(t *testing.T) {
	testCases := []struct {
		name     string
		input    []byte
		expected option.CredentialsType
	}{
		{
			name:     "service account",
			input:    []byte(`{"type":"service_account","project_id":"p"}`),
			expected: option.ServiceAccount,
		},
		{
			name:     "authorized user",
			input:    []byte(`{"type":"authorized_user"}`),
			expected: option.AuthorizedUser,
		},
		{
			name:     "impersonated service account",
			input:    []byte(`{"type":"impersonated_service_account"}`),
			expected: option.ImpersonatedServiceAccount,
		},
		{
			name:     "external account",
			input:    []byte(`{"type":"external_account"}`),
			expected: option.ExternalAccount,
		},
		{
			name:     "external account authorized user (not exported by option)",
			input:    []byte(`{"type":"external_account_authorized_user"}`),
			expected: option.CredentialsType("external_account_authorized_user"),
		},
		{
			name:     "gdc service account (not exported by option)",
			input:    []byte(`{"type":"gdc_service_account"}`),
			expected: option.CredentialsType("gdc_service_account"),
		},
		{
			name:     "missing type field falls back to unknown",
			input:    []byte(`{"project_id":"p"}`),
			expected: option.CredentialsType(""),
		},
		{
			name:     "unrecognised type falls back to unknown",
			input:    []byte(`{"type":"something_else"}`),
			expected: option.CredentialsType(""),
		},
		{
			name:     "non-string type falls back to unknown",
			input:    []byte(`{"type":123}`),
			expected: option.CredentialsType(""),
		},
		{
			name:     "empty byte slice falls back to unknown",
			input:    []byte{},
			expected: option.CredentialsType(""),
		},
		{
			name:     "nil byte slice falls back to unknown",
			input:    nil,
			expected: option.CredentialsType(""),
		},
		{
			name:     "malformed json falls back to unknown",
			input:    []byte(`{not valid json`),
			expected: option.CredentialsType(""),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				got := driver.CredentialsTypeFromJSON(tc.input)
				require.Equal(t, tc.expected, got, "unexpected credentials type for input %q", string(tc.input))
			}, "deriving credentials type should never panic")
		})
	}
}

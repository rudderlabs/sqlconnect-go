package bigquery_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/bigquery"
)

func sourceConfig(t *testing.T, googleDoc string) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(map[string]string{
		"project":     "test-project",
		"credentials": googleDoc,
	})
	require.NoError(t, err, "it should marshal the config")
	return b
}

func TestConfigCredentials(t *testing.T) {
	// Not every Google credential type is a key. external_account and
	// impersonated_service_account are instruction documents the SDK acts on,
	// naming a local file to read and a URL to send it to.
	t.Run("rejects credential types that are not service accounts", func(t *testing.T) {
		for name, doc := range map[string]string{
			"external_account":                 `{"type":"external_account","token_url":"https://example.invalid/"}`,
			"impersonated_service_account":     `{"type":"impersonated_service_account","service_account_impersonation_url":"https://example.invalid/"}`,
			"external_account_authorized_user": `{"type":"external_account_authorized_user","token_url":"https://example.invalid/"}`,
			"authorized_user":                  `{"type":"authorized_user"}`,
			"missing type":                     `{"project_id":"x"}`,
			"empty type":                       `{"type":""}`,
			"malformed json":                   `{"type":`,
		} {
			t.Run(name, func(t *testing.T) {
				var config bigquery.Config
				err := config.Parse(sourceConfig(t, doc))
				require.Error(t, err, "it should reject %s", name)
				require.ErrorContains(t, err, "bigquery credentials")
			})
		}
	})

	t.Run("accepts a service account document", func(t *testing.T) {
		doc := `{"type":"service_account","project_id":"test-project","client_email":"svc@test-project.iam.gserviceaccount.com"}`
		var config bigquery.Config
		require.NoError(t, config.Parse(sourceConfig(t, doc)), "it should accept a service account")
	})

	// No credentials means "use application default credentials", which is how
	// workload-identity deployments authenticate. Rejecting these would break
	// them, so they must stay valid.
	t.Run("accepts absent credentials so ADC still works", func(t *testing.T) {
		for name, doc := range map[string]string{
			"empty string": ``,
			"empty object": `{}`,
			"whitespace":   `  `,
		} {
			t.Run(name, func(t *testing.T) {
				var config bigquery.Config
				require.NoError(t, config.Parse(sourceConfig(t, doc)),
					"it should accept %s so application default credentials still apply", name)
			})
		}
	})
}

func TestConfigCredentialEndpoints(t *testing.T) {
	tests := []struct {
		name          string
		doc           string
		wantError     string
		rejectedValue string
	}{
		{name: "empty string", doc: ``},
		{name: "empty object", doc: `{}`},
		{name: "whitespace", doc: `  `},
		{name: "service account without endpoints", doc: `{"type":"service_account"}`},
		{name: "OAuth token endpoint", doc: `{"type":"service_account","token_uri":"https://oauth2.googleapis.com/token"}`},
		{name: "STS token endpoint", doc: `{"type":"service_account","token_uri":"https://sts.googleapis.com/v1/token"}`},
		{name: "Google universe domain", doc: `{"type":"service_account","universe_domain":"googleapis.com"}`},
		{
			name:          "host with Google prefix",
			doc:           `{"type":"service_account","token_uri":"https://oauth2.googleapis.com.evil.example/token"}`,
			wantError:     "unsupported bigquery credential endpoint",
			rejectedValue: "oauth2.googleapis.com.evil.example",
		},
		{
			name:          "IP token endpoint",
			doc:           `{"type":"service_account","token_uri":"https://10.0.0.1/token"}`,
			wantError:     "unsupported bigquery credential endpoint",
			rejectedValue: "10.0.0.1",
		},
		{
			name:          "HTTP token endpoint",
			doc:           `{"type":"service_account","token_uri":"http://oauth2.googleapis.com/token"}`,
			wantError:     "unsupported bigquery credential endpoint",
			rejectedValue: "oauth2.googleapis.com",
		},
		{
			name:          "unparseable token endpoint",
			doc:           `{"type":"service_account","token_uri":"://broken"}`,
			wantError:     "unsupported bigquery credential endpoint",
			rejectedValue: "://broken",
		},
		{
			name:          "unsupported universe domain",
			doc:           `{"type":"service_account","universe_domain":"evil.example"}`,
			wantError:     "unsupported bigquery universe domain",
			rejectedValue: "evil.example",
		},
		{
			name:      "other credential type",
			doc:       `{"type":"authorized_user"}`,
			wantError: "unsupported credential type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config bigquery.Config
			err := config.Parse(sourceConfig(t, tt.doc))
			if tt.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantError)
			if tt.rejectedValue != "" {
				require.NotContains(t, err.Error(), tt.rejectedValue)
			}
		})
	}
}

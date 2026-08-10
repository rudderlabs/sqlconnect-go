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

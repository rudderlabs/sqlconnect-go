package bigquery

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestGetClientOptions(t *testing.T) {
	config := Config{CredentialsJSON: `{"type":"service_account"}`}

	t.Run("uses a service account credentials option by default", func(t *testing.T) {
		clientOptions := getClientOptions(config, nil)
		require.Len(t, clientOptions, 1)
	})

	t.Run("uses only caller options when supplied", func(t *testing.T) {
		customOption := option.WithEndpoint("https://bigquery.example.invalid")
		clientOptions := getClientOptions(config, []option.ClientOption{customOption})
		require.Equal(t, []option.ClientOption{customOption}, clientOptions)
	})
}

func TestNewDBWithClientOptionsUsesCallerAuthentication(t *testing.T) {
	configJSON, err := json.Marshal(map[string]string{
		"project":     "test-project",
		"credentials": `{"type":"service_account"}`,
	})
	require.NoError(t, err)

	db, err := NewDBWithClientOptions(configJSON, option.WithoutAuthentication())
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestNewDBRejectsUnsupportedCredentialTypes(t *testing.T) {
	for name, credentials := range map[string]string{
		"external_account":             `{"type":"external_account","token_url":"https://example.invalid/"}`,
		"impersonated_service_account": `{"type":"impersonated_service_account","service_account_impersonation_url":"https://example.invalid/"}`,
		"authorized_user":              `{"type":"authorized_user"}`,
		"malformed json":               `{"type":`,
	} {
		t.Run(name, func(t *testing.T) {
			configJSON, err := json.Marshal(map[string]string{
				"project":     "test-project",
				"credentials": credentials,
			})
			require.NoError(t, err)

			db, err := NewDB(configJSON)
			require.Error(t, err)
			require.Nil(t, db)
		})
	}
}

func TestNewDBWithClientOptionsRejectsUnsupportedCredentialTypes(t *testing.T) {
	for name, credentials := range map[string]string{
		"external_account":             `{"type":"external_account","token_url":"https://example.invalid/"}`,
		"impersonated_service_account": `{"type":"impersonated_service_account","service_account_impersonation_url":"https://example.invalid/"}`,
		"authorized_user":              `{"type":"authorized_user"}`,
		"malformed json":               `{"type":`,
	} {
		t.Run(name, func(t *testing.T) {
			configJSON, err := json.Marshal(map[string]string{
				"project":     "test-project",
				"credentials": credentials,
			})
			require.NoError(t, err)

			db, err := NewDBWithClientOptions(configJSON, option.WithoutAuthentication())
			require.Error(t, err)
			require.Nil(t, db)
		})
	}
}

func TestNewDBWithClientOptionsAcceptsEmptyCredentials(t *testing.T) {
	for name, credentials := range map[string]any{
		"absent":       nil,
		"empty string": "",
		"empty object": "{}",
		"whitespace":   "  ",
	} {
		t.Run(name, func(t *testing.T) {
			config := map[string]any{"project": "test-project"}
			if credentials != nil {
				config["credentials"] = credentials
			}
			configJSON, err := json.Marshal(config)
			require.NoError(t, err)

			db, err := NewDBWithClientOptions(configJSON, option.WithoutAuthentication())
			require.NoError(t, err)
			require.NoError(t, db.Close())
		})
	}
}

package bigquery

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

func TestSelectClientOptions(t *testing.T) {
	t.Run("uses only caller-supplied token source option", func(t *testing.T) {
		tokenSource := staticTokenSource{}

		selected := selectClientOptions(Config{
			CredentialsJSON: `{"type":"service_account"}`,
		}, tokenSource)

		require.Equal(t, []option.ClientOption{option.WithTokenSource(tokenSource)}, selected,
			"it should not combine the caller token source with the built-in credentials option")
	})

	t.Run("preserves the built-in credentials option when no token source is supplied", func(t *testing.T) {
		const credentialsJSON = `{"type":"service_account","project_id":"test-project"}`

		selected := selectClientOptions(Config{CredentialsJSON: credentialsJSON}, nil)

		require.Equal(t, []option.ClientOption{
			option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(credentialsJSON)),
		}, selected, "it should preserve the existing service account JSON credentials option")
	})
}

func TestNewDBWithTokenSourceStillValidatesCredentialsJSON(t *testing.T) {
	configJSON, err := json.Marshal(map[string]string{
		"project":     "test-project",
		"credentials": `{"type":"external_account","token_url":"https://example.invalid/"}`,
	})
	require.NoError(t, err)

	_, err = NewDBWithTokenSource(configJSON, staticTokenSource{})
	require.ErrorContains(t, err, "unsupported credential type")
}

type staticTokenSource struct{}

func (staticTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "test-token"}, nil
}

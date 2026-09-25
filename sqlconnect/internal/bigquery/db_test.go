package bigquery

import (
	"context"
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

func TestNewDBWithTokenSourceRejectsCredentialsJSON(t *testing.T) {
	configJSON, err := json.Marshal(map[string]string{
		"project":     "test-project",
		"credentials": `{"type":"service_account","project_id":"test-project"}`,
	})
	require.NoError(t, err, "it should marshal the config")

	_, err = NewDBWithTokenSource(configJSON, staticTokenSource{})
	require.ErrorIs(t, err, errCredentialsWithTokenSource,
		"it should refuse to silently drop a configured service account key in favour of the token source")
}

func TestNewDBWithTokenSourceAuthenticatesWithTokenSource(t *testing.T) {
	db, err := NewDBWithTokenSource(json.RawMessage(`{"project":"test-project"}`), staticTokenSource{})
	require.NoError(t, err, "it should create the db")
	t.Cleanup(func() { _ = db.Close() })

	// Conn builds the BigQuery client, which rejects conflicting credential options
	// and fails without any credentials, so this proves the token source alone reaches it.
	conn, err := db.Conn(context.Background())
	require.NoError(t, err, "it should build the bigquery client with only the token source")
	require.NoError(t, conn.Close(), "it should close the connection")
}

type staticTokenSource struct{}

func (staticTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "test-token"}, nil
}

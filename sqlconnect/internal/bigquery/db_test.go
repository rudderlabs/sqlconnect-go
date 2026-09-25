package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
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
	require.NoError(t, err, "it should marshal the config")

	_, err = NewDBWithTokenSource(configJSON, staticTokenSource{})
	require.ErrorContains(t, err, "unsupported credential type",
		"it should preserve credential type validation when a token source is supplied")
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
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))

	db, err := NewDBWithTokenSource(json.RawMessage(`{"project":"test-project"}`), staticTokenSource{})
	require.NoError(t, err, "it should create the db")
	t.Cleanup(func() { _ = db.Close() })

	// Conn builds the BigQuery client. The missing ADC file makes this fail if the
	// caller's token source is dropped, while empty credentials JSON does not count
	// as a conflicting credentials option.
	conn, err := db.Conn(context.Background())
	require.NoError(t, err, "it should build the bigquery client with only the token source")
	require.NoError(t, conn.Close(), "it should close the connection")
}

func TestNewDBRegistrationUsesTokenSource(t *testing.T) {
	sentinelErr := errors.New("sentinel token error")
	tokenSource := &errorTokenSource{err: sentinelErr}

	db, err := sqlconnect.NewDB(
		DatabaseType,
		json.RawMessage(`{"project":"test-project"}`),
		sqlconnect.WithBigQueryTokenSource(tokenSource),
	)
	require.NoError(t, err, "it should construct a BigQuery DB through the registered factory")
	t.Cleanup(func() { _ = db.Close() })

	rows, err := db.QueryContext(context.Background(), "SELECT 1")
	if rows != nil {
		require.NoError(t, rows.Err(), "it should not return rows carrying another query error")
		require.NoError(t, rows.Close(), "it should close any rows returned with the query error")
	}
	require.ErrorIs(t, err, sentinelErr,
		"it should propagate the caller token source error through the real BigQuery registration")
	require.True(t, tokenSource.called.Load(), "it should call the caller-supplied token source")
}

type staticTokenSource struct{}

func (staticTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "test-token"}, nil
}

type errorTokenSource struct {
	called atomic.Bool
	err    error
}

func (s *errorTokenSource) Token() (*oauth2.Token, error) {
	s.called.Store(true)
	return nil, s.err
}

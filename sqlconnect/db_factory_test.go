package sqlconnect_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

func TestNewDB(t *testing.T) {
	_, err := sqlconnect.NewDB("invalid", []byte{})
	require.Error(t, err, "should return error for invalid db name")
}

func TestNewDBUsesRegisteredFactoryWithoutOptions(t *testing.T) {
	const factoryName = "test-without-options"
	credentialsJSON := json.RawMessage(`{"credentials":"unchanged"}`)
	sentinelErr := errors.New("factory called")
	var receivedCredentials json.RawMessage

	sqlconnect.RegisterDBFactory(factoryName, func(credentialsJSON json.RawMessage) (sqlconnect.DB, error) {
		receivedCredentials = credentialsJSON
		return nil, sentinelErr
	})

	_, err := sqlconnect.NewDB(factoryName, credentialsJSON)
	require.ErrorIs(t, err, sentinelErr)
	require.Equal(t, credentialsJSON, receivedCredentials,
		"it should preserve the existing factory path when no options are supplied")
}

func TestNewDBWithBigQueryTokenSource(t *testing.T) {
	const factoryName = "test-with-options"
	tokenSource := &staticTokenSource{}
	sentinelErr := errors.New("factory called")
	var receivedOptions sqlconnect.DBFactoryOptions

	sqlconnect.RegisterDBFactoryWithOptions(factoryName, func(_ json.RawMessage, opts sqlconnect.DBFactoryOptions) (sqlconnect.DB, error) {
		receivedOptions = opts
		return nil, sentinelErr
	})

	_, err := sqlconnect.NewDB(factoryName, nil, sqlconnect.WithBigQueryTokenSource(tokenSource))
	require.ErrorIs(t, err, sentinelErr)
	require.Same(t, tokenSource, receivedOptions.BigQueryTokenSource(),
		"it should pass the BigQuery token source to an option-aware factory")
}

type staticTokenSource struct{}

func (staticTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "test-token"}, nil
}

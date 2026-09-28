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
	require.ErrorIs(t, err, sentinelErr, "it should return the registered factory's error")
	require.Equal(t, credentialsJSON, receivedCredentials,
		"it should preserve the existing factory path when no options are supplied")
}

func TestNewDBRejectsBigQueryTokenSourceForOtherOptionAwareFactory(t *testing.T) {
	const factoryName = "test-with-options"
	unexpectedCallErr := errors.New("unexpected factory call")

	sqlconnect.RegisterDBFactoryWithOptions(factoryName, func(json.RawMessage, sqlconnect.DBFactoryOptions) (sqlconnect.DB, error) {
		t.Fatal("it should not pass a BigQuery option to another option-aware factory")
		return nil, unexpectedCallErr
	})

	_, err := sqlconnect.NewDB(factoryName, nil, sqlconnect.WithBigQueryTokenSource(&staticTokenSource{}))
	require.ErrorContains(t, err, "only supported for bigquery",
		"it should bind the BigQuery token source option to the BigQuery factory")
}

func TestNewDBRejectsBigQueryTokenSourceForFactoryWithoutOptions(t *testing.T) {
	const factoryName = "test-rejects-options"
	unexpectedCallErr := errors.New("unexpected factory call")
	sqlconnect.RegisterDBFactory(factoryName, func(json.RawMessage) (sqlconnect.DB, error) {
		t.Fatal("it should not call a factory that cannot honour the supplied options")
		return nil, unexpectedCallErr
	})

	_, err := sqlconnect.NewDB(factoryName, nil, sqlconnect.WithBigQueryTokenSource(&staticTokenSource{}))
	require.ErrorContains(t, err, "only supported for bigquery",
		"it should bind the BigQuery token source option to the BigQuery factory")
}

func TestNewDBRejectsNilBigQueryTokenSource(t *testing.T) {
	const factoryName = "test-nil-token-source"
	unexpectedCallErr := errors.New("unexpected factory call")
	sqlconnect.RegisterDBFactoryWithOptions(factoryName, func(json.RawMessage, sqlconnect.DBFactoryOptions) (sqlconnect.DB, error) {
		t.Fatal("it should not call the factory with a nil token source")
		return nil, unexpectedCallErr
	})

	_, err := sqlconnect.NewDB(factoryName, nil, sqlconnect.WithBigQueryTokenSource(nil))
	require.ErrorContains(t, err, "token source is nil",
		"it should reject a nil token source instead of falling back to other credentials")
}

func TestNewDBRejectsTypedNilBigQueryTokenSource(t *testing.T) {
	const factoryName = "test-typed-nil-token-source"
	unexpectedCallErr := errors.New("unexpected factory call")
	sqlconnect.RegisterDBFactoryWithOptions(factoryName, func(json.RawMessage, sqlconnect.DBFactoryOptions) (sqlconnect.DB, error) {
		t.Fatal("it should not call the factory with a typed-nil token source")
		return nil, unexpectedCallErr
	})
	var tokenSource *pointerTokenSource

	_, err := sqlconnect.NewDB(factoryName, nil, sqlconnect.WithBigQueryTokenSource(tokenSource))
	require.ErrorContains(t, err, "token source is nil",
		"it should reject a typed-nil token source before the first query can panic")
}

func TestNewDBRejectsNilOption(t *testing.T) {
	const factoryName = "test-nil-option"
	unexpectedCallErr := errors.New("unexpected factory call")
	sqlconnect.RegisterDBFactory(factoryName, func(json.RawMessage) (sqlconnect.DB, error) {
		t.Fatal("it should not call the factory with a nil option")
		return nil, unexpectedCallErr
	})
	var option sqlconnect.DBOption

	_, err := sqlconnect.NewDB(factoryName, nil, option)
	require.ErrorContains(t, err, "database option is nil",
		"it should reject a nil DBOption instead of panicking")
}

func TestRegisterDBFactoryReplacesFactoryWithOptions(t *testing.T) {
	const factoryName = "test-replaced-factory"
	unexpectedCallErr := errors.New("unexpected factory call")
	sqlconnect.RegisterDBFactoryWithOptions(factoryName, func(json.RawMessage, sqlconnect.DBFactoryOptions) (sqlconnect.DB, error) {
		t.Fatal("it should not call a factory that was replaced")
		return nil, unexpectedCallErr
	})
	sentinelErr := errors.New("replacement factory called")
	sqlconnect.RegisterDBFactory(factoryName, func(json.RawMessage) (sqlconnect.DB, error) {
		return nil, sentinelErr
	})

	_, err := sqlconnect.NewDB(factoryName, nil)
	require.ErrorIs(t, err, sentinelErr, "it should use the most recently registered factory")
}

type staticTokenSource struct{}

func (staticTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "test-token"}, nil
}

type pointerTokenSource struct{}

func (*pointerTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "test-token"}, nil
}

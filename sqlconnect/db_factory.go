package sqlconnect

import (
	"encoding/json"
	"errors"
	"fmt"

	"golang.org/x/oauth2"
)

var errNilBigQueryTokenSource = errors.New("bigquery token source is nil")

// NewDB creates a new database client for the provided name. It returns an error
// if options are supplied for a factory that does not support them.
func NewDB(name string, credentialsJSON json.RawMessage, opts ...DBOption) (DB, error) {
	factoryOptions := dbOptions{}
	for _, opt := range opts {
		if err := opt.apply(&factoryOptions); err != nil {
			return nil, err
		}
	}

	if factory, ok := dbFactoriesWithOptions[name]; ok {
		return factory(credentialsJSON, DBFactoryOptions(factoryOptions))
	}

	factory, ok := dbfactories[name]
	if !ok {
		return nil, fmt.Errorf("unknown client factory: %s", name)
	}
	if len(opts) > 0 {
		return nil, fmt.Errorf("client factory %s does not support options", name)
	}
	return factory(credentialsJSON)
}

// DBOption configures database client construction.
type DBOption interface {
	apply(*dbOptions) error
}

type dbOptions struct {
	bigQueryTokenSource oauth2.TokenSource
}

type withBigQueryTokenSource struct {
	tokenSource oauth2.TokenSource
}

func (o withBigQueryTokenSource) apply(opts *dbOptions) error {
	if o.tokenSource == nil {
		return errNilBigQueryTokenSource
	}
	opts.bigQueryTokenSource = o.tokenSource
	return nil
}

// WithBigQueryTokenSource configures BigQuery clients to authenticate with the
// provided OAuth2 token source instead of service account JSON credentials. The
// database config must then carry no credentials: NewDB returns an error rather
// than silently discarding a configured key. A nil token source is rejected.
func WithBigQueryTokenSource(tokenSource oauth2.TokenSource) DBOption {
	return withBigQueryTokenSource{tokenSource: tokenSource}
}

type DBFactory func(credentialsJSON json.RawMessage) (DB, error)

// DBFactoryOptions contains options for database factories that support them.
type DBFactoryOptions struct {
	bigQueryTokenSource oauth2.TokenSource
}

// BigQueryTokenSource returns the caller-supplied token source for BigQuery, if any.
func (o DBFactoryOptions) BigQueryTokenSource() oauth2.TokenSource {
	return o.bigQueryTokenSource
}

// DBFactoryWithOptions creates a database client with factory options.
type DBFactoryWithOptions func(credentialsJSON json.RawMessage, opts DBFactoryOptions) (DB, error)

var (
	dbfactories            = map[string]DBFactory{}
	dbFactoriesWithOptions = map[string]DBFactoryWithOptions{}
)

func RegisterDBFactory(name string, factory DBFactory) {
	delete(dbFactoriesWithOptions, name)
	dbfactories[name] = factory
}

// RegisterDBFactoryWithOptions registers a database factory that accepts factory options.
func RegisterDBFactoryWithOptions(name string, factory DBFactoryWithOptions) {
	delete(dbfactories, name)
	dbFactoriesWithOptions[name] = factory
}
